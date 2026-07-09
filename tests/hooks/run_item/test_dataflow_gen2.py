"""
Tests for the Dataflows Gen2 hook, operator, and trigger.

Coverage:
- DataflowGen2Config: serialization, deserialization, defaults, field filtering
- MSFabricRunDataflowGen2Hook: run_item, get_run_status, cancel_run,
  generate_deep_link, _parse_status, _parse_error_details
- MSFabricRunDataflowGen2Operator: construction, create_hook, create_trigger,
  render_template_fields
- MSFabricRunDataflowGen2Trigger: initialization, serialize,
  initialize_hook_and_tracker
"""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from airflow.providers.microsoft.fabric.hooks.run_item.dataflow_gen2 import (
    DATAFLOWS_GEN2_ITEM_TYPE,
    DataflowGen2Config,
    MSFabricRunDataflowGen2Hook,
)
from contextlib import ExitStack

from airflow.providers.microsoft.fabric.hooks.run_item.base import MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition,
    MSFabricRunItemStatus,
    RunItemTracker,
)
from airflow.providers.microsoft.fabric.operators.run_item.dataflow_gen2 import (
    MSFabricRunDataflowGen2Operator,
)
from airflow.providers.microsoft.fabric.triggers.run_item.dataflow_gen2 import (
    MSFabricRunDataflowGen2Trigger,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

WORKSPACE_ID = "ws-00000000-0000-0000-0000-000000000001"
ITEM_ID = "df-00000000-0000-0000-0000-000000000002"
RUN_ID = "run-00000000-0000-0000-0000-000000000003"
LOCATION_URL = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
    f"/items/{ITEM_ID}/jobs/instances/{RUN_ID}"
)


def _make_config(**overrides) -> DataflowGen2Config:
    defaults = dict(
        fabric_conn_id="fabric_default",
        timeout_seconds=3600,
        poll_interval_seconds=30,
    )
    defaults.update(overrides)
    return DataflowGen2Config(**defaults)


def _patch_connection():
    """Context-manager that prevents MSFabricRestConnection from hitting Airflow's DB.

    Both base.py and dataflow_gen2.py import and instantiate MSFabricRestConnection,
    so we must patch both module paths.
    """
    return ExitStack().__enter__  # placeholder - use _mock_conn() helper instead


def _mock_conn():
    """Return a context manager that patches MSFabricRestConnection in both locations."""
    from contextlib import ExitStack as _ES
    stack = _ES()
    m1 = stack.enter_context(
        patch("airflow.providers.microsoft.fabric.hooks.run_item.base.MSFabricRestConnection")
    )
    m2 = stack.enter_context(
        patch("airflow.providers.microsoft.fabric.hooks.run_item.dataflow_gen2.MSFabricRestConnection")
    )
    return stack, m1, m2


def _make_tracker(run_id: str = RUN_ID, timeout: int = 3600) -> RunItemTracker:
    return RunItemTracker(
        item=ItemDefinition(
            workspace_id=WORKSPACE_ID,
            item_type=DATAFLOWS_GEN2_ITEM_TYPE,
            item_id=ITEM_ID,
            item_name="My Dataflow",
        ),
        run_id=run_id,
        location_url=LOCATION_URL,
        run_timeout_in_seconds=timeout,
        start_time=datetime(2024, 1, 15, 8, 0, 0),
        retry_after=timedelta(seconds=60),
    )


# ---------------------------------------------------------------------------
# DataflowGen2Config tests
# ---------------------------------------------------------------------------

class TestDataflowGen2Config:

    def test_defaults(self):
        cfg = DataflowGen2Config(
            fabric_conn_id="my_conn",
            timeout_seconds=3600,
            poll_interval_seconds=30,
        )
        assert cfg.api_host == "https://api.fabric.microsoft.com"
        assert cfg.api_scope == "https://api.fabric.microsoft.com/.default"
        assert cfg.timeout_seconds == 3600
        assert cfg.poll_interval_seconds == 30

    def test_to_dict_excludes_tenacity_retry(self):
        cfg = _make_config()
        d = cfg.to_dict()
        assert "tenacity_retry" not in d
        assert d["fabric_conn_id"] == "fabric_default"
        assert d["timeout_seconds"] == 3600
        assert d["poll_interval_seconds"] == 30
        assert d["api_host"] == "https://api.fabric.microsoft.com"
        assert d["api_scope"] == "https://api.fabric.microsoft.com/.default"

    def test_round_trip(self):
        cfg = _make_config(timeout_seconds=1800, poll_interval_seconds=60)
        restored = DataflowGen2Config.from_dict(cfg.to_dict())
        assert restored.fabric_conn_id == cfg.fabric_conn_id
        assert restored.timeout_seconds == cfg.timeout_seconds
        assert restored.poll_interval_seconds == cfg.poll_interval_seconds
        assert restored.api_host == cfg.api_host
        assert restored.api_scope == cfg.api_scope
        assert restored.tenacity_retry is None

    def test_from_dict_applies_defaults(self):
        cfg = DataflowGen2Config.from_dict({"fabric_conn_id": "my_conn"})
        assert cfg.timeout_seconds == 3600
        assert cfg.poll_interval_seconds == 30
        assert cfg.tenacity_retry is None

    def test_from_dict_conn_id_alias(self):
        cfg = DataflowGen2Config.from_dict({"conn_id": "alias_conn"})
        assert cfg.fabric_conn_id == "alias_conn"

    def test_from_dict_strips_unknown_fields(self):
        cfg = DataflowGen2Config.from_dict({
            "fabric_conn_id": "my_conn",
            "unknown_field": "ignored",
        })
        assert not hasattr(cfg, "unknown_field")

    def test_from_dict_custom_api_host_and_scope(self):
        cfg = DataflowGen2Config.from_dict({
            "fabric_conn_id": "my_conn",
            "api_host": "https://custom.api.com",
            "api_scope": "https://custom.api.com/.default",
        })
        assert cfg.api_host == "https://custom.api.com"
        assert cfg.api_scope == "https://custom.api.com/.default"


# ---------------------------------------------------------------------------
# MSFabricRunDataflowGen2Hook tests
# ---------------------------------------------------------------------------

class TestMSFabricRunDataflowGen2Hook:

    def _make_hook(self, **overrides) -> MSFabricRunDataflowGen2Hook:
        cfg = _make_config(**overrides)
        stack, _, _ = _mock_conn()
        hook = MSFabricRunDataflowGen2Hook(config=cfg)
        stack.close()
        return hook

    # --- run_item ---

    @pytest.mark.asyncio
    async def test_run_item_success(self):
        cfg = _make_config()
        hook = self._make_hook()

        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {
                "Location": LOCATION_URL,
                "x-ms-job-id": RUN_ID,
                "RequestId": "req-abc",
                "Retry-After": "60",
            },
            "body": {},
        }

        with patch.object(hook, "get_item_name", return_value="My Dataflow"):
            item = ItemDefinition(
                workspace_id=WORKSPACE_ID,
                item_type=DATAFLOWS_GEN2_ITEM_TYPE,
                item_id=ITEM_ID,
            )
            tracker = await hook.run_item(mock_conn, item)

        assert tracker.run_id == RUN_ID
        assert tracker.location_url == LOCATION_URL
        assert tracker.item.item_name == "My Dataflow"
        assert tracker.item.workspace_id == WORKSPACE_ID
        assert tracker.item.item_id == ITEM_ID
        assert tracker.item.item_type == DATAFLOWS_GEN2_ITEM_TYPE
        assert tracker.run_timeout_in_seconds == cfg.timeout_seconds
        assert tracker.retry_after == timedelta(seconds=60)

        called_url = mock_conn.request.call_args[0][1]
        assert "/jobs/instances?jobType=Refresh" in called_url
        assert WORKSPACE_ID in called_url
        assert ITEM_ID in called_url

    @pytest.mark.asyncio
    async def test_run_item_missing_location_header_raises(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {"headers": {}, "body": {}}

        with pytest.raises(MSFabricRunItemException, match="Missing Location header"):
            item = ItemDefinition(
                workspace_id=WORKSPACE_ID,
                item_type=DATAFLOWS_GEN2_ITEM_TYPE,
                item_id=ITEM_ID,
            )
            await hook.run_item(mock_conn, item)

    @pytest.mark.asyncio
    async def test_run_item_invalid_retry_after_uses_default(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {
                "Location": LOCATION_URL,
                "x-ms-job-id": RUN_ID,
                "Retry-After": "not-a-number",
            },
            "body": {},
        }
        with patch.object(hook, "get_item_name", return_value="My Dataflow"):
            item = ItemDefinition(
                workspace_id=WORKSPACE_ID,
                item_type=DATAFLOWS_GEN2_ITEM_TYPE,
                item_id=ITEM_ID,
            )
            tracker = await hook.run_item(mock_conn, item)

        # Falls back to the 60-second default
        assert tracker.retry_after == timedelta(seconds=60)

    @pytest.mark.asyncio
    async def test_run_item_missing_run_id_defaults_to_unknown(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {"Location": LOCATION_URL},
            "body": {},
        }
        with patch.object(hook, "get_item_name", return_value="My Dataflow"):
            item = ItemDefinition(
                workspace_id=WORKSPACE_ID,
                item_type=DATAFLOWS_GEN2_ITEM_TYPE,
                item_id=ITEM_ID,
            )
            tracker = await hook.run_item(mock_conn, item)

        assert tracker.run_id == "unknown"

    # --- get_run_status ---

    @pytest.mark.asyncio
    @pytest.mark.parametrize("api_status,expected_enum", [
        ("InProgress", MSFabricRunItemStatus.IN_PROGRESS),
        ("Completed", MSFabricRunItemStatus.COMPLETED),
        ("Failed", MSFabricRunItemStatus.FAILED),
        ("Cancelled", MSFabricRunItemStatus.CANCELLED),
        ("NotStarted", MSFabricRunItemStatus.NOT_STARTED),
    ])
    async def test_get_run_status_happy_path(self, api_status, expected_enum):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {},
            "body": {"status": api_status},
        }
        status, error = await hook.get_run_status(mock_conn, _make_tracker())
        assert status == expected_enum
        assert error is None

    @pytest.mark.asyncio
    async def test_get_run_status_with_error_details(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {},
            "body": {
                "status": "Failed",
                "failureReason": {
                    "errorCode": "DataflowRefreshError",
                    "message": "Downstream source unavailable",
                    "requestId": "req-xyz",
                },
            },
        }
        status, error = await hook.get_run_status(mock_conn, _make_tracker())
        assert status == MSFabricRunItemStatus.FAILED
        assert "DataflowRefreshError" in error
        assert "Downstream source unavailable" in error
        assert "req-xyz" in error

    @pytest.mark.asyncio
    async def test_get_run_status_unknown_status_raises(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {
            "headers": {},
            "body": {"status": "WeirdUnknownState"},
        }
        with pytest.raises(MSFabricRunItemException, match="WeirdUnknownState"):
            await hook.get_run_status(mock_conn, _make_tracker())

    @pytest.mark.asyncio
    async def test_get_run_status_null_status_raises(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {"headers": {}, "body": {}}
        with pytest.raises(MSFabricRunItemException, match="null or empty"):
            await hook.get_run_status(mock_conn, _make_tracker())

    # --- cancel_run ---

    @pytest.mark.asyncio
    async def test_cancel_run_success(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.return_value = {"headers": {}, "body": {}}
        result = await hook.cancel_run(mock_conn, _make_tracker())
        assert result is True
        called_url = mock_conn.request.call_args[0][1]
        assert RUN_ID in called_url
        assert "/cancel" in called_url

    @pytest.mark.asyncio
    async def test_cancel_run_failure_returns_false(self):
        hook = self._make_hook()
        mock_conn = AsyncMock()
        mock_conn.request.side_effect = Exception("Network error")
        result = await hook.cancel_run(mock_conn, _make_tracker())
        assert result is False

    # --- generate_deep_link ---

    @pytest.mark.asyncio
    async def test_generate_deep_link(self):
        hook = self._make_hook()
        tracker = _make_tracker()
        link = await hook.generate_deep_link(tracker)
        assert WORKSPACE_ID in link
        assert ITEM_ID in link
        assert "dataflows" in link

    @pytest.mark.asyncio
    async def test_generate_deep_link_custom_base_url(self):
        hook = self._make_hook()
        tracker = _make_tracker()
        link = await hook.generate_deep_link(tracker, base_url="https://custom.fabric.com")
        assert link.startswith("https://custom.fabric.com")

    @pytest.mark.asyncio
    async def test_generate_deep_link_empty_workspace_returns_empty(self):
        hook = self._make_hook()
        tracker = _make_tracker()
        tracker.item.workspace_id = ""
        link = await hook.generate_deep_link(tracker)
        assert link == ""

    @pytest.mark.asyncio
    async def test_generate_deep_link_empty_item_id_returns_empty(self):
        hook = self._make_hook()
        tracker = _make_tracker()
        tracker.item.item_id = ""
        link = await hook.generate_deep_link(tracker)
        assert link == ""

    # --- _parse_status ---

    @pytest.mark.parametrize("raw,expected", [
        ("InProgress", MSFabricRunItemStatus.IN_PROGRESS),
        ("Completed", MSFabricRunItemStatus.COMPLETED),
        ("Failed", MSFabricRunItemStatus.FAILED),
        ("Cancelled", MSFabricRunItemStatus.CANCELLED),
        ("NotStarted", MSFabricRunItemStatus.NOT_STARTED),
        ("Deduped", MSFabricRunItemStatus.DEDUPED),
        ("TimedOut", MSFabricRunItemStatus.TIMED_OUT),
    ])
    def test_parse_status_known_values(self, raw, expected):
        hook = self._make_hook()
        assert hook._parse_status(raw) == expected

    def test_parse_status_null_raises(self):
        hook = self._make_hook()
        with pytest.raises(MSFabricRunItemException):
            hook._parse_status(None)

    def test_parse_status_empty_string_raises(self):
        hook = self._make_hook()
        with pytest.raises(MSFabricRunItemException):
            hook._parse_status("")

    def test_parse_status_unknown_raises(self):
        hook = self._make_hook()
        with pytest.raises(MSFabricRunItemException, match="SomethingRandom"):
            hook._parse_status("SomethingRandom")

    # --- _parse_error_details ---

    def test_parse_error_details_none_returns_none(self):
        hook = self._make_hook()
        assert hook._parse_error_details(None) is None

    def test_parse_error_details_empty_dict_returns_none(self):
        hook = self._make_hook()
        # Empty dict is falsy — treated the same as None
        result = hook._parse_error_details({})
        assert result is None

    def test_parse_error_details_with_all_fields(self):
        hook = self._make_hook()
        result = hook._parse_error_details({
            "errorCode": "GatewayTimeout",
            "message": "The gateway timed out",
            "requestId": "req-123",
        })
        assert "GatewayTimeout" in result
        assert "The gateway timed out" in result
        assert "req-123" in result

    def test_parse_error_details_without_request_id(self):
        hook = self._make_hook()
        result = hook._parse_error_details({
            "errorCode": "AccessDenied",
            "message": "Permission denied",
        })
        assert "AccessDenied" in result
        assert "Permission denied" in result
        assert "RequestId" not in result


# ---------------------------------------------------------------------------
# MSFabricRunDataflowGen2Operator tests
# ---------------------------------------------------------------------------

class TestMSFabricRunDataflowGen2Operator:

    def _make_operator(self, **overrides) -> MSFabricRunDataflowGen2Operator:
        defaults = dict(
            task_id="test_dataflow_refresh",
            fabric_conn_id="fabric_default",
            workspace_id=WORKSPACE_ID,
            item_id=ITEM_ID,
        )
        defaults.update(overrides)
        return MSFabricRunDataflowGen2Operator(**defaults)

    def test_construction_defaults(self):
        op = self._make_operator()
        assert op.fabric_conn_id == "fabric_default"
        assert op.workspace_id == WORKSPACE_ID
        assert op.item_id == ITEM_ID
        assert op.timeout == 3600
        assert op.check_interval == 30
        assert op.deferrable is True
        assert op.api_host == "https://api.fabric.microsoft.com"
        assert op.scope == "https://api.fabric.microsoft.com/.default"
        assert op.link_base_url == "https://app.fabric.microsoft.com"

    def test_item_definition_set_on_construction(self):
        op = self._make_operator()
        assert op.item.workspace_id == WORKSPACE_ID
        assert op.item.item_id == ITEM_ID
        assert op.item.item_type == DATAFLOWS_GEN2_ITEM_TYPE

    def test_construction_custom_values(self):
        op = self._make_operator(
            timeout=1800,
            check_interval=60,
            deferrable=False,
            api_host="https://custom.api.com",
            scope="https://custom.scope/.default",
        )
        assert op.timeout == 1800
        assert op.check_interval == 60
        assert op.deferrable is False
        assert op.api_host == "https://custom.api.com"
        assert op.scope == "https://custom.scope/.default"

    def test_create_hook_returns_correct_type(self):
        op = self._make_operator()
        stack, _, _ = _mock_conn()
        hook = op.create_hook()
        stack.close()
        assert isinstance(hook, MSFabricRunDataflowGen2Hook)
        assert hook.config.fabric_conn_id == "fabric_default"
        assert hook.config.timeout_seconds == 3600
        assert hook.config.poll_interval_seconds == 30

    def test_create_trigger_returns_correct_type(self):
        op = self._make_operator()
        tracker = _make_tracker()
        trigger = op.create_trigger(tracker)
        assert isinstance(trigger, MSFabricRunDataflowGen2Trigger)
        assert trigger.config_dict["fabric_conn_id"] == "fabric_default"
        assert trigger.tracker_dict["run_id"] == RUN_ID

    def test_render_template_fields_rebuilds_item(self):
        op = self._make_operator()
        new_workspace = "ws-new-111"
        op.workspace_id = new_workspace

        mock_context = MagicMock()
        with patch.object(
            MSFabricRunDataflowGen2Operator.__bases__[0],
            "render_template_fields",
        ):
            op.render_template_fields(context=mock_context)

        assert op.item.workspace_id == new_workspace
        assert op.item.item_type == DATAFLOWS_GEN2_ITEM_TYPE

    def test_template_fields_contains_required_fields(self):
        op = self._make_operator()
        for field in ("fabric_conn_id", "workspace_id", "item_id", "timeout",
                      "check_interval", "deferrable", "api_host", "scope", "link_base_url"):
            assert field in op.template_fields, f"'{field}' missing from template_fields"

    def test_item_type_constant(self):
        assert DATAFLOWS_GEN2_ITEM_TYPE == "DataflowsGen2"


# ---------------------------------------------------------------------------
# MSFabricRunDataflowGen2Trigger tests
# ---------------------------------------------------------------------------

class TestMSFabricRunDataflowGen2Trigger:

    def _make_trigger(self) -> MSFabricRunDataflowGen2Trigger:
        config = _make_config().to_dict()
        tracker = _make_tracker().to_dict()
        return MSFabricRunDataflowGen2Trigger(config=config, tracker=tracker)

    def test_construction_stores_dicts(self):
        trigger = self._make_trigger()
        assert trigger.config_dict["fabric_conn_id"] == "fabric_default"
        assert trigger.tracker_dict["run_id"] == RUN_ID

    def test_serialize_roundtrip(self):
        trigger = self._make_trigger()
        class_path, kwargs = trigger.serialize()
        assert "MSFabricRunDataflowGen2Trigger" in class_path
        assert kwargs["config"] == trigger.config_dict
        assert kwargs["tracker"] == trigger.tracker_dict

    def test_initialize_hook_and_tracker(self):
        trigger = self._make_trigger()
        stack, _, _ = _mock_conn()
        hook, tracker = trigger.initialize_hook_and_tracker()
        stack.close()

        assert isinstance(hook, MSFabricRunDataflowGen2Hook)
        assert isinstance(tracker, RunItemTracker)
        assert tracker.run_id == RUN_ID
        assert tracker.item.workspace_id == WORKSPACE_ID
        assert tracker.item.item_id == ITEM_ID

    def test_initialize_hook_config_values(self):
        trigger = self._make_trigger()
        stack, _, _ = _mock_conn()
        hook, _ = trigger.initialize_hook_and_tracker()
        stack.close()

        assert hook.config.fabric_conn_id == "fabric_default"
        assert hook.config.timeout_seconds == 3600
        assert hook.config.poll_interval_seconds == 30

    def test_from_dict_reconstructed_config(self):
        """Verify that config_dict serialized by operator survives a full round-trip."""
        op = MSFabricRunDataflowGen2Operator(
            task_id="rt",
            fabric_conn_id="fabric_default",
            workspace_id=WORKSPACE_ID,
            item_id=ITEM_ID,
            timeout=7200,
            check_interval=45,
        )
        tracker = _make_tracker()
        trigger = op.create_trigger(tracker)

        stack, _, _ = _mock_conn()
        hook, restored_tracker = trigger.initialize_hook_and_tracker()
        stack.close()

        assert hook.config.timeout_seconds == 7200
        assert hook.config.poll_interval_seconds == 45
        assert restored_tracker.run_id == tracker.run_id


# ---------------------------------------------------------------------------
# Integration: config → hook → trigger round-trip
# ---------------------------------------------------------------------------

class TestDataflowGen2RoundTrip:

    def test_operator_to_trigger_to_hook_config_preserved(self):
        op = MSFabricRunDataflowGen2Operator(
            task_id="roundtrip",
            fabric_conn_id="my_conn",
            workspace_id=WORKSPACE_ID,
            item_id=ITEM_ID,
            timeout=1800,
            check_interval=15,
            api_host="https://custom.api.com",
            scope="https://custom.scope/.default",
        )
        tracker = _make_tracker()
        trigger = op.create_trigger(tracker)

        stack, _, _ = _mock_conn()
        hook, _ = trigger.initialize_hook_and_tracker()
        stack.close()

        assert hook.config.fabric_conn_id == "my_conn"
        assert hook.config.timeout_seconds == 1800
        assert hook.config.poll_interval_seconds == 15
        assert hook.config.api_host == "https://custom.api.com"
        assert hook.config.api_scope == "https://custom.scope/.default"

    def test_tracker_serialization_within_trigger(self):
        tracker = _make_tracker()
        trigger = MSFabricRunDataflowGen2Trigger(
            config=_make_config().to_dict(),
            tracker=tracker.to_dict(),
        )
        stack, _, _ = _mock_conn()
        _, restored = trigger.initialize_hook_and_tracker()
        stack.close()

        assert restored.run_id == tracker.run_id
        assert restored.location_url == tracker.location_url
        assert restored.run_timeout_in_seconds == tracker.run_timeout_in_seconds
        assert restored.item.item_type == DATAFLOWS_GEN2_ITEM_TYPE
