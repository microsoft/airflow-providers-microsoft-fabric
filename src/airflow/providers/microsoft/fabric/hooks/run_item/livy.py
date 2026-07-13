"""
Microsoft Fabric **Livy** hooks, plugged into the ``run_item`` framework.

These subclass ``BaseFabricRunItemHook`` and reuse its ``wait_for_completion``
polling loop, so the Livy operators inherit all of the run_item deferral / XCom /
status-plugin machinery. Two hooks are provided:

* ``MSFabricLivyBatchHook`` — submits a Livy **batch** (``POST /batches``) and
  polls ``GET /batches/{id}`` (async pattern, like ``MSFabricRunJobHook``). The
  Livy status URL maps naturally onto ``RunItemTracker.location_url``.
* ``MSFabricLivySessionHook`` — creates a session, runs a **statement**, and
  returns its stdout immediately (synchronous pattern, like the User Data
  Function hook: ``run_item`` sets ``RunItemTracker.output``).

Livy is Lakehouse-scoped, so ``ItemDefinition.item_id`` carries the
``lakehouse_id`` and ``item_type`` is ``"LivyBatch"`` / ``"LivySession"``.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.hooks.run_item.base import (
    BaseFabricRunItemHook,
    MSFabricRunItemException,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition,
    MSFabricRunItemStatus,
    RunItemConfig,
    RunItemTracker,
)

LIVY_API_VERSION = "2023-12-01"
_JSON_HEADERS = {"Content-Type": "application/json"}


def _livy_base_url(api_host: str, workspace_id: str, lakehouse_id: str, version: str) -> str:
    return (
        f"{api_host}/v1"
        f"/workspaces/{workspace_id}"
        f"/lakehouses/{lakehouse_id}"
        f"/livyApi/versions/{version}"
    )


def _map_livy_status(state: Optional[str], result: Optional[str]) -> MSFabricRunItemStatus:
    """Map a Livy ``state`` (+ Fabric ``result``) to ``MSFabricRunItemStatus``."""
    s = (state or "").lower()
    if result == "Succeeded" or s == "success":
        return MSFabricRunItemStatus.COMPLETED
    if result in ("Failed", "Cancelled") or s in ("dead", "killed", "error"):
        return MSFabricRunItemStatus.FAILED
    if s in ("", "not_started"):
        return MSFabricRunItemStatus.NOT_STARTED
    # starting / running / busy / shutting_down
    return MSFabricRunItemStatus.IN_PROGRESS


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass(kw_only=True)
class LivyBatchConfig(RunItemConfig):
    lakehouse_id: str = ""
    api_host: str = "https://api.fabric.microsoft.com"
    api_scope: str = "https://api.fabric.microsoft.com/.default"
    livy_api_version: str = LIVY_API_VERSION
    # JSON-encoded Livy batch body (built by the operator).
    livy_body: str = "{}"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict() if hasattr(super(), "to_dict") else {
            "fabric_conn_id": self.fabric_conn_id,
            "timeout_seconds": self.timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
        }
        data.update({
            "lakehouse_id": self.lakehouse_id,
            "api_host": self.api_host,
            "api_scope": self.api_scope,
            "livy_api_version": self.livy_api_version,
            "livy_body": self.livy_body,
        })
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LivyBatchConfig":
        d = dict(data or {})
        if "fabric_conn_id" not in d and "conn_id" in d:
            d["fabric_conn_id"] = d.pop("conn_id")
        d.setdefault("timeout_seconds", 3600)
        d.setdefault("poll_interval_seconds", 30)
        d["tenacity_retry"] = None
        allowed = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in allowed})


@dataclass(kw_only=True)
class LivySessionConfig(RunItemConfig):
    lakehouse_id: str = ""
    api_host: str = "https://api.fabric.microsoft.com"
    api_scope: str = "https://api.fabric.microsoft.com/.default"
    livy_api_version: str = LIVY_API_VERSION
    # JSON-encoded Livy session body + the statement code.
    session_body: str = "{}"
    code: str = ""
    session_timeout_seconds: int = 900
    delete_session_on_finish: bool = True

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict() if hasattr(super(), "to_dict") else {
            "fabric_conn_id": self.fabric_conn_id,
            "timeout_seconds": self.timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
        }
        data.update({
            "lakehouse_id": self.lakehouse_id,
            "api_host": self.api_host,
            "api_scope": self.api_scope,
            "livy_api_version": self.livy_api_version,
            "session_body": self.session_body,
            "code": self.code,
            "session_timeout_seconds": self.session_timeout_seconds,
            "delete_session_on_finish": self.delete_session_on_finish,
        })
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LivySessionConfig":
        d = dict(data or {})
        d.setdefault("timeout_seconds", 900)
        d.setdefault("poll_interval_seconds", 10)
        d["tenacity_retry"] = None
        allowed = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in allowed})


# ---------------------------------------------------------------------------
# Batch hook (async polling, like MSFabricRunJobHook)
# ---------------------------------------------------------------------------
class MSFabricLivyBatchHook(BaseFabricRunItemHook):
    """Submit and monitor a Livy batch on Microsoft Fabric."""

    hook_name = "Microsoft Fabric Livy Batch"
    conn_type = None
    conn_name_attr = None

    def __init__(self, config: LivyBatchConfig):
        super().__init__(config)
        self.config = config
        self.conn = MSFabricRestConnection(config.fabric_conn_id, tenacity_retry=config.tenacity_retry)

    def _batches_url(self, workspace_id: str) -> str:
        return _livy_base_url(
            self.config.api_host, workspace_id, self.config.lakehouse_id, self.config.livy_api_version
        ) + "/batches"

    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker:
        url = self._batches_url(item.workspace_id)
        self.log.info("Submitting Livy batch to %s", url)
        response = await connection.request(
            "POST", url, self.config.api_scope, data=self.config.livy_body, headers=_JSON_HEADERS
        )
        body = response.get("body") or {}
        batch_id = body.get("id")
        if not batch_id:
            raise MSFabricRunItemException(f"Livy batch submission returned no id: {response}")
        self.log.info("Submitted Livy batch id=%s", batch_id)

        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item.item_name,
            ),
            run_id=str(batch_id),
            location_url=f"{url}/{batch_id}",
            run_timeout_in_seconds=self.config.timeout_seconds,
            start_time=datetime.now(),
            retry_after=None,
        )

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker):
        response = await connection.request("GET", tracker.location_url, self.config.api_scope)
        body = response.get("body") or {}
        status = _map_livy_status(body.get("state"), body.get("result"))
        error = None
        info = body.get("errorInfo")
        if info:
            error = "; ".join(e.get("message", "") for e in info) if isinstance(info, list) else str(info)
        app_id = body.get("appId")
        if app_id:
            self.log.info("Livy batch %s appId=%s", tracker.run_id, app_id)
        return status, error

    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> bool:
        try:
            await connection.request("DELETE", tracker.location_url, self.config.api_scope)
            return True
        except Exception as e:
            self.log.warning("Failed to cancel Livy batch %s: %s", tracker.run_id, e)
            return False

    async def generate_deep_link(self, tracker: RunItemTracker, base_url: str = "https://app.fabric.microsoft.com") -> str:
        ws = tracker.item.workspace_id
        lh = tracker.item.item_id
        if not ws or not lh:
            return ""
        return f"{base_url}/groups/{ws}/lakehouses/{lh}"


# ---------------------------------------------------------------------------
# Session hook (synchronous statement, like the UDF hook)
# ---------------------------------------------------------------------------
class MSFabricLivySessionHook(BaseFabricRunItemHook):
    """Create a Livy session, run a statement, and return its output."""

    hook_name = "Microsoft Fabric Livy Session"
    conn_type = None
    conn_name_attr = None

    def __init__(self, config: LivySessionConfig):
        super().__init__(config)
        self.config = config
        self.conn = MSFabricRestConnection(config.fabric_conn_id, tenacity_retry=config.tenacity_retry)

    def _sessions_url(self, workspace_id: str) -> str:
        return _livy_base_url(
            self.config.api_host, workspace_id, self.config.lakehouse_id, self.config.livy_api_version
        ) + "/sessions"

    async def _poll(self, get_state, is_done, timeout_seconds: int, description: str):
        deadline = datetime.now() + timedelta(seconds=timeout_seconds)
        last = None
        while datetime.now() < deadline:
            state, payload = await get_state()
            if state != last:
                self.log.info("%s state=%s", description, state)
                last = state
            done, value = is_done(state, payload)
            if done:
                return value
            await asyncio.sleep(max(self.config.poll_interval_seconds, 1))
        raise MSFabricRunItemException(f"Timeout waiting for {description}")

    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker:
        sessions_url = self._sessions_url(item.workspace_id)

        # 1. create session
        response = await connection.request(
            "POST", sessions_url, self.config.api_scope,
            data=self.config.session_body, headers=_JSON_HEADERS,
        )
        session_id = (response.get("body") or {}).get("id")
        if not session_id:
            raise MSFabricRunItemException(f"Livy session creation returned no id: {response}")
        self.log.info("Created Livy session id=%s", session_id)

        try:
            # 2. wait for idle
            async def get_session_state():
                r = await connection.request("GET", f"{sessions_url}/{session_id}", self.config.api_scope)
                return (r.get("body") or {}).get("state"), None

            def session_done(state, _):
                if state in ("error", "dead", "killed", "shutting_down"):
                    raise MSFabricRunItemException(f"Session {session_id} entered '{state}' before idle")
                return (state == "idle", None)

            await self._poll(get_session_state, session_done, self.config.session_timeout_seconds, f"session {session_id}")

            # 3. submit statement
            r = await connection.request(
                "POST", f"{sessions_url}/{session_id}/statements", self.config.api_scope,
                data=json.dumps({"code": self.config.code, "kind": "pyspark"}), headers=_JSON_HEADERS,
            )
            statement_id = (r.get("body") or {}).get("id")
            if statement_id is None:
                raise MSFabricRunItemException(f"Statement submission returned no id: {r}")

            # 4. wait for statement available
            statement_url = f"{sessions_url}/{session_id}/statements/{statement_id}"

            async def get_stmt_state():
                rr = await connection.request("GET", statement_url, self.config.api_scope)
                b = rr.get("body") or {}
                return b.get("state"), b

            def stmt_done(state, _):
                if state in ("error", "cancelled"):
                    raise MSFabricRunItemException(f"Statement {statement_id} entered '{state}'")
                return (state == "available", None)

            body = await self._poll(get_stmt_state, stmt_done, self.config.timeout_seconds, f"statement {statement_id}")
            # get_stmt_state returns (state, payload); _poll returns value from is_done -> None.
            # Re-fetch final payload once for the output.
            final = await connection.request("GET", statement_url, self.config.api_scope)
            output_obj = (final.get("body") or {}).get("output", {}) or {}
            if output_obj.get("status") != "ok":
                raise MSFabricRunItemException(
                    f"Statement failed: {output_obj.get('ename')}: {output_obj.get('evalue')}\n"
                    + "\n".join(output_obj.get("traceback", []) or [])
                )
            text = output_obj.get("data", {}).get("text/plain", "")

            return RunItemTracker(
                item=ItemDefinition(
                    workspace_id=item.workspace_id, item_type=item.item_type,
                    item_id=item.item_id, item_name=item.item_name,
                ),
                run_id=str(session_id),
                location_url="",
                run_timeout_in_seconds=0,  # signals completed-with-output
                start_time=datetime.now(),
                retry_after=timedelta(seconds=0),
                output=text,
            )
        finally:
            if self.config.delete_session_on_finish:
                try:
                    await connection.request("DELETE", f"{sessions_url}/{session_id}", self.config.api_scope)
                    self.log.info("Deleted session %s", session_id)
                except Exception as e:
                    self.log.warning("Failed to delete session %s: %s", session_id, e)

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker):
        return MSFabricRunItemStatus.COMPLETED, None  # run_item raises on failure

    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> bool:
        raise MSFabricRunItemException("Livy session does not support cancellation.")

    async def generate_deep_link(self, tracker: RunItemTracker, base_url: str = "https://app.fabric.microsoft.com") -> str:
        ws = tracker.item.workspace_id
        lh = tracker.item.item_id
        if not ws or not lh:
            return ""
        return f"{base_url}/groups/{ws}/lakehouses/{lh}"
