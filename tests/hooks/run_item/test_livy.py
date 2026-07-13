from datetime import datetime

import pytest

from airflow.providers.microsoft.fabric.hooks.run_item.livy import (
    LivyBatchConfig,
    LivySessionConfig,
    _map_livy_status,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import MSFabricRunItemStatus


class TestLivyStatusMapping:
    def test_success(self):
        assert _map_livy_status("success", None) == MSFabricRunItemStatus.COMPLETED
        assert _map_livy_status("running", "Succeeded") == MSFabricRunItemStatus.COMPLETED

    def test_failure(self):
        assert _map_livy_status("dead", None) == MSFabricRunItemStatus.FAILED
        assert _map_livy_status("error", None) == MSFabricRunItemStatus.FAILED
        assert _map_livy_status("running", "Failed") == MSFabricRunItemStatus.FAILED

    def test_in_progress(self):
        assert _map_livy_status("running", None) == MSFabricRunItemStatus.IN_PROGRESS
        assert _map_livy_status("starting", "Uncertain") == MSFabricRunItemStatus.IN_PROGRESS

    def test_not_started(self):
        assert _map_livy_status("not_started", None) == MSFabricRunItemStatus.NOT_STARTED
        assert _map_livy_status(None, None) == MSFabricRunItemStatus.NOT_STARTED


class TestLivyBatchConfig:
    def test_roundtrip(self):
        cfg = LivyBatchConfig(
            fabric_conn_id="c", timeout_seconds=120, poll_interval_seconds=15,
            lakehouse_id="lh", livy_body='{"file": "abfss://x/app.py"}',
        )
        d = cfg.to_dict()
        assert "tenacity_retry" not in d
        assert d["lakehouse_id"] == "lh"
        assert d["livy_body"] == '{"file": "abfss://x/app.py"}'
        restored = LivyBatchConfig.from_dict(d)
        assert restored.fabric_conn_id == "c"
        assert restored.timeout_seconds == 120
        assert restored.lakehouse_id == "lh"

    def test_from_dict_defaults(self):
        cfg = LivyBatchConfig.from_dict({"fabric_conn_id": "c", "lakehouse_id": "lh"})
        assert cfg.timeout_seconds == 3600
        assert cfg.poll_interval_seconds == 30
        assert cfg.tenacity_retry is None


class TestLivySessionConfig:
    def test_roundtrip(self):
        cfg = LivySessionConfig(
            fabric_conn_id="c", timeout_seconds=300, poll_interval_seconds=10,
            lakehouse_id="lh", code="print(1)",
            session_body='{"name": "s"}', session_timeout_seconds=300,
        )
        restored = LivySessionConfig.from_dict(cfg.to_dict())
        assert restored.code == "print(1)"
        assert restored.session_body == '{"name": "s"}'
        assert restored.session_timeout_seconds == 300
        assert restored.delete_session_on_finish is True
