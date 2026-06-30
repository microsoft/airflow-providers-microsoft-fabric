from __future__ import annotations

import json
import importlib.util
import os
import sys
import pytest

# Load the module file directly to avoid the package __init__.py
# which imports Airflow dependencies not needed for these pure-logic tests.
_src = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "src",
    "airflow", "providers", "microsoft", "fabric",
    "operators", "run_item", "notebook_parameters.py",
)
_spec = importlib.util.spec_from_file_location("notebook_parameters", _src)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

MSFabricNotebookJobParameters = _mod.MSFabricNotebookJobParameters
NotebookConfiguration = _mod.NotebookConfiguration
NotebookParams = _mod.NotebookParams


class TestNotebookParams:
    """Tests for the NotebookParams dataclass."""

    def test_set_explicit_type(self):
        params = NotebookParams()
        params.set("p1", "hello", "string")
        assert params.to_dict() == {"p1": {"value": "hello", "type": "string"}}

    def test_infer_string(self):
        params = NotebookParams()
        params.set("s", "text")
        assert params.to_dict()["s"]["type"] == "string"

    def test_infer_int(self):
        params = NotebookParams()
        params.set("i", 42)
        assert params.to_dict()["i"]["type"] == "int"

    def test_infer_float(self):
        params = NotebookParams()
        params.set("f", 3.14)
        assert params.to_dict()["f"]["type"] == "float"

    def test_infer_bool(self):
        params = NotebookParams()
        params.set("b", True)
        assert params.to_dict()["b"] == {"value": True, "type": "bool"}

    def test_bool_before_int(self):
        """bool is a subclass of int — must be detected as bool."""
        params = NotebookParams()
        params.set("flag", False)
        assert params.to_dict()["flag"]["type"] == "bool"

    def test_overwrite(self):
        params = NotebookParams()
        params.set("x", 1)
        params.set("x", 2)
        assert params.to_dict()["x"]["value"] == 2


class TestNotebookConfiguration:
    """Tests for the NotebookConfiguration dataclass."""

    def test_empty_config(self):
        cfg = NotebookConfiguration()
        assert cfg.to_dict() == {}

    def test_conf_only(self):
        cfg = NotebookConfiguration(conf={"spark.key": "val"})
        assert cfg.to_dict() == {"conf": {"spark.key": "val"}}

    def test_environment(self):
        cfg = NotebookConfiguration(environment_id="eid", environment_name="ename")
        result = cfg.to_dict()
        assert result["environment"] == {"id": "eid", "name": "ename"}

    def test_default_lakehouse(self):
        cfg = NotebookConfiguration(
            default_lakehouse_name="lh",
            default_lakehouse_id="lid",
            default_lakehouse_workspace_id="wid",
        )
        result = cfg.to_dict()
        assert result["defaultLakehouse"] == {"name": "lh", "id": "lid", "workspaceId": "wid"}

    def test_use_starter_pool(self):
        cfg = NotebookConfiguration(use_starter_pool=False)
        assert cfg.to_dict()["useStarterPool"] is False

    def test_use_workspace_pool(self):
        cfg = NotebookConfiguration(use_workspace_pool="pool1")
        assert cfg.to_dict()["useWorkspacePool"] == "pool1"

    # --- High Concurrency Mode ---

    def test_hc_both_fields(self):
        cfg = NotebookConfiguration(
            high_concurrency_enabled=True,
            high_concurrency_session_tag="my-tag",
        )
        result = cfg.to_dict()
        assert result["highConcurrencyModeOptions"] == {
            "enabled": True,
            "sessionTag": "my-tag",
        }

    def test_hc_enabled_only(self):
        cfg = NotebookConfiguration(high_concurrency_enabled=True)
        result = cfg.to_dict()
        assert result["highConcurrencyModeOptions"] == {"enabled": True}

    def test_hc_enabled_false(self):
        cfg = NotebookConfiguration(high_concurrency_enabled=False)
        result = cfg.to_dict()
        assert result["highConcurrencyModeOptions"] == {"enabled": False}

    def test_hc_session_tag_only(self):
        cfg = NotebookConfiguration(high_concurrency_session_tag="tag-only")
        result = cfg.to_dict()
        assert result["highConcurrencyModeOptions"] == {"sessionTag": "tag-only"}

    def test_hc_omitted_when_unset(self):
        cfg = NotebookConfiguration(conf={"spark.k": "v"})
        result = cfg.to_dict()
        assert "highConcurrencyModeOptions" not in result

    def test_hc_with_other_fields(self):
        cfg = NotebookConfiguration(
            conf={"spark.x": "y"},
            use_starter_pool=False,
            high_concurrency_enabled=True,
            high_concurrency_session_tag="sess",
        )
        result = cfg.to_dict()
        assert result["conf"] == {"spark.x": "y"}
        assert result["useStarterPool"] is False
        assert result["highConcurrencyModeOptions"] == {
            "enabled": True,
            "sessionTag": "sess",
        }


class TestMSFabricNotebookJobParameters:
    """Tests for the top-level payload builder."""

    def test_params_only(self):
        builder = MSFabricNotebookJobParameters()
        builder.set_parameter("k", "v")
        result = builder.to_dict()
        assert result == {
            "executionData": {
                "parameters": {"k": {"value": "v", "type": "string"}},
            }
        }
        assert "configuration" not in result["executionData"]

    def test_params_with_config(self):
        builder = (
            MSFabricNotebookJobParameters()
            .set_parameter("n", 1)
            .set_conf("spark.a", "b")
        )
        result = builder.to_dict()
        assert result["executionData"]["configuration"]["conf"] == {"spark.a": "b"}

    def test_set_high_concurrency_mode_enabled_with_tag(self):
        builder = MSFabricNotebookJobParameters()
        builder.set_high_concurrency_mode(True, "my-tag")
        result = builder.to_dict()
        hc = result["executionData"]["configuration"]["highConcurrencyModeOptions"]
        assert hc == {"enabled": True, "sessionTag": "my-tag"}

    def test_set_high_concurrency_mode_enabled_no_tag(self):
        builder = MSFabricNotebookJobParameters()
        builder.set_high_concurrency_mode(True)
        result = builder.to_dict()
        hc = result["executionData"]["configuration"]["highConcurrencyModeOptions"]
        assert hc == {"enabled": True}

    def test_set_high_concurrency_mode_disabled(self):
        builder = MSFabricNotebookJobParameters()
        builder.set_high_concurrency_mode(False)
        result = builder.to_dict()
        hc = result["executionData"]["configuration"]["highConcurrencyModeOptions"]
        assert hc == {"enabled": False}

    def test_chaining_all_options(self):
        """Full builder chain produces correct structure including HC mode."""
        builder = (
            MSFabricNotebookJobParameters()
            .set_parameter("p1", "v1", "string")
            .set_parameter("p2", 42)
            .set_conf("spark.key", "val")
            .set_environment(environment_id="env-id")
            .set_default_lakehouse(name="lh", id="lh-id", workspace_id="ws-id")
            .set_use_starter_pool(False)
            .set_use_workspace_pool("mypool")
            .set_high_concurrency_mode(True, "hc-tag")
        )
        result = builder.to_dict()
        ed = result["executionData"]

        assert ed["parameters"]["p1"] == {"value": "v1", "type": "string"}
        assert ed["parameters"]["p2"] == {"value": 42, "type": "int"}

        cfg = ed["configuration"]
        assert cfg["conf"] == {"spark.key": "val"}
        assert cfg["environment"] == {"id": "env-id"}
        assert cfg["defaultLakehouse"] == {"name": "lh", "id": "lh-id", "workspaceId": "ws-id"}
        assert cfg["useStarterPool"] is False
        assert cfg["useWorkspacePool"] == "mypool"
        assert cfg["highConcurrencyModeOptions"] == {"enabled": True, "sessionTag": "hc-tag"}

    def test_to_json_roundtrip(self):
        builder = (
            MSFabricNotebookJobParameters()
            .set_parameter("x", "y")
            .set_high_concurrency_mode(True, "tag")
        )
        json_str = builder.to_json()
        parsed = json.loads(json_str)
        assert parsed == builder.to_dict()

    def test_no_hc_when_unset(self):
        """Regression: HC block must not appear when not explicitly set."""
        builder = (
            MSFabricNotebookJobParameters()
            .set_parameter("a", 1)
            .set_conf("spark.x", "y")
        )
        cfg = builder.to_dict()["executionData"]["configuration"]
        assert "highConcurrencyModeOptions" not in cfg
