import pytest

from airflow.providers.microsoft.fabric.hooks.run_item.base import MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker
from airflow.providers.microsoft.fabric.operators.run_item import (
    MSFabricLivyBatchOperator,
    MSFabricLivyBatchParameters,
    MSFabricLivySessionOperator,
)

from datetime import datetime

COMMON = dict(fabric_conn_id="fabric-conn", workspace_id="ws-1", lakehouse_id="lh-1")


class TestMSFabricLivyBatchParameters:
    def test_requires_file(self):
        with pytest.raises(ValueError):
            MSFabricLivyBatchParameters().to_dict()

    def test_full_body(self):
        body = (
            MSFabricLivyBatchParameters()
            .set_file("abfss://x/app.py")
            .add_py_file("abfss://x/lib.whl")
            .add_jar("abfss://x/lib.jar")
            .set_executors(num=2, cores=8, memory="56g")
            .set_driver(cores=8, memory="56g")
            .set_conf("spark.dynamicAllocation.enabled", "false")
            .set_name("b")
            .to_dict()
        )
        assert body["file"] == "abfss://x/app.py"
        assert body["pyFiles"] == ["abfss://x/lib.whl"]
        assert body["jars"] == ["abfss://x/lib.jar"]
        assert body["numExecutors"] == 2
        assert body["conf"]["spark.dynamicAllocation.enabled"] == "false"


class TestMSFabricLivyBatchOperator:
    def test_item_is_lakehouse_scoped(self):
        op = MSFabricLivyBatchOperator(task_id="t", file="abfss://x/app.py", **COMMON)
        assert isinstance(op.item, ItemDefinition)
        assert op.item.item_type == "LivyBatch"
        assert op.item.item_id == "lh-1"  # lakehouse id
        assert op.item.workspace_id == "ws-1"

    def test_build_body_from_fields(self):
        op = MSFabricLivyBatchOperator(
            task_id="t", file="abfss://x/app.py", py_files=["abfss://x/lib.whl"],
            num_executors=3, executor_cores=8, executor_memory="56g",
            conf={"spark.dynamicAllocation.enabled": "false"}, **COMMON,
        )
        body = op.build_body()
        assert body["file"] == "abfss://x/app.py"
        assert body["pyFiles"] == ["abfss://x/lib.whl"]
        assert body["numExecutors"] == 3
        assert body["name"] == "airflow-fabric-livy-batch"

    def test_build_body_from_job_params(self):
        params = MSFabricLivyBatchParameters().set_file("abfss://x/app.py").set_name("jp")
        op = MSFabricLivyBatchOperator(task_id="t", job_params=params.to_json(), **COMMON)
        body = op.build_body()
        assert body["file"] == "abfss://x/app.py"
        assert body["name"] == "jp"

    def test_build_body_with_class_name(self):
        op = MSFabricLivyBatchOperator(
            task_id="t", file="abfss://x/app.jar", class_name="com.example.Main", **COMMON,
        )
        body = op.build_body()
        assert body["file"] == "abfss://x/app.jar"
        assert body["className"] == "com.example.Main"

    def test_params_class_name(self):
        body = (
            MSFabricLivyBatchParameters()
            .set_file("abfss://x/app.jar")
            .set_class_name("com.example.Main")
            .to_dict()
        )
        assert body["className"] == "com.example.Main"

    def test_build_body_requires_file(self):
        op = MSFabricLivyBatchOperator(task_id="t", num_executors=1, **COMMON)
        with pytest.raises(MSFabricRunItemException):
            op.build_body()

    def test_config_has_livy_body_and_lakehouse(self):
        op = MSFabricLivyBatchOperator(task_id="t", file="abfss://x/app.py", num_executors=2, **COMMON)
        cfg = op._config()
        assert cfg.lakehouse_id == "lh-1"
        assert '"file": "abfss://x/app.py"' in cfg.livy_body
        assert '"numExecutors": 2' in cfg.livy_body

    def test_create_trigger_serializes(self):
        op = MSFabricLivyBatchOperator(task_id="t", file="abfss://x/app.py", **COMMON)
        tracker = RunItemTracker(
            item=op.item, run_id="guid-1", location_url="http://x/batches/guid-1",
            run_timeout_in_seconds=10, start_time=datetime(2024, 1, 1), retry_after=None,
        )
        trigger = op.create_trigger(tracker=tracker)
        classpath, kwargs = trigger.serialize()
        assert classpath.endswith("MSFabricLivyBatchTrigger")
        assert "config" in kwargs and "tracker" in kwargs

    def test_extra_link(self):
        assert any(l.name == "Microsoft Fabric Link" for l in MSFabricLivyBatchOperator.operator_extra_links)


class TestMSFabricLivySessionOperator:
    def test_item_is_lakehouse_scoped(self):
        op = MSFabricLivySessionOperator(task_id="t", code="print(1)", **COMMON)
        assert op.item.item_type == "LivySession"
        assert op.item.item_id == "lh-1"

    def test_session_body_has_no_file(self):
        op = MSFabricLivySessionOperator(
            task_id="t", code="print(1)", num_executors=2, executor_cores=8,
            conf={"spark.dynamicAllocation.enabled": "false"}, **COMMON,
        )
        body = op.build_session_body()
        assert body["numExecutors"] == 2
        assert "file" not in body

    def test_session_no_async(self):
        op = MSFabricLivySessionOperator(task_id="t", code="print(1)", **COMMON)
        with pytest.raises(MSFabricRunItemException):
            op.create_trigger(tracker=None)
