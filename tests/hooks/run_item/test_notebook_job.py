import json
import logging
from unittest.mock import AsyncMock

import pytest

from airflow.providers.microsoft.fabric.hooks.run_item.job import JobSchedulerConfig, MSFabricRunJobHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, MSFabricRunItemStatus
from airflow.providers.microsoft.fabric.operators.run_item.notebook_parameters import MSFabricNotebookJobParameters


def make_hook(job_params):
    hook = MSFabricRunJobHook.__new__(MSFabricRunJobHook)
    hook.log = logging.getLogger(__name__)
    hook.config = JobSchedulerConfig(
        fabric_conn_id="test", timeout_seconds=600, poll_interval_seconds=5, job_params=job_params
    )
    hook.get_item_name = AsyncMock(return_value="Notebook")
    return hook


def test_high_concurrency_payload_and_non_hc_compatibility():
    builder = MSFabricNotebookJobParameters().set_parameter("sleep_seconds", 40, "int")
    original = builder.to_dict()
    assert original == {"executionData": {"parameters": {"sleep_seconds": {"value": 40, "type": "int"}}}}

    builder.set_high_concurrency_mode(True, "shared")
    assert builder.to_dict() == {
        "parameters": [{"name": "sleep_seconds", "value": 40, "type": "Integer"}],
        "executionData": {
            "compute": "Spark",
            "computeConfiguration": {
                "highConcurrencyModeOptions": {"enabled": True, "sessionTag": "shared"}
            },
        },
    }
    builder.set_high_concurrency_mode(False)
    assert builder.to_dict() == original


def test_high_concurrency_parameter_types_and_configuration():
    builder = (
        MSFabricNotebookJobParameters()
        .set_parameter("text", "hi")
        .set_parameter("fraction", 1.5)
        .set_parameter("flag", False)
        .set_use_starter_pool(False)
        .set_high_concurrency_mode(True)
    )
    assert builder.to_dict() == {
        "parameters": [
            {"name": "text", "value": "hi", "type": "String"},
            {"name": "fraction", "value": 1.5, "type": "Float"},
            {"name": "flag", "value": False, "type": "Boolean"},
        ],
        "executionData": {
            "compute": "Spark",
            "computeConfiguration": {
                "useStarterPool": False,
                "highConcurrencyModeOptions": {"enabled": True},
            },
        },
    }


@pytest.mark.asyncio
async def test_high_concurrency_uses_notebook_background_job():
    params = MSFabricNotebookJobParameters().set_parameter("sleep_seconds", 40, "int")
    params.set_high_concurrency_mode(True, "shared")
    hook = make_hook(params.to_json())
    connection = AsyncMock()
    connection.request.return_value = {
        "headers": {
            "Location": "https://api.fabric.microsoft.com/v1/workspaces/ws/notebooks/nb/jobs/instances/job",
            "x-ms-job-id": "job",
        }
    }
    item = ItemDefinition(workspace_id="ws", item_id="nb", item_type="RunNotebook")

    tracker = await hook.run_item(connection, item)
    assert tracker.run_id == "job"
    connection.request.assert_awaited_once_with(
        "POST",
        "https://api.fabric.microsoft.com/v1/workspaces/ws/notebooks/nb/jobs/execute/instances?beta=false",
        hook.config.api_scope,
        data=params.to_json(),
        headers={"Content-Type": "application/json"},
    )

    connection.request.return_value = {"body": {"status": "Completed"}}
    assert await hook.get_run_status(connection, tracker) == (MSFabricRunItemStatus.COMPLETED, None)
    connection.request.assert_awaited_with("GET", tracker.location_url, hook.config.api_scope)

    assert await hook.cancel_run(connection, tracker)
    connection.request.assert_awaited_with(
        "POST",
        "https://api.fabric.microsoft.com/v1/workspaces/ws/notebooks/nb/jobs/instances/job/cancel",
        hook.config.api_scope,
    )


@pytest.mark.parametrize("job_type", ["RunNotebook", "Pipeline"])
def test_non_hc_jobs_keep_generic_endpoint(job_type):
    for payload in (
        "", "not JSON", "[]", json.dumps({"executionData": {"parameters": {}}}),
        json.dumps({"executionData": {"compute": "Spark", "computeConfiguration": {
            "highConcurrencyModeOptions": {"enabled": False}
        }}}),
    ):
        hook = make_hook(payload)
        item = ItemDefinition(workspace_id="ws", item_id="item", item_type=job_type)
        assert hook.generate_run_item_api_url(item) == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws/items/item/jobs/Execute/instances"
        )
