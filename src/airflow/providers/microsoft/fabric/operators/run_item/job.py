from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Sequence
from airflow.providers.microsoft.fabric.hooks.run_item.job import JobSchedulerConfig, MSFabricRunJobHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition, RunItemTracker,
)
from airflow.providers.microsoft.fabric.operators.run_item.base import MSFabricItemLink, BaseFabricRunItemOperator
from airflow.providers.microsoft.fabric.triggers.run_item.job import MSFabricRunJobTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

class MSFabricRunJobOperator(BaseFabricRunItemOperator):
    """Run a Fabric job via the Job Scheduler.
    
    Supported job types:
    - "RunNotebook": Execute a Fabric notebook
    - "RunPipeline" or "Pipeline": Execute a Fabric data pipeline
    - "RunSparkJob" or "SparkJob": Execute a Spark job definition
    - "DataBuildToolJob" or "DBT": Execute a DBT job
    """

    @staticmethod
    def _map_job_type_for_api(job_type: str) -> str:
        """Map user-friendly job type names to API-compatible names."""
        """Updates this mapping should be reflected in hook generate_deep_link method."""
        """List all suported names for clarity"""
        if job_type == "RunPipeline" or job_type == "Pipeline":
            return "Pipeline"
        elif job_type == "RunNotebook" or job_type == "Notebook":
            return "RunNotebook" # as defined in job api
        elif job_type == "RunSparkJob" or job_type == "SparkJob":
            return "sparkjob"
        elif job_type == "DataBuildToolJob" or job_type == "DBT":
            return "DataBuildToolJob"
        return job_type

    # Keep template-able primitives as top-level attributes
    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "item_id",
        "job_type",
        "timeout",
        "check_interval",
        "deferrable",
        "job_params",
        "api_host",
        "scope",
        "link_base_url",
    )
    template_fields_renderers = {"job_params": "json"}

    operator_extra_links = (MSFabricItemLink(),)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        job_type: str,
        timeout: int = 60 * 60,   # 1 hour
        check_interval: int = 30,
        deferrable: bool = True,
        job_params: str = "",
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://api.fabric.microsoft.com/.default",
        link_base_url: str = "https://app.fabric.microsoft.com",
        wait_for_termination = True,
        **kwargs,
    ) -> None:
        # Store raw values so Airflow can template them later
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.job_params = job_params or ""
        self.api_host = api_host
        self.scope = scope
        self.link_base_url = link_base_url
        self.wait_for_termination = wait_for_termination # do not document this, available for backwards compatibility only

        # Build initial item definition with API-compatible job type
        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self._map_job_type_for_api(job_type),
            item_id=self.item_id,
        )

        # Pass required args to the base class (no hook needed anymore)
        super().__init__(item=item, **kwargs)

    def create_hook(self) -> MSFabricRunJobHook:
        """Create and return the hook instance."""
        config = JobSchedulerConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
            job_params=self.job_params,
        )
        return MSFabricRunJobHook(config=config)

    # Optional but recommended: ensure post-templating objects are rebuilt
    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)

        # Rebuild item with the *rendered* values and API-compatible job type
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self._map_job_type_for_api(self.job_type),
            item_id=self.item_id,
        )

    def create_trigger(self, tracker: RunItemTracker) -> MSFabricRunJobTrigger:
        """Create and return the trigger."""
        config = JobSchedulerConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
            job_params=self.job_params,
        )
        return MSFabricRunJobTrigger(
            config=config.to_dict(),
            tracker=tracker.to_dict())

    def execute(self, context: Context) -> None:
        """Execute the Fabric item run."""
        self.log.info("Starting Fabric item run - workspace_id: %s, job_type: %s, item_id: %s",
                      self.item.workspace_id, self.item.item_type, self.item.item_id)

        # Create hook at execution time
        hook = self.create_hook()
        asyncio.run(self._execute_core(context, self.deferrable, hook, self.wait_for_termination))
