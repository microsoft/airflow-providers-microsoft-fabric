from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Optional, Sequence
from airflow.providers.microsoft.fabric.hooks.run_item.base import MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.user_data_function import UserDataFunctionConfig, MSFabricRunUserDataFunctionHook
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger

from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition, RunItemTracker,
)
from airflow.providers.microsoft.fabric.operators.run_item.base import MSFabricItemLink, BaseFabricRunItemOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MSFabricRunUserDataFunctionOperator(BaseFabricRunItemOperator):
    """Run a Fabric job via the Job Scheduler."""

    # Keep template-able primitives as top-level attributes
    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "item_id",
        "item_name",
        "parameters",
        "api_host",
        "scope",
    )
    template_fields_renderers = {"parameters": "json"}  # optional

    operator_extra_links = (MSFabricItemLink(),)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        item_name: str,
        parameters: Optional[dict] | None = None,
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://analysis.windows.net/powerbi/api/.default",
        **kwargs,
    ) -> None:
        # Store raw values so Airflow can template them later
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.item_name = item_name
        self.job_type = "UserDataFunction"
        self.timeout = 0 
        self.parameters = parameters or {}
        self.api_host = api_host
        self.scope = scope

        # Build initial dataclasses from the *current* values
        config = UserDataFunctionConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=0,
            api_host=self.api_host,
            api_scope=self.scope,
            parameters=self.parameters,
        )
        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.item_id,
            item_name=self.item_name,
        )

        # If your hook needs more than conn_id, add it here
        hook = MSFabricRunUserDataFunctionHook(config=config)

        # Pass required args to the base class (fixes the missing kwargs error)
        super().__init__(hook=hook, item=item, **kwargs)

        # Keep the config around if you want to pass it to triggers, etc.
        self.config = config

    # Optional but recommended: ensure post-templating objects are rebuilt
    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)

        # Rebuild objects with the *rendered* values so they’re up to date
        self.config = UserDataFunctionConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=0,
            api_host=self.api_host,
            api_scope=self.scope,
            parameters=self.parameters,
        )
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.item_id,
            item_name=self.item_name,
        )
        self.hook = MSFabricRunUserDataFunctionHook(self.config)

    def create_trigger(self, tracker: RunItemTracker) -> BaseFabricRunItemTrigger:
        """Create and return the FabricHook (cached)."""
        raise MSFabricRunItemException("User data function does not support asynchronous execution.")

    def execute(self, context: Context) -> None:
        """Execute the Fabric item run."""
        self.log.info("Starting User Data Function Run - workspace_id: %s, job_type: %s, item_id: %s",
                      self.item.workspace_id, self.item.item_type, self.item.item_id)

        asyncio.run(self._execute_core(context, False))
