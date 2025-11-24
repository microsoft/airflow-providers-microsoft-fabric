from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Sequence
from airflow.providers.microsoft.fabric.hooks.run_item.semantic_model_refresh import SemanticModelRefreshConfig, MSFabricRunSemanticModelRefreshHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition, RunItemTracker,
)
from airflow.providers.microsoft.fabric.operators.run_item.base import MSFabricItemLink, BaseFabricRunItemOperator
from airflow.providers.microsoft.fabric.triggers.run_item.semantic_model_refresh import MSFabricRunSemanticModelRefreshTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MSFabricRunSemanticModelRefreshOperator(BaseFabricRunItemOperator):
    """Trigger a Semantic Model Refresh in Fabric."""
    """ Required Permissions: Dataset.ReadWrite.All"""
    """ Recommended Scope: https://analysis.windows.net/powerbi/api/.default"""

    # Keep template-able primitives as top-level attributes
    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "item_id",
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
        timeout: int = 60 * 60,   # 1 hour
        check_interval: int = 30,
        deferrable: bool = True,
        job_params: dict | None = None,
        api_host: str = "https://api.powerbi.com",
        scope: str = "https://analysis.windows.net/powerbi/api/.default",
        link_base_url: str = "https://fabric.microsoft.com",
        **kwargs,
    ) -> None:
        # Store raw values so Airflow can template them later
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.job_params = job_params or {}
        self.api_host = api_host
        self.scope = scope
        self.link_base_url = link_base_url

        if (check_interval < 30):
            self.log.warning("check_interval interval is too short, which can lead to throttling.")

        # Build initial item definition
        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type="PowerBISemanticModel",
            item_id=self.item_id,
        )

        # Pass required args to the base class (no hook needed anymore)
        super().__init__(item=item, **kwargs)

    def create_hook(self) -> MSFabricRunSemanticModelRefreshHook:
        """Create and return the hook instance."""
        config = SemanticModelRefreshConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
            job_params=self.job_params,
        )
        return MSFabricRunSemanticModelRefreshHook(config=config)

    # Optional but recommended: ensure post-templating objects are rebuilt
    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)

        # Rebuild item with the *rendered* values so they're up to date
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type="PowerBISemanticModel",
            item_id=self.item_id,
        )

    def create_trigger(self, tracker: RunItemTracker) -> MSFabricRunSemanticModelRefreshTrigger:
        """Create and return the trigger."""
        config = SemanticModelRefreshConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
            job_params=self.job_params,
        )
        return MSFabricRunSemanticModelRefreshTrigger(
            config=config.to_dict(),
            tracker=tracker.to_dict())

    def execute(self, context: Context) -> None:
        """Execute the Fabric item run."""
        self.log.info("Starting Semantic Model Refresh - workspace_id: %s, item_id: %s",
                      self.item.workspace_id, self.item.item_id)

        # Create hook at execution time
        hook = self.create_hook()
        asyncio.run(self._execute_core(context, self.deferrable, hook))
