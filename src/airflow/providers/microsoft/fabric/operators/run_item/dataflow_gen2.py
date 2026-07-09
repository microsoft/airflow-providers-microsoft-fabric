from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Sequence

from airflow.providers.microsoft.fabric.hooks.run_item.dataflow_gen2 import (
    DATAFLOWS_GEN2_ITEM_TYPE,
    DataflowGen2Config,
    MSFabricRunDataflowGen2Hook,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker
from airflow.providers.microsoft.fabric.operators.run_item.base import (
    BaseFabricRunItemOperator,
    MSFabricItemLink,
)
from airflow.providers.microsoft.fabric.triggers.run_item.dataflow_gen2 import (
    MSFabricRunDataflowGen2Trigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MSFabricRunDataflowGen2Operator(BaseFabricRunItemOperator):
    """
    Trigger and monitor a Microsoft Fabric Dataflows Gen2 refresh.

    Submits a refresh job via the Fabric REST API Job Scheduler and optionally
    waits for completion, either synchronously or by deferring to an async
    trigger (recommended for production use).

    Required permissions: Workspace Member (or higher) on the target workspace.
    Recommended scope: ``https://api.fabric.microsoft.com/.default``

    :param fabric_conn_id: Airflow connection ID for Microsoft Fabric authentication.
    :param workspace_id: GUID of the Fabric workspace containing the dataflow.
    :param item_id: GUID of the Dataflows Gen2 item to refresh.
    :param timeout: Maximum seconds to wait for the refresh to complete (default: 3600).
    :param check_interval: Polling interval in seconds (default: 30).
    :param deferrable: When True (default), defer to an async trigger instead of
        blocking a worker slot during the polling loop.
    :param api_host: Fabric REST API base URL (default: ``https://api.fabric.microsoft.com``).
    :param scope: OAuth2 scope for the Fabric API
        (default: ``https://api.fabric.microsoft.com/.default``).
    :param link_base_url: Base URL used to build the portal deep link pushed to XCom
        (default: ``https://app.fabric.microsoft.com``).
    """

    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "item_id",
        "timeout",
        "check_interval",
        "deferrable",
        "api_host",
        "scope",
        "link_base_url",
    )

    operator_extra_links = (MSFabricItemLink(),)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        timeout: int = 60 * 60,
        check_interval: int = 30,
        deferrable: bool = True,
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://api.fabric.microsoft.com/.default",
        link_base_url: str = "https://app.fabric.microsoft.com",
        **kwargs,
    ) -> None:
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.api_host = api_host
        self.scope = scope
        self.link_base_url = link_base_url

        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=DATAFLOWS_GEN2_ITEM_TYPE,
            item_id=self.item_id,
        )

        super().__init__(item=item, **kwargs)

    def create_hook(self) -> MSFabricRunDataflowGen2Hook:
        """Build and return the Dataflows Gen2 hook."""
        config = DataflowGen2Config(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
        )
        return MSFabricRunDataflowGen2Hook(config=config)

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=DATAFLOWS_GEN2_ITEM_TYPE,
            item_id=self.item_id,
        )

    def create_trigger(self, tracker: RunItemTracker) -> MSFabricRunDataflowGen2Trigger:
        """Build and return the deferrable trigger."""
        config = DataflowGen2Config(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            api_host=self.api_host,
            api_scope=self.scope,
        )
        return MSFabricRunDataflowGen2Trigger(
            config=config.to_dict(),
            tracker=tracker.to_dict(),
        )

    def execute(self, context: Context) -> None:
        """Execute the Dataflows Gen2 refresh."""
        self.log.info(
            "Starting Dataflows Gen2 refresh - workspace_id: %s, item_id: %s",
            self.item.workspace_id,
            self.item.item_id,
        )
        hook = self.create_hook()
        asyncio.run(self._execute_core(context, self.deferrable, hook))
