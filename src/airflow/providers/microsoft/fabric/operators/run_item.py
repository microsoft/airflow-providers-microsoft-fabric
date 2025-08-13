from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Sequence
from functools import cached_property

from airflow.configuration import conf
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.microsoft.fabric.hooks.run_item import (
    MSFabricRunItemHook,
    MSFabricRunItemException,
)
from airflow.providers.microsoft.fabric.triggers.run_item import MSFabricRunItemTrigger

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


# XCom key constants
class XComKeys:
    """Constants for XCom keys used by the MSFabricRunItemOperator."""
    WORKSPACE_ID = "workspace_id"
    ITEM_ID = "item_id"
    RUN_ID = "run_id"
    LOCATION = "location"
    ITEM_NAME = "item_name"
    RUN_STATUS = "run_status"
    RUN_DETAILS = "run_details"
    ERROR_MESSAGE = "error_message"

class MSFabricRunItemLink(BaseOperatorLink):
    """
    Link to the Fabric item run details page.

    :param run_id: The item run ID.
    :type run_id: str
    """
    @property
    def name(self) -> str:
        return "Monitor Item Run"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        run_id = XCom.get_value(key=XComKeys.RUN_ID, ti_key=ti_key)
        item_name = XCom.get_value(key=XComKeys.ITEM_NAME, ti_key=ti_key)

        workspace_id = getattr(operator, "workspace_id", None)
        item_id = getattr(operator, "item_id", None)
        job_type = getattr(operator, "job_type", None)

        base_url = "https://app.fabric.microsoft.com"

        if not run_id or not workspace_id or not item_id:
            return ""

        if job_type == "RunNotebook":
            return f"{base_url}/groups/{workspace_id}/synapsenotebooks/{item_id}?experience=data-factory"

        elif job_type == "Pipeline" and item_name:
            return f"{base_url}/workloads/data-pipeline/monitoring/workspaces/{workspace_id}/pipelines/{item_name}/{run_id}?experience=data-factory"

        return ""


class MSFabricRunItemOperator(BaseOperator):
    """Operator to run a Fabric item (e.g. a notebook) in a workspace."""

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
    )
    template_fields_renderers = {"parameters": "json"}

    operator_extra_links = (MSFabricRunItemLink(),)

    @cached_property
    def hook(self) -> MSFabricRunItemHook:
        """Create and return the FabricHook (cached)."""
        return MSFabricRunItemHook(
            fabric_conn_id=self.fabric_conn_id, 
            workspace_id=self.workspace_id,
            api_host=self.api_host,
            scope=self.scope)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        item_id: str,
        job_type: str,
        timeout: int = 60 * 60 * 1,  # 1 hour default timeout
        check_interval: int = 5,
        # Backward compatibility - individual retry parameters
        deferrable: bool = True,
        job_params: dict | None = None,
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://api.fabric.microsoft.com/.default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.job_params = job_params or {}
        self.api_host = api_host
        self.scope = scope
        
    def execute(self, context: Context) -> None:
        """Execute the Fabric item run."""
        self.log.info("Starting Fabric item run - workspace_id: %s, job_type: %s, item_id: %s", 
                      self.workspace_id, self.job_type, self.item_id)
        
        if self.deferrable:
            # For deferrable mode, run each step separately (triggers handle their own connections)
            asyncio.run(self._execute_core(context, deferrable=True))
        else:
            # For synchronous mode, run everything in a single event loop
            asyncio.run(self._execute_core(context, deferrable=False))

    async def _execute_core(self, context: Context, deferrable: bool) -> None:
        """Core execution logic that works for both deferrable and synchronous modes."""
        item_run = None

        try:
            # Initialize the run using the hook and get ItemRun object
            item_run = await self.hook.initialize_run(
                item_id=self.item_id,
                job_type=self.job_type,
                job_params=self.job_params
            )
                       
            self.log.info(
                "Run initialized - item_name: %s, run_id: %s, location: %s, start_time: %s, start_polling_time: %s, timeout_time: %s",
                item_run.item_name, item_run.run_id, item_run.location_url, item_run.start_time, item_run.start_polling_time, item_run.timeout_time
            )

            # Store item name and run_id in XCom
            ti = context.get("ti")
            if ti:
                ti.xcom_push(key=XComKeys.RUN_ID, value=item_run.run_id)
                ti.xcom_push(key=XComKeys.ITEM_NAME, value=item_run.item_name)

            if deferrable:
                self.log.info("Deferring the task to wait for item run to complete.")

                self.defer(
                    trigger=MSFabricRunItemTrigger(item_run=item_run),
                    method_name="execute_complete",
                )
            else:
                # Wait for completion synchronously in the same event loop
                self.log.warning("Waiting for task completion synchronously, in operator. Discouraged for long running tasks as workers may restart.")
                event_data, status_payload = await self.hook.wait_for_completion(item_run)

                # Handle completion
                self._handle_completion(
                    context=context,
                    event_data=event_data,
                    status_payload=status_payload
                )

        except MSFabricRunItemException as e:
            # Handle Fabric exceptions directly
            error_event_data = {
                "status": "error",
                "run_id": item_run.run_id if item_run else None,
                "location": item_run.location_url if item_run else None,
                "failed": True,
                "failed_reason": str(e)
            }
            self._handle_completion(context=context, event_data=error_event_data)
            
        except Exception as e:
            # Handle unexpected exceptions
            error_event_data = {
                "status": "error", 
                "run_id": item_run.run_id if item_run else None,
                "location": item_run.location_url if item_run else None,
                "failed": True,
                "failed_reason": f"Unexpected error: {str(e)}"
            }
            self._handle_completion(context=context, event_data=error_event_data)
            
        finally:
            # Clean up resources for synchronous execution
            if not deferrable:
                try:
                    await self.hook.close()
                except Exception as e:
                    self.log.warning("Failed to close hook connections: %s", str(e))

    def execute_complete(self, context: Context, event) -> None:
        """Handle completion from trigger event by parsing and delegating to core completion logic."""
        if not event:
            return
            
        # The event from trigger should already be in the correct format
        # Extract status_payload if provided by trigger
        status_payload = event.get("status_payload")
        
        # Delegate to the core completion handler
        self._handle_completion(
            context=context,
            event_data=event,
            status_payload=status_payload
        )

    def _handle_completion(
        self, 
        context: Context,
        event_data: dict,
        status_payload: dict | None = None
    ) -> None:
        """
        Core completion logic shared between deferrable and synchronous execution.
        Handles timeouts, failures, and success scenarios with proper XCom population.
        """
        # Fetch timeout, failed and failed_message from event_data
        timeout = event_data.get("timeout") == "true" or event_data.get("timeout") is True
        failed = event_data.get("failed") == "true" or event_data.get("failed") is True
        failed_reason = event_data.get("failed_reason")
        
        # Fetch status and check if it means success
        status = event_data.get("status")
        is_success = (
            status is not None and self.hook.is_run_successful(status) and
            not timeout and
            not failed and
            status not in ("timeout", "error")
        )

        # Create appropriate log message
        run_id = event_data.get("run_id")
        location = event_data.get("location")
        
        if timeout:
            message = f"Timeout waiting for run {run_id} to complete"
            self.log.error(message)
        elif failed:
            message = f"Run {run_id} failed: {failed_reason}"
            self.log.error(message)
        elif status == "error":
            message = f"Run {run_id} encountered an error"
            self.log.error(message)
        else:
            message = f"Run {run_id} completed with status {status}"
            self.log.info(message)
        
        # If task is successful and was deferred, fetch the run details
        # (status_payload will be None for deferred operations)
        if is_success and status_payload is None and location:
            try:
                self.log.info("Fetching run details for successful deferred operation - run_id: %s", run_id)
                status_payload = asyncio.run(self.hook.get_run_status(location))
            except Exception as e:
                self.log.warning("Failed to fetch run details for deferred completion: %s", str(e))
                # Continue without status_payload - populate_xcom will handle gracefully
    
        # Populate XCom
        ti = context.get("ti")
        self._populate_xcom(
            ti=ti,
            success=is_success,
            event_data=event_data,
            status_payload=status_payload
        )
        
        # Raise exception in case of failures
        if not is_success:
            error_msg = failed_reason or message
            self.log.info("Handle_Completion: An error occurred while waiting for the task to complete: %s", message)
            if ti is not None:
                ti.xcom_push(key=XComKeys.ERROR_MESSAGE, value=error_msg)
            raise MSFabricRunItemException(error_msg)

    def _populate_xcom(
        self,
        ti: TaskInstance | None,
        success: bool,
        event_data: dict,
        status_payload: dict | None = None
    ) -> None:
        """
        Populate task instance XCom with run information.
        Does not fetch any details - expects status_payload to be provided if available.
        """
        if ti is None:
            return
        
        # Extract basic info from event data
        status = event_data.get("status")
        run_id = event_data.get("run_id")
        location = event_data.get("location")
        
        # Always push basic information
        ti.xcom_push(key=XComKeys.WORKSPACE_ID, value=self.workspace_id)
        ti.xcom_push(key=XComKeys.ITEM_ID, value=self.item_id)
        if run_id:
            ti.xcom_push(key=XComKeys.RUN_ID, value=run_id)
        if status:
            ti.xcom_push(key=XComKeys.RUN_STATUS, value=status)
        if location:
            ti.xcom_push(key=XComKeys.LOCATION, value=location)
        if status_payload:
            ti.xcom_push(key=XComKeys.RUN_DETAILS, value=status_payload)

        self.log.info("Run details stored in XCom - run_id: %s, success: %s, status: %s, details: %s", run_id, success, status, status_payload)
