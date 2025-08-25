from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.microsoft.fabric.hooks.run_item.base import (
    MSFabricRunItemException,
    BaseFabricRunItemHook,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, MSFabricRunItemStatus, RunItemOutput, RunItemTracker
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


# XCom key constants
class XComKeys:
    """Constants for XCom keys used by the MSFabricRunItemOperator."""
    WORKSPACE_ID = "workspace_id"
    ITEM_TYPE = "item_type"
    ITEM_ID = "item_id"
    RUN_ID = "run_id"
    ITEM_NAME = "item_name"
    RUN_STATUS = "run_status"
    ERROR_MESSAGE = "error_message"
    OUTPUT = "output"


class MSFabricItemLink(BaseOperatorLink):
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


class BaseFabricRunItemOperator(BaseOperator):
    """Operator to run a Fabric item (e.g. a notebook) in a workspace."""

    operator_extra_links = (MSFabricItemLink(),)
    
    def __init__(self, *, hook: BaseFabricRunItemHook, item: ItemDefinition, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hook = hook
        self.item = item


    @abstractmethod
    def execute(self, context: Context) -> None: ...

    @abstractmethod
    def create_trigger(self, tracker: RunItemTracker) -> BaseFabricRunItemTrigger: ...

    async def _execute_core(self, context: Context, deferrable: bool, wait_for_termination: bool = True) -> None:
        """Core execution logic that works for both deferrable and synchronous modes."""
        tracker = None

        try:
            # Initialize the run using the hook and get RunItemTracker object
            tracker = await self.hook.initialize_run(self.item)
                       
            self.log.info(
                "Run initialized successfully - workspace_id: %s, item_id: %s, item_name: %s, run_id: %s, location: %s, start_time: %s, retry_after: %s, timeout: %ds",
                self.item.workspace_id, self.item.item_id, tracker.item.item_name, tracker.run_id, 
                tracker.location_url, tracker.start_time.isoformat() if tracker.start_time else "None", 
                tracker.retry_after, tracker.run_timeout_in_seconds
            )

            # Store item name and run_id in XCom
            ti = context.get("ti")
            if ti:
                # Populate item information
                ti.xcom_push(key=XComKeys.WORKSPACE_ID, value=self.item.workspace_id)
                ti.xcom_push(key=XComKeys.ITEM_ID, value=self.item.item_id)
                ti.xcom_push(key=XComKeys.ITEM_TYPE, value=self.item.item_type)

                # Add run information
                ti.xcom_push(key=XComKeys.RUN_ID, value=tracker.run_id)
                ti.xcom_push(key=XComKeys.ITEM_NAME, value=tracker.item.item_name)

            # If not waiting for termination, return immediately after initialization
            if not wait_for_termination:
                self.log.warning("Not waiting for termination, Airflow won't report proper output. Job info: run_id: %s, location_url: %s", tracker.run_id, tracker.location_url)
                return

            if deferrable:
                self.log.info("Deferring task to trigger - run_id: %s will be monitored asynchronously", tracker.run_id)

                self.defer(
                    trigger=self.create_trigger(tracker=tracker),
                    method_name="execute_complete",
                )
            else:
                # Wait for completion synchronously in the same event loop
                self.log.warning(
                    "Waiting for task completion synchronously, in operator. Discouraged for long running tasks as workers may restart.")
                output = await self.hook.wait_for_completion(tracker=tracker)

                # Handle completion using the centralized method
                self.handle_complete(context=context, output=output)

        except MSFabricRunItemException as e:
            # Handle Fabric exceptions directly
            self.log.error("Fabric run failed - error: %s", str(e))

            output = self.create_failed_output(
                tracker=tracker,
                error=f"Unexpected error: {str(e)}"
            )
            
            self.handle_complete(context=context, output=output)

        except Exception as e:
            # Handle unexpected exceptions
            self.log.error("Unexpected error during run execution - error: %s", str(e))

            output = self.create_failed_output(
                tracker=tracker,
                error=f"Unexpected error: {str(e)}"
            )
            
            self.handle_complete(context=context, output=output)
            
        finally:
            # Clean up resources for synchronous execution
            if not deferrable and self.hook:
                try:
                    await self.hook.close()
                except Exception as e:
                    self.log.warning("Failed to close hook connections: %s", str(e))


    def create_failed_output(self, tracker: Optional[RunItemTracker], error: str) -> RunItemOutput:
        return RunItemOutput(
            tracker=tracker if tracker else RunItemTracker(item=self.item, run_id="Unknown", location_url="Unknown", run_timeout_in_seconds=0, start_time=datetime.now(), retry_after=None),
            status=MSFabricRunItemStatus.FAILED,
            failed_reason=error
        )


    def execute_complete(self, context: Context, event: dict) -> None:
        """Handle completion from trigger event by parsing and delegating to core completion logic."""

        self.log.info(event)
        if not event:
            raise MSFabricRunItemException("No output received from trigger event.")

        output = RunItemOutput.from_dict(event)
        self.log.info(output)

        self.handle_complete(
            context=context, 
            output=output)


    def handle_complete(self, context: Context, output: RunItemOutput) -> None:
        """Handle completion logic for both synchronous and asynchronous execution."""

        if not output:
            raise MSFabricRunItemException("Output not available, check execution logs for more details.")

        status = output.status or MSFabricRunItemStatus.FAILED
        is_success = self.hook.is_run_successful(status)

        # push xcom
        ti = context.get("ti")
        if ti:                
            ti.xcom_push(key=XComKeys.RUN_STATUS, value=status.name)

            if is_success and output.result:
                ti.xcom_push(key=XComKeys.OUTPUT, value=output.result)
            
            if not is_success and output.failed_reason:
                ti.xcom_push(key=XComKeys.ERROR_MESSAGE, value=output.failed_reason)

        run_id  = output.tracker.run_id if output.tracker else None
        item_name = output.tracker.item.item_name if output.tracker else None

        if not is_success:    
            self.log.error(
                "Run failed - workspace_id: %s, item_id: %s, run_id: %s, status: %s, error_message: '%s'",
                self.item.workspace_id, self.item.item_id, run_id, output.status, output.failed_reason)

            raise MSFabricRunItemException(
                f"Run failed with status: {output.status} for run_id: {run_id}, item_id: {self.item.item_id}, workspace_id: {self.item.workspace_id}, error_message: {output.failed_reason}"
            )

        self.log.info(
            "Run completed successfully - workspace_id: %s, item_id: %s, item_name: %s, run_id: %s, status: %s",
            self.item.workspace_id, self.item.item_id, item_name, run_id, output.status)