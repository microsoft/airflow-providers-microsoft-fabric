from __future__ import annotations
from abc import abstractmethod
from typing import AsyncIterator, Tuple
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.providers.microsoft.fabric.hooks.run_item.base import BaseFabricRunItemHook
from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemOutput, RunItemTracker, MSFabricRunItemStatus


class BaseFabricRunItemTrigger(BaseTrigger):
    """Base trigger for Fabric item runs."""

    def __init__(self):
        super().__init__()

    @abstractmethod
    def initialize_hook_and_tracker(self) -> Tuple[BaseFabricRunItemHook, RunItemTracker]:
        """Initialize and return hook and tracker instances. Called by run method."""
        ...

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the fabric and polls for the item run status."""
        hook = None
        tracker = None

        try:
            hook, tracker = self.initialize_hook_and_tracker()
        except Exception as init_error:
            self.log.error("Failed to initialize hook and tracker: %s", str(init_error))
            # Return initialization failure event
            yield TriggerEvent({
                "status": "FAILED",
                "failed_reason": f"Trigger initialization failed: {str(init_error)}"
            })
            return

        try:
            # Initialize hook and tracker using specialized implementation            
            self.log.info(
                "Starting trigger polling - workspace_id: %s, item_id: %s, run_id: %s, start_time: %s, retry_after: %s, run_timeout_in_seconds: %s",
                tracker.item.workspace_id, tracker.item.item_id, tracker.run_id,
                tracker.start_time.isoformat() if tracker.start_time else "None",
                tracker.retry_after, tracker.run_timeout_in_seconds
            )
            
            output = await hook.wait_for_completion(tracker=tracker)
            
            self.log.info(
                "Completed trigger polling - workspace_id: %s, item_id: %s, run_id: %s, start_time: %s",
                tracker.item.workspace_id, tracker.item.item_id, tracker.run_id,
                tracker.start_time.isoformat() if tracker.start_time else "None"
            )

            yield TriggerEvent(output.to_dict())

        except Exception as error:
            # Unified error handling for execution errors
            self.log.error("Trigger execution failed: %s", str(error))
            
            # Create failure event with tracker (guaranteed to exist at this point)
            assert tracker is not None, "Tracker should be initialized before reaching this error handler"
            yield TriggerEvent(
                RunItemOutput(
                    tracker=tracker,
                    status=MSFabricRunItemStatus.FAILED,
                    failed_reason=str(error)
                ).to_dict()
            )

        finally:
            # Ensure the hook's session is properly closed
            if hook:
                try:
                    await hook.close()
                except Exception as close_error:
                    self.log.warning("Failed to close hook session: %s", str(close_error))
