from __future__ import annotations
from typing import AsyncIterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.providers.microsoft.fabric.hooks.run_item_hook import MSFabricRunItemHook
from airflow.providers.microsoft.fabric.hooks.run_item_model import RunItemOutput, RunItemTracker, MSFabricRunItemStatus


class MSFabricRunItemTrigger(BaseTrigger):
    """Trigger when a Fabric item run finishes."""

    def __init__(
        self,
        hook: MSFabricRunItemHook,
        tracker: RunItemTracker
    ):
        super().__init__()

        self.hook = hook
        self.tracker = tracker


    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the fabric and polls for the item run status."""

        try:
            self.log.info(
                "Starting trigger polling - start_time: %s, retry_after: %s, run_timeout_in_seconds: %s",
                self.tracker.start_time.isoformat() if self.tracker.start_time else "None",
                self.tracker.retry_after, self.tracker.run_timeout_in_seconds
            )
            output = await self.hook.wait_for_completion(tracker=self.tracker)
            
            yield TriggerEvent(output.to_dict())

        except Exception as error:
            # Error handling
            yield TriggerEvent(
                RunItemOutput(
                    tracker=self.tracker,
                    status=MSFabricRunItemStatus.FAILED,
                    failed_reason=str(error)
                )
            )

        finally:
            # Ensure the hook's session is properly closed
            try:
                await self.hook.close()
            except Exception as close_error:
                self.log.warning("Failed to close hook session: %s", str(close_error))
