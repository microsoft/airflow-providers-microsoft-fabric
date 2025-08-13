from __future__ import annotations

import asyncio
from typing import AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricRunItemHook
from airflow.providers.microsoft.fabric.hooks.item_run import ItemRun

class MSFabricRunItemTrigger(BaseTrigger):
    """Trigger when a Fabric item run finishes."""

    def __init__(
        self,
        item_run: ItemRun,
    ):
        super().__init__()

        if isinstance(item_run, dict):
            self.item_run = ItemRun.from_dict(item_run)
        else:
            self.item_run = item_run

    def serialize(self):
        """Serialize the MSFabricRunItemTrigger instance."""
        return (
            "airflow.providers.microsoft.fabric.triggers.run_item.MSFabricRunItemTrigger",
            {
                "item_run": self.item_run.to_dict(),
            },
        )

    def _create_trigger_event(
        self,
        hook: MSFabricRunItemHook,
        run_id: str,
        location: str,
        status: str,
        timeout: bool = False,
        failed: bool = False,
        failed_reason: str | None = None
    ) -> TriggerEvent:
        """Create a standardized trigger event using hook's event data creation."""
        event_data = hook.create_event_data(
            item_name=self.item_run.item_name,
            run_id=run_id,
            location=location,
            status=status,
            timeout=timeout,
            failed=failed,
            failed_reason=failed_reason,            
        )

        return TriggerEvent(event_data)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the fabric and polls for the item run status."""

        hook = MSFabricRunItemHook(
            fabric_conn_id=self.item_run.fabric_conn_id,
            workspace_id=self.item_run.workspace_id,
            api_host=self.item_run.api_host,
            scope=self.item_run.scope,
        )

        status = "unknown"

        try:
            # Validate item run has required fields
            if not self.item_run.is_initialized():
                raise ValueError("ItemRun must be properly initialized with run_id, location_url, and start_time")

            self.log.info(
                "Starting trigger polling - start_time: %s, start_polling_time: %s, timeout_time: %s",
                self.item_run.start_time.isoformat() if self.item_run.start_time else "None",
                self.item_run.start_polling_time.isoformat() if self.item_run.start_polling_time else "None",
                self.item_run.timeout_time.isoformat() if self.item_run.timeout_time else "None"
            )

            # Wait before starting to poll if needed (retry-after delay)
            if self.item_run.should_wait_before_polling():
                wait_seconds = self.item_run.get_wait_seconds_before_polling()
                self.log.info(
                    "Waiting %.1f seconds until %s before starting to poll (retry-after delay)",
                    wait_seconds, self.item_run.start_polling_time.isoformat() if self.item_run.start_polling_time else "None"
                )
                await asyncio.sleep(wait_seconds)
                
            # Poll until timeout
            while not self.item_run.is_timed_out():
                # Check if run has finished
                has_finished, run_id, status, status_payload = await hook.has_run_finished(self.item_run.location_url)

                if has_finished:
                    yield self._create_trigger_event(
                        hook=hook,
                        run_id=run_id,
                        location=self.item_run.location_url,
                        status=status
                    )
                    return

                elapsed = self.item_run.get_elapsed_seconds() or 0
                remaining = self.item_run.get_remaining_timeout_seconds() or 0
                self.log.info(
                    "Sleeping for %s seconds. Item state: %s (elapsed: %.1fs, remaining: %.1fs)",
                    self.item_run.check_interval, status, elapsed, remaining
                )
                                
                await asyncio.sleep(self.item_run.check_interval)
        
            # Timeout
            yield self._create_trigger_event(
                hook=hook,
                run_id=self.item_run.run_id,
                location=self.item_run.location_url,
                status=status,
                timeout=True
            )

        except Exception as error:
            # Error handling
            yield self._create_trigger_event(
                hook=hook,
                run_id=self.item_run.run_id or "unknown",
                location=self.item_run.location_url or "unknown",
                status=status,
                failed=True,
                failed_reason=str(error)
            )
        finally:
            # Ensure the hook's session is properly closed
            try:
                await hook.close()
            except Exception as close_error:
                self.log.warning("Failed to close hook session: %s", str(close_error))