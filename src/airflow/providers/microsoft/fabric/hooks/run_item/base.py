from abc import abstractmethod
import asyncio
import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, MSFabricRunItemStatus, RunItemConfig, RunItemOutput, RunItemTracker
from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection


class MSFabricRunItemException(AirflowException):
    """Raised when a Fabric item run fails or times out."""


class BaseFabricRunItemHook:
    """
    Logical hook for triggering and monitoring Fabric item runs.

    This hook delegates all connection logic to MSFabricRestConnection.
    """

    hook_name = "Microsoft Fabric Run Item"
    conn_type = None
    conn_name_attr = None

    TERMINAL_STATUSES = frozenset({MSFabricRunItemStatus.COMPLETED, MSFabricRunItemStatus.FAILED, MSFabricRunItemStatus.CANCELLED, MSFabricRunItemStatus.TIMED_OUT})
    FAILURE_STATUSES = frozenset({MSFabricRunItemStatus.FAILED, MSFabricRunItemStatus.CANCELLED, MSFabricRunItemStatus.DEDUPED, MSFabricRunItemStatus.TIMED_OUT})

    def __init__(
        self,
        runItemConfig: RunItemConfig
    ):
        self.log = logging.getLogger(__name__)
        self.runItemConfig = runItemConfig

        self.log.debug("Initializing MS Fabric Run Item Hook")

        try:
            self.conn = MSFabricRestConnection(
                runItemConfig.fabric_conn_id,
                tenacity_retry=runItemConfig.tenacity_retry,
            )
            self.log.info("Successfully initialized MSFabricRunItemHook - conn_id: %s, poll interval (secs): %s, timeout (secs): %s, retry_config: %s",
            runItemConfig.fabric_conn_id, runItemConfig.poll_interval_seconds, runItemConfig.timeout_seconds,  runItemConfig.tenacity_retry
        )
        except Exception as e:
            self.log.error("Failed to initialize MS Fabric Run Item Hook: %s", str(e))
            raise

    @abstractmethod
    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker: ...

    @abstractmethod
    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> MSFabricRunItemStatus: ...

    @abstractmethod
    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> bool: ...

    @abstractmethod
    async def generate_deep_link(self, tracker: RunItemTracker, base_url: str = "https://app.fabric.microsoft.com") -> str:
        """
        Generate a deep link to the Fabric item run in the portal.
        Each subclass must implement item-type-specific link generation.
        
        :param tracker: RunItemTracker with run details
        :param base_url: Base URL for the Fabric portal
        :return: Deep link URL to the item run
        """
        ...


    async def initialize_run(
        self, 
        item: ItemDefinition,
    ) -> RunItemTracker:
        """
        Initialize the Fabric item run and return ItemRun object with runtime information.
        
        :param item_id: The ID of the item to run
        :param job_type: The type of job to run
        :param job_params: Optional parameters for the job
        :param timeout_seconds: Total timeout in seconds
        :param check_interval: Polling interval in seconds
        :return: ItemRun object with runtime information populated
        """

        # Start the item run
        self.log.info(
            "Starting item run - workspace_id: %s, item_id: %s, job_type: %s",
            item.workspace_id, item.item_id, item.item_type, 
        )

        tracker = await self.run_item(self.conn, item)

        return tracker

    async def wait_for_completion(
        self,
        tracker: RunItemTracker
    ) -> RunItemOutput:
        """
        Wait for completion and return standardized event data with payload.
        :param item: ItemDefinition containing item details
        :param tracker: RunItemTracker object containing all necessary runtime information
        :return: Tuple of (event_data, status_payload) - payload is None on timeout/failure
        """

        # if tracker contains an output or timeout == 0, run_item completed with a 200
        # in case of failures, the run_item should raise an exception
        if tracker.output or tracker.run_timeout_in_seconds == 0:
            self.log.info("Run Completed: item_name: %s, run_id: %s, has_output: %s, run_timeout_in_seconds: %s", tracker.item.item_name, tracker.run_id, bool(tracker.output), tracker.run_timeout_in_seconds)
            return RunItemOutput(
                tracker=tracker,
                status=MSFabricRunItemStatus.COMPLETED,
                result=tracker.output
            )

        # Wait for completion - this is a long-running operation
        timeout_time = tracker.start_time + timedelta(seconds=tracker.run_timeout_in_seconds)
        start_polling_time = tracker.start_time + timedelta(seconds=tracker.retry_after.total_seconds()) if tracker.retry_after else datetime.now()

        self.log.info(
            "Waiting for completion - item_name: %s, run_id: %s, start_time: %s, start_polling_time: %s, timeout_time: %s, location_url: %s",
            tracker.item.item_name,
            tracker.run_id,
            tracker.start_time.isoformat(),
            start_polling_time.isoformat(),
            timeout_time.isoformat(),
            tracker.location_url
        )

        # Check if we should wait before starting to poll (retry-after delay)
        if datetime.now() < start_polling_time:
            wait_seconds = (start_polling_time - datetime.now()).total_seconds()
            self.log.info(
                "Waiting %.1f seconds until %s before starting to poll (retry-after delay)",
                wait_seconds, start_polling_time.isoformat()
            )
            await asyncio.sleep(wait_seconds)

        attempt = 0
        
        # Poll until timeout
        while datetime.now() < timeout_time:
            attempt += 1
            elapsed = (datetime.now() - tracker.start_time).total_seconds() if tracker.start_time else 0
            remaining = (timeout_time - datetime.now()).total_seconds() if timeout_time else 0

            self.log.debug(
                "Status check attempt %d (elapsed: %.1fs, remaining: %.1fs)", 
                attempt, elapsed, remaining)

            # Get status and check if run has finished
            status = await self.get_run_status(self.conn, tracker)
            has_finished = status in self.TERMINAL_STATUSES

            if has_finished:
                # Return success event data with payload
                return RunItemOutput(
                    tracker=tracker,
                    status=status,
                )

            self.log.debug(
                "Run still in progress, with status '%s'. Sleeping for %ds.", 
                status, self.runItemConfig.poll_interval_seconds)
            
            await asyncio.sleep(self.runItemConfig.poll_interval_seconds)
        
        elapsed = (datetime.now() - tracker.start_time).total_seconds() if tracker.start_time else 0

        return RunItemOutput(
                    tracker=tracker,
                    status=MSFabricRunItemStatus.TIMED_OUT,
                    failed_reason=f"Timeout waiting for run to complete after {elapsed:.1f} seconds."
                )            
        
    @staticmethod
    def is_run_successful(status: MSFabricRunItemStatus) -> bool:
        return status == MSFabricRunItemStatus.COMPLETED

    async def close(self):
        """Gracefully close reusable resources like the aiohttp session."""
        if self.conn:
            try:
                await self.conn.close()
            except Exception as e:
                self.log.warning("Error closing connection: %s", str(e))


    async def get_item_name(self, item: ItemDefinition) -> str:

        try:
            self.log.debug("Getting item details - workspace_id: %s, item_id: %s", item.workspace_id, item.item_id)
            response = await self.conn.request(
                "GET", 
                f"https://api.fabric.microsoft.com/v1/workspaces/{item.workspace_id}/items/{item.item_id}", 
                "https://api.fabric.microsoft.com/.default")
            body = response["body"]
            name = body.get("displayName", "Unknown")
            return name

        except Exception as e:
            self.log.warning("Failed to get item metadata, name won't be available: %s", str(e))
            return "Unknown"

