import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
from tenacity import Retrying

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.fabric.hooks.item_run import ItemRun, ItemRunConfig
from airflow.providers.microsoft.fabric.hooks.rest_connection import MSFabricRestConnection


class MSFabricRunItemStatus:
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    NOT_STARTED = "NotStarted"
    DEDUPED = "Deduped"

    TERMINAL_STATUSES = {COMPLETED, FAILED, CANCELLED}
    FAILURE_STATUSES = {FAILED, CANCELLED, DEDUPED}


class MSFabricRunItemException(AirflowException):
    """Raised when a Fabric item run fails or times out."""


class MSFabricRunItemHook:
    """
    Logical hook for triggering and monitoring Fabric item runs.

    This hook delegates all connection logic to MSFabricRestConnection.
    """

    hook_name = "Microsoft Fabric Run Item"
    conn_type = None
    conn_name_attr = None

    def __init__(
        self,
        fabric_conn_id: str,
        workspace_id: str,
        api_host: str,
        scope: str = "https://api.fabric.microsoft.com/.default",
        tenacity_retry: Optional[Retrying] = None,
    ):
        self.log = logging.getLogger(__name__)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.api_host = api_host.rstrip("/")
        self.api_version = "v1"
        self.scope = scope

        self.log.info(
            "Initializing MS Fabric Run Item Hook - conn_id: %s, workspace_id: %s, api_host: %s, scope: %s, retry_config: %s",
            fabric_conn_id, workspace_id, self.api_host, scope, tenacity_retry
        )

        try:
            self.conn = MSFabricRestConnection(
                fabric_conn_id,
                tenacity_retry=tenacity_retry,
                scope=scope,
            )
            self.log.info("Successfully initialized hook with workspace_id: %s", self.workspace_id)

        except Exception as e:
            self.log.error("Failed to initialize MS Fabric Run Item Hook: %s", str(e))
            raise

    async def initialize_run(
        self, 
        item_id: str, 
        job_type: str, 
        job_params: dict | None = None,
        timeout_seconds: int = 3600,  # Default 1 hour
        check_interval: int = 30      # Default 30 seconds
    ) -> ItemRun:        
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
            "Starting item run - workspace_id: %s, item_id: %s, job_type: %s, job_params: %s",
            self.workspace_id, item_id, job_type, job_params if job_params else "None"
        )

        url = f"{self.api_host}/{self.api_version}/workspaces/{self.workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"
        body = {"executionData": {"parameters": job_params}} if job_params else {}

        response = await self.conn.request("POST", url, json=body)    

        # Validate response and extract location
        headers = response.get("headers", {})    
        location = headers.get("Location")
        if not location:
            self.log.error("Missing Location header in response for item %s", item_id)
            raise MSFabricRunItemException("Missing Location header in run response.")

        # Extract run_id from x-ms-job-id header
        run_id = headers.get("x-ms-job-id")

        # Extract retry-after header and convert to integer with fallback
        retry_after_raw = headers.get("Retry-After")
        if retry_after_raw:
            try:
                retry_after_seconds = int(retry_after_raw)
            except (ValueError, TypeError):
                self.log.warning("Invalid Retry-After header value: %s, using check_interval", retry_after_raw)
                retry_after_seconds = check_interval
        else:
            retry_after_seconds = check_interval
                    
        self.log.debug(
            "Successfully started item run - location: %s, retry_after: %s, run_id: %s", 
            location, retry_after_seconds, run_id)

        # Try to get item name (optional)
        try:
            item_metadata = await self.get_item_details(item_id)
            item_name = item_metadata.get("displayName")
        except Exception as e:
            self.log.warning("Failed to get item metadata: %s", str(e))
            item_name = None

        # Calculate timing values
        start_time = datetime.now()
        start_polling_time = start_time + timedelta(seconds=retry_after_seconds)
        timeout_time = start_time + timedelta(seconds=timeout_seconds)

        # Create ItemRun object with all the information including computed timing
        item_run = ItemRun(
            fabric_conn_id=self.fabric_conn_id,
            workspace_id=self.workspace_id,
            item_id=item_id,
            job_type=job_type,
            api_host=self.api_host,
            scope=self.scope,
            check_interval=check_interval,
            api_version=self.api_version,
            run_id=run_id,
            location_url=location,
            item_name=item_name,
            start_time=start_time,
            start_polling_time=start_polling_time,
            timeout_time=timeout_time
        )

        return item_run

    async def run_item(self, item_id: str, job_type: str, job_params: dict | None = None) -> str:
        self.log.info(
            "Starting item run - workspace_id: %s, item_id: %s, job_type: %s, job_params: %s",
            self.workspace_id, item_id, job_type, job_params if job_params else "None"
        )

        url = f"{self.api_host}/{self.api_version}/workspaces/{self.workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"
        body = {"executionData": {"parameters": job_params}} if job_params else {}

        response = await self.conn.request("POST", url, json=body)    

        headers = response.get("headers", {})    
        location = headers.get("Location")
        if not location:
            self.log.error("Missing Location header in response for item %s", item_id)
            raise MSFabricRunItemException("Missing Location header in run response.")

        self.log.info("Successfully started item run - location: %s", location)
        return location

    async def wait_for_completion(
        self,
        itemRun: ItemRun
    ) -> tuple[dict, dict | None]:
        """
        Wait for completion and return standardized event data with payload.
        :param itemRun: ItemRun object containing all necessary information
        :return: Tuple of (event_data, status_payload) - payload is None on timeout/failure
        """
        self.log.info(
            "Waiting for completion - start_time: %s, start_polling_time: %s, timeout_time: %s, location_url: %s",
            itemRun.start_time.isoformat() if itemRun.start_time else "None", 
            itemRun.start_polling_time.isoformat() if itemRun.start_polling_time else "None",
            itemRun.timeout_time.isoformat() if itemRun.timeout_time else "None",
            itemRun.location_url
        )

        # Wait before starting to poll if needed (retry-after delay)
        if itemRun.should_wait_before_polling():
            wait_seconds = itemRun.get_wait_seconds_before_polling()
            self.log.info(
                "Waiting %.1f seconds until %s before starting to poll (retry-after delay)",
                wait_seconds, itemRun.start_polling_time.isoformat() if itemRun.start_polling_time else "None"
            )
            await asyncio.sleep(wait_seconds)

        attempt = 0
        
        # Poll until timeout
        while not itemRun.is_timed_out():
            attempt += 1
            elapsed = itemRun.get_elapsed_seconds() or 0
            remaining = itemRun.get_remaining_timeout_seconds() or 0

            self.log.debug(
                "Status check attempt %d (elapsed: %.1fs, remaining: %.1fs)", 
                attempt, elapsed, remaining)

            # Check if run has finished
            has_finished, run_id, status, status_payload = await self.has_run_finished(itemRun.location_url)
            
            if has_finished:
                # Return success event data with payload
                event_data = self.create_event_data(
                    run_id=run_id,
                    item_name=itemRun.item_name,
                    location=itemRun.location_url,
                    status=status
                )
                return event_data, status_payload

            self.log.debug("Run still in progress, with status '%s'. Sleeping for %ds.", 
                          status, itemRun.check_interval)
            await asyncio.sleep(itemRun.check_interval)
            
        # Timeout - create timeout event data with no payload
        elapsed = itemRun.get_elapsed_seconds() or 0
        event_data = self.create_event_data(
            run_id=itemRun.run_id,
            item_name=itemRun.item_name,
            location=itemRun.location_url,
            status="timeout",
            timeout=True,
            failed_reason=f"Timeout waiting for run to complete after {elapsed:.1f} seconds."
        )
        return event_data, None

    async def get_run_status(self, location_url: str) -> dict:
        """
        Get run status and details from location URL.
        
        :param location_url: URL to fetch run status from
        :param validate: Whether to validate the run status for known failure patterns
        :return: Run status data
        :raises MSFabricRunItemException: If run has failed with known error patterns (when validate=True)
        """
        self.log.debug("Getting run status from: %s", location_url)
        
        response = await self.conn.request("GET", location_url)
        status_data = response["body"]
        
        run_id, status = self.extract_run_info(status_data)
        self.log.debug("Successfully retrieved run details - id: %s, status: %s", run_id, status)
                
        return status_data

    def extract_run_info(self, status_data: dict) -> tuple[str, str]:
        """
        Extract run ID and status from run status data.
        
        :param status_data: The status data returned from get_run_status()
        :return: Tuple of (run_id, status)
        """
        run_id = status_data.get("id", "Unknown")
        status = status_data.get("status", "Unknown")
        return run_id, status

    async def cancel_run(self, item_id: str, run_id: str) -> bool:
        self.log.info("Cancelling run - workspace_id: %s, item_id: %s, run_id: %s",
                  self.workspace_id, item_id, run_id)

        try:
            url = f"{self.api_host}/{self.api_version}/workspaces/{self.workspace_id}/items/{item_id}/jobs/instances/{run_id}/cancel"
            await self.conn.request("POST", url)
            self.log.info("Successfully cancelled run %s for item %s", run_id, item_id)
            return True
        except Exception as e:
            self.log.warning("Failed to cancel run %s for item %s: %s", run_id, item_id, str(e))
            return False

    async def get_item_details(self, item_id: str) -> dict:
        self.log.debug("Getting item details - workspace_id: %s, item_id: %s", self.workspace_id, item_id)

        url = f"{self.api_host}/{self.api_version}/workspaces/{self.workspace_id}/items/{item_id}"
        response = await self.conn.request("GET", url)
        return response["body"]

    async def close(self):
        """Gracefully close reusable resources like the aiohttp session."""
        if self.conn:
            try:
                await self.conn.close()
            except Exception as e:
                self.log.warning("Error closing connection: %s", str(e))

    async def has_run_finished(
        self, 
        location_url: str, 
    ) -> tuple[bool, str, str, dict]:
        """
        Check if a run has finished by fetching its status.
        """
        # Get status without validation during polling
        status_payload = await self.get_run_status(location_url)
        run_id, status = self.extract_run_info(status_payload)

        if status in MSFabricRunItemStatus.TERMINAL_STATUSES:            
            return True, run_id, status, status_payload
        
        return False, run_id, status, status_payload

    def is_run_successful(self, status: str) -> bool:
        return status == MSFabricRunItemStatus.COMPLETED

    def create_event_data(
        self,
        item_name: str | None,
        run_id: str,        
        location: str,
        status: str,
        timeout: bool = False,
        failed: bool = False,
        failed_reason: str | None = None
    ) -> dict:
        """
        Create standardized event data for triggers and operators.
        """
        event_data = {
            "status": status,
            "run_id": run_id,
            "location": location,
            "item_name": item_name,
        }
        
        # Build log message based on event type
        log_parts = [f"Creating event for task completion: run_id={run_id}, status={status}"]
        
        if timeout:
            event_data["timeout"] = "true"
            log_parts.append("timeout=true")
        
        if failed:
            event_data["failed"] = "true"
            event_data["failed_reason"] = failed_reason or "Unknown error"
            log_parts.append(f"failed=true, reason={failed_reason or 'Unknown error'}")
        
        # Log the event creation
        self.log.info(", ".join(log_parts))
        
        return event_data