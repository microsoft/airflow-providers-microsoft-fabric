from datetime import datetime, timedelta
from dataclasses import dataclass, fields
from typing import Optional, Dict, Any

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.hooks.run_item.base import BaseFabricRunItemHook, MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker, RunItemConfig, MSFabricRunItemStatus

@dataclass(kw_only=True)
class JobSchedulerConfig(RunItemConfig):
    # API configuration parameters
    api_host: str = "https://api.fabric.microsoft.com"
    api_scope: str = "https://api.fabric.microsoft.com/.default"
    job_params: str = ""

    def to_dict(self) -> Dict[str, Any]:
        # Base handles fabric_conn_id/timeout/poll and drops tenacity_retry
        data = super().to_dict() if hasattr(super(), "to_dict") else {
            "fabric_conn_id": self.fabric_conn_id,
            "timeout_seconds": self.timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
        }
        data.update({
            "api_host": self.api_host,
            "api_scope": self.api_scope,
            "job_params": self.job_params,
        })
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobSchedulerConfig":
        d = dict(data or {})

        # Accept alias used sometimes in DAGs
        if "fabric_conn_id" not in d and "conn_id" in d:
            d["fabric_conn_id"] = d.pop("conn_id")

        # Defaults if older payloads omitted them
        d.setdefault("timeout_seconds", 600)
        d.setdefault("poll_interval_seconds", 5)

        # Ensure non-serializable runtime-only field is absent/None
        d["tenacity_retry"] = None

        # Keep only known dataclass fields (includes base + subclass)
        allowed = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in d.items() if k in allowed}
        return cls(**filtered)


class MSFabricRunJobHook(BaseFabricRunItemHook):
    """
    Logical hook for triggering and monitoring Fabric item runs.

    This hook delegates all connection logic to MSFabricRestConnection.
    """

    hook_name = "Microsoft Fabric Job Scheduler"
    conn_type = None
    conn_name_attr = None

    def __init__(
        self,
        config: JobSchedulerConfig,
    ):
        super().__init__(config)
        
        # Store config for access to api_host, api_scope, and job_params
        self.config = config

        self.log.info(
            "Initializing MS Fabric Job Scheduler Hook - conn_id: %s, poll interval: %s, timeout: %s, api_host: %s, api_scope: %s",
            config.fabric_conn_id, 
            config.poll_interval_seconds, 
            config.timeout_seconds,
            config.api_host,
            config.api_scope
        )

        try:
            self.conn = MSFabricRestConnection(
                config.fabric_conn_id,
                tenacity_retry=config.tenacity_retry
            )
            self.log.info(
                "Successfully initialized hook with connection_id: %s, poll_interval_seconds: %s, timeout_seconds: %s, api_host: %s, api_scope: %s", 
                config.fabric_conn_id, config.poll_interval_seconds, config.timeout_seconds, config.api_host, config.api_scope)

        except Exception as e:
            self.log.error("Failed to initialize MS Fabric Job Scheduler Hook: %s", str(e))
            raise

    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker:
        """
        Start a Fabric item run using the Job API.
        
        :param connection: MSFabricRestConnection instance for making API calls
        :param item: ItemDefinition containing the item configuration
        :return: RunItemTracker with run details
        """
        self.log.info(
            "Starting item run - workspace_id: %s, item_id: %s, item_type: %s",
            item.workspace_id, item.item_id, item.item_type
        )
        #self.log.info("Job parameters: %s", self.config.job_params) # may contain sensitive data

        url = f"{self.config.api_host}/v1/workspaces/{item.workspace_id}/items/{item.item_id}/jobs/instances?jobType={item.item_type}"
        # if item.item_type == "DataBuildToolJob":
        #     url = f"{self.config.api_host}/v1/workspaces/{item.workspace_id}/DataBuildToolJobs/{item.item_id}/jobs/Execute/instances"
        # else:
        #     url = f"{self.config.api_host}/v1/workspaces/{item.workspace_id}/items/{item.item_id}/jobs/instances?jobType={item.item_type}"    

        # send data and content-type = json instead of json= to avoid double encoding
        response = await connection.request(
            "POST",
            url,
            self.config.api_scope,
            data=self.config.job_params,   # JSON string
            headers={"Content-Type": "application/json"}
        )

        headers = response.get("headers", {})
        location = headers.get("Location")
        if not location:
            self.log.error("Missing Location header in response for item %s", item.item_id)
            raise MSFabricRunItemException("Missing Location header in run response.")

        # Extract run_id from x-ms-job-id header
        run_id = headers.get("x-ms-job-id")
        if not run_id:
            self.log.warning("Missing x-ms-job-id header, run_id will be unknown")
            run_id = "unknown"

        # Extract retry-after header and convert to timedelta
        retry_after = timedelta(seconds=30)
        retry_after_raw = headers.get("Retry-After")
        if retry_after_raw:
            try:
                retry_after_seconds = int(retry_after_raw)
                retry_after = timedelta(seconds=retry_after_seconds)
            except (ValueError, TypeError):
                self.log.warning("Invalid Retry-After header value: %s", retry_after_raw)

        # fetch artifact name
        item_name = await self.get_item_name(item)

        self.log.debug("Successfully started item run - name: %s, run_id: %s, retry_after: %s, location: %s", item_name, run_id, retry_after, location)

        # Create and return RunItemTracker using config timeout
        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item_name
            ),
            run_id=run_id,
            location_url=location,
            run_timeout_in_seconds=self.config.timeout_seconds,
            start_time=datetime.now(),
            retry_after=retry_after
        )

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> MSFabricRunItemStatus:
        """
        Get run status and details from location URL.

        :param connection: MSFabricRestConnection instance for making API calls
        :param tracker: RunItemTracker containing the run details
        :return: Run status data
        :raises MSFabricRunItemException: If run has failed with known error patterns
        """
        self.log.debug("Getting run status from: %s", tracker.location_url)

        # Use api_scope from config instead of hardcoded scope
        response = await connection.request("GET", tracker.location_url, self.config.api_scope)
        headers = response.get("headers", {})
        body = response.get("body") or {}

        # Parse Status
        status = self._parse_status(body.get("status"))

        self.log.info("Successfully retrieved run details for run_id: %s, status: %s, request_id: %s", tracker.run_id, status, headers.get("RequestId"))
        return status

    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker ) -> bool:
        """
        Cancel a running Fabric item job.
        
        :param connection: MSFabricRestConnection instance for making API calls
        :param item: ItemDefinition containing item details
        :param tracker: RunItemTracker containing run details
        :return: True if cancellation was successful, False otherwise
        """
        self.log.info("Cancelling run - workspace_id: %s, item_id: %s, run_id: %s",
                     tracker.item.workspace_id, tracker.item.item_id, tracker.run_id)

        try:
            # Use api_host from config instead of hardcoded URL
            url = f"{self.config.api_host}/v1/workspaces/{tracker.item.workspace_id}/items/{tracker.item.item_id}/jobs/instances/{tracker.run_id}/cancel"

            # Use api_scope from config instead of hardcoded scope
            await connection.request("POST", url, self.config.api_scope)
            self.log.info("Successfully cancelled run %s for item %s", tracker.run_id, tracker.item.item_id)
            return True
        except Exception as e:
            self.log.warning("Failed to cancel run %s for item %s: %s", tracker.run_id, tracker.item.item_id, str(e))
            return False
        
    async def generate_deep_link(self, tracker: RunItemTracker, base_url: str = "https://app.fabric.microsoft.com") -> str:
        """
        Generate deep links for job items: notebooks, pipelines, spark jobs, and DBT jobs.
        Uses the same URL patterns as MSFabricItemLink.
        
        :param tracker: RunItemTracker with run details
        :param base_url: Base URL for the Fabric portal
        :return: Deep link URL to the job item run
        """
        item_type = tracker.item.item_type
        workspace_id = tracker.item.workspace_id
        item_id = tracker.item.item_id
        run_id = tracker.run_id
        item_name = tracker.item.item_name

        if not workspace_id or not item_id or not run_id or not item_type:
            return ""

        # Use the same URL patterns as MSFabricItemLink
        if item_type == "RunNotebook":
            # interin solution, waiting for api to release deep link and exit value
            # https://dev.azure.com/powerbi/Trident/_git/Fabric-APIs/pullrequest/713597?_a=files
            return f"{base_url}/groups/{workspace_id}/synapsenotebooks/{item_id}?experience=fabric-developer" 
        
        elif item_type == "sparkjob":
            # interin solution while api does not report monitor url
            return f"{base_url}/groups/{workspace_id}/sparkjobdefinitions/{item_id}?experience=fabric-developer" # interin solution

        elif item_type == "Pipeline" and item_name:
            return f"{base_url}/workloads/data-pipeline/monitoring/workspaces/{workspace_id}/pipelines/{item_name}/{run_id}"

        elif item_type == "DataBuildToolJob":
            return f"{base_url}/workloads/data-pipeline/monitoring/workspaces/{workspace_id}/dbtitems/{item_id}/{run_id}"

        else:
            self.log.warning("Unsupported item type for job hook generate_deep_link: %s", item_type)
            return ""
        
    def _parse_status(self, sourceStatus: Optional[str]) -> MSFabricRunItemStatus:

        if (sourceStatus is None) or (sourceStatus == ""):
            raise MSFabricRunItemException("Invalid 'status' - null or empty.")

        # Fast path: exact value match (e.g., "Completed")
        try:
            return MSFabricRunItemStatus(sourceStatus)
        except ValueError:
            raise MSFabricRunItemException("Invalid 'status' - mapping to MSFabricRunItemStatus failed.")