from dataclasses import dataclass, fields
from typing import Optional, Dict, Any

from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemConfig
from datetime import datetime, timedelta
from typing import Optional, Tuple

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.hooks.run_item.base import BaseFabricRunItemHook, MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker, MSFabricRunItemStatus


@dataclass(kw_only=True)
class SemanticModelRefreshConfig(RunItemConfig):
    """
    Config for refreshing a Power BI Semantic Model (dataset) via REST API.
    """
    api_host: str = "https://api.powerbi.com"
    api_scope: str = "https://analysis.windows.net/powerbi/api/.default"
    job_params: Optional[dict] = None

    def to_dict(self) -> Dict[str, Any]:
        base = (
            super().to_dict()
            if hasattr(super(), "to_dict")
            else {
                "fabric_conn_id": self.fabric_conn_id,
                "timeout_seconds": self.timeout_seconds,
                "poll_interval_seconds": self.poll_interval_seconds,
            }
        )
        base.update({
            "api_host": self.api_host,
            "api_scope": self.api_scope,
            "job_params": self.job_params,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SemanticModelRefreshConfig":
        d = dict(data or {})
        if "fabric_conn_id" not in d and "conn_id" in d:
            d["fabric_conn_id"] = d.pop("conn_id")
        d.setdefault("timeout_seconds", 600)
        d.setdefault("poll_interval_seconds", 5)
        d["tenacity_retry"] = None
        allowed = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in allowed})


class MSFabricRunSemanticModelRefreshHook(BaseFabricRunItemHook):
    """
    MSFabricRunSemanticModelRefreshHook and monitor a Semantic Model (dataset) refresh using the Power BI REST API.
    """

    hook_name = "Power BI Semantic Model Refresh"
    conn_type = None
    conn_name_attr = None

    def __init__(self, config: SemanticModelRefreshConfig):
        super().__init__(config)
        self.config = config
        self.conn = MSFabricRestConnection(config.fabric_conn_id, tenacity_retry=config.tenacity_retry)
        self.log.info(
            "Init PowerBISemanticModelRefreshHook conn_id=%s poll=%s timeout=%s host=%s scope=%s",
            config.fabric_conn_id, config.poll_interval_seconds, config.timeout_seconds, config.api_host, config.api_scope
        )


    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker:
        """
        Start a dataset refresh (semantic model) via REST API.
        Assumes item.item_id is the datasetId (common in Fabric). If your item id differs, pass the datasetId there.
        """
        url = f"{self.config.api_host}/v1.0/myorg/groups/{item.workspace_id}/datasets/{item.item_id}/refreshes"
        body = self.config.job_params or {}
        response = await connection.request("POST", url, self.config.api_scope, json=body)

        headers = response.get("headers", {})
        run_id = headers.get("RequestId")
        if not run_id:
            self.log.error("Missing RequestId header in response for item %s", item.item_id)
            raise MSFabricRunItemException("Missing RequestId header in run response.")

        # location_url is same as request, with /RequestId at the end - not provided as a header
        location_url = f"{url}"

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

        self.log.info("Started semantic model refresh: name: %s, run_id=%s refresh_id=%s, retry_after: %s, location=%s", item_name, run_id, run_id, retry_after, location_url)

        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item_name,
            ),
            run_id=run_id,
            location_url=location_url,
            run_timeout_in_seconds=self.config.timeout_seconds,
            start_time=datetime.now(),
            retry_after=retry_after,
        )

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> MSFabricRunItemStatus:
        """
        Poll the refresh status using refresh history.
        Semantic Model Eviction: Operation can be retried, see URL below for error handling
        PowerBI Embedded: Operation may remain in progress for up to 6 hours if capacity is paused       
            - https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh#semantic-model-eviction
        """
        # Get refresh history instead of direct status check
        history_url = f"{self.config.api_host}/v1.0/myorg/groups/{tracker.item.workspace_id}/datasets/{tracker.item.item_id}/refreshes"
        resp = await connection.request("GET", history_url, self.config.api_scope)
        
        try:
            status, error_message = self._get_status_from_history(resp.get("body", {}), tracker.run_id)
            
            if status == MSFabricRunItemStatus.FAILED and error_message:
                self.log.error("Refresh failed: run_id=%s error=%s", tracker.run_id, error_message)

            return status
                        
        except MSFabricRunItemException as e:
            self.log.error("Failed to get status from history: %s. Falling back to direct status check.", str(e))                    
            raise

    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> bool:
        """
        Cancel an in-flight refresh when we have a refreshId (enhanced refresh).
        Send a delete request to the tracker location
        """
        # Only possible if we know the refreshId (present if POST returned Location)
        if not tracker.location_url or not tracker.location_url.endswith(f"/refreshes/{tracker.run_id}"):
            self.log.info("Cancel not available - Invalid tracker location URL.")
            return False

        try:
            await connection.request("DELETE", tracker.location_url, self.config.api_scope)
            self.log.info("Cancelled semantic model refresh run for %s", tracker.location_url)
            return True
        except Exception as e:
            self.log.warning("Cancel failed for %s - ERROR: %s", tracker.location_url, e)
            return False

    async def generate_deep_link(self, tracker: RunItemTracker, base_url: str = "https://app.fabric.microsoft.com") -> str:
        """
        Generate deep links for PowerBISemanticModel items.
        Uses the same URL patterns as MSFabricItemLink.
        
        :param tracker: RunItemTracker with run details
        :param base_url: Base URL for the Fabric portal
        :return: Deep link URL to the semantic model refresh
        """
        item_type = tracker.item.item_type
        workspace_id = tracker.item.workspace_id
        item_id = tracker.item.item_id
        run_id = tracker.run_id

        if not workspace_id or not item_id or item_type != "PowerBISemanticModel":
            return ""

        # Use the same URL patterns as MSFabricItemLink
        if run_id:
            return f"{base_url}/groups/{workspace_id}/datasets/{item_id}/refreshdetails/{run_id}"
        else:
            return f"{base_url}/onelake/details/dataset/{item_id}/overview"

    def _parse_status(self, sourceStatus: Optional[str]) -> MSFabricRunItemStatus:
        """
        Map Power BI status strings to your enum.
            Failed: indicates the refresh operation failed.
            Unknown: indicates that the completion state can't be determined. With this status, endTime is empty.
            Disabled: indicates that the refresh was disabled by selective refresh.
            Cancelled: indicates the refresh was canceled successfully.
        """

        if (sourceStatus is None) or (sourceStatus == ""):
            raise MSFabricRunItemException("Invalid 'status' - null or empty.")

        match sourceStatus:
            case "InProgress":
                return MSFabricRunItemStatus.IN_PROGRESS
            
            case "Unknown":
                # Per this documentation, Unknown is reported for in progress, check samples
                # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history
                return MSFabricRunItemStatus.IN_PROGRESS

            case "Completed":
                return MSFabricRunItemStatus.COMPLETED

            case "Failed":
                return MSFabricRunItemStatus.FAILED
                        
            case "Cancelled":
                return MSFabricRunItemStatus.CANCELLED
            
            case "Pending":
                return MSFabricRunItemStatus.NOT_STARTED
            
            case _:
                self.log.warning("Unknown state found '%s' - mapping to NOT_STARTED", sourceStatus)
                return MSFabricRunItemStatus.NOT_STARTED

                        
    def _get_status_from_history(self, json_data: Dict[str, Any], request_id: str) -> Tuple[MSFabricRunItemStatus, str]:

        if "value" not in json_data:
            raise MSFabricRunItemException(f"Bad refresh history response.")

        for refresh_item in json_data["value"]:
            if refresh_item.get("requestId") == request_id:

                raw_status = refresh_item.get("status");
                status = self._parse_status(raw_status)

                if status ==  MSFabricRunItemStatus.FAILED:
                    error_message = refresh_item.get("serviceExceptionJson")
                    return (MSFabricRunItemStatus.FAILED, error_message)

                return (status, "")

        raise MSFabricRunItemException(f"RequestId {request_id} not found in response.")