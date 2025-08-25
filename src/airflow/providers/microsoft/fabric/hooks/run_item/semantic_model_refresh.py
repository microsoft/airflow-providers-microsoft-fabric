from dataclasses import dataclass, fields
from typing import Optional, Dict, Any

from airflow.providers.microsoft.fabric.hooks.run_item.model import RunItemConfig
from datetime import datetime, timedelta
from typing import Optional

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


    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition, item_name: str) -> RunItemTracker:
        """
        Start a dataset refresh (semantic model) via REST API.
        Assumes item.item_id is the datasetId (common in Fabric). If your item id differs, pass the datasetId there.
        """
        url = f"{self.config.api_host}/v1.0/myorg/groups/{item.workspace_id}/datasets/{item.item_id}/refreshes"
        body = self.config.job_params or {}

        self.log.info("POST refresh group=%s dataset=%s body=%s", item.workspace_id, item.item_id, body)
        response = await connection.request("POST", url, self.config.api_scope, json=body)

        headers = response.get("headers", {})
        run_id = headers.get("RequestId")
        if not run_id:
            self.log.error("Missing RequestId header in response for item %s", item.item_id)
            raise MSFabricRunItemException("Missing RequestId header in run response.")
        
        # location_url is same as request, with /RequestId at the end - not provided as a header
        location_url = f"{url}/{run_id}"

        # Extract retry-after header and convert to timedelta
        retry_after = timedelta(seconds=30)
        retry_after_raw = headers.get("Retry-After", "0")
        if retry_after_raw:
            try:
                retry_after_seconds = int(retry_after_raw)
                retry_after = timedelta(seconds=retry_after_seconds)
            except (ValueError, TypeError):
                self.log.warning("Invalid Retry-After header value: %s", retry_after_raw)

        self.log.info("Started refresh: run_id=%s refresh_id=%s location=%s", run_id, run_id, location_url)

        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item.item_name,
            ),
            run_id=run_id,
            location_url=location_url,
            run_timeout_in_seconds=self.config.timeout_seconds,
            start_time=datetime.now(),
            retry_after=retry_after,
        )

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> MSFabricRunItemStatus:
        """
        Poll the refresh status.
        Semantic Model Eviction: Operation can be retried, see URL below for error handling
        PowerBI Embedded: Operation may remain in progress for up to 6 hours if capacity is paused       
            - https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh#semantic-model-eviction
        """
        resp = await connection.request("GET", tracker.location_url, self.config.api_scope)
        sourceStatus = resp.get("status")
        status = self._parse_status(sourceStatus)
        body = resp.get("body") or {}

        self.log.info("GET refresh status: run_id=%s status=%s(SourceStatus: %s) body=%s", tracker.run_id, status, sourceStatus, body)
        return status

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
            
            case "Failed":
                return MSFabricRunItemStatus.FAILED
            
            case "Disabled":
                return MSFabricRunItemStatus.DISABLED
            
            case "Cancelled":
                return MSFabricRunItemStatus.CANCELLED
            
            case _:
                self.log.warning("Unknown state found '%s' - mapping to IN_PROGRESS", sourceStatus)
                return MSFabricRunItemStatus.IN_PROGRESS # NEED TO TEST UNKNOWN STATE - what would be the best status for it?