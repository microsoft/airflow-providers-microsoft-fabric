from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.hooks.run_item.base import BaseFabricRunItemHook, MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import (
    ItemDefinition,
    MSFabricRunItemStatus,
    RunItemConfig,
    RunItemTracker,
)

DATAFLOWS_GEN2_ITEM_TYPE = "DataflowsGen2"


@dataclass(kw_only=True)
class DataflowGen2Config(RunItemConfig):
    """
    Configuration for triggering a Microsoft Fabric Dataflows Gen2 refresh.

    Uses the Fabric REST API Job Scheduler with jobType=Refresh.
    Required permissions: Workspace Member (or higher).
    Recommended scope: https://api.fabric.microsoft.com/.default
    """

    api_host: str = "https://api.fabric.microsoft.com"
    api_scope: str = "https://api.fabric.microsoft.com/.default"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict() if hasattr(super(), "to_dict") else {
            "fabric_conn_id": self.fabric_conn_id,
            "timeout_seconds": self.timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
        }
        data.update({
            "api_host": self.api_host,
            "api_scope": self.api_scope,
        })
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataflowGen2Config":
        d = dict(data or {})
        if "fabric_conn_id" not in d and "conn_id" in d:
            d["fabric_conn_id"] = d.pop("conn_id")
        d.setdefault("timeout_seconds", 3600)
        d.setdefault("poll_interval_seconds", 30)
        d["tenacity_retry"] = None
        allowed = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in allowed})


class MSFabricRunDataflowGen2Hook(BaseFabricRunItemHook):
    """
    Hook to trigger and monitor a Dataflows Gen2 refresh in Microsoft Fabric.

    Uses the Fabric REST API Job Scheduler endpoint:
        POST /v1/workspaces/{workspaceId}/items/{dataflowId}/jobs/instances?jobType=Refresh

    The operation is asynchronous — a 202 Accepted response is returned with a
    Location header that is polled until the refresh reaches a terminal state.

    Required permissions: Workspace Member (or higher) on the target workspace.
    """

    hook_name = "Microsoft Fabric Dataflows Gen2"
    conn_type = None
    conn_name_attr = None

    def __init__(self, config: DataflowGen2Config):
        super().__init__(config)
        self.config = config
        self.conn = MSFabricRestConnection(
            config.fabric_conn_id,
            tenacity_retry=config.tenacity_retry,
        )
        self.log.info(
            "Init MSFabricRunDataflowGen2Hook conn_id=%s poll=%ss timeout=%ss host=%s scope=%s",
            config.fabric_conn_id,
            config.poll_interval_seconds,
            config.timeout_seconds,
            config.api_host,
            config.api_scope,
        )

    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition) -> RunItemTracker:
        """
        Trigger a Dataflows Gen2 refresh via the Fabric Job Scheduler API.

        :param connection: MSFabricRestConnection instance for making API calls
        :param item: ItemDefinition containing the Dataflows Gen2 item details
        :return: RunItemTracker populated with run details from the API response
        :raises MSFabricRunItemException: If the API response is missing required headers
        """
        url = (
            f"{self.config.api_host}/v1/workspaces/{item.workspace_id}"
            f"/items/{item.item_id}/jobs/instances?jobType=Refresh"
        )

        self.log.info(
            "Triggering Dataflows Gen2 refresh - workspace_id: %s, item_id: %s",
            item.workspace_id,
            item.item_id,
        )

        response = await connection.request(
            "POST",
            url,
            self.config.api_scope,
            json={},
        )

        headers = response.get("headers", {})

        location = headers.get("Location")
        if not location:
            raise MSFabricRunItemException(
                "Missing Location header in Dataflows Gen2 refresh response."
            )

        run_id = headers.get("x-ms-job-id", "unknown")
        request_id = headers.get("RequestId", "unknown")

        retry_after = timedelta(seconds=60)
        retry_after_raw = headers.get("Retry-After")
        if retry_after_raw:
            try:
                retry_after = timedelta(seconds=int(retry_after_raw))
            except (ValueError, TypeError):
                self.log.warning("Invalid Retry-After header value: %s", retry_after_raw)

        item_name = await self.get_item_name(item)

        self.log.info(
            "Dataflows Gen2 refresh started - name: %s, run_id: %s, request_id: %s, location: %s",
            item_name,
            run_id,
            request_id,
            location,
        )

        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item_name,
            ),
            run_id=run_id,
            location_url=location,
            run_timeout_in_seconds=self.config.timeout_seconds,
            start_time=datetime.now(),
            retry_after=retry_after,
        )

    async def get_run_status(
        self,
        connection: MSFabricRestConnection,
        tracker: RunItemTracker,
    ) -> tuple[MSFabricRunItemStatus, Optional[str]]:
        """
        Poll the Fabric Job Scheduler status endpoint for the current refresh state.

        :param connection: MSFabricRestConnection instance for making API calls
        :param tracker: RunItemTracker containing the run details and location URL
        :return: Tuple of (status, error_details) where error_details is None on success
        """
        self.log.debug("Polling Dataflows Gen2 status from: %s", tracker.location_url)

        response = await connection.request("GET", tracker.location_url, self.config.api_scope)
        body = response.get("body") or {}

        status = self._parse_status(body.get("status"))
        error_details = self._parse_error_details(body.get("failureReason"))

        self.log.info(
            "Dataflows Gen2 run status - run_id: %s, status: %s, error_details: %s",
            tracker.run_id,
            status,
            error_details,
        )

        return status, error_details

    async def cancel_run(
        self,
        connection: MSFabricRestConnection,
        tracker: RunItemTracker,
    ) -> bool:
        """
        Cancel an in-progress Dataflows Gen2 refresh.

        :param connection: MSFabricRestConnection instance for making API calls
        :param tracker: RunItemTracker containing the run details
        :return: True if cancellation was successful, False otherwise
        """
        try:
            url = (
                f"{self.config.api_host}/v1/workspaces/{tracker.item.workspace_id}"
                f"/items/{tracker.item.item_id}/jobs/instances/{tracker.run_id}/cancel"
            )
            await connection.request("POST", url, self.config.api_scope)
            self.log.info(
                "Cancelled Dataflows Gen2 refresh - run_id: %s, item_id: %s",
                tracker.run_id,
                tracker.item.item_id,
            )
            return True
        except Exception as e:
            self.log.warning(
                "Failed to cancel Dataflows Gen2 refresh - run_id: %s, error: %s",
                tracker.run_id,
                e,
            )
            return False

    async def generate_deep_link(
        self,
        tracker: RunItemTracker,
        base_url: str = "https://app.fabric.microsoft.com",
    ) -> str:
        """
        Generate a deep link to the Dataflows Gen2 item in the Fabric portal.

        :param tracker: RunItemTracker with run details
        :param base_url: Base URL for the Fabric portal
        :return: Deep link URL to the Dataflows Gen2 item
        """
        workspace_id = tracker.item.workspace_id
        item_id = tracker.item.item_id

        if not workspace_id or not item_id:
            return ""

        return f"{base_url}/groups/{workspace_id}/dataflows/{item_id}"

    def _parse_status(self, source_status: Optional[str]) -> MSFabricRunItemStatus:
        """
        Map Fabric Job Scheduler status strings to MSFabricRunItemStatus.

        :param source_status: Raw status string from the API response
        :return: Corresponding MSFabricRunItemStatus enum value
        :raises MSFabricRunItemException: If the status is null, empty, or unrecognized
        """
        if not source_status:
            raise MSFabricRunItemException("Invalid 'status' — null or empty.")

        try:
            return MSFabricRunItemStatus(source_status)
        except ValueError:
            self.log.error(
                "Unknown Dataflows Gen2 status: '%s'. Valid statuses: %s",
                source_status,
                [s.value for s in MSFabricRunItemStatus],
            )
            raise MSFabricRunItemException(
                f"Invalid 'status' value '{source_status}' — could not map to MSFabricRunItemStatus."
            )

    def _parse_error_details(self, error: Optional[Dict[str, Any]]) -> Optional[str]:
        """
        Extract a human-readable error message from the API failureReason field.

        :param error: failureReason object from API response (dict or None)
        :return: Formatted error string, or None if no error
        """
        if not error:
            return None

        error_code = error.get("errorCode", "Unknown")
        message = error.get("message", "No message provided")
        request_id = error.get("requestId")

        error_str = f"{error_code}: {message}."
        if request_id:
            error_str += f" [RequestId: '{request_id}']"

        return error_str
