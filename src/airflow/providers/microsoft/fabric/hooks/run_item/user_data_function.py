from datetime import datetime, timedelta
from dataclasses import dataclass, fields
from typing import Optional, Dict, List, Any

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.hooks.run_item.base import BaseFabricRunItemHook, MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker, RunItemConfig, MSFabricRunItemStatus

@dataclass(kw_only=True)
class UserDataFunctionConfig(RunItemConfig):
    # API configuration parameters
    api_host: str = "https://api.fabric.microsoft.com"
    api_scope: str = "https://analysis.windows.net/powerbi/api/user_impersonation/.default"
    parameters: Optional[Dict] = None

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
            "parameters": self.parameters,
        })
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserDataFunctionConfig":
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


class MSFabricRunUserDataFunctionHook(BaseFabricRunItemHook):
    """
    Logical hook for triggering and monitoring Fabric User Data Functions runs.

    This hook delegates all connection logic to MSFabricRestConnection.
    """

    hook_name = "Microsoft Fabric User Data Function"
    conn_type = None
    conn_name_attr = None

    def __init__(
        self,
        config: UserDataFunctionConfig,
    ):
        super().__init__(config)
        
        # Store config for access to api_host, api_scope, and parameters
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

    async def run_item(self, connection: MSFabricRestConnection, item: ItemDefinition, item_name: str) -> RunItemTracker:
        """
        Start a run for a user data function. 
        Based off this documentation: https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/tutorial-invoke-from-python-app
        Run will complete and return 200 in case of success, together with the output. 

        :param connection: MSFabricRestConnection instance for making API calls
        :param item: ItemDefinition containing the item configuration
        :return: RunItemTracker with run details
        """
        self.log.info(
            "Starting item run - workspace_id: %s, item_id: %s, item_type: %s, item_name: %s",
            item.workspace_id, item.item_id, item.item_type, item.item_name
        )

        # Use api_host from config instead of hardcoded URL
        url = f"{self.config.api_host}/v1/workspaces/{item.workspace_id}/userDataFunctions/{item.item_id}/functions/{item.item_name}/invoke"
        
        # Use parameters from config
        parameters = self.config.parameters
        body = parameters or {}

        # Use api_scope from config instead of hardcoded scope
        response = await connection.request("POST", url, self.config.api_scope, json=body)    

        body = response["body"]
        invocation_id = body.get("invocationId")
        status = body.get("status")
        output = body.get("output")
        errors = body.get("errors", [])

        self.log.info(
            "User Data Function response - invocation_id: %s, status: %s, has_output: %s, error_count: %d",
            invocation_id, status, output is not None, len(errors)
        )

        # Validate and log errors - will raise exception if errors exist
        self._validate_and_log_errors(errors)

        # Create and return RunItemTracker using config timeout
        return RunItemTracker(
            item=ItemDefinition(
                workspace_id=item.workspace_id,
                item_type=item.item_type,
                item_id=item.item_id,
                item_name=item.item_name,                
            ),
            run_id=invocation_id,
            location_url="",
            run_timeout_in_seconds=0, # 
            start_time=datetime.now(),
            retry_after=timedelta(seconds=0),
            output=output  # Store output directly in tracker, signals operation completed with 200
        )

    async def get_run_status(self, connection: MSFabricRestConnection, tracker: RunItemTracker) -> MSFabricRunItemStatus:
        return MSFabricRunItemStatus.COMPLETED #run_init would fail in case of error

    async def cancel_run(self, connection: MSFabricRestConnection, tracker: RunItemTracker ) -> bool:
        raise MSFabricRunItemException("User Data Function does not support cancellation.")
        
    def _parse_status(self, sourceStatus: Optional[str]) -> MSFabricRunItemStatus:

        if (sourceStatus is None) or (sourceStatus == ""):
            raise MSFabricRunItemException("Invalid 'status' - null or empty.")

        # Fast path: exact value match (e.g., "Completed")
        try:
            return MSFabricRunItemStatus(sourceStatus)
        except ValueError:
            raise MSFabricRunItemException("Invalid 'status' - mapping to MSFabricRunItemStatus failed.")

    def _validate_and_log_errors(self, errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and log detailed error information from the User Data Function response.
        
        :param errors: List of error dictionaries from the response
        :return: The errors list (for chaining/inspection)
        :raises MSFabricRunItemException: If errors are present
        """
        if not errors or len(errors) == 0:
            self.log.info("No errors found in User Data Function response")
            return []
        
        self.log.error("User Data Function execution failed with %d error(s):", len(errors))
        
        detailed_messages = []
        for i, error in enumerate(errors, 1):
            error_name = error.get("name", "Unknown Error")
            error_message = error.get("message", "No error message provided")
            error_properties = error.get("properties", {})
            
            # Log detailed error information
            self.log.error(
                "Error %d - Name: '%s', Message: '%s'", 
                i, error_name, error_message
            )
            
            # Log properties if they exist
            if error_properties:
                self.log.error("Error %d Properties:", i)
                for key, value in error_properties.items():
                    self.log.error("  %s: %s", key, value)
            else:
                self.log.error("Error %d - No additional properties", i)
                       
            detailed_messages.append(f"Error {i}: {error_name} - {error_message}")
        
        # Raise exception with all error details
        error_summary = "; ".join(detailed_messages)
        raise MSFabricRunItemException(f"User Data Function failed with errors: {error_summary}")
