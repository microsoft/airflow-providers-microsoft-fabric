import asyncio
import aiohttp
import logging
from typing import Mapping, Tuple, Optional
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.exceptions import AirflowException, AirflowNotFoundException
from tenacity import Retrying, retry, retry_if_exception_type, wait_random_exponential, stop_after_attempt

from airflow.providers.microsoft.fabric.hooks.http_client import HttpClient
from airflow.providers.microsoft.fabric.hooks.rest_connection_spn import MSFabricRestConnectionSPN

from wtforms import StringField, PasswordField
from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
from flask_babel import gettext


AUTH_CLASSES = {
    "spn": MSFabricRestConnectionSPN,
    # Future: "pat": MSFabricRestConnectionPAT,
    # Future: "msi": MSFabricRestConnectionMSI,
}

class MSFabricRestConnection(BaseHook):
    """
    Manages REST-level authentication and API transport for Microsoft Fabric.
    Delegates authentication logic based on 'auth_type' in connection extras.
    All HTTP operations are handled by HttpClient.
    """
    conn_type = "microsoft-fabric"
    conn_name_attr = "fabric_conn_id"
    default_conn_name = "fabric_default"
    hook_name = "Microsoft Fabric"

    def __init__(
        self, 
        conn_id: str, 
        tenacity_retry: Optional[Retrying] = None,
        scope: str = "https://api.fabric.microsoft.com/.default",
    ):
        self.conn_id = conn_id
        self.scope = scope
        self.http_client = HttpClient(log=logging.getLogger(__name__))

        if tenacity_retry is None:
            # Create default retry configuration if not provided
            RETRYABLE_EXCEPTIONS = (
                aiohttp.ClientError,
                asyncio.TimeoutError,
            )
            self.tenacity_retry = retry(
                retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
                wait=wait_random_exponential(multiplier=1, max=16),  # 1s, 2s, 4s... up to 16s
                stop=stop_after_attempt(5),  # max 5 attempts
                reraise=True,
            )
        else:
            self.tenacity_retry = tenacity_retry
        
        self.log.info(
            "Initializing MS Fabric REST connection for conn_id: %s, scope: %s, retry_config: %s",
            conn_id, scope, tenacity_retry
        )

        self._initialize_authentication(conn_id, scope)

    def _get_and_validate_connection(self, conn_id: str) -> Connection:
        """Get and validate connection exists."""
        if not conn_id or not conn_id.strip():
            self.log.error("Connection ID is empty or None")
            raise AirflowException("Connection ID cannot be empty or None")

        try:
            conn = BaseHook.get_connection(conn_id)
            if not conn:
                self.log.error("Connection '%s' returned None from get_connection", conn_id)
                raise AirflowException(f"Connection '{conn_id}' not found")
            return conn
        except AirflowNotFoundException as e:
            self.log.error("Connection '%s' not found in Airflow: %s", conn_id, str(e))
            raise AirflowException(
                f"Connection '{conn_id}' not found. Please create the connection in Airflow with the required configuration."
            )
        except Exception as e:
            self.log.error("Unexpected error retrieving connection '%s': %s", conn_id, str(e))
            raise AirflowException(f"Failed to retrieve connection '{conn_id}': {str(e)}")

    def _initialize_authentication(self, conn_id: str, scope: str) -> None:
        """
        Initialize the authentication handler based on connection configuration.
        
        :param conn_id: Connection ID
        :param scope: OAuth2 scope for token requests
        :raises AirflowException: If authentication initialization fails
        """
        self.conn = self._get_and_validate_connection(conn_id)

        if not hasattr(self.conn, 'extra_dejson') or self.conn.extra_dejson is None:
            self.log.error("Connection '%s' has no extra configuration", conn_id)
            raise AirflowException(
                f"Connection '{conn_id}' missing extra configuration. "
                "Please configure tenantId, clientId, and clientSecret."
            )

        available_params = list(self.conn.extra_dejson.keys())
        self.log.debug("Available connection parameters for '%s': %s", conn_id, available_params)
        
        # Stick with spn for now due to lack of support for select fields in 2.10.5
        # extras = self.conn.extra_dejson
        # auth_type = extras.get("auth_type", "spn").lower()
        auth_type = "spn"
        
        self.log.debug("Using auth_type: %s for connection '%s'", auth_type, conn_id)
        
        auth_class = AUTH_CLASSES.get(auth_type)
        if not auth_class:
            self.log.error("Unsupported auth_type '%s' for connection '%s'", auth_type, conn_id)
            raise AirflowException(
                f"Unsupported auth_type '{auth_type}' in connection '{conn_id}'. "
                f"Supported types: {list(AUTH_CLASSES.keys())}"
            )

        # Initialize authentication handler
        try:
            self.auth = auth_class(self.conn, scope)
            self.log.debug("Successfully initialized auth handler for connection '%s'", conn_id)
        except Exception as e:
            self.log.error("Failed to initialize auth handler for connection '%s': %s", conn_id, str(e))
            raise AirflowException(f"Failed to initialize authentication for connection '{conn_id}': {str(e)}")

    def get_access_token(self) -> str:
        """Get access token from the authentication handler."""
        self.log.debug("Requesting access token for connection '%s'", self.conn_id)
        try:
            token = self.auth.get_access_token()
            self.log.debug("Successfully obtained access token for connection '%s'", self.conn_id)
            return token
        except Exception as e:
            self.log.error("Failed to get access token for connection '%s': %s", self.conn_id, str(e))
            raise

    def get_headers(self) -> Mapping[str, str]:
        """Get HTTP headers with authorization token."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}"
        }

    async def request(self, method: str, url: str, **kwargs) -> dict:
        """
        Make an authenticated HTTP request with automatic retries using HttpClient.

        :param method: HTTP method (GET, POST, etc.)
        :param url: Request URL
        :param kwargs: Additional arguments for the request (currently unused)
        :return: Parsed response data from HttpClient
        """       
        if self.http_client is None:
            raise AirflowException("HTTP client has been closed. Cannot make request.")
        
        # Get authentication headers
        headers = self.get_headers()
                
        # Delegate to HttpClient
        response_data = await self.http_client.make_request_json(
            method=method,
            url=url,
            headers=headers,
            tenacity_retry=self.tenacity_retry,
            **kwargs
        )
        
        return response_data

    async def close(self):
        """Close the HTTP client session."""
        if self.http_client:
            await self.http_client.close_session()
        self.http_client = None


    @classmethod
    def get_connection_form_widgets(cls) -> dict:
        return {
            "tenantId": StringField(str(gettext("Tenant ID")), widget=BS3TextFieldWidget()),
            "clientId": StringField(str(gettext("Client ID")), widget=BS3TextFieldWidget()),
            "clientSecret": PasswordField(str(gettext("Client Secret")), widget=BS3PasswordFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        return {
            "hidden_fields": ["schema", "port", "host", "extra", "login", "password"],
            "relabeling": {},
            "placeholders": {},
        }

    def test_connection(self) -> Tuple[bool, str]:
        """Test the connection by attempting to get an access token."""
        try:
            self.log.info("Testing connection '%s'", self.conn_id)
            token = self.get_access_token()

            if not token or len(token) < 10:
                return False, "Invalid access token received"

            self.log.info("Connection test successful for '%s'", self.conn_id)
            return True, f"Successfully obtained access token for connection '{self.conn_id}'"

        except Exception as e:
            self.log.error("Connection test failed for '%s': %s", self.conn_id, str(e))
            return False, f"Connection test failed: {str(e)}"
