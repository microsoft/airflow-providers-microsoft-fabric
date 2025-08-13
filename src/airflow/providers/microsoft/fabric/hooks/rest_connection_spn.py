import re
import time
import logging
from datetime import datetime, timezone
import requests
from typing import Optional
from airflow.models import Connection
from airflow.exceptions import AirflowException


class MSFabricRestConnectionSPN:
    """
    Handles SPN-based authentication for Microsoft Fabric.

    Reads required SPN fields from Connection.extra and acquires an access token
    using the OAuth2 client credentials flow. Caches tokens and automatically
    renews them when they expire.
    """

    auth_type = "spn"
    _GUID_REGEX = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

    def __init__(self, conn: Connection, scope):
        self.log = logging.getLogger(__name__)
        
        self.connection_id = conn.conn_id
        self.extras = conn.extra_dejson
        self.scope = scope
        
        self.log.info("Initializing MS Fabric SPN connection for connection_id: %s, scope: %s", self.connection_id, scope)
        
        # Log available connection parameters (without sensitive data)
        available_params = list(self.extras.keys())
        self.log.debug("Available connection parameters: %s", available_params)
        
        # Validate and extract required parameters
        self.tenant_id = self._validate_guid(self.extras.get("tenantId"), "tenantId")
        self.client_id = self._validate_guid(self.extras.get("clientId"), "clientId")
        self.client_secret = self._validate_non_empty(self.extras.get("clientSecret"), "clientSecret")
        
        # Log connection configuration (mask sensitive data)
        self.log.info(
            "Connection configured - Tenant ID: %s, Client ID: %s, Client Secret: %s, Scope: %s",
            self.tenant_id,
            self.client_id,
            f"{self.client_secret[:4]}***{self.client_secret[-4:]}" if len(self.client_secret) > 8 else "***",
            scope
        )
        
        # Token cache - now single token since scope is fixed
        self._cached_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_acquired_at: float = 0.0

    def get_access_token(self) -> str:
        """
        Get access token for the configured scope.
        
        Returns cached token if valid, otherwise acquires a new one.
        
        :return: Access token
        """
        self.log.debug("Requesting access token for scope: %s", self.scope)
        
        # Check if we have a valid cached token
        if self._is_token_valid():
            expires_utc = datetime.fromtimestamp(self._token_expires_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            
            self.log.debug(
                "Using cached token for scope '%s' (expires: %s, token: %s...)",
                self.scope,
                expires_utc,
                str(self._cached_token)[:10] if self._cached_token else "None"
            )
            return str(self._cached_token)
        
        # Log token expiration or absence
        if self._cached_token:
            expires_utc = datetime.fromtimestamp(self._token_expires_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            self.log.debug("Token expired for scope '%s' (expired at: %s). Acquiring new token.", self.scope, expires_utc)
        else:
            self.log.debug("No cached token found for scope '%s'. Acquiring new token.", self.scope)
        
        # Token is expired or doesn't exist, get a new one
        return self._acquire_new_token()

    def _is_token_valid(self) -> bool:
        """
        Check if cached token is valid (exists and not expired).
        
        :return: True if token is valid, False otherwise
        """
        if not self._cached_token:
            self.log.debug("No token cached for scope: %s", self.scope)
            return False
        
        current_time = time.time()
        
        # Add a 5-minute buffer before expiration to avoid edge cases
        expiry_buffer = 300  # 5 minutes in seconds
        is_valid = current_time < (self._token_expires_at - expiry_buffer)
        
        if not is_valid:
            expires_utc = datetime.fromtimestamp(self._token_expires_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            current_utc = datetime.fromtimestamp(current_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            self.log.debug(
                "Token validation for scope '%s': expired (current: %s, expires: %s)",
                self.scope, current_utc, expires_utc
            )
        
        return is_valid

    def _acquire_new_token(self) -> str:
        """
        Acquire a new access token and cache it.
        
        :return: Access token
        """
        self.log.debug("Acquiring new access token for scope: %s", self.scope)
        
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        self.log.debug("Token endpoint: %s", token_url)
        
        request_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }
        
        try:
            response = requests.post(
                url=token_url,
                data=request_data,
                timeout=30,
            )
            
            self.log.debug(
                "Token request completed - Status: %d, Response size: %d bytes",
                response.status_code,
                len(response.content)
            )
            
            if not response.ok:
                error_detail = ""
                try:
                    error_data = response.json()
                    error_detail = f" (Error: {error_data.get('error', 'unknown')}, Description: {error_data.get('error_description', 'no description')})"
                except:
                    error_detail = f" (Raw response: {response.text[:200]})"
                
                raise AirflowException(
                    f"Failed to get access token from Azure AD. Status: {response.status_code}{error_detail}"
                )

            token_data = response.json()
            access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            
            current_time = time.time()
            expires_at = current_time + expires_in
            expires_utc = datetime.fromtimestamp(expires_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            
            # Cache the token
            self._cached_token = access_token
            self._token_expires_at = expires_at
            self._token_acquired_at = current_time
            
            self.log.info(
                "Successfully acquired access token for scope '%s' (expires: %s, valid for: %d seconds, token: %s...)",
                self.scope,
                expires_utc,
                expires_in,
                access_token[:10]
            )
            
            return access_token
            
        except requests.exceptions.RequestException as e:
            self.log.error("Network error while acquiring token for scope '%s': %s", self.scope, str(e))
            raise AirflowException(f"Network error while acquiring access token: {str(e)}")
        except Exception as e:
            self.log.error("Unexpected error while acquiring token for scope '%s': %s", self.scope, str(e))
            raise AirflowException(f"Unexpected error while acquiring access token: {str(e)}")

    def clear_token_cache(self) -> None:
        """
        Clear cached token.
        """
        if self._cached_token:
            self.log.info("Clearing cached token for scope: %s", self.scope)
            self._cached_token = None
            self._token_expires_at = 0.0
            self._token_acquired_at = 0.0
        else:
            self.log.debug("No cached token found to clear for scope: %s", self.scope)

    def get_token_info(self) -> Optional[dict[str, str | float]]:
        """
        Get information about cached token for debugging.
        
        :return: Token info or None if not cached
        """
        if self._cached_token:
            # Convert timestamps to human-readable format
            expires_utc = datetime.fromtimestamp(self._token_expires_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            acquired_utc = datetime.fromtimestamp(self._token_acquired_at, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            
            token_info = {
                "access_token": f"{self._cached_token[:10]}...",  # Masked for security
                "expires_at": self._token_expires_at,
                "expires_at_utc": expires_utc,
                "acquired_at": self._token_acquired_at,
                "acquired_at_utc": acquired_utc,
                "scope": self.scope,
                "is_valid": self._is_token_valid()
            }
            
            self.log.debug("Token info for scope '%s': %s", self.scope, token_info)
            return token_info
        
        self.log.debug("No token info available for scope: %s", self.scope)
        return None

    def _validate_non_empty(self, value: Optional[str], key: str) -> str:
        if not value or not value.strip():
            self.log.error("Missing or empty required parameter: %s", key)
            raise AirflowException(f"Missing or empty required extra parameter: '{key}' in connection '{self.connection_id}'")
        
        self.log.debug("Validated parameter '%s': present and non-empty", key)
        return value.strip()

    def _validate_guid(self, value: Optional[str], key: str) -> str:
        value = self._validate_non_empty(value, key)
        if not self._GUID_REGEX.match(value):
            self.log.error("Invalid GUID format for parameter '%s': %s", key, value)
            raise AirflowException(f"Invalid GUID format for '{key}': {value} in connection '{self.connection_id}'")
        
        self.log.debug("Validated GUID parameter '%s': %s", key, value)
        return value
