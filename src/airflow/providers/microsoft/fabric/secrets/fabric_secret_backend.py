import json
import logging
import os
import re
import requests
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend

from azure.identity import DefaultAzureCredential

log = logging.getLogger(__name__)

_GUID_REGEX = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class FabricSecretBackend(BaseSecretsBackend):
    """
    THIS IS NOT INTENDED FOR USE OUTSIDE OF FABRIC AIRFLOW JOBS.

    Airflow secrets backend that fetches pre-minted access tokens 
    from a Microsoft Fabric connections API.  

    Connections in Fabric are represented as a GUID — the GUID is used as a
    lookup key.  For non-GUID connection IDs the lookup is skipped (returns
    ``None``) so that remaining backends in the chain (e.g. environment
    variables, Airflow metadata DB) can handle them.

    Setup
    -----
    **airflow.cfg**::

        [secrets]
        backend = airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend
        backend_kwargs = {
            "api_base_url": "https://<fabric-api-host>",
            "api_scope": "5d13f7d7-0567-429c-9880-320e9555e5fc/.default",
            "expiry_buffer_seconds": 300
        }

    **Environment variable overrides** (useful in containerised / Kubernetes
    deployments where airflow.cfg is not easy to edit).  All constructor
    parameters can be passed as a JSON string via
    ``AIRFLOW__SECRETS__BACKEND_KWARGS``::

        AIRFLOW__SECRETS__BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend
        AIRFLOW__SECRETS__BACKEND_KWARGS={
            "api_base_url": "https://<fabric-api-host>",
            "api_scope": "API SCOPE TO BE USED",
            "expiry_buffer_seconds": 300
        }

    Alternatively, ``api_base_url`` and ``api_scope`` can be supplied via
    dedicated environment variables (``expiry_buffer_seconds`` will use its
    default)::

        FABRIC_SECRET_BACKEND_API_URL=https://<fabric-api-host>
        FABRIC_API_SCOPE=API scope to be used

    """

    def __init__(
        self,
        expiry_buffer_seconds: int = 300,
        api_scope: Optional[str] = None,
        api_base_url: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Cache: conn_id -> (Connection, expires_at_epoch)
        self._cache: Dict[str, Tuple[Connection, float]] = {}

        self._expiry_buffer_seconds = expiry_buffer_seconds

        self._api_scope = api_scope or os.environ.get("FABRIC_API_SCOPE")
        if not self._api_scope:
            raise AirflowException(
                "FabricSecretBackend requires 'api_scope' or the "
                "FABRIC_API_SCOPE environment variable to be set."
            )
        
        self._api_base_url = api_base_url or os.environ.get("FABRIC_SECRET_BACKEND_API_URL")
        if not self._api_base_url:
            raise AirflowException(
                "FabricSecretBackend requires 'api_base_url' or the "
                "FABRIC_SECRET_BACKEND_API_URL environment variable to be set."
            )

    # ------------------------------------------------------------------
    # Public API (BaseSecretsBackend contract)
    # ------------------------------------------------------------------

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """
        Return a ``Connection`` for *conn_id* if it is a GUID that the Fabric
        secret-store API can resolve.  Returns ``None`` otherwise so that
        Airflow falls through to the next secrets backend.
        """
        if not _GUID_REGEX.match(conn_id):
            return None

        # Check the in-memory cache first
        cached = self._get_cached(conn_id)
        if cached is not None:
            return cached

        # Fetch from the Fabric API and cache
        return self._fetch_and_cache(conn_id)

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Not used - we override ``get_connection`` directly."""
        return None

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _get_cached(self, conn_id: str) -> Optional[Connection]:
        if conn_id not in self._cache:
            return None

        conn, expires_at = self._cache[conn_id]
        now = time.time()

        if now >= (expires_at - self._expiry_buffer_seconds):
            log.debug(
                "Cached connection '%s' expired or within buffer window -- evicting.",
                conn_id,
            )
            del self._cache[conn_id]
            return None

        log.debug("Returning cached connection for '%s'.", conn_id)
        return conn

    # ------------------------------------------------------------------
    # Fabric API interaction
    # ------------------------------------------------------------------

    def _fetch_and_cache(self, conn_id: str) -> Optional[Connection]:
        """Call the Fabric credential API, build a Connection, and cache it."""
        url = f"{self._api_base_url.rstrip('/')}/connections/{conn_id}"
        log.debug("Fetching credential from Fabric API: %s", url)

        try:
            credential = DefaultAzureCredential()
            azure_token = credential.get_token(self._api_scope)

            start = time.monotonic()
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {azure_token.token}"},
                timeout=30,
                json={},
            )
            elapsed = time.monotonic() - start

            if elapsed > 5:
                log.warning(
                    "Fabric API call for conn_id '%s' took %.2f seconds.", conn_id, elapsed
                )
            else:
                log.info(
                    "Fabric API call for conn_id '%s' took %.2f seconds.", conn_id, elapsed
                )

            if response.status_code == 404:
                log.debug("Fabric API returned 404 for conn_id '%s'.", conn_id)
                return None

            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            log.error(
                "Network error fetching credential for '%s': %s", conn_id, exc
            )
            raise AirflowException(
                f"Failed to fetch credential from Fabric API for '{conn_id}': {exc}"
            )

        return self._parse_and_cache(conn_id, payload)

    # ------------------------------------------------------------------
    # Payload parsing
    # ------------------------------------------------------------------

    def _parse_and_cache(
        self, conn_id: str, payload: Dict[str, Any]
    ) -> Connection:
        """
        Extract the access token and expiry from the Fabric API response and
        return a fully-formed ``Connection`` ready for the token auth handler.

        Expected payload structure::

            {
              "datasourceDetails": {
                "credentialDetails": {
                  "credentials": "{\"credentialData\":[{\"name\":\"AccessToken\",\"value\":\"eyJ...\"},{\"name\":\"Expires\",\"value\":\"2026-03-08T...\"}]}"
                }
              }
            }
        """
        try:
            credentials_json = (
                payload["datasourceDetails"]["credentialDetails"]["credentials"]
            )
            credential_data = json.loads(credentials_json)["credentialData"]
        except (KeyError, json.JSONDecodeError, TypeError) as exc:
            log.error(
                "Unexpected payload structure for conn_id '%s': %s", conn_id, exc
            )
            raise AirflowException(
                f"Cannot parse Fabric credential payload for '{conn_id}': {exc}"
            )

        access_token: Optional[str] = None
        expires_str: Optional[str] = None

        for item in credential_data:
            name = item.get("name")
            if name == "AccessToken":
                access_token = item.get("value")
            elif name == "Expires":
                expires_str = item.get("value")

        if not access_token:
            raise AirflowException(
                f"AccessToken not found in Fabric credential payload for '{conn_id}'."
            )
        if not expires_str:
            raise AirflowException(
                f"Expires not found in Fabric credential payload for '{conn_id}'."
            )

        # Parse expiry into epoch seconds
        expires_at = self._parse_expiry(expires_str)

        conn = Connection(
            conn_id=conn_id,
            conn_type="microsoft-fabric",
            extra=json.dumps(
                {
                    "auth_type": "token",
                    "accessToken": access_token,
                    "expiresAt": expires_at,
                    "expiryBufferSeconds": self._expiry_buffer_seconds,
                }
            ),
        )

        self._cache[conn_id] = (conn, expires_at)

        log.info(
            "Cached Fabric connection '%s' (expires %s).",
            conn_id,
            datetime.fromtimestamp(expires_at, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            ),
        )
        return conn

    @staticmethod
    def _parse_expiry(expires_str: str) -> float:
        """Parse an expiry string into a UTC epoch timestamp.

        Supports ISO-8601 format and the ``M/D/YYYY h:mm:ss AM/PM +HH:MM``
        format returned by some Fabric API responses.
        """
        try:
            dt = datetime.fromisoformat(expires_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            pass

        # Fallback: "3/9/2026 12:31:13 AM +00:00"
        try:
            dt = datetime.strptime(expires_str, "%m/%d/%Y %I:%M:%S %p %z")
            return dt.timestamp()
        except (ValueError, TypeError) as exc:
            raise AirflowException(
                f"Cannot parse expiry timestamp '{expires_str}': {exc}"
            )
