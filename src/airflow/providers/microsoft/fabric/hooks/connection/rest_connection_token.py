import logging
import time
from datetime import datetime, timezone
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection


class MSFabricRestConnectionToken:
    """
    Authentication handler for pre-minted access tokens supplied by a
    secrets backend (e.g. ``FabricSecretBackend``).

    The token and its expiry are read from ``Connection.extra_dejson``.
    When the token is close to expiry the handler re-fetches the Connection
    via ``BaseHook.get_connection`` which triggers the secrets backend to
    supply a fresh token.
    """

    auth_type = "token"

    def __init__(self, conn: Connection) -> None:
        self.log = logging.getLogger(__name__)
        self.connection_id = conn.conn_id

        self._access_token: str = ""
        self._expires_at: float = 0.0
        self._expiry_buffer: int = 300  # seconds

        self._load_token(conn)

    # ------------------------------------------------------------------
    # Public API (same contract as MSFabricRestConnectionSPN)
    # ------------------------------------------------------------------

    def get_access_token(self, scope: str) -> str:
        """
        Return the pre-minted access token.

        *scope* is accepted for interface compatibility but is not used
        because the token was already minted for a specific audience by the
        Fabric API.
        """
        if self._is_token_valid():
            self.log.debug(
                "Using cached pre-minted token for connection '%s' (expires %s).",
                self.connection_id,
                self._fmt_ts(self._expires_at),
            )
            return self._access_token

        # Token expired – re-fetch the Connection (triggers secrets backend)
        self.log.info(
            "Pre-minted token for connection '%s' expired or within buffer. "
            "Re-fetching connection from secrets backend.",
            self.connection_id,
        )
        conn = BaseHook.get_connection(self.connection_id)
        self._load_token(conn)

        if not self._is_token_valid():
            raise AirflowException(
                f"Re-fetched token for connection '{self.connection_id}' is already "
                "expired. Check the Fabric secret-store API."
            )

        return self._access_token

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_token(self, conn: Connection) -> None:
        extras = conn.extra_dejson
        token = extras.get("accessToken")
        expires_at = extras.get("expiresAt")

        if not token:
            raise AirflowException(
                f"Connection '{self.connection_id}' missing 'accessToken' in extras."
            )
        if expires_at is None:
            raise AirflowException(
                f"Connection '{self.connection_id}' missing 'expiresAt' in extras."
            )

        self._access_token = token
        self._expires_at = float(expires_at)

        self.log.debug(
            "Loaded pre-minted token for connection '%s' (expires %s).",
            self.connection_id,
            self._fmt_ts(self._expires_at),
        )

    #TODO: self._expiry_buffer should always be higher than the SecretBackend buffer, or we may not refresh and say it is still invalid. 
    def _is_token_valid(self) -> bool:
        return time.time() < (self._expires_at - self._expiry_buffer)

    @staticmethod
    def _fmt_ts(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
