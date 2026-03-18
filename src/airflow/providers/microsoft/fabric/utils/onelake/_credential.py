from __future__ import annotations

import time

from azure.core.credentials import AccessToken


class _FabricTokenCredential:
    """
    Bridges :class:`MSFabricRestConnection` to the Azure SDK ``TokenCredential``
    protocol so that ``DataLakeServiceClient`` can authenticate using the
    existing Airflow connection infrastructure.

    The Azure SDK calls ``get_token()`` when it needs a bearer token.
    Token caching and renewal are delegated entirely to the underlying
    ``MSFabricRestConnection`` (which already manages a per-scope cache with
    a 5-minute expiry buffer).  We tell the SDK each token expires in one
    hour so it does not call back too aggressively, while the connection class
    silently renews before the actual Azure AD token expires.
    """

    def __init__(self, conn, scope: str) -> None:
        """
        :param conn: An object that exposes ``get_access_token(scope: str) -> str``.
            Typically :class:`MSFabricRestConnection`.
        :param scope: The OAuth2 scope to request (e.g. ``https://storage.azure.com/.default``).
        """
        self._conn = conn
        self._scope = scope

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        token = self._conn.get_access_token(self._scope)
        return AccessToken(token, int(time.time()) + 3600)
