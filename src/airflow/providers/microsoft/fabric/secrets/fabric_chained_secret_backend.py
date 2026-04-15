"""
Chained Secrets Backend for the Microsoft Fabric Airflow Provider.

Chains an optional user-defined backend with the Fabric secrets backend.
All configuration is read from environment variables.

Lookup order
------------

1. User-defined secondary backend (if configured)
2. ``FabricSecretBackend`` (GUID filter)
3. Airflow metastore (appended automatically by the framework)

Environment variables
---------------------

+-------------------------------------------+----------------------------------------------+
| Variable                                  | Description                                  |
+===========================================+==============================================+
| ``AIRFLOW_SECRETS_FABRIC_BACKEND``        | Fully qualified class name of the Fabric     |
|                                           | backend.                                     |
+-------------------------------------------+----------------------------------------------+
| ``AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS`` | JSON string of kwargs forwarded to the       |
|                                           | Fabric backend constructor (e.g.             |
|                                           | ``api_base_url``, ``api_scope``).            |
+-------------------------------------------+----------------------------------------------+
| ``AIRFLOW_SECRETS_USER_BACKEND``          | Fully qualified class name of the user's     |
|                                           | secrets backend (e.g. HashiCorp Vault).       |
+-------------------------------------------+----------------------------------------------+
| ``AIRFLOW_SECRETS_USER_BACKEND_KWARGS``   | JSON string of kwargs forwarded to the       |
|                                           | user backend constructor.                    |
+-------------------------------------------+----------------------------------------------+

Setup
-----
Set the chained backend as the Airflow secrets backend and configure both
backends via their dedicated environment variables::

    AIRFLOW__SECRETS__BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_chained_secret_backend.FabricChainedSecretBackend

    AIRFLOW_SECRETS_FABRIC_BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend
    AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS={"api_base_url": "https://<fabric-api-host>", "api_scope": "<api-scope>"}

    AIRFLOW_SECRETS_USER_BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
    AIRFLOW_SECRETS_USER_BACKEND_KWARGS={"url": "https://vault.example.com"}

If no user backend is configured the chain contains only the Fabric backend.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend

log = logging.getLogger(__name__)

# Environment variables for the Fabric backend.
_FABRIC_BACKEND_ENV = "AIRFLOW_SECRETS_FABRIC_BACKEND"
_FABRIC_BACKEND_KWARGS_ENV = "AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS"

# Environment variables for the customer's own backend.
_USER_BACKEND_ENV = "AIRFLOW_SECRETS_USER_BACKEND"
_USER_BACKEND_KWARGS_ENV = "AIRFLOW_SECRETS_USER_BACKEND_KWARGS"


def _import_backend_class(class_path: str) -> type:
    """Import a secrets-backend class from its fully-qualified dotted path."""
    module_path, _, class_name = class_path.rpartition(".")
    if not module_path:
        raise ImportError(
            f"Invalid backend class path '{class_path}': "
            "expected 'module.ClassName' format."
        )
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class FabricChainedSecretBackend(BaseSecretsBackend):
    """
    THIS IS NOT INTENDED FOR USE OUTSIDE OF FABRIC AIRFLOW JOBS.

    Airflow secrets backend that chains an optional user-defined backend
    with the Fabric backend.  All configuration is read from environment
    variables.

    Lookup order:

    1. User-defined backend (if configured via ``AIRFLOW_SECRETS_USER_BACKEND``)
    2. Fabric backend (configured via ``AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS``)
    3. Airflow metastore (handled automatically by the framework)

    Environment variables
    ---------------------
    ``AIRFLOW_SECRETS_USER_BACKEND``
        Fully qualified class name of the user's secrets backend.

    ``AIRFLOW_SECRETS_USER_BACKEND_KWARGS``
        JSON string of kwargs forwarded to the user backend constructor.

    ``AIRFLOW_SECRETS_FABRIC_BACKEND``
        Fully qualified class name of the Fabric backend.

    ``AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS``
        JSON string of kwargs forwarded to the Fabric backend constructor
        (e.g. ``api_base_url``, ``api_scope``).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self._backends: List[BaseSecretsBackend] = []

        # User backend is registered first so it gets priority.
        self._init_user_backend()
        self._init_fabric_backend()

        names = [type(b).__name__ for b in self._backends]
        log.info("FabricChainedSecretBackend initialised with chain: %s", names)

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_fabric_backend(self) -> None:
        """Create and append the Fabric backend from env vars."""
        fabric_class_path = os.environ.get(_FABRIC_BACKEND_ENV, "").strip()
        if not fabric_class_path:
            raise ValueError(
                f"FabricChainedSecretBackend requires the "
                f"{_FABRIC_BACKEND_ENV} environment variable to be set."
            )

        fabric_kwargs: Dict[str, Any] = {}
        raw_kwargs = os.environ.get(_FABRIC_BACKEND_KWARGS_ENV, "").strip()
        if raw_kwargs:
            try:
                fabric_kwargs = json.loads(raw_kwargs)
            except json.JSONDecodeError:
                log.exception(
                    "Could not parse %s as JSON -- using no kwargs.",
                    _FABRIC_BACKEND_KWARGS_ENV,
                )

        cls = _import_backend_class(fabric_class_path)
        backend = cls(**fabric_kwargs)
        self._backends.append(backend)
        log.info("Fabric secret backend '%s' loaded successfully.", fabric_class_path)

    def _init_user_backend(self) -> None:
        """Load the customer's backend from env vars."""
        user_class_path = os.environ.get(_USER_BACKEND_ENV, "").strip()
        if not user_class_path:
            log.info(
                "No user backend configured (%s not set). "
                "Chain contains only the Fabric backend.",
                _USER_BACKEND_ENV,
            )
            return

        user_kwargs: Dict[str, Any] = {}
        raw_kwargs = os.environ.get(_USER_BACKEND_KWARGS_ENV, "").strip()
        if raw_kwargs:
            try:
                user_kwargs = json.loads(raw_kwargs)
            except json.JSONDecodeError:
                log.exception(
                    "Could not parse %s as JSON -- ignoring kwargs.",
                    _USER_BACKEND_KWARGS_ENV,
                )

        try:
            cls = _import_backend_class(user_class_path)
            backend = cls(**user_kwargs)
            self._backends.append(backend)
            log.info("User secret backend '%s' loaded successfully.", user_class_path)
        except Exception:
            log.exception(
                "Failed to initialise user secret backend '%s'. "
                "It will be skipped in the chain.",
                user_class_path,
            )

    # ------------------------------------------------------------------
    # BaseSecretsBackend contract
    # ------------------------------------------------------------------

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """Walk the chain; return the first non-None Connection."""
        for backend in self._backends:
            conn = backend.get_connection(conn_id)
            if conn is not None:
                log.debug(
                    "Connection '%s' resolved by %s.",
                    conn_id,
                    type(backend).__name__,
                )
                return conn
        return None

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Walk the chain for raw connection values (URI / JSON)."""
        for backend in self._backends:
            try:
                value = backend.get_conn_value(conn_id)
                if value is not None:
                    return value
            except NotImplementedError:
                continue
            except Exception:
                log.exception(
                    "Error in %s.get_conn_value for '%s'; trying next.",
                    type(backend).__name__,
                    conn_id,
                )
        return None

    def get_variable(self, key: str) -> Optional[str]:
        """Walk the chain for Airflow Variables."""
        for backend in self._backends:
            try:
                value = backend.get_variable(key)
                if value is not None:
                    return value
            except NotImplementedError:
                continue
            except Exception:
                log.exception(
                    "Error in %s.get_variable for '%s'; trying next.",
                    type(backend).__name__,
                    key,
                )
        return None

    def get_config(self, key: str) -> Optional[str]:
        """Walk the chain for Airflow config values."""
        for backend in self._backends:
            try:
                value = backend.get_config(key)
                if value is not None:
                    return value
            except NotImplementedError:
                continue
            except Exception:
                log.exception(
                    "Error in %s.get_config for '%s'; trying next.",
                    type(backend).__name__,
                    key,
                )
        return None
