"""
Chained Secrets Backend for the Microsoft Fabric Airflow Provider.

The problem
-----------
When the Fabric provider is installed it needs its own secret backend
(``FabricSecretBackend``) to resolve Fabric connection GUIDs.  However, the
customer may *already* have a custom backend configured via the standard
Airflow environment variables::

    AIRFLOW__SECRETS__BACKEND
    AIRFLOW__SECRETS__BACKEND_KWARGS

Overwriting those variables would break the customer's setup.

The solution
------------
``FabricChainedSecretBackend`` wraps **two** backends in order:

1. **Fabric backend** -- runs first because it has a fast GUID-based filter;
   non-GUID connection IDs are skipped immediately (returns ``None``).
2. **User backend** -- the backend the customer originally configured.  Its
   class path and kwargs are read from a pair of dedicated environment
   variables so they do not collide with the Airflow-level settings:

   * ``AIRFLOW_SECRETS_USER_BACKEND`` -- fully qualified class name
   * ``AIRFLOW_SECRETS_USER_BACKEND_KWARGS`` -- JSON string of kwargs

If the user backend is not configured the chain simply contains only the
Fabric backend.

After both backends are tried Airflow's built-in *metadata DB* lookup runs
automatically (it is always appended by the framework), so there is no need
to handle it here.

Activation
----------
There are two ways to register the chain and preserve the customer's backend:

**Option A -- Single env var (everything in BACKEND_KWARGS)**

Only use the chained backend when the customer has their own backend::

    AIRFLOW__SECRETS__BACKEND=airflow...FabricChainedSecretBackend
    AIRFLOW__SECRETS__BACKEND_KWARGS={
        "api_base_url": "https://...",
        "api_scope": "https://...",
        "user_backend": "airflow.providers.hashicorp.secrets.vault.VaultBackend",
        "user_backend_kwargs": {"url": "https://vault.example.com"}
    }

``user_backend`` and ``user_backend_kwargs`` are extracted by the chained
backend; everything else is forwarded to ``FabricSecretBackend``.

**Option B -- Entrypoint swap (for platform-managed environments)**

The container entrypoint conditionally picks the right backend class
before Airflow starts.  If the customer defined their own backend we
install the chained wrapper; otherwise we use ``FabricSecretBackend``
directly (no chain overhead)::

    #!/bin/bash
    # entrypoint.sh -- runs before "airflow webserver" / "airflow scheduler"

    FABRIC_BACKEND_KWARGS='{"api_base_url":"...","api_scope":"..."}'

    if [ -n "$AIRFLOW__SECRETS__BACKEND" ]; then
        # Customer has a backend -- chain it behind Fabric
        export AIRFLOW_SECRETS_USER_BACKEND="$AIRFLOW__SECRETS__BACKEND"
        [ -n "$AIRFLOW__SECRETS__BACKEND_KWARGS" ] && \
            export AIRFLOW_SECRETS_USER_BACKEND_KWARGS="$AIRFLOW__SECRETS__BACKEND_KWARGS"
        export AIRFLOW__SECRETS__BACKEND="airflow.providers.microsoft.fabric.secrets.fabric_chained_secret_backend.FabricChainedSecretBackend"
    else
        # No customer backend -- use Fabric backend directly
        export AIRFLOW__SECRETS__BACKEND="airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend"
    fi

    export AIRFLOW__SECRETS__BACKEND_KWARGS="$FABRIC_BACKEND_KWARGS"
    exec "$@"

The chained backend reads the user backend from these env vars::

    AIRFLOW_SECRETS_USER_BACKEND
    AIRFLOW_SECRETS_USER_BACKEND_KWARGS

Constructor kwargs (Option A) take precedence over env vars (Option B).
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

# Environment variables the customer can set to declare their own backend.
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
    Secrets backend that chains the Fabric backend with an optional
    user-defined backend.

    Lookup order:

    1. ``FabricSecretBackend`` (fast GUID filter)
    2. User-defined backend (if configured)
    3. Airflow metastore (handled automatically by the framework)

    The user backend can be specified in two ways (checked in this order):

    **Via constructor kwargs** (passed through ``AIRFLOW__SECRETS__BACKEND_KWARGS``)::

        {
            "api_base_url": "...",
            "api_scope": "...",
            "user_backend": "some.module.TheirBackend",
            "user_backend_kwargs": {"url": "https://vault.example.com"}
        }

    **Via environment variables**::

        AIRFLOW_SECRETS_USER_BACKEND=some.module.TheirBackend
        AIRFLOW_SECRETS_USER_BACKEND_KWARGS={"url": "https://vault.example.com"}

    All remaining constructor kwargs (after extracting ``user_backend`` and
    ``user_backend_kwargs``) are forwarded to ``FabricSecretBackend``.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self._backends: List[BaseSecretsBackend] = []

        # Pop user-backend config before forwarding the rest to Fabric
        user_backend_class = kwargs.pop("user_backend", None)
        user_backend_kwargs = kwargs.pop("user_backend_kwargs", None)

        self._init_fabric_backend(kwargs)
        self._init_user_backend(user_backend_class, user_backend_kwargs)

        names = [type(b).__name__ for b in self._backends]
        log.info("FabricChainedSecretBackend initialised with chain: %s", names)

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_fabric_backend(self, kwargs: Dict[str, Any]) -> None:
        """Create and append the Fabric backend."""
        from airflow.providers.microsoft.fabric.secrets.fabric_secret_backend import (
            FabricSecretBackend,
        )

        try:
            backend = FabricSecretBackend(**kwargs)
            self._backends.append(backend)
            log.info("Fabric secret backend loaded successfully.")
        except Exception:
            log.exception(
                "Failed to initialise FabricSecretBackend. "
                "Fabric GUID connections will not be resolved."
            )

    def _init_user_backend(
        self,
        user_backend_class: Optional[str],
        user_backend_kwargs: Optional[Dict[str, Any]],
    ) -> None:
        """Load the customer's original backend.

        Resolution order for the class path:
        1. ``user_backend`` constructor kwarg
        2. ``AIRFLOW_SECRETS_USER_BACKEND`` env var
        """
        user_class_path = (
            (user_backend_class or "").strip()
            or os.environ.get(_USER_BACKEND_ENV, "").strip()
        )
        if not user_class_path:
            log.info(
                "No user backend configured (neither 'user_backend' kwarg "
                "nor %s env var set). "
                "Chain contains only the Fabric backend.",
                _USER_BACKEND_ENV,
            )
            return

        # Resolve kwargs: constructor kwarg takes precedence over env var
        user_kwargs: Dict[str, Any] = user_backend_kwargs or {}
        if not user_kwargs:
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
            try:
                conn = backend.get_connection(conn_id)
                if conn is not None:
                    log.debug(
                        "Connection '%s' resolved by %s.",
                        conn_id,
                        type(backend).__name__,
                    )
                    return conn
            except Exception:
                log.exception(
                    "Error in %s while resolving '%s'; trying next backend.",
                    type(backend).__name__,
                    conn_id,
                )
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
