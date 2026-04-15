import json
import pytest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend

from airflow.providers.microsoft.fabric.secrets.fabric_chained_secret_backend import (
    FabricChainedSecretBackend,
    _import_backend_class,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class StubBackend(BaseSecretsBackend):
    """Minimal backend for testing the chain logic."""

    def __init__(self, connections=None, variables=None, configs=None, **kwargs):
        super().__init__()
        self._connections = connections or {}
        self._variables = variables or {}
        self._configs = configs or {}

    def get_connection(self, conn_id):
        return self._connections.get(conn_id)

    def get_conn_value(self, conn_id):
        conn = self._connections.get(conn_id)
        if conn is not None:
            return conn.get_uri()
        return None

    def get_variable(self, key):
        return self._variables.get(key)

    def get_config(self, key):
        return self._configs.get(key)


class ExplodingBackend(BaseSecretsBackend):
    """Backend that raises on every call."""

    def __init__(self, exc=None, **kwargs):
        super().__init__()
        self._exc = exc or RuntimeError("boom")

    def get_connection(self, conn_id):
        raise self._exc

    def get_conn_value(self, conn_id):
        raise self._exc

    def get_variable(self, key):
        raise self._exc

    def get_config(self, key):
        raise self._exc


STUB_BACKEND_PATH = f"{StubBackend.__module__}.{StubBackend.__qualname__}"


def _make_chained(monkeypatch, user_backend_env=None, user_backend_kwargs_env=None,
                  fabric_backend_env=None, fabric_backend_kwargs_env=None):
    """Build a FabricChainedSecretBackend with the Fabric backend mocked out."""
    if user_backend_env is not None:
        monkeypatch.setenv("AIRFLOW_SECRETS_USER_BACKEND", user_backend_env)
    if user_backend_kwargs_env is not None:
        monkeypatch.setenv("AIRFLOW_SECRETS_USER_BACKEND_KWARGS", user_backend_kwargs_env)
    if fabric_backend_env is not None:
        monkeypatch.setenv("AIRFLOW_SECRETS_FABRIC_BACKEND", fabric_backend_env)
    if fabric_backend_kwargs_env is not None:
        monkeypatch.setenv("AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS", fabric_backend_kwargs_env)

    # Mock FabricSecretBackend so we don't need real API credentials
    mock_fabric = MagicMock(spec=BaseSecretsBackend)
    mock_fabric.get_connection.return_value = None
    mock_fabric.get_conn_value.return_value = None
    mock_fabric.get_variable.return_value = None
    mock_fabric.get_config.return_value = None
    type(mock_fabric).__name__ = "FabricSecretBackend"

    with patch(
        "airflow.providers.microsoft.fabric.secrets.fabric_chained_secret_backend."
        "FabricChainedSecretBackend._init_fabric_backend"
    ) as patched_init:
        # Simulate successful fabric backend init
        patched_init.side_effect = lambda: None

        backend = FabricChainedSecretBackend()
        # Append the mocked fabric backend (simulates what _init_fabric_backend does)
        backend._backends.append(mock_fabric)
        return backend, mock_fabric


# ---------------------------------------------------------------------------
# _import_backend_class
# ---------------------------------------------------------------------------

class TestImportBackendClass:

    def test_valid_class_path(self):
        cls = _import_backend_class(
            "airflow.secrets.BaseSecretsBackend"
        )
        assert cls is BaseSecretsBackend

    def test_invalid_no_module(self):
        with pytest.raises(ImportError, match="expected 'module.ClassName'"):
            _import_backend_class("NoModule")

    def test_nonexistent_module(self):
        with pytest.raises(ModuleNotFoundError):
            _import_backend_class("nonexistent.module.SomeClass")

    def test_nonexistent_class(self):
        with pytest.raises(AttributeError):
            _import_backend_class("airflow.secrets.TotallyFakeClass")


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:

    def test_fabric_backend_env_required(self, monkeypatch):
        """Raises ValueError when AIRFLOW_SECRETS_FABRIC_BACKEND is not set."""
        monkeypatch.delenv("AIRFLOW_SECRETS_FABRIC_BACKEND", raising=False)
        monkeypatch.delenv("AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS", raising=False)
        with pytest.raises(ValueError, match="AIRFLOW_SECRETS_FABRIC_BACKEND"):
            FabricChainedSecretBackend()

    def test_chain_with_no_user_backend(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        # Only the mocked fabric backend should be in the chain
        assert len(backend._backends) == 1
        assert backend._backends[0] is mock_fabric

    def test_chain_with_user_backend_via_env(self, monkeypatch):
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
            user_backend_kwargs_env="{}",
        )
        assert len(backend._backends) == 2
        # User backend is registered first
        assert isinstance(backend._backends[0], StubBackend)
        assert backend._backends[1] is mock_fabric

    def test_user_backend_registered_before_fabric(self, monkeypatch):
        """User backend must be first in the chain."""
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        assert isinstance(backend._backends[0], StubBackend)
        assert backend._backends[1] is mock_fabric

    def test_user_backend_init_failure_is_skipped(self, monkeypatch):
        """A broken user backend is skipped, chain still has fabric."""
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env="nonexistent.module.DoesNotExist",
        )
        assert len(backend._backends) == 1
        assert backend._backends[0] is mock_fabric

    def test_malformed_user_backend_kwargs_env(self, monkeypatch):
        """Invalid JSON in env var is ignored, user backend gets no kwargs."""
        backend, _ = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
            user_backend_kwargs_env="not-json!!!",
        )
        assert len(backend._backends) == 2
        assert isinstance(backend._backends[0], StubBackend)

    def test_user_backend_kwargs_env_forwarded(self, monkeypatch):
        """Kwargs from env var are passed to the user backend."""
        backend, _ = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
            user_backend_kwargs_env=json.dumps({
                "variables": {"my_var": "my_value"},
            }),
        )
        stub = backend._backends[0]
        assert stub.get_variable("my_var") == "my_value"


# ---------------------------------------------------------------------------
# get_connection
# ---------------------------------------------------------------------------

class TestGetConnection:

    def test_user_backend_resolves_first(self, monkeypatch):
        """User backend is first in chain — resolves before fabric."""
        expected = Connection(conn_id="my-conn", conn_type="http")
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
            user_backend_kwargs_env=json.dumps({
                "connections": {"my-conn": {"conn_id": "my-conn", "conn_type": "http"}},
            }),
        )
        # Replace the StubBackend's connections with real Connection objects
        backend._backends[0]._connections = {"my-conn": expected}

        result = backend.get_connection("my-conn")
        assert result is expected
        # Fabric backend should NOT have been called
        mock_fabric.get_connection.assert_not_called()

    def test_falls_through_to_fabric_backend(self, monkeypatch):
        """User backend returns None → fabric backend is tried."""
        expected = Connection(conn_id="guid-1", conn_type="microsoft-fabric")
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        mock_fabric.get_connection.return_value = expected

        result = backend.get_connection("guid-1")
        assert result is expected

    def test_returns_none_when_no_backend_has_it(self, monkeypatch):
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        mock_fabric.get_connection.return_value = None

        result = backend.get_connection("unknown")
        assert result is None

    def test_exception_propagates(self, monkeypatch):
        """Exceptions are not swallowed."""
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_connection.side_effect = AirflowException("API down")

        with pytest.raises(AirflowException, match="API down"):
            backend.get_connection("some-guid")

    def test_runtime_error_propagates(self, monkeypatch):
        """Even non-AirflowException errors propagate (no silent swallow)."""
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_connection.side_effect = RuntimeError("unexpected")

        with pytest.raises(RuntimeError, match="unexpected"):
            backend.get_connection("some-id")


# ---------------------------------------------------------------------------
# get_conn_value
# ---------------------------------------------------------------------------

class TestGetConnValue:

    def test_returns_first_non_none(self, monkeypatch):
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        backend._backends[0]._connections = {
            "c1": Connection(conn_id="c1", conn_type="http"),
        }
        mock_fabric.get_conn_value.return_value = None

        result = backend.get_conn_value("c1")
        assert result is not None

    def test_returns_none_when_empty(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_conn_value.return_value = None
        assert backend.get_conn_value("missing") is None

    def test_not_implemented_skipped(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_conn_value.side_effect = NotImplementedError
        assert backend.get_conn_value("x") is None


# ---------------------------------------------------------------------------
# get_variable
# ---------------------------------------------------------------------------

class TestGetVariable:

    def test_user_backend_resolves_variable(self, monkeypatch):
        """Variables from user backend are found first."""
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        backend._backends[0]._variables = {"my_var": "my_value"}
        mock_fabric.get_variable.return_value = None

        assert backend.get_variable("my_var") == "my_value"
        mock_fabric.get_variable.assert_not_called()

    def test_returns_none_when_not_found(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_variable.return_value = None
        assert backend.get_variable("missing") is None

    def test_not_implemented_skipped(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_variable.side_effect = NotImplementedError
        assert backend.get_variable("x") is None


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------

class TestGetConfig:

    def test_user_backend_resolves_config(self, monkeypatch):
        backend, mock_fabric = _make_chained(
            monkeypatch,
            user_backend_env=STUB_BACKEND_PATH,
        )
        backend._backends[0]._configs = {"my_cfg": "cfg_value"}
        mock_fabric.get_config.return_value = None

        assert backend.get_config("my_cfg") == "cfg_value"
        mock_fabric.get_config.assert_not_called()

    def test_returns_none_when_not_found(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_config.return_value = None
        assert backend.get_config("missing") is None

    def test_not_implemented_skipped(self, monkeypatch):
        backend, mock_fabric = _make_chained(monkeypatch)
        mock_fabric.get_config.side_effect = NotImplementedError
        assert backend.get_config("x") is None
