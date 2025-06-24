import pytest
from unittest.mock import MagicMock, patch
from airflow.models import Connection
from airflow.utils.session import provide_session


@pytest.fixture
def create_mock_connection():
    """Fixture to create mock connections for testing."""
    connections = []
    
    def _create_connection(connection: Connection):
        connections.append(connection)
        return connection
    
    with patch('airflow.hooks.base.BaseHook.get_connection') as mock_get_connection:
        def get_connection_side_effect(conn_id):
            for conn in connections:
                if conn.conn_id == conn_id:
                    return conn
            raise Exception(f"Connection {conn_id} not found")
        
        mock_get_connection.side_effect = get_connection_side_effect
        yield _create_connection


@pytest.fixture
def mock_connection_factory():
    """Alternative connection factory for more complex scenarios."""
    connections = {}
    
    def _add_connection(conn_id: str, connection: Connection):
        connections[conn_id] = connection
        return connection
    
    with patch('airflow.hooks.base.BaseHook.get_connection') as mock_get_connection:
        def get_connection_side_effect(conn_id):
            if conn_id in connections:
                return connections[conn_id]
            raise Exception(f"Connection {conn_id} not found")
        
        mock_get_connection.side_effect = get_connection_side_effect
        yield _add_connection
