from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from airflow.providers.microsoft.fabric.utils.onelake import OneLakeReader

CONN_ID = "fabric_default"
WORKSPACE = "MyWorkspace"
LAKEHOUSE = "MyLakehouse"
FILE_PATH = "folder/data.csv"

READER_MODULE = "airflow.providers.microsoft.fabric.utils.onelake.reader"


# --------------------------------------------------------------------------- #
# Shared fixtures                                                               #
# --------------------------------------------------------------------------- #

@pytest.fixture
def mock_rest_conn(mocker):
    """Prevent MSFabricRestConnection from touching the Airflow metastore."""
    mock = mocker.patch(f"{READER_MODULE}.MSFabricRestConnection")
    mock.return_value.get_access_token.return_value = "test-token"
    return mock


@pytest.fixture
def mock_file_client(mocker, mock_rest_conn):
    """
    Return a mock DataLakeFileClient, wiring up the full SDK client chain.

    Patch is applied in the reader module's own namespace so that the import
    inside reader.py is intercepted correctly.
    """
    mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
    file_client = MagicMock()
    file_client.path_name = f"{LAKEHOUSE}.Lakehouse/Files/{FILE_PATH}"
    mock_service.return_value \
        .get_file_system_client.return_value \
        .get_file_client.return_value = file_client
    return file_client


@pytest.fixture
def reader(mock_file_client):
    return OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)


# --------------------------------------------------------------------------- #
# SDK client construction                                                       #
# --------------------------------------------------------------------------- #

class TestOneLakeReaderClientConstruction:
    def test_service_client_uses_correct_endpoint(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_service.return_value.get_file_system_client.return_value \
            .get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        _, kwargs = mock_service.call_args
        assert kwargs["account_url"] == "https://onelake.dfs.fabric.microsoft.com"

    def test_file_system_client_uses_workspace_name(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        mock_service.return_value.get_file_system_client.assert_called_once_with(WORKSPACE)

    def test_file_client_path_includes_lakehouse_suffix(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        path_arg = mock_fs.get_file_client.call_args[0][0]
        assert f"{LAKEHOUSE}.Lakehouse" in path_arg

    def test_file_client_path_includes_files_segment(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        path_arg = mock_fs.get_file_client.call_args[0][0]
        assert "Files/" in path_arg

    def test_file_client_path_strips_leading_slash(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, "/folder/file.csv")

        path_arg = mock_fs.get_file_client.call_args[0][0]
        assert "Files//folder" not in path_arg
        assert "Files/folder" in path_arg

    def test_custom_endpoint_is_forwarded_to_service_client(self, mocker, mock_rest_conn):
        custom = "https://custom.dfs.example.com"
        mock_service = mocker.patch(f"{READER_MODULE}.DataLakeServiceClient")
        mock_service.return_value.get_file_system_client.return_value \
            .get_file_client.return_value = MagicMock()

        OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH, endpoint=custom)

        _, kwargs = mock_service.call_args
        assert kwargs["account_url"] == custom


# --------------------------------------------------------------------------- #
# read()                                                                        #
# --------------------------------------------------------------------------- #

class TestOneLakeReaderRead:
    def test_read_returns_bytes_from_readall(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.readall.return_value = b"hello,world\n"

        result = reader.read()

        assert result == b"hello,world\n"

    def test_read_calls_download_file_once(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.readall.return_value = b""

        reader.read()

        mock_file_client.download_file.assert_called_once_with()

    def test_read_calls_readall_on_downloader(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.readall.return_value = b"data"

        reader.read()

        mock_file_client.download_file.return_value.readall.assert_called_once_with()

    def test_read_propagates_sdk_exception(self, reader, mock_file_client):
        from azure.core.exceptions import ResourceNotFoundError
        mock_file_client.download_file.side_effect = ResourceNotFoundError("not found")

        with pytest.raises(ResourceNotFoundError):
            reader.read()


# --------------------------------------------------------------------------- #
# iter_chunks()                                                                 #
# --------------------------------------------------------------------------- #

class TestOneLakeReaderIterChunks:
    def test_iter_chunks_yields_chunks_until_empty(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.read.side_effect = [
            b"chunk1", b"chunk2", b""
        ]

        chunks = list(reader.iter_chunks(chunk_size=6))

        assert chunks == [b"chunk1", b"chunk2"]

    def test_iter_chunks_calls_read_with_chunk_size(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.read.side_effect = [b"x" * 50, b""]

        list(reader.iter_chunks(chunk_size=50))

        calls = mock_file_client.download_file.return_value.read.call_args_list
        assert calls[0] == call(50)

    def test_iter_chunks_opens_single_download_connection(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.read.side_effect = [
            b"a", b"b", b""
        ]

        list(reader.iter_chunks())

        mock_file_client.download_file.assert_called_once_with()

    def test_iter_chunks_empty_file_yields_nothing(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.read.return_value = b""

        chunks = list(reader.iter_chunks())

        assert chunks == []

    def test_iter_chunks_single_chunk_file(self, reader, mock_file_client):
        mock_file_client.download_file.return_value.read.side_effect = [b"only", b""]

        chunks = list(reader.iter_chunks(chunk_size=100))

        assert chunks == [b"only"]


# --------------------------------------------------------------------------- #
# __iter__ (line-by-line)                                                       #
# --------------------------------------------------------------------------- #

class TestOneLakeReaderIter:
    def test_iter_yields_lines_from_single_chunk(self, reader, mocker):
        mocker.patch.object(reader, "iter_chunks", return_value=[b"line1\nline2\nline3"])

        lines = list(reader)

        assert lines == ["line1", "line2", "line3"]

    def test_iter_handles_line_spanning_chunk_boundary(self, reader, mocker):
        mocker.patch.object(reader, "iter_chunks", return_value=[b"lin", b"e1\nline2"])

        lines = list(reader)

        assert lines == ["line1", "line2"]

    def test_iter_handles_no_trailing_newline(self, reader, mocker):
        mocker.patch.object(reader, "iter_chunks", return_value=[b"line1\nline2"])

        lines = list(reader)

        assert lines == ["line1", "line2"]

    def test_iter_empty_file_yields_nothing(self, reader, mocker):
        mocker.patch.object(reader, "iter_chunks", return_value=[])

        lines = list(reader)

        assert lines == []

    def test_iter_decodes_utf8(self, reader, mocker):
        mocker.patch.object(reader, "iter_chunks", return_value=["héllo\n".encode()])

        lines = list(reader)

        assert lines == ["héllo"]


# --------------------------------------------------------------------------- #
# Context manager                                                               #
# --------------------------------------------------------------------------- #

class TestOneLakeReaderContextManager:
    def test_context_manager_returns_self(self, mock_file_client):
        r = OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)
        assert r.__enter__() is r

    def test_context_manager_exits_without_error(self, mock_file_client):
        with OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH):
            pass

    def test_context_manager_does_not_suppress_exception(self, mock_file_client):
        with pytest.raises(RuntimeError, match="boom"):
            with OneLakeReader(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH):
                raise RuntimeError("boom")
