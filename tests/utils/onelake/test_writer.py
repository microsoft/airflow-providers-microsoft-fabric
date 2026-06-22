from __future__ import annotations

import pytest
from unittest.mock import MagicMock, call

from azure.core.exceptions import ResourceNotFoundError

from airflow.providers.microsoft.fabric.utils.onelake import (
    OneLakeError,
    OneLakeWriter,
    OneLakeWriterClosedError,
)

CONN_ID = "fabric_default"
WORKSPACE = "MyWorkspace"
LAKEHOUSE = "MyLakehouse"
FILE_PATH = "output/result.csv"
BUFFER_SIZE = 10  # small value for easy testing

WRITER_MODULE = "airflow.providers.microsoft.fabric.utils.onelake.writer"


# --------------------------------------------------------------------------- #
# Shared fixtures                                                               #
# --------------------------------------------------------------------------- #

@pytest.fixture
def mock_rest_conn(mocker):
    mock = mocker.patch(f"{WRITER_MODULE}.MSFabricRestConnection")
    mock.return_value.get_access_token.return_value = "test-token"
    return mock


@pytest.fixture
def mock_file_client(mocker, mock_rest_conn):
    mock_service = mocker.patch(f"{WRITER_MODULE}.DataLakeServiceClient")
    file_client = MagicMock()
    file_client.path_name = f"{LAKEHOUSE}.Lakehouse/Files/{FILE_PATH}"
    mock_service.return_value \
        .get_file_system_client.return_value \
        .get_file_client.return_value = file_client
    return file_client


@pytest.fixture
def writer(mock_file_client):
    return OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH, buffer_size=BUFFER_SIZE)


# --------------------------------------------------------------------------- #
# SDK client construction                                                       #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterClientConstruction:
    def test_service_client_uses_correct_endpoint(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{WRITER_MODULE}.DataLakeServiceClient")
        mock_service.return_value.get_file_system_client.return_value \
            .get_file_client.return_value = MagicMock()

        OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        _, kwargs = mock_service.call_args
        assert kwargs["account_url"] == "https://onelake.dfs.fabric.microsoft.com"

    def test_file_system_client_uses_workspace_name(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{WRITER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        mock_service.return_value.get_file_system_client.assert_called_once_with(WORKSPACE)

    def test_file_client_path_includes_lakehouse_suffix(self, mocker, mock_rest_conn):
        mock_service = mocker.patch(f"{WRITER_MODULE}.DataLakeServiceClient")
        mock_fs = MagicMock()
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        mock_fs.get_file_client.return_value = MagicMock()

        OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)

        path_arg = mock_fs.get_file_client.call_args[0][0]
        assert f"{LAKEHOUSE}.Lakehouse" in path_arg

    def test_overwrite_stored_as_attribute(self, mock_file_client):
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH, overwrite=False)
        assert w.overwrite is False

    def test_buffer_size_stored_as_attribute(self, mock_file_client):
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH, buffer_size=1024)
        assert w.buffer_size == 1024


# --------------------------------------------------------------------------- #
# Initialisation — overwrite=True (default)                                    #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterInitialiseOverwrite:
    def test_create_file_called_on_first_write(self, writer, mock_file_client):
        writer.write(b"hello")

        mock_file_client.create_file.assert_called_once_with()

    def test_create_file_called_only_once(self, writer, mock_file_client):
        writer.write(b"hello")
        writer.write(b"world")

        mock_file_client.create_file.assert_called_once()

    def test_position_starts_at_zero_after_create(self, writer, mock_file_client):
        writer.write(b"x")

        assert writer._position == 0 or writer._position == len(b"x") % BUFFER_SIZE

    def test_get_file_properties_not_called_for_overwrite(self, writer, mock_file_client):
        writer.write(b"data")

        mock_file_client.get_file_properties.assert_not_called()


# --------------------------------------------------------------------------- #
# Initialisation — overwrite=False (append mode)                               #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterInitialiseAppend:
    def test_appends_from_existing_file_eof(self, mock_file_client):
        mock_file_client.get_file_properties.return_value = {"size": 500}
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                          overwrite=False, buffer_size=BUFFER_SIZE)

        w.write(b"x")

        assert w._position >= 500  # started at 500, advanced by data sent

    def test_get_file_properties_called_when_not_overwrite(self, mock_file_client):
        mock_file_client.get_file_properties.return_value = {"size": 0}
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                          overwrite=False, buffer_size=BUFFER_SIZE)

        w.write(b"data")

        mock_file_client.get_file_properties.assert_called_once()

    def test_create_file_called_when_file_not_found_in_append_mode(self, mock_file_client):
        mock_file_client.get_file_properties.side_effect = ResourceNotFoundError("not found")
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                          overwrite=False, buffer_size=BUFFER_SIZE)

        w.write(b"hello")

        mock_file_client.create_file.assert_called_once_with()

    def test_position_zero_when_file_missing_in_append_mode(self, mock_file_client):
        mock_file_client.get_file_properties.side_effect = ResourceNotFoundError("not found")
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                          overwrite=False, buffer_size=BUFFER_SIZE)

        w.write(b"x" * 5)  # less than buffer_size, no flush yet

        assert w._position == 0


# --------------------------------------------------------------------------- #
# Buffer management                                                             #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterBufferManagement:
    def test_append_data_not_called_before_buffer_full(self, writer, mock_file_client):
        writer.write(b"hello")  # 5 bytes < buffer_size=10

        mock_file_client.append_data.assert_not_called()
        assert len(writer._buffer) == 5

    def test_append_data_called_when_buffer_reaches_buffer_size(self, writer, mock_file_client):
        writer.write(b"1234567890")  # exactly buffer_size=10

        mock_file_client.append_data.assert_called_once()

    def test_append_data_called_at_correct_offset(self, writer, mock_file_client):
        writer.write(b"1234567890")  # first flush at offset=0

        call_kwargs = mock_file_client.append_data.call_args[1]
        assert call_kwargs["offset"] == 0

    def test_position_advances_after_flush(self, writer, mock_file_client):
        writer.write(b"a" * 20)  # 2 full chunks of 10

        assert writer._position == 20

    def test_two_full_chunks_produce_two_append_calls(self, writer, mock_file_client):
        writer.write(b"a" * 20)

        assert mock_file_client.append_data.call_count == 2

    def test_second_append_uses_advanced_offset(self, writer, mock_file_client):
        writer.write(b"a" * 20)

        calls = mock_file_client.append_data.call_args_list
        assert calls[0][1]["offset"] == 0
        assert calls[1][1]["offset"] == 10

    def test_partial_buffer_not_sent_before_close(self, writer, mock_file_client):
        writer.write(b"abc")  # 3 bytes < buffer_size=10

        mock_file_client.append_data.assert_not_called()

    def test_string_data_encoded_utf8(self, writer, mock_file_client):
        # "héllo" is 6 bytes in UTF-8 + padding to exceed buffer_size=10
        writer.write("héllo12345")  # > 10 bytes when encoded

        mock_file_client.append_data.assert_called()
        sent_data = mock_file_client.append_data.call_args[1]["data"]
        assert isinstance(sent_data, bytes)


# --------------------------------------------------------------------------- #
# close()                                                                       #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterClose:
    def test_close_flushes_remaining_buffer(self, writer, mock_file_client):
        writer.write(b"abc")  # 3 bytes in buffer
        writer.close()

        mock_file_client.append_data.assert_called_once()

    def test_close_calls_flush_data(self, writer, mock_file_client):
        writer.write(b"abc")
        writer.close()

        mock_file_client.flush_data.assert_called_once()

    def test_close_flush_data_position_equals_total_bytes(self, writer, mock_file_client):
        # Write 13 bytes: 10 auto-flushed + 3 remaining on close
        writer.write(b"a" * 13)
        writer.close()

        mock_file_client.flush_data.assert_called_once_with(13)

    def test_close_is_idempotent(self, writer, mock_file_client):
        writer.write(b"data")
        writer.close()
        writer.close()

        # flush_data should only be called once across both close() calls
        mock_file_client.flush_data.assert_called_once()

    def test_close_without_write_makes_no_api_calls(self, writer, mock_file_client):
        writer.close()

        mock_file_client.create_file.assert_not_called()
        mock_file_client.append_data.assert_not_called()
        mock_file_client.flush_data.assert_not_called()

    def test_closed_flag_set_after_close(self, writer, mock_file_client):
        writer.write(b"data")
        writer.close()

        assert writer._closed is True

    def test_write_after_close_raises(self, writer, mock_file_client):
        writer.write(b"data")
        writer.close()

        with pytest.raises(OneLakeWriterClosedError):
            writer.write(b"more")


# --------------------------------------------------------------------------- #
# Context manager                                                               #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterContextManager:
    def test_context_manager_commits_on_clean_exit(self, mock_file_client):
        with OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                           buffer_size=BUFFER_SIZE) as w:
            w.write(b"data")

        assert w._closed is True
        mock_file_client.flush_data.assert_called_once()

    def test_context_manager_does_not_commit_on_exception(self, mock_file_client):
        with pytest.raises(ValueError):
            with OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                               buffer_size=100) as w:
                w.write(b"partial")
                raise ValueError("simulated error")

        mock_file_client.flush_data.assert_not_called()

    def test_context_manager_sets_closed_on_exception(self, mock_file_client):
        with pytest.raises(RuntimeError):
            with OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH,
                               buffer_size=100) as w:
                w.write(b"data")
                raise RuntimeError("fail")

        assert w._closed is True

    def test_context_manager_does_not_suppress_exception(self, mock_file_client):
        with pytest.raises(RuntimeError, match="boom"):
            with OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH) as w:
                raise RuntimeError("boom")

    def test_returns_self_on_enter(self, mock_file_client):
        w = OneLakeWriter(CONN_ID, WORKSPACE, LAKEHOUSE, FILE_PATH)
        assert w.__enter__() is w


# --------------------------------------------------------------------------- #
# Blob Service API compatibility error                                          #
# --------------------------------------------------------------------------- #

class TestOneLakeWriterBlobServiceAPIError:
    def test_raises_onelake_error_for_blob_api_conflict(self, writer, mock_file_client):
        mock_file_client.append_data.side_effect = Exception(
            "The resource was created or modified by the Azure Blob Service API"
        )

        with pytest.raises(OneLakeError, match="Azure Blob Service API"):
            writer.write(b"1234567890")  # exactly buffer_size — triggers immediate flush

    def test_other_exceptions_propagate_unchanged(self, writer, mock_file_client):
        mock_file_client.append_data.side_effect = Exception("some other error")

        with pytest.raises(Exception, match="some other error"):
            writer.write(b"1234567890")
