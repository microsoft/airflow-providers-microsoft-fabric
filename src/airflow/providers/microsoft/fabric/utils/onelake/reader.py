from __future__ import annotations

import logging
from typing import Iterator

from azure.storage.filedatalake import DataLakeServiceClient

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.utils.onelake.constants import (
    DEFAULT_READ_CHUNK_SIZE,
    ONELAKE_ENDPOINT,
    ONELAKE_SCOPE,
)
from airflow.providers.microsoft.fabric.utils.onelake._credential import _FabricTokenCredential


class OneLakeReader:
    """
    Synchronous utility for reading files from Microsoft Fabric OneLake
    via the Azure Data Lake Storage Gen2 SDK.

    Not an Airflow operator or hook subclass; safe to use inside any Python
    callable including PythonOperator tasks.

    Example — full read::

        with OneLakeReader("fabric_default", "MyWorkspace", "MyLakehouse",
                           "folder/file.csv") as reader:
            data = reader.read()

    Example — streaming by chunks::

        with OneLakeReader("fabric_default", "MyWorkspace", "MyLakehouse",
                           "folder/file.csv") as reader:
            for chunk in reader.iter_chunks():
                process(chunk)

    Example — line-by-line::

        with OneLakeReader("fabric_default", "MyWorkspace", "MyLakehouse",
                           "folder/file.csv") as reader:
            for line in reader:
                print(line)

    :param fabric_conn_id: Airflow connection ID of type ``microsoft-fabric``.
    :param workspace_name: OneLake workspace name.
    :param lakehouse_name: Lakehouse name (the ``.Lakehouse`` suffix is added automatically).
    :param file_path: Path to the file relative to the ``Files/`` root of the lakehouse.
    :param scope: OAuth2 scope for token acquisition. Defaults to the OneLake storage scope.
    :param endpoint: OneLake DFS endpoint. Defaults to the public endpoint.
    """

    def __init__(
        self,
        fabric_conn_id: str,
        workspace_name: str,
        lakehouse_name: str,
        file_path: str,
        scope: str = ONELAKE_SCOPE,
        endpoint: str = ONELAKE_ENDPOINT,
    ) -> None:
        self.log = logging.getLogger(__name__)
        conn = MSFabricRestConnection(conn_id=fabric_conn_id)
        credential = _FabricTokenCredential(conn, scope)
        service_client = DataLakeServiceClient(account_url=endpoint, credential=credential)
        fs_client = service_client.get_file_system_client(workspace_name)
        self._file_client = fs_client.get_file_client(
            f"{lakehouse_name}.Lakehouse/Files/{file_path.lstrip('/')}"
        )

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def read(self) -> bytes:
        """
        Read the entire file into memory and return it as bytes.

        :return: Complete file content.
        :raises azure.core.exceptions.ResourceNotFoundError: If the file does not exist.
        """
        self.log.debug("OneLakeReader.read: downloading %s", self._file_client.path_name)
        return self._file_client.download_file().readall()

    def iter_chunks(self, chunk_size: int = DEFAULT_READ_CHUNK_SIZE) -> Iterator[bytes]:
        """
        Stream the file content as successive byte chunks.

        Opens one streaming HTTP connection and reads *chunk_size* bytes at a
        time.  The SDK handles HTTP-level retries transparently.

        :param chunk_size: Bytes per read call. Defaults to 4 MiB.
        :yields: Successive chunks of file content.
        :raises azure.core.exceptions.ResourceNotFoundError: If the file does not exist.
        """
        self.log.debug(
            "OneLakeReader.iter_chunks: streaming %s chunk_size=%d",
            self._file_client.path_name,
            chunk_size,
        )
        downloader = self._file_client.download_file()
        while True:
            chunk = downloader.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over the file line-by-line, decoded as UTF-8.

        Chunks are fetched lazily via :meth:`iter_chunks`; lines that span
        chunk boundaries are assembled transparently via a carry-over buffer.

        :yields: Decoded text lines (without trailing newline).
        """
        carry = b""
        for chunk in self.iter_chunks():
            buf = carry + chunk
            lines = buf.split(b"\n")
            for line in lines[:-1]:
                yield line.decode("utf-8")
            carry = lines[-1]
        if carry:
            yield carry.decode("utf-8")

    # ------------------------------------------------------------------ #
    # Context manager                                                       #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "OneLakeReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None
