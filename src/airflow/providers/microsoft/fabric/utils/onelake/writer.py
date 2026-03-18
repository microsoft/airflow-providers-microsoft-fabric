from __future__ import annotations

import logging
from typing import Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import MSFabricRestConnection
from airflow.providers.microsoft.fabric.utils.onelake.constants import (
    DEFAULT_WRITE_BUFFER_SIZE,
    ONELAKE_ENDPOINT,
    ONELAKE_SCOPE,
)
from airflow.providers.microsoft.fabric.utils.onelake.exceptions import (
    OneLakeError,
    OneLakeWriterClosedError,
)
from airflow.providers.microsoft.fabric.utils.onelake._credential import _FabricTokenCredential


class OneLakeWriter:
    """
    Synchronous utility for writing files to Microsoft Fabric OneLake
    via the Azure Data Lake Storage Gen2 SDK.

    Data is buffered in memory and sent to the API in *buffer_size* chunks via
    ``append_data`` calls.  The file is not visible/readable until :meth:`close`
    issues the final ``flush_data`` commit. If an exception propagates out of a
    ``with`` block the flush is skipped; ADLS Gen2 automatically garbage-collects
    uncommitted append sessions.

    **Overwrite vs. append mode**

    * ``overwrite=True`` (default) — always creates a fresh file. Any existing
      content at the same path is replaced.
    * ``overwrite=False`` — if the file already exists, new data is appended
      starting from the current end of the file. If the file does not exist it
      is created automatically.

    Example — overwrite (default)::

        with OneLakeWriter("fabric_default", "MyWorkspace", "MyLakehouse",
                           "output/result.csv") as writer:
            for row in rows:
                writer.write(row + "\\n")

    Example — append to existing file::

        with OneLakeWriter("fabric_default", "MyWorkspace", "MyLakehouse",
                           "output/result.csv", overwrite=False) as writer:
            writer.write(new_rows)

    :param fabric_conn_id: Airflow connection ID of type ``microsoft-fabric``.
    :param workspace_name: OneLake workspace name.
    :param lakehouse_name: Lakehouse name (the ``.Lakehouse`` suffix is added automatically).
    :param file_path: Path to the file relative to the ``Files/`` root of the lakehouse.
    :param buffer_size: Internal write buffer in bytes. An ``append_data`` call is issued
        each time the buffer fills to this size. Defaults to 4 MiB.
    :param overwrite: ``True`` replaces any existing file; ``False`` appends to it
        (or creates it if absent). Defaults to ``True``.
    :param scope: OAuth2 scope for token acquisition. Defaults to the OneLake storage scope.
    :param endpoint: OneLake DFS endpoint. Defaults to the public endpoint.
    """

    def __init__(
        self,
        fabric_conn_id: str,
        workspace_name: str,
        lakehouse_name: str,
        file_path: str,
        buffer_size: int = DEFAULT_WRITE_BUFFER_SIZE,
        overwrite: bool = True,
        scope: str = ONELAKE_SCOPE,
        endpoint: str = ONELAKE_ENDPOINT,
    ) -> None:
        self.log = logging.getLogger(__name__)
        self.buffer_size = buffer_size
        self.overwrite = overwrite

        conn = MSFabricRestConnection(conn_id=fabric_conn_id)
        credential = _FabricTokenCredential(conn, scope)
        service_client = DataLakeServiceClient(account_url=endpoint, credential=credential)
        fs_client = service_client.get_file_system_client(workspace_name)
        self._file_client = fs_client.get_file_client(
            f"{lakehouse_name}.Lakehouse/Files/{file_path.lstrip('/')}"
        )

        # Internal buffer state
        self._buffer: bytearray = bytearray()
        self._position: int = 0       # bytes acknowledged by the server so far
        self._initialised: bool = False
        self._closed: bool = False

    # ------------------------------------------------------------------ #
    # Initialisation (lazy — happens on first write)                       #
    # ------------------------------------------------------------------ #

    def _initialise(self) -> None:
        """
        Prepare the remote file for writing.

        * ``overwrite=True``: Create (or truncate) the file unconditionally.
        * ``overwrite=False``: If the file exists, seek to its current end so
          that new data is appended.  If the file does not exist, create it.

        Called once, on the first :meth:`write` call.
        """
        if self.overwrite:
            self._file_client.create_file()
            self._position = 0
            self.log.debug(
                "OneLakeWriter._initialise: created/truncated %s",
                self._file_client.path_name,
            )
        else:
            try:
                props = self._file_client.get_file_properties()
                self._position = props["size"]
                self.log.debug(
                    "OneLakeWriter._initialise: appending to existing file %s at offset %d",
                    self._file_client.path_name,
                    self._position,
                )
            except ResourceNotFoundError:
                self._file_client.create_file()
                self._position = 0
                self.log.debug(
                    "OneLakeWriter._initialise: file not found, created %s",
                    self._file_client.path_name,
                )
        self._initialised = True

    # ------------------------------------------------------------------ #
    # Buffer management                                                    #
    # ------------------------------------------------------------------ #

    def _send_chunk(self, data: bytes) -> None:
        """
        Send *data* to the server via ``append_data`` at the current position.
        Updates ``self._position`` on success.
        """
        try:
            self._file_client.append_data(
                data=data, offset=self._position, length=len(data)
            )
        except Exception as exc:
            if "The resource was created or modified by the Azure Blob Service API" in str(exc):
                raise OneLakeError(
                    f"Cannot append to '{self._file_client.path_name}' because it was written "
                    "using the Azure Blob Service API. Use overwrite=True to replace it."
                ) from exc
            raise
        self.log.debug(
            "OneLakeWriter._send_chunk: appended %d bytes at offset %d",
            len(data),
            self._position,
        )
        self._position += len(data)

    def _flush_full_chunks(self) -> None:
        """Drain complete *buffer_size* slices from the buffer via ``append_data``."""
        while len(self._buffer) >= self.buffer_size:
            chunk = bytes(self._buffer[: self.buffer_size])
            self._send_chunk(chunk)
            del self._buffer[: self.buffer_size]

    def _flush_remaining(self) -> None:
        """Send any leftover bytes in the buffer. Called once from :meth:`close`."""
        if self._buffer:
            self._send_chunk(bytes(self._buffer))
            self._buffer.clear()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def write(self, data: Union[bytes, str]) -> None:
        """
        Buffer *data* for writing to OneLake.

        On the first call the remote file is created (or located, for append
        mode) before any data is sent.  Subsequent calls accumulate data in
        the internal buffer; full *buffer_size* chunks are sent immediately
        via ``append_data`` so memory usage remains bounded.

        :param data: Bytes or str (str is UTF-8 encoded automatically).
        :raises OneLakeWriterClosedError: If called after :meth:`close`.
        :raises OneLakeError: If the file was written by the Blob Service API
            and ``overwrite=False`` was requested.
        """
        if self._closed:
            raise OneLakeWriterClosedError("Cannot write to a closed OneLakeWriter")
        if isinstance(data, str):
            data = data.encode("utf-8")
        if not self._initialised:
            self._initialise()
        self._buffer.extend(data)
        self._flush_full_chunks()

    def close(self) -> None:
        """
        Flush remaining buffered data and commit the file.

        ``flush_data`` is called only once, with the total byte count, to make
        the file visible and readable in OneLake.

        This method is idempotent: calling it more than once is a no-op.
        If :meth:`write` was never called no API requests are made.
        """
        if self._closed:
            return
        self._closed = True

        if not self._initialised:
            self.log.debug("OneLakeWriter.close: no data written, skipping API calls")
            return

        self._flush_remaining()
        self._file_client.flush_data(self._position)
        self.log.info(
            "OneLakeWriter.close: committed %d bytes to %s",
            self._position,
            self._file_client.path_name,
        )

    # ------------------------------------------------------------------ #
    # Context manager                                                       #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "OneLakeWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        On clean exit: calls :meth:`close` to flush and commit.
        On exception: marks the writer closed but skips the commit so the file
        remains uncommitted (ADLS Gen2 garbage-collects the append session).
        Exceptions are never suppressed.
        """
        if exc_type is None:
            self.close()
        else:
            self._closed = True
            self.log.warning(
                "OneLakeWriter.__exit__: exception during write — "
                "file '%s' not committed. exc_type=%s exc=%s",
                self._file_client.path_name,
                exc_type,
                exc_val,
            )
        return False
