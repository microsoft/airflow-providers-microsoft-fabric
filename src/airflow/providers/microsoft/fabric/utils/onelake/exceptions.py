from __future__ import annotations


class OneLakeError(OSError):
    """Base exception for OneLake I/O errors raised by this library."""


class OneLakeWriterClosedError(OneLakeError):
    """Raised when write() is called on an already-closed OneLakeWriter."""