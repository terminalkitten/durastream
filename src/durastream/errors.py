"""Exceptions shared by both engines (the native core raises these same classes)."""


class DurastreamError(Exception):
    """Base class for durastream errors."""


class StreamClosed(DurastreamError):
    """The stream is closed, deleted, or disabled after a failed write."""


class StreamLocked(DurastreamError):
    """Another process owns this stream's log (one writer per stream); this handle is read-only."""


class CorruptStream(DurastreamError):
    """Bytes on disk no longer match their checksum."""
