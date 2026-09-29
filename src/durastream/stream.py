import errno
import os
import threading
import zlib
from typing import BinaryIO

from .codec import HEADER, HEADER_SIZE, iter_frames, pack_frame
from .errors import CorruptStream, StreamClosed, StreamLocked

try:
    import fcntl
except ImportError:  # Windows: no flock, one-writer-per-stream is unenforced
    fcntl = None

__all__ = ["DurableStream", "StreamClosed"]

# fdatasync skips the inode-metadata sync; safe for append and faster.
# Availability: Unix, not macOS, not iOS.
_fsync = getattr(os, "fdatasync", os.fsync)

# flock unavailable on this file system (e.g. some NFS setups)
_LOCK_UNSUPPORTED = {errno.ENOLCK, errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS}


def _try_lock(f: BinaryIO) -> bool:
    """Take the log's exclusive flock. False: another handle owns it.

    Same lock as the native engine (std's File::try_lock is flock), so the two
    engines coordinate on one store.
    """
    if fcntl is None:
        return True
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    except OSError as e:
        # ponytail: no flock here, proceed as writer (unenforced, as on Windows)
        if e.errno in _LOCK_UNSUPPORTED:
            return True
        raise
    return True


def _scan(path: str, size: int) -> list[int]:
    """Offsets of every intact frame, read sequentially (no mmap: a concurrent
    truncate by another process must not SIGBUS us)."""
    index = [0]
    pos = 0
    with open(path, "rb", buffering=1 << 20) as r:
        while pos + HEADER_SIZE <= size:
            header = r.read(HEADER_SIZE)
            if len(header) < HEADER_SIZE:
                break  # file shrank under us
            length, crc = HEADER.unpack(header)
            end = pos + HEADER_SIZE + length
            if end > size:
                break  # torn tail: header promises bytes we don't have
            payload = r.read(length)
            if len(payload) < length or zlib.crc32(payload) != crc:
                break  # file shrank under us, or corruption
            index.append(end)
            pos = end
    return index


def _write_all(f: BinaryIO, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[f.write(view) :]


class DurableStream:
    """Append-only log file for one stream.

    The first handle to open a log takes an exclusive flock on it and becomes the
    writer. Handles in other processes (or other Stores) get a read-only view.
    """

    def __init__(self, meta, name: str, path: str, content_type: str, closed: bool):
        self._meta = meta  # not the Store: see store._Meta
        self.name = name
        self._path = path
        self.content_type = content_type
        self._closed = closed
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        # One unbuffered handle for everything: a failed write must not linger in
        # a buffer and be re-sent later. Closed by _close_fds() (or GC).
        self._file: BinaryIO | None
        try:
            self._file = open(path, "a+b", buffering=0)  # noqa: SIM115
            self._owner = _try_lock(self._file)
        except OSError as e:
            if e.errno not in (errno.EACCES, errno.EPERM, errno.EROFS):
                raise
            # no write access (permissions, read-only mount): a read-only view
            self._file = open(path, "rb", buffering=0)  # noqa: SIM115
            self._owner = False
        # A forked child shares the lock (flock belongs to the open file), so only
        # the process that took it may write.
        self._pid = os.getpid()
        self._index = self._recover()  # byte offset of each record; last = end of log

    def _recover(self) -> list[int]:
        """Scan + CRC-verify the log and build the index. The writer truncates a
        torn tail so appends stay contiguous; readers leave the file alone."""
        assert self._file is not None
        fd = self._file.fileno()
        size = os.fstat(fd).st_size
        index = _scan(self._path, size)
        if self._owner and index[-1] != size:
            # ponytail: torn/corrupt tail, drop it so appends stay contiguous.
            os.ftruncate(fd, index[-1])
            _fsync(fd)
        return index

    @property
    def next_offset(self) -> int:
        return len(self._index) - 1

    @property
    def closed(self) -> bool:
        """True once closed, deleted, or disabled after a failed write."""
        return self._closed

    @property
    def writable(self) -> bool:
        """True when this handle holds the log's lock in this process (can append)."""
        return self._owner and self._pid == os.getpid()

    def append(self, payload: bytes) -> int:
        """Frame + fsync one record. Returns the new next_offset."""
        return self.append_many([payload])

    def append_many(self, payloads: list[bytes]) -> int:
        """Frame + fsync a batch of records in one flush. Returns new next_offset."""
        frames = [pack_frame(p) for p in payloads]  # outside the lock
        with self._cond:
            if self._closed:
                raise StreamClosed(self.name)
            if not self.writable:
                raise StreamLocked(self.name)
            if not frames:
                return self.next_offset
            if self._file is None:
                raise StreamClosed(self.name)
            fd = self._file.fileno()
            end = self._index[-1]
            try:
                _write_all(self._file, b"".join(frames))
                _fsync(fd)
            except OSError:
                # A partial frame would shift every later record, and after a
                # failed fsync the page cache is in an unknown state ("fsyncgate"):
                # cut back to the last durable end. If even that fails, refuse
                # further appends.
                try:
                    os.ftruncate(fd, end)
                    _fsync(fd)
                except OSError:
                    self._closed = True
                    self._cond.notify_all()
                raise
            for frame in frames:
                end += len(frame)
                self._index.append(end)
            self._cond.notify_all()
            return self.next_offset

    def read(self, offset: int = 0, end: int | None = None) -> list[bytes]:
        """Record payloads for [offset, end), checksum-verified."""
        with self._lock:
            last = self.next_offset
            end = last if end is None else min(end, last)
            offset = max(offset, 0)
            if offset >= end:
                return []
            if self._file is None:
                raise StreamClosed(self.name)
            start_byte = self._index[offset]
            want = self._index[end] - start_byte
            self._file.seek(start_byte)
            chunks, got = [], 0
            while got < want:
                chunk = self._file.read(want - got)
                if not chunk:
                    raise CorruptStream(f"{self.name}: log is shorter than its index")
                chunks.append(chunk)
                got += len(chunk)
        records = [payload for payload, _ in iter_frames(b"".join(chunks))]
        if len(records) != end - offset:
            raise CorruptStream(
                f"{self.name}: record {offset + len(records)} fails its checksum"
            )
        return records

    def subscribe(self, offset: int = 0):
        """Tail -f: yield records from `offset`, then block for new ones (in-process only).

        Ends when the stream is closed (or deleted) and drained.
        """
        while True:
            batch = self.read(offset)
            yield from batch
            offset += len(batch)
            with self._cond:
                while self.next_offset <= offset and not self._closed:
                    self._cond.wait()
                if self._closed and self.next_offset <= offset:
                    return

    def close(self) -> None:
        """Mark the stream closed (persisted). Reads still work."""
        with self._cond:
            if not self.writable:
                raise StreamLocked(self.name)
            # persist first, so memory never claims a state the disk doesn't have
            self._meta.execute("UPDATE streams SET closed=1 WHERE name=?", (self.name,))
            self._closed = True
            self._cond.notify_all()

    def _close_fds(self) -> None:
        """Release the file handle (and the lock) on delete / store close; wakes subscribers."""
        with self._cond:
            if self._file is not None:
                self._file.close()
                self._file = None
            self._owner = False
            self._closed = True
            self._cond.notify_all()
