import os
import threading

from .codec import iter_frames, pack_frame

# fdatasync skips the inode-metadata sync; safe for append and faster.
# Availability: Unix, not macOS, not iOS.
_fsync = getattr(os, "fdatasync", os.fsync)


class StreamClosed(Exception):
    pass


class DurableStream:
    """Append-only log file"""

    def __init__(self, store, name: str, path: str, content_type: str, closed: bool):
        self._store = store
        self.name = name
        self._path = path
        self.content_type = content_type
        self._closed = closed
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._index = [0]  # byte offset of each record; last entry = end of log
        self._recover()
        # Persistent handles, reused for every append/read; closed by _close_fds().
        self._writer = open(self._path, "ab")  # noqa: SIM115
        self._reader = open(self._path, "rb")  # noqa: SIM115

    def _recover(self) -> None:
        """Scan + CRC-verify the log, build the index, truncate a torn tail."""
        if not os.path.exists(self._path):
            open(self._path, "ab").close()
            return
        with open(self._path, "rb") as f:
            data = f.read()
        index = [0]
        for _payload, end in iter_frames(data):
            index.append(end)
        self._index = index
        if index[-1] != len(data):
            # ponytail: torn/corrupt tail, drop it so appends stay contiguous.
            with open(self._path, "r+b") as f:
                f.truncate(index[-1])
                f.flush()
                _fsync(f.fileno())

    @property
    def next_offset(self) -> int:
        return len(self._index) - 1

    @property
    def closed(self) -> bool:
        return self._closed

    def append(self, payload: bytes) -> int:
        """Frame + fsync one record. Returns the new next_offset."""
        return self.append_many([payload])

    def append_many(self, payloads: list[bytes]) -> int:
        """Frame + fsync a batch of records in one flush. Returns new next_offset."""
        with self._cond:
            if self._closed:
                raise StreamClosed(self.name)
            if not payloads:
                return self.next_offset
            frames = [pack_frame(p) for p in payloads]
            self._writer.write(b"".join(frames))
            self._writer.flush()
            _fsync(self._writer.fileno())
            for frame in frames:
                self._index.append(self._index[-1] + len(frame))
            self._cond.notify_all()
            return self.next_offset

    def read(self, offset: int = 0, end: int | None = None) -> list[bytes]:
        """Return raw record payloads for [offset, end); content-type shaping is caller's job."""
        with self._lock:
            last = self.next_offset
            end = last if end is None else min(end, last)
            offset = max(offset, 0)
            if offset >= end:
                return []
            start_byte = self._index[offset]
            stop_byte = self._index[end]
            self._reader.seek(start_byte)
            data = self._reader.read(stop_byte - start_byte)
        return [payload for payload, _ in iter_frames(data)]

    def subscribe(self, offset: int = 0):
        """Tail -f: yield records from `offset`, then block for new ones (in-process only)."""
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
        """Mark the stream closed. Reads still work; fds stay open."""
        with self._cond:
            self._closed = True
            self._store._mark_closed(self.name)
            self._cond.notify_all()

    def _close_fds(self) -> None:
        """Release the persistent file handles (on delete / store close)."""
        self._writer.close()
        self._reader.close()
