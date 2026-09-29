import os
import sqlite3
import threading
import time
from typing import BinaryIO

from .errors import DurastreamError, StreamLocked
from .stream import DurableStream, _try_lock, fcntl
from .utils import DEFAULT_CONTENT_TYPE, check_name


class _Meta:
    """The metadata DB, shared by a Store and its streams.

    Streams hold this, not the Store: a Store <-> stream cycle would keep log
    files (and their locks) open until the cyclic GC happens to run.
    """

    def __init__(self, path: str):
        self._lock = threading.Lock()
        try:
            self._db: sqlite3.Connection | None = sqlite3.connect(
                path, check_same_thread=False
            )
            self._db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error as e:
            raise DurastreamError(str(e)) from e
        self.execute(
            "CREATE TABLE IF NOT EXISTS streams("
            "  name TEXT PRIMARY KEY,"
            "  content_type TEXT NOT NULL,"
            "  closed INTEGER NOT NULL DEFAULT 0,"
            "  created_at REAL NOT NULL)"
        )

    def execute(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self._lock:
            if self._db is None:
                raise DurastreamError("store is closed")
            try:
                rows = self._db.execute(sql, params).fetchall()
                self._db.commit()
            except sqlite3.Error as e:  # same exception type as the native engine
                raise DurastreamError(str(e)) from e
            return rows

    def check_open(self) -> None:
        with self._lock:
            if self._db is None:
                raise DurastreamError("store is closed")

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None


def _remove_if_exists(path: str) -> bool:
    try:
        os.remove(path)
    except FileNotFoundError:
        return False
    return True


def _lock_for_removal(name: str, path: str) -> BinaryIO | None:
    """Lock the log before unlinking it; close the returned file after the unlink.

    Raises StreamLocked if another handle owns the log. None if there is no log,
    or no flock (Windows, where an open file can't be unlinked anyway).
    """
    if fcntl is None:
        return None
    try:
        f = open(path, "rb")  # noqa: SIM115
    except FileNotFoundError:
        return None
    if not _try_lock(f):
        f.close()
        raise StreamLocked(name)
    return f


class Store:
    """A directory of streams: meta.db plus streams/<name>.log."""

    def __init__(self, root: str | os.PathLike[str]):
        self.root = os.fspath(root)
        self._streams_dir = os.path.join(self.root, "streams")
        os.makedirs(self._streams_dir, exist_ok=True)
        self._meta = _Meta(os.path.join(self.root, "meta.db"))
        # Lock order: store lock -> stream lock -> meta lock.
        self._lock = threading.Lock()
        self._open: dict[str, DurableStream] = {}  # one DurableStream instance per name

    def _path(self, name: str) -> str:
        return os.path.join(self._streams_dir, name + ".log")

    def _fsync_dir(self) -> None:
        # persist dir entries (new/removed logs), so they survive a crash
        if os.name == "nt":
            return  # can't open a directory on Windows; NTFS journals metadata
        fd = os.open(self._streams_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _row(self, name: str) -> tuple[str, bool] | None:
        rows = self._meta.execute(
            "SELECT content_type, closed FROM streams WHERE name=?", (name,)
        )
        return (rows[0][0], bool(rows[0][1])) if rows else None

    def create(self, name: str, content_type: str | None = None) -> DurableStream:
        """Create a stream, or return the existing one (idempotent).

        Raises ValueError if content_type is given and differs from the existing one.
        """
        check_name(name)
        with self._lock:
            existing = self._open.get(name)
            if existing is not None:
                _check_type(name, existing.content_type, content_type)
                return existing
            row = self._row(name)
            if row is not None:
                _check_type(name, row[0], content_type)
                return self._open_stream(name, row[0], row[1])
            if content_type is None:
                content_type = DEFAULT_CONTENT_TYPE
            # A log without a row is left over from a delete that crashed midway:
            # never resurrect its records into the new stream.
            guard = _lock_for_removal(name, self._path(name))
            try:
                _remove_if_exists(self._path(name))
            finally:
                if guard is not None:
                    guard.close()
            self._meta.execute(
                "INSERT INTO streams(name, content_type, closed, created_at)"
                " VALUES(?,?,0,?)",
                (name, content_type, time.time()),
            )
            s = self._open_stream(name, content_type, False)  # creates the log
            self._fsync_dir()
            return s

    def open(self, name: str) -> DurableStream:
        """Open an existing stream. Read-only if another process owns its log."""
        with self._lock:
            if name in self._open:
                return self._open[name]
            row = self._row(name)
            if row is None:
                raise KeyError(name)
            return self._open_stream(name, *row)

    def _open_stream(self, name: str, content_type: str, closed: bool) -> DurableStream:
        s = DurableStream(self._meta, name, self._path(name), content_type, closed)
        self._open[name] = s
        return s

    def delete(self, name: str) -> None:
        """Delete a stream: its log and its metadata.

        Raises StreamLocked if another process owns the log.
        """
        s = None
        try:
            with self._lock:
                self._meta.check_open()  # before any file is touched
                path = self._path(name)
                s = self._open.get(name)
                # Hold the log's lock across the unlink (ours if we're the writer,
                # else a probe), so no other process can start writing a file that
                # is about to vanish.
                guard = (
                    None
                    if s is not None and s.writable
                    else _lock_for_removal(name, path)
                )
                try:
                    if s is not None and fcntl is None:
                        s._close_fds()  # Windows can't unlink an open file
                    # file first: a crash after this leaves a row with no log, which
                    # reopens empty; the reverse order could resurrect deleted records
                    removed = _remove_if_exists(path)
                finally:
                    if guard is not None:
                        guard.close()
                if s is not None:
                    self._open.pop(name)._close_fds()
                if removed:
                    self._fsync_dir()
                self._meta.execute("DELETE FROM streams WHERE name=?", (name,))
        finally:
            if s is not None:
                s._notify()  # outside the store lock: listeners may call back into it

    def list(self) -> list[str]:
        """Stream names, sorted."""
        return [
            r[0] for r in self._meta.execute("SELECT name FROM streams ORDER BY name")
        ]

    def close(self) -> None:
        """Release every stream's file handle and close the metadata DB."""
        with self._lock:
            streams = list(self._open.values())
            self._open.clear()
            for s in streams:
                s._close_fds()
            self._meta.close()
        for s in streams:
            s._notify()  # outside the store lock: listeners may call back into it


def _check_type(name: str, existing: str, given: str | None) -> None:
    if given is not None and given != existing:
        raise ValueError(
            f"content_type mismatch for {name!r}: {existing!r} != {given!r}"
        )
