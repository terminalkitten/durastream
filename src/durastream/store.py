import os
import sqlite3
import threading
import time

from .stream import DurableStream
from .utils import DEFAULT_CONTENT_TYPE, check_name


class Store:
    def __init__(self, root: str):
        self.root = root
        self._streams_dir = os.path.join(root, "streams")
        os.makedirs(self._streams_dir, exist_ok=True)
        self._db = sqlite3.connect(
            os.path.join(root, "meta.db"), check_same_thread=False
        )
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS streams("
            "  name TEXT PRIMARY KEY,"
            "  content_type TEXT NOT NULL,"
            "  closed INTEGER NOT NULL DEFAULT 0,"
            "  created_at REAL NOT NULL)"
        )
        self._db.commit()
        self._lock = threading.Lock()
        self._open: dict[str, DurableStream] = {}  # one DurableStream instance per name

    def _path(self, name: str) -> str:
        return os.path.join(self._streams_dir, name + ".log")

    def _fsync_dir(self) -> None:
        # persist the dir entry, so it survives a crash
        fd = os.open(self._streams_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def create(self, name: str, content_type: str | None = None) -> DurableStream:
        """Create a stream, or return the existing one (idempotent).

        Raises ValueError if content_type is given and differs from the existing one.
        """
        check_name(name)
        with self._lock:
            existing = self._open.get(name)
            if existing is None:
                row = self._db.execute(
                    "SELECT content_type, closed FROM streams WHERE name=?", (name,)
                ).fetchone()
            else:
                row = (existing.content_type, existing.closed)
            if row is None:
                content_type = content_type or DEFAULT_CONTENT_TYPE
                self._db.execute(
                    "INSERT INTO streams(name, content_type, closed, created_at)"
                    " VALUES(?,?,0,?)",
                    (name, content_type, time.time()),
                )
                self._db.commit()
                self._fsync_dir()
                closed = False
            else:
                content_type_existing, closed = row[0], bool(row[1])
                if content_type is not None and content_type != content_type_existing:
                    raise ValueError(
                        f"content_type mismatch for {name!r}: "
                        f"{content_type_existing!r} != {content_type!r}"
                    )
                content_type = content_type_existing
            if existing is not None:
                return existing
            return self._open_stream(name, content_type, closed)

    def open(self, name: str) -> DurableStream:
        with self._lock:
            if name in self._open:
                return self._open[name]
            row = self._db.execute(
                "SELECT content_type, closed FROM streams WHERE name=?", (name,)
            ).fetchone()
            if row is None:
                raise KeyError(name)
            return self._open_stream(name, row[0], bool(row[1]))

    def _open_stream(self, name: str, content_type: str, closed: bool) -> DurableStream:
        s = DurableStream(self, name, self._path(name), content_type, closed)
        self._open[name] = s
        return s

    def delete(self, name: str) -> None:
        with self._lock:
            s = self._open.pop(name, None)
            if s is not None:
                s._close_fds()
            self._db.execute("DELETE FROM streams WHERE name=?", (name,))
            self._db.commit()
            try:
                os.remove(self._path(name))
                self._fsync_dir()
            except FileNotFoundError:
                pass

    def list(self) -> list[str]:
        with self._lock:
            return [
                r[0] for r in self._db.execute("SELECT name FROM streams ORDER BY name")
            ]

    def _mark_closed(self, name: str) -> None:
        with self._lock:
            self._db.execute("UPDATE streams SET closed=1 WHERE name=?", (name,))
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            for s in self._open.values():
                s._close_fds()
            self._open.clear()
            self._db.close()
