"""Async wrappers over the sync engine, via asyncio.to_thread (no asgiref)."""

import asyncio
import warnings
from collections.abc import AsyncIterator
from typing import Self

from ._engine import DurableStream, Store
from .errors import DurastreamError


class AsyncDurableStream:
    def __init__(self, stream: DurableStream) -> None:
        self._s = stream

    @property
    def name(self) -> str:
        return self._s.name

    @property
    def content_type(self) -> str:
        return self._s.content_type

    @property
    def next_offset(self) -> int:
        return self._s.next_offset

    @property
    def closed(self) -> bool:
        return self._s.closed

    @property
    def writable(self) -> bool:
        return self._s.writable

    async def append(self, payload: bytes) -> int:
        return await asyncio.to_thread(self._s.append, payload)

    async def append_many(self, payloads: list[bytes]) -> int:
        return await asyncio.to_thread(self._s.append_many, payloads)

    async def read(self, offset: int = 0, end: int | None = None) -> list[bytes]:
        start = max(offset, 0)
        if start >= self._s.next_offset or (end is not None and end <= start):
            return []  # nothing new: skip the thread hop (next_offset is lock-free)
        return await asyncio.to_thread(self._s.read, offset, end)

    async def close(self) -> None:
        await asyncio.to_thread(self._s.close)

    async def wait(self, offset: int) -> bool:
        """Wait until a record past `offset` exists, woken by the append itself
        (no polling). False once the stream is closed with nothing past `offset`.

        Wakes for appends made in this process (any thread); another process's
        writes are not visible here (see Concurrency).
        """
        loop = asyncio.get_running_loop()
        wake = asyncio.Event()

        def notify() -> None:  # runs in the writer's thread
            try:
                loop.call_soon_threadsafe(wake.set)
            except RuntimeError:
                pass  # loop already closed

        lid = self._s.add_listener(notify)
        try:
            while self._s.next_offset <= offset:
                if self._s.closed:
                    return False
                await wake.wait()
                wake.clear()
            return True
        finally:
            self._s.remove_listener(lid)

    async def subscribe(
        self, offset: int = 0, poll: float | None = None
    ) -> AsyncIterator[bytes]:
        """Tail -f: yield records from `offset`, then each new one as it is
        appended (push, not polling). Ends once the stream is closed and drained.

        `poll` is ignored (subscribe no longer polls) and will be removed.
        """
        if poll is not None:
            warnings.warn(
                "subscribe(poll=...) is ignored: subscribe is push-based now",
                DeprecationWarning,
                stacklevel=2,
            )
        while True:
            batch = await self.read(offset)
            for record in batch:
                yield record
            offset += len(batch)
            if not await self.wait(offset):
                return


class AsyncBatchWriter:
    """Group many small appends (e.g. LLM tokens) into one fsync.

    `write()` buffers a record and returns at once; buffered records are appended
    together, one `append_many` and one fsync, at most `window` seconds later.
    Durability is unchanged: a record is on disk before any reader can see it, and
    `await flush()` returns once everything written so far is durable. A crash can
    only lose records still in the buffer, which no reader has seen.

        async with AsyncBatchWriter(stream) as w:
            async for token in llm:
                w.write(token)
        # every token is durable here
        await stream.close()

    If a background flush fails, its records stay buffered (retried by the next
    flush) and the error is raised by the next `write`, `flush` or `close`.
    """

    def __init__(self, stream: AsyncDurableStream, window: float = 0.02) -> None:
        self._s = stream
        self._window = window
        self._buf: list[bytes] = []
        self._lock = asyncio.Lock()  # one append_many at a time, in order
        self._timer: asyncio.Task | None = None
        self._error: Exception | None = None
        self._closed = False

    def write(self, payload: bytes) -> None:
        """Buffer one record; it is appended within `window` seconds."""
        self._raise_pending()
        if self._closed:
            raise DurastreamError("batch writer is closed")
        self._buf.append(payload)
        if self._timer is None:
            self._timer = asyncio.get_running_loop().create_task(self._flush_later())

    async def flush(self) -> int:
        """Append everything written so far. Returns next_offset once it is durable."""
        self._raise_pending()
        async with self._lock:
            batch, self._buf = self._buf, []
            if not batch:
                return self._s.next_offset
            return await self._append(batch)

    async def close(self) -> None:
        """Flush and stop. Does not close the stream."""
        timer, self._timer = self._timer, None
        if timer is not None:
            timer.cancel()  # only cancels a timer still sleeping, see _flush_later
        await self.flush()
        self._closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def _flush_later(self) -> None:
        try:
            await asyncio.sleep(self._window)
        except asyncio.CancelledError:
            return
        self._timer = None  # from here on close() lets this flush finish
        try:
            await self.flush()
        except Exception as e:  # noqa: BLE001 - any failure is re-raised by the next call
            self._error = e

    async def _append(self, batch: list[bytes]) -> int:
        """append_many, keeping order even if the caller is cancelled: the append
        runs on in its thread, so wait for it before the next batch may go."""
        task = asyncio.ensure_future(self._s.append_many(batch))
        try:
            return await asyncio.shield(task)
        finally:
            if not task.done():  # caller cancelled mid-append
                await asyncio.wait([task])
            if not task.cancelled() and task.exception() is not None:
                self._buf[:0] = batch  # not durable: retry with the next flush

    def _raise_pending(self) -> None:
        if self._error is not None:
            error, self._error = self._error, None
            raise error


class AsyncStore:
    def __init__(self, root: str) -> None:
        self._store = Store(root)  # brief one-time blocking IO at startup

    @property
    def root(self) -> str:
        return self._store.root

    async def create(
        self, name: str, content_type: str | None = None
    ) -> AsyncDurableStream:
        s = await asyncio.to_thread(self._store.create, name, content_type)
        return AsyncDurableStream(s)

    async def open(self, name: str) -> AsyncDurableStream:
        s = await asyncio.to_thread(self._store.open, name)
        return AsyncDurableStream(s)

    async def delete(self, name: str) -> None:
        await asyncio.to_thread(self._store.delete, name)

    async def list(self) -> list[str]:
        return await asyncio.to_thread(self._store.list)

    async def close(self) -> None:
        await asyncio.to_thread(self._store.close)
