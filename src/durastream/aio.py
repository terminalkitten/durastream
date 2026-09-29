"""Async wrappers over the sync engine, via asyncio.to_thread (no asgiref)."""

import asyncio
import collections
from collections.abc import AsyncGenerator, Sequence
from typing import Self

from .errors import DurastreamError
from .store import Store
from .stream import DurableStream

# How far a subscriber may fall behind live before it drops its buffer and reads
# from disk instead: bounds memory per slow client, by count and by size.
_LAG_LIMIT = 10_000  # records
_LAG_BYTES = 16 * 1024 * 1024


class _Sub:
    """One async subscriber's view of the live tail, fed by its stream's _Hub."""

    __slots__ = ("buf", "event", "nbytes", "stale", "start")

    def __init__(self) -> None:
        self.buf: collections.deque[bytes] = collections.deque()
        self.nbytes = 0  # total size of buf
        self.start = 0  # offset of buf[0], or of the next live record when empty
        self.stale = True  # buf can't be trusted: read from disk first
        self.event = asyncio.Event()

    def deliver(self, start: int, records: list[bytes]) -> None:
        if records and not self.stale:
            end = self.start + len(self.buf)
            if start > end:
                self.stale = True  # missed records: catch up from disk
            elif start + len(records) > end:
                new = records[end - start :]
                self.buf.extend(new)
                self.nbytes += sum(map(len, new))
                if len(self.buf) > _LAG_LIMIT or self.nbytes > _LAG_BYTES:
                    self.stale = True  # too far behind live: fall back to disk
        if self.stale:
            self.clear()
        self.event.set()

    def clear(self) -> None:
        self.buf.clear()
        self.nbytes = 0

    def popleft(self) -> bytes:
        record = self.buf.popleft()
        self.nbytes -= len(record)
        self.start += 1
        return record


class _Hub:
    """Fans out one stream's appended records to all its async subscribers.

    One engine listener per stream and one call_soon_threadsafe per append, however
    many subscribers; records reach them without a disk read. Listeners only fire
    after the fsync, so subscribers still only ever see durable records.
    """

    def __init__(self, stream: DurableStream) -> None:
        self.stream = stream
        self._subs: set[_Sub] = set()
        self._lid: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def join(self) -> _Sub:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop.is_closed():
            # subscribers of a loop that shut down without closing them: they can
            # never run again, so drop them and serve this loop (keeps the listener)
            self._subs.clear()
            self._loop = loop
        if self._lid is None:
            self._loop = loop
            self._lid = self.stream.add_listener(self._on_change)
        elif loop is not self._loop:
            raise RuntimeError("a stream's async subscribers must share one event loop")
        sub = _Sub()
        self._subs.add(sub)
        return sub

    def leave(self, sub: _Sub) -> None:
        self._subs.discard(sub)
        if not self._subs and self._lid is not None:
            self.stream.remove_listener(self._lid)
            self._lid = self._loop = None

    def _on_change(self, start: int, records: list[bytes]) -> None:  # writer thread
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._deliver, start, records)
            except RuntimeError:
                pass  # loop already closed

    def _deliver(self, start: int, records: list[bytes]) -> None:  # event loop
        for sub in self._subs:
            sub.deliver(start, records)


class AsyncDurableStream:
    def __init__(self, stream: DurableStream, hub: _Hub | None = None) -> None:
        self._s = stream
        self._hub = hub or _Hub(stream)

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

    async def append(self, payload: bytes | bytearray) -> int:
        return await asyncio.to_thread(self._s.append, payload)

    async def append_many(self, payloads: Sequence[bytes | bytearray]) -> int:
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
        sub = self._hub.join()  # stale forever: only its wake-ups are used
        try:
            while self._s.next_offset <= offset:
                if self._s.closed:
                    return False
                sub.event.clear()
                if self._s.next_offset <= offset and not self._s.closed:
                    await sub.event.wait()
            return True
        finally:
            self._hub.leave(sub)

    async def subscribe(self, offset: int = 0) -> AsyncGenerator[bytes, None]:
        """Tail -f: yield records from `offset`, then each new one as it is
        appended. Live records are handed over by the append itself (no polling,
        no disk read); only the replay, and a subscriber that falls far behind,
        read from disk. Ends once the stream is closed and drained.
        """
        pos = max(offset, 0)
        sub = self._hub.join()  # before the first read: no append can slip between
        try:
            while True:
                if sub.stale:  # replay / catch up from disk
                    sub.stale = False
                    sub.clear()
                    sub.start = self._s.next_offset  # live records from here on
                    batch = await self.read(pos)
                    for record in batch:
                        yield record
                    pos += len(batch)
                    continue
                while sub.buf and sub.start < pos:  # already yielded from disk
                    sub.popleft()
                if not sub.buf:
                    sub.start = max(sub.start, pos)
                if sub.start > pos:
                    sub.stale = True  # hole between disk and live: read it
                    continue
                if sub.buf:
                    pos += 1
                    yield sub.popleft()
                    continue
                if self._s.closed:
                    if pos >= self._s.next_offset:
                        return
                    sub.stale = True  # close overtook deliveries: finish from disk
                    continue
                sub.event.clear()
                if not (sub.buf or sub.stale or self._s.closed):
                    await sub.event.wait()
        finally:
            self._hub.leave(sub)


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
        self._hubs: dict[str, _Hub] = {}  # one fan-out hub per stream name

    @property
    def root(self) -> str:
        return self._store.root

    def _wrap(self, s: DurableStream) -> AsyncDurableStream:
        hub = self._hubs.get(s.name)
        if hub is None or (hub.stream is not s and hub.stream.closed):
            hub = self._hubs[s.name] = _Hub(s)  # new, or deleted and recreated
        return AsyncDurableStream(s, hub)

    async def create(
        self, name: str, content_type: str | None = None
    ) -> AsyncDurableStream:
        return self._wrap(
            await asyncio.to_thread(self._store.create, name, content_type)
        )

    async def open(self, name: str) -> AsyncDurableStream:
        return self._wrap(await asyncio.to_thread(self._store.open, name))

    async def delete(self, name: str) -> None:
        await asyncio.to_thread(self._store.delete, name)
        self._hubs.pop(name, None)

    async def list(self) -> list[str]:
        return await asyncio.to_thread(self._store.list)

    async def close(self) -> None:
        await asyncio.to_thread(self._store.close)
        self._hubs.clear()
