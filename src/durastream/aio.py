"""Async wrappers over the sync engine, via asyncio.to_thread (no asgiref)."""

import asyncio
import warnings
from collections.abc import AsyncIterator

from ._engine import DurableStream, Store


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
