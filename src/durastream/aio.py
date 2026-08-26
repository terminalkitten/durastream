"""Async wrappers over the sync engine, via asyncio.to_thread (no asgiref)."""

import asyncio
from collections.abc import AsyncIterator

from .store import Store
from .stream import DurableStream
from .utils import DEFAULT_CONTENT_TYPE


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

    async def append(self, payload: bytes) -> int:
        return await asyncio.to_thread(self._s.append, payload)

    async def append_many(self, payloads: list[bytes]) -> int:
        return await asyncio.to_thread(self._s.append_many, payloads)

    async def read(self, offset: int = 0, end: int | None = None) -> list[bytes]:
        return await asyncio.to_thread(self._s.read, offset, end)

    async def close(self) -> None:
        await asyncio.to_thread(self._s.close)

    async def subscribe(
        self, offset: int = 0, poll: float = 0.05
    ) -> AsyncIterator[bytes]:
        """Tail -f, poll-based; latency <= poll."""
        while True:
            batch = await self.read(offset)
            for record in batch:
                yield record
            offset += len(batch)
            if self.closed and offset >= self.next_offset:
                return
            await asyncio.sleep(poll)


class AsyncStore:
    def __init__(self, root: str) -> None:
        self._store = Store(root)  # brief one-time blocking IO at startup

    @property
    def root(self) -> str:
        return self._store.root

    async def create(
        self, name: str, content_type: str = DEFAULT_CONTENT_TYPE
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
