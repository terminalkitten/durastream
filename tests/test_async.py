import asyncio
import tempfile

from durastream import AsyncStore


async def test_async_roundtrip():
    with tempfile.TemporaryDirectory() as root:
        store = AsyncStore(root)
        s = await store.create("t", "text/plain")
        assert await s.append(b"a") == 1
        assert await s.append_many([b"b", b"c"]) == 3
        assert await s.read(0) == [b"a", b"b", b"c"]
        assert s.next_offset == 3
        await s.close()
        assert s.closed
        await store.close()


async def test_async_subscribe():
    with tempfile.TemporaryDirectory() as root:
        store = AsyncStore(root)
        s = await store.create("t")
        got = []

        async def consume():
            async for record in s.subscribe(0):
                got.append(record)

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.02)
        await s.append(b"one")
        await s.append(b"two")
        await s.close()
        await asyncio.wait_for(task, timeout=2)
        assert got == [b"one", b"two"]
