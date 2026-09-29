import asyncio
import tempfile
import threading

import pytest

from durastream import AsyncStore


async def test_async_roundtrip():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
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
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
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


async def test_subscribe_is_pushed_not_polled():
    """Each record arrives well within the old 50 ms poll interval."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        it = s.subscribe(0)
        loop = asyncio.get_running_loop()
        delays = []
        for i in range(5):
            nxt = asyncio.ensure_future(anext(it))
            await asyncio.sleep(0.01)  # subscriber is parked in wait()
            t = loop.time()
            await s.append(b"%d" % i)
            assert await asyncio.wait_for(nxt, 1) == b"%d" % i
            delays.append(loop.time() - t)
        assert sorted(delays)[2] < 0.02, delays  # median
        await s.close()
        assert [r async for r in it] == []  # ends once closed and drained
        await store.close()


async def test_sync_writer_thread_wakes_async_subscriber():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        sync_stream = s._s  # the engine stream behind the async wrapper
        waiter = asyncio.ensure_future(s.wait(0))
        await asyncio.sleep(0.01)
        threading.Thread(target=sync_stream.append, args=(b"x",)).start()
        assert await asyncio.wait_for(waiter, 1) is True


async def test_wait_returns_false_on_close_and_delete():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        a, b = await store.create("a"), await store.create("b")
        wa, wb = asyncio.ensure_future(a.wait(0)), asyncio.ensure_future(b.wait(0))
        await asyncio.sleep(0.01)
        await a.close()
        await store.delete("b")
        assert await asyncio.wait_for(wa, 1) is False
        assert await asyncio.wait_for(wb, 1) is False


async def test_read_past_the_end_skips_the_thread_hop(monkeypatch):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        await s.append(b"x")

        def no_threads(*a, **k):
            raise AssertionError("empty read must not hop to a thread")

        monkeypatch.setattr(asyncio, "to_thread", no_threads)
        assert await s.read(1) == []
        assert await s.read(5) == []
        assert await s.read(0, 0) == []


async def test_subscribe_poll_argument_is_deprecated():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        await s.close()
        with pytest.warns(DeprecationWarning):
            assert [r async for r in s.subscribe(0, poll=0.1)] == []
