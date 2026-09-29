import asyncio
import contextlib
import tempfile
import threading

import pytest

from durastream import AsyncBatchWriter, AsyncStore


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


async def test_batch_writer_groups_appends_and_is_durable(monkeypatch):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        calls = []
        real = s.append_many

        async def counting(batch):
            calls.append(len(batch))
            return await real(batch)

        monkeypatch.setattr(s, "append_many", counting)
        async with AsyncBatchWriter(s, window=0.05) as w:
            for i in range(100):
                w.write(b"%d" % i)
        assert sum(calls) == 100 and len(calls) < 5, calls  # grouped
        await store.close()
        # everything was durable once the writer closed
        s2 = await AsyncStore(root).open("t")
        assert await s2.read(0) == [b"%d" % i for i in range(100)]


async def test_batch_writer_keeps_records_when_a_flush_fails(monkeypatch):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        real = s.append_many
        fail = [True]

        async def flaky(batch):
            if fail.pop() if fail else False:
                raise OSError(28, "No space left on device")
            return await real(batch)

        monkeypatch.setattr(s, "append_many", flaky)
        w = AsyncBatchWriter(s, window=60)  # flush only when asked
        w.write(b"a")
        w.write(b"b")
        with pytest.raises(OSError):
            await w.flush()
        w.write(b"c")
        assert await w.flush() == 3  # a, b retried, in order
        await w.close()
        assert await s.read(0) == [b"a", b"b", b"c"]


async def test_batch_writer_keeps_order_when_a_flush_is_cancelled(monkeypatch):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        real = s.append_many

        async def slow(batch):
            await asyncio.sleep(0.05)
            return await real(batch)

        monkeypatch.setattr(s, "append_many", slow)
        w = AsyncBatchWriter(s, window=60)
        w.write(b"first")
        f = asyncio.ensure_future(w.flush())
        await asyncio.sleep(0.01)  # mid-append
        f.cancel()
        w.write(b"second")
        await w.close()
        assert await s.read(0) == [b"first", b"second"]


def count_disk_reads(monkeypatch) -> list[int]:
    """Count asyncio.to_thread calls that run a stream read."""
    calls = [0]
    real = asyncio.to_thread

    async def counting(func, *args, **kwargs):
        if getattr(func, "__name__", "") == "read":
            calls[0] += 1
        return await real(func, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", counting)
    return calls


async def collect(stream, offset, n):
    got = []
    async with contextlib.aclosing(stream.subscribe(offset)) as records:
        async for record in records:
            got.append(record)
            if len(got) == n:
                break
    return got


async def test_fan_out_delivers_live_records_without_disk_reads(monkeypatch):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        await s.append(b"backlog")
        readers = [await store.open("t") for _ in range(5)]  # separate handles, one hub
        tasks = [asyncio.ensure_future(collect(r, 0, 101)) for r in readers]
        await asyncio.sleep(0.05)  # all replayed the backlog and went live
        reads = count_disk_reads(monkeypatch)
        for i in range(100):
            await s.append(b"%d" % i)
        want = [b"backlog"] + [b"%d" % i for i in range(100)]
        for t in tasks:
            assert await asyncio.wait_for(t, 5) == want
        assert reads[0] == 0, f"{reads[0]} live-path disk reads"
        assert s._hub._lid is None  # last subscriber left: engine listener removed


async def test_subscriber_joining_mid_stream_has_no_gaps_or_duplicates():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        await s.append_many([b"%d" % i for i in range(50)])

        async def writer():
            for i in range(50, 100):
                await s.append(b"%d" % i)

        w = asyncio.ensure_future(writer())  # appends race the subscriber's replay
        got = await asyncio.wait_for(collect(s, 10, 90), 5)
        await w
        assert got == [b"%d" % i for i in range(10, 100)]


async def test_slow_subscriber_falls_back_to_disk(monkeypatch):
    import durastream.aio

    monkeypatch.setattr(durastream.aio, "_LAG_LIMIT", 5)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        it = s.subscribe(0)
        first = asyncio.ensure_future(anext(it))
        await s.append(b"0")
        assert await first == b"0"
        await s.append_many([b"%d" % i for i in range(1, 50)])  # overflows its buffer
        rest = [await anext(it) for _ in range(49)]
        assert rest == [b"%d" % i for i in range(1, 50)]
        await it.aclose()


async def test_concurrent_writer_threads_keep_subscriber_exact():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        sync = s._s
        task = asyncio.ensure_future(collect(s, 0, 400))
        await asyncio.sleep(0.02)
        threads = [
            threading.Thread(target=lambda: [sync.append(b"x") for _ in range(100)])
            for _ in range(4)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        got = await asyncio.wait_for(task, 5)
        assert len(got) == 400  # nothing lost or duplicated, whatever the order
        assert got == await s.read(0)


async def test_slow_subscriber_buffer_is_bounded_by_bytes(monkeypatch):
    import durastream.aio

    monkeypatch.setattr(durastream.aio, "_LAG_BYTES", 1000)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)
        s = await store.create("t")
        it = s.subscribe(0)
        first = asyncio.ensure_future(anext(it))
        await s.append(b"0")
        assert await first == b"0"
        big = [bytes([i]) * 400 for i in range(1, 11)]  # 4 KB > 1000-byte budget
        reads = count_disk_reads(monkeypatch)
        await s.append_many(big)
        await asyncio.sleep(0)  # let the delivery land (and overflow)
        assert [await anext(it) for _ in range(10)] == big
        assert reads[0] >= 1  # the overflowed buffer was dropped: served from disk
        await it.aclose()


def test_new_event_loop_can_subscribe_after_one_died_mid_subscription():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = AsyncStore(root)

        async def leave_open():
            s = await store.create("t")
            it = s.subscribe(0)
            asyncio.ensure_future(anext(it))  # parked, never closed
            await asyncio.sleep(0.01)
            return it

        loop = asyncio.new_event_loop()
        leaked = loop.run_until_complete(leave_open())  # noqa: F841 - keep it open
        loop.close()

        async def subscribe_again():
            s = await store.open("t")
            it = s.subscribe(0)
            nxt = asyncio.ensure_future(anext(it))
            await asyncio.sleep(0.01)
            await s.append(b"x")
            return await asyncio.wait_for(nxt, 1)

        assert asyncio.run(subscribe_again()) == b"x"
