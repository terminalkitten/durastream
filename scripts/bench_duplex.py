"""LLM -> durastream -> SSE duplex benchmark.

Writers append LLM tokens at a steady rate; SSE-style readers tail each stream,
either polling (read + sleep) or pushed (AsyncDurableStream.subscribe), and some
disconnect and resume mid-stream. The batched writer uses AsyncBatchWriter. Each
scenario runs in its own child process, so CPU and memory don't carry over.

Run: make bench-duplex   (or uv run python scripts/bench_duplex.py [--quick])
"""

import argparse
import asyncio
import contextlib
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

RATE = 50  # tokens/s per chat, a typical LLM
POLL = 0.05  # polling readers: sleep between reads
BATCH_WINDOW = 0.02  # batched writer: AsyncBatchWriter's default window
DISCONNECT = 0.2  # a resuming client is gone this long

# name: (chats, readers per chat, batched writer, push readers)
SCENARIOS = {
    "single poll": (1, 1, False, False),
    "single push": (1, 1, False, True),
    "fan-out poll": (200, 3, False, False),
    "fan-out push": (200, 3, False, True),
    "fan-out push+batch": (200, 3, True, True),
}


def pct(data: list[float], p: int) -> float:
    if len(data) < 2:
        return data[0] if data else 0.0
    return statistics.quantiles(data, n=100, method="inclusive")[p - 1]


async def writer(s, tokens: int, batched: bool) -> int:
    """Emit `tokens` at RATE on an absolute schedule; returns number of appends."""
    from durastream import AsyncBatchWriter

    appends = 0
    append_many = s.append_many

    async def counted(batch: list[bytes]) -> int:
        nonlocal appends
        appends += 1
        return await append_many(batch)

    s.append_many = counted
    w = AsyncBatchWriter(s, window=BATCH_WINDOW) if batched else None
    start = time.perf_counter()
    for i in range(tokens):
        delay = start + i / RATE - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        tok = f"{i}:{time.perf_counter_ns()}".encode()
        if w is not None:
            w.write(tok)
        else:
            await s.append(tok)
            appends += 1
    if w is not None:
        await w.close()  # all tokens durable
    await s.close()
    return appends


def check(rec: bytes, expect: int, now: int, lat: list[float] | None) -> None:
    i, t = rec.split(b":")
    assert int(i) == expect, f"out of order: got {int(i)}, want {expect}"
    if lat is not None:  # replayed backlog is not live latency
        lat.append((now - int(t)) / 1e6)


async def poll_reader(
    s, tokens: int, lat: list[float], resume: list[float] | None
) -> None:
    """SSE loop: read from offset, sleep, repeat. With `resume`, disconnect halfway
    and reconnect from the saved offset, timing the first replayed read."""
    offset = 0
    disconnect_at = tokens // 2 if resume is not None else None
    reconnected_at = None
    while True:
        records = await s.read(offset)
        now = time.perf_counter_ns()
        replay = reconnected_at is not None
        if replay and records:
            assert resume is not None
            resume.append((time.perf_counter() - reconnected_at) * 1e3)
            reconnected_at = None
        for rec in records:
            check(rec, offset, now, None if replay else lat)
            offset += 1
        if s.closed and offset >= s.next_offset:
            break
        if disconnect_at is not None and offset >= disconnect_at:
            disconnect_at = None
            await asyncio.sleep(DISCONNECT)  # client gone; tokens pile up
            reconnected_at = time.perf_counter()
            continue
        await asyncio.sleep(POLL)
    assert offset == tokens, f"saw {offset} of {tokens} tokens"


async def push_reader(
    s, tokens: int, lat: list[float], resume: list[float] | None
) -> None:
    """subscribe(): records are pushed by the append. With `resume`, drop the
    subscription halfway and resubscribe from the saved offset."""
    offset = 0
    disconnect_at = tokens // 2 if resume is not None else None
    while True:
        reconnected_at = time.perf_counter() if offset else None
        live_from = s.next_offset if offset else 0  # below this is replay
        async with contextlib.aclosing(s.subscribe(offset)) as records:
            async for rec in records:
                if reconnected_at is not None:
                    assert resume is not None
                    resume.append((time.perf_counter() - reconnected_at) * 1e3)
                    reconnected_at = None
                replay = offset < live_from
                check(rec, offset, time.perf_counter_ns(), None if replay else lat)
                offset += 1
                if disconnect_at is not None and offset >= disconnect_at:
                    break
            else:
                break  # stream closed and drained
        disconnect_at = None
        await asyncio.sleep(DISCONNECT)  # client gone; tokens pile up
    assert offset == tokens, f"saw {offset} of {tokens} tokens"


async def lag_probe(samples: list[float], stop: asyncio.Event) -> None:
    """Event-loop lag: how late a 10 ms sleep wakes up."""
    while not stop.is_set():
        t = time.perf_counter()
        await asyncio.sleep(0.01)
        samples.append((time.perf_counter() - t - 0.01) * 1e3)


async def run(chats: int, readers: int, batched: bool, push: bool, tokens: int) -> dict:
    import durastream

    root = tempfile.mkdtemp()
    try:
        store = durastream.AsyncStore(root)
        streams = [await store.create(f"chat.{c}", "text/plain") for c in range(chats)]
        lat: list[float] = []
        resume: list[float] = []
        lag: list[float] = []
        stop = asyncio.Event()
        probe = asyncio.create_task(lag_probe(lag, stop))
        jobs = []
        for c, s in enumerate(streams):
            jobs.append(writer(s, tokens, batched))
            for r in range(readers):
                resuming = (c * readers + r) % 4 == 0  # a quarter of the readers
                read = push_reader if push else poll_reader
                jobs.append(read(s, tokens, lat, resume if resuming else None))
        cpu, t0 = time.process_time(), time.perf_counter()
        results = await asyncio.gather(*jobs)
        wall, cpu = time.perf_counter() - t0, time.process_time() - cpu
        stop.set()
        await probe
        await store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {
        "p50": statistics.median(lat),
        "p99": pct(lat, 99),
        "max": max(lat),
        "cpu_per_1k": cpu * 1e3 / (chats * tokens / 1000),
        "lag_p99": pct(lag, 99),
        "resume_p50": statistics.median(resume) if resume else 0.0,
        "appends": sum(r for r in results if isinstance(r, int)),
        "wall_ratio": wall / (tokens / RATE),
    }


def child(name: str, quick: bool) -> None:
    chats, readers, batched, push = SCENARIOS[name]
    tokens = 100 if quick else 200
    if quick:
        chats = max(1, chats // 2)
    print(json.dumps(asyncio.run(run(chats, readers, batched, push, tokens))))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--quick", action="store_true", help="half the chats and tokens")
    ap.add_argument("--child", help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.child:
        return child(args.child, args.quick)

    print(
        f"{RATE} tok/s per chat, SSE poll {POLL * 1e3:.0f} ms; latency = token produced -> read by client\n"
    )
    hdr = (
        f"{'scenario':20} {'p50 ms':>7} {'p99 ms':>7} {'max ms':>7} "
        f"{'cpu ms/1k':>9} {'lag p99':>7} {'resume':>7} {'appends':>7} {'wall/ideal':>10}"
    )
    print(hdr)
    print("-" * len(hdr))
    for name in SCENARIOS:
        cmd = [sys.executable, __file__, "--child", name]
        out = subprocess.run(
            cmd + (["--quick"] if args.quick else []),
            capture_output=True,
            text=True,
            check=True,
        )
        m = json.loads(out.stdout.strip().splitlines()[-1])
        print(
            f"{name:20} {m['p50']:7.1f} {m['p99']:7.1f} {m['max']:7.1f} "
            f"{m['cpu_per_1k']:9.1f} {m['lag_p99']:7.1f} {m['resume_p50']:7.1f} "
            f"{m['appends']:7,} {m['wall_ratio']:10.2f}"
        )


if __name__ == "__main__":
    main()
