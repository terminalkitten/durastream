"""LLM -> durastream -> SSE duplex benchmark: pure Python vs native engine.

Writers append LLM tokens at a steady rate; SSE-style readers tail each stream
with the read + sleep loop from demos/fastapi_resume.py, and some disconnect and
resume mid-stream. Each scenario runs in a child process per engine (the engine
is picked at import), so the whole app runs on one engine, as in production.

Run: make bench-duplex   (or uv run python scripts/bench_duplex.py [--quick])
"""

import argparse
import asyncio
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

RATE = 50  # tokens/s per chat, a typical LLM
POLL = 0.05  # SSE loop sleep, as in demos/fastapi_resume.py
BATCH_WINDOW = 0.05  # batched writer: one append_many per window
DISCONNECT = 0.2  # a resuming client is gone this long

# name: (chats, readers per chat, batched writer)
SCENARIOS = {
    "single": (1, 1, False),
    "50 chats": (50, 1, False),
    "fan-out": (200, 3, False),
    "fan-out batched": (200, 3, True),
}


def pct(data: list[float], p: int) -> float:
    if len(data) < 2:
        return data[0] if data else 0.0
    return statistics.quantiles(data, n=100, method="inclusive")[p - 1]


async def writer(s, tokens: int, batched: bool) -> int:
    """Emit `tokens` at RATE on an absolute schedule; returns number of appends."""
    start = time.perf_counter()
    buf: list[bytes] = []
    flushed = start
    appends = 0
    for i in range(tokens):
        delay = start + i / RATE - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        tok = f"{i}:{time.perf_counter_ns()}".encode()
        if not batched:
            await s.append(tok)
            appends += 1
            continue
        buf.append(tok)
        if time.perf_counter() - flushed >= BATCH_WINDOW:
            await s.append_many(buf)
            buf, flushed, appends = [], time.perf_counter(), appends + 1
    if buf:
        await s.append_many(buf)
        appends += 1
    await s.close()
    return appends


async def reader(s, tokens: int, lat: list[float], resume: list[float] | None) -> None:
    """SSE loop: read from offset, sleep, repeat. With `resume`, disconnect halfway
    and reconnect from the saved offset, timing the first replayed read."""
    offset = expect = 0
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
            i, t = rec.split(b":")
            assert int(i) == expect, f"out of order: got {int(i)}, want {expect}"
            expect += 1
            if not replay:  # replayed backlog is not live latency
                lat.append((now - int(t)) / 1e6)
        offset += len(records)
        if s.closed and offset >= s.next_offset:
            break
        if disconnect_at is not None and offset >= disconnect_at:
            disconnect_at = None
            await asyncio.sleep(DISCONNECT)  # client gone; tokens pile up
            reconnected_at = time.perf_counter()
            continue
        await asyncio.sleep(POLL)
    assert expect == tokens, f"saw {expect} of {tokens} tokens"


async def lag_probe(samples: list[float], stop: asyncio.Event) -> None:
    """Event-loop lag: how late a 10 ms sleep wakes up."""
    while not stop.is_set():
        t = time.perf_counter()
        await asyncio.sleep(0.01)
        samples.append((time.perf_counter() - t - 0.01) * 1e3)


async def run(chats: int, readers: int, batched: bool, tokens: int) -> dict:
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
                jobs.append(reader(s, tokens, lat, resume if resuming else None))
        cpu, t0 = time.process_time(), time.perf_counter()
        results = await asyncio.gather(*jobs)
        wall, cpu = time.perf_counter() - t0, time.process_time() - cpu
        stop.set()
        await probe
        await store.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return {
        "engine": durastream.ENGINE,
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
    chats, readers, batched = SCENARIOS[name]
    tokens = 100 if quick else 200
    if quick:
        chats = max(1, chats // 2)
    print(json.dumps(asyncio.run(run(chats, readers, batched, tokens))))


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
        f"{'scenario':16} {'engine':7} {'p50 ms':>7} {'p99 ms':>7} {'max ms':>7} "
        f"{'cpu ms/1k':>9} {'lag p99':>7} {'resume':>7} {'appends':>7} {'wall/ideal':>10}"
    )
    print(hdr)
    print("-" * len(hdr))
    for name in SCENARIOS:
        rows = {}
        for engine, pure in (("python", "1"), ("native", "")):
            env = {**os.environ, "DURASTREAM_PURE": pure}
            cmd = [sys.executable, __file__, "--child", name] + (
                ["--quick"] if args.quick else []
            )
            out = subprocess.run(
                cmd, env=env, capture_output=True, text=True, check=True
            )
            m = json.loads(out.stdout.strip().splitlines()[-1])
            assert m["engine"] == engine, f"expected {engine}, got {m['engine']}"
            rows[engine] = m
            print(
                f"{name:16} {engine:7} {m['p50']:7.1f} {m['p99']:7.1f} {m['max']:7.1f} "
                f"{m['cpu_per_1k']:9.1f} {m['lag_p99']:7.1f} {m['resume_p50']:7.1f} "
                f"{m['appends']:7,} {m['wall_ratio']:10.2f}"
            )
        py, nat = rows["python"], rows["native"]
        print(
            f"{'':16} native vs pure: p99 {py['p99'] / nat['p99']:.1f}x, "
            f"cpu {py['cpu_per_1k'] / nat['cpu_per_1k']:.1f}x\n"
        )


if __name__ == "__main__":
    main()
