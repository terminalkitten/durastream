"""durastream engines: pure Python vs native durastream.core (Rust + PyO3), same API, same disk format.

Run: make bench   (or uv run python scripts/bench.py [--quick])
Each case runs REPEAT times on a fresh temp store; median wall time is reported.
"""

import json
import random
import statistics
import sys
import tempfile
import threading
import time

from durastream import core as native
from durastream import store as pure

IMPLS = {"python": pure, "native": native}
QUICK = "--quick" in sys.argv
REPEAT = 3 if QUICK else 9
N = 20_000 if QUICK else 100_000  # records for bulk cases
N_SYNC = 200 if QUICK else 1_000  # records for fsync-per-record cases
BATCH = 1_000


def small(i: int) -> bytes:
    return json.dumps({"id": i, "temp": 20 + (i % 100) / 10}).encode()


BIG = bytes(random.Random(0).randbytes(4096))

PAYLOADS = {
    "small": [small(i) for i in range(N)],
    "4KiB": [BIG] * (N // 10),
}


def fill(stream, payloads):
    for b in range(0, len(payloads), BATCH):
        stream.append_many(payloads[b : b + BATCH])


# Each case: (setup(mod, root) -> ctx, run(ctx) -> records processed)


def case_append_single(mod, root):
    s = mod.Store(root).create("s")
    recs = PAYLOADS["small"][:N_SYNC]
    return lambda: [s.append(r) for r in recs] and len(recs)


def case_append_many(kind):
    def setup(mod, root):
        s = mod.Store(root).create("s")
        recs = PAYLOADS[kind]
        return lambda: fill(s, recs) or len(recs)

    return setup


def case_read_all(kind):
    def setup(mod, root):
        s = mod.Store(root).create("s")
        fill(s, PAYLOADS[kind])
        return lambda: len(s.read(0))

    return setup


def case_read_slices(mod, root):
    s = mod.Store(root).create("s")
    fill(s, PAYLOADS["small"])
    rng = random.Random(1)
    starts = [rng.randrange(N - 10) for _ in range(10_000)]
    return lambda: sum(len(s.read(o, o + 10)) for o in starts)


def case_recover(mod, root):
    store = mod.Store(root)
    fill(store.create("s"), PAYLOADS["small"])
    store.close()
    return lambda: mod.Store(root).open("s").next_offset


def case_subscribe(mod, root):
    s = mod.Store(root).create("s")
    fill(s, PAYLOADS["small"])
    s.close()  # closed stream: subscribe drains then stops
    return lambda: sum(1 for _ in s.subscribe(0))


def case_tail_live(mod, root):
    """Producer appends batches while a consumer thread tails live."""
    s = mod.Store(root).create("s")
    recs = PAYLOADS["small"]

    def run():
        seen = [0]

        def consume():
            for _ in s.subscribe(0):
                seen[0] += 1

        th = threading.Thread(target=consume)
        th.start()
        fill(s, recs)
        s.close()
        th.join()
        assert seen[0] == len(recs), seen[0]
        return seen[0]

    return run


def case_concurrent(mod, root):
    """4 threads, one stream each, fsync per record: shows GIL release around IO."""
    store = mod.Store(root)
    streams = [store.create(f"s{i}") for i in range(4)]
    recs = PAYLOADS["small"][: N_SYNC // 2]

    def run():
        ths = [
            threading.Thread(target=lambda s=s: [s.append(r) for r in recs])
            for s in streams
        ]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        return len(recs) * len(streams)

    return run


CASES = [
    (f"append() x{N_SYNC}  (fsync each)", case_append_single),
    (f"append() 4 threads x{N_SYNC // 2}", case_concurrent),
    (f"append_many small x{N:,}", case_append_many("small")),
    (f"append_many 4KiB x{N // 10:,}", case_append_many("4KiB")),
    (f"read(0) small x{N:,}", case_read_all("small")),
    (f"read(0) 4KiB x{N // 10:,}", case_read_all("4KiB")),
    ("read(o, o+10) x10,000", case_read_slices),
    (f"reopen+recover x{N:,}", case_recover),
    (f"subscribe drain x{N:,}", case_subscribe),
    (f"subscribe live x{N:,}", case_tail_live),
]


def bench(setup):
    """Median seconds per impl; impls interleaved per repeat so disk/thermal drift hits both."""
    times = {k: [] for k in IMPLS}
    for _ in range(REPEAT):
        for key, mod in IMPLS.items():
            with tempfile.TemporaryDirectory() as root:
                run = setup(mod, root)
                t0 = time.perf_counter()
                n = run()
                times[key].append(time.perf_counter() - t0)
    return statistics.median(times["python"]), statistics.median(times["native"]), n


def check_compat():
    """Same disk format both ways: each impl reads what the other wrote."""
    for w, r in (("python", "native"), ("native", "python")):
        with tempfile.TemporaryDirectory() as root:
            st = IMPLS[w].Store(root)
            s = st.create("x", "application/json")
            s.append_many(PAYLOADS["small"][:100])
            s.close()
            st.close()
            s2 = IMPLS[r].Store(root).open("x")
            assert s2.read(0) == PAYLOADS["small"][:100], (w, r)
            assert s2.closed and s2.content_type == "application/json"
    print("disk-format compat: python <-> native OK\n")


def main():
    check_compat()
    print(f"median of {REPEAT} runs\n")
    hdr = f"{'case':34} {'python':>10} {'native':>10} {'py rec/s':>12} {'nat rec/s':>12} {'speedup':>8}"
    print(hdr)
    print("-" * len(hdr))
    for name, setup in CASES:
        tp, tr, n = bench(setup)
        print(
            f"{name:34} {tp * 1e3:8.1f}ms {tr * 1e3:8.1f}ms"
            f" {n / tp:12,.0f} {n / tr:12,.0f} {tp / tr:7.1f}x"
        )


if __name__ == "__main__":
    main()
