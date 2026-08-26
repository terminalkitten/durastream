import sys
import time

from utils import append_range, temp_store

from durastream import Store

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
SESSIONS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
BATCH = 10_000


def record(i: int) -> bytes:
    return b"sensor-%d-%d" % (i, 20 + i % 100)


def main() -> None:
    per_session = N // SESSIONS
    print(f"ingesting {N:,} records across {SESSIONS} stop/start sessions\n")

    with temp_store() as (root, setup):
        setup.create("events", "text/plain")  # create the stream once
        setup.close()

        produced = 0
        for s in range(SESSIONS):
            t0 = time.perf_counter()
            store = Store(root)  # mimic fresh process
            stream = store.open("events")
            resume = stream.next_offset
            scan = time.perf_counter() - t0  # reopen: full-log recovery scan
            assert resume == produced, f"resumed at {resume}, expected {produced}"

            # last session absorbs the remainder
            end = N if s == SESSIONS - 1 else produced + per_session
            append_range(stream, produced, end, record, BATCH)
            added = end - produced
            produced = end
            store.close()  # stop

            dt = time.perf_counter() - t0
            print(
                f"session {s + 1}/{SESSIONS}: resumed @ {resume:>10,}  "
                f"reopen-scan {scan:5.2f}s  +{added:,} in {dt:5.2f}s  "
                f"-> next_offset {produced:,}"
            )

        # final restart: verify all survived
        t0 = time.perf_counter()
        stream = Store(root).open("events")
        scan = time.perf_counter() - t0
        assert stream.next_offset == produced == N
        sample = stream.read(produced - 1, produced)  # last record
        print(
            f"\nfinal reopen: next_offset {stream.next_offset:,} in {scan:.2f}s "
            f"(scanned + CRC-verified the whole log)"
        )
        print(f"last record: {sample[0]!r}")
        print(f"durable OK - all {produced:,} records survived {SESSIONS} restarts.")


if __name__ == "__main__":
    main()
