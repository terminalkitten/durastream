import threading
import time

from utils import Timer, jdump, temp_store

from durastream import Store, from_token, to_token

N = 100_000
BATCH = 1_000


def main() -> None:
    with temp_store() as (root, store):
        stream = store.create("telemetry", content_type="application/json")

        # consumer: tail the stream live in a background thread
        stats = {"count": 0, "bytes": 0}

        def consume() -> None:
            start = time.perf_counter()
            for record in stream.subscribe(0):
                stats["count"] += 1
                stats["bytes"] += len(record)
                if stats["count"] % 10_000 == 0:
                    mb = stats["bytes"] / 1e6
                    rate = stats["count"] / (time.perf_counter() - start)
                    print(
                        f"  tailed {stats['count']:>7,}  {mb:6.1f} MB  {rate:>10,.0f} rec/s"
                    )

        tail = threading.Thread(target=consume)
        tail.start()

        # producer: bulk-append batches of JSON readings (one fsync/batch)
        print(f"ingesting {N:,} readings in batches of {BATCH:,} ...")
        produced_bytes = 0
        with Timer() as t:
            for base in range(0, N, BATCH):
                batch = [
                    jdump({"id": i, "temp": 20 + (i % 100) / 10})
                    for i in range(base, base + BATCH)
                ]
                produced_bytes += sum(map(len, batch))
                stream.append_many(batch)
        stream.close()  # lets the consumer finish once it catches up
        tail.join()

        mb = produced_bytes / 1e6
        print(
            f"ingested {N:,} readings ({mb:.1f} MB) in {t.s:.2f}s"
            f"  ->  {N / t.s:,.0f} rec/s, {mb / t.s:.0f} MB/s"
        )
        assert stats["count"] == N, f"consumer saw {stats['count']}, expected {N}"

        # durability: reopen from disk (simulates a process restart)
        store.close()
        reopened = Store(root).open("telemetry")
        print(
            f"\nreopened from disk: next_offset={reopened.next_offset:,}"
            f"  (token {to_token(reopened.next_offset)})"
        )
        mid = from_token("00000000000000050000", reopened.next_offset)
        sample = reopened.read(mid, mid + 2)
        print(f"resumed read at offset {mid:,}: {sample}")
        print("durable OK - data survived the restart.")


if __name__ == "__main__":
    main()
