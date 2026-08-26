import json
import os

from utils import Timer, append_range, jdump, temp_store

from durastream import Store

N = 50_000
BATCH = 1_000
CHUNK = 1_000


def read_checkpoint(path: str) -> int:
    try:
        with open(path) as f:
            return int(f.read())
    except FileNotFoundError:
        return 0


def write_checkpoint(path: str, offset: int) -> None:
    # atomic: write a temp file then rename old checkpoint
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(offset))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def run_worker(
    store: Store, ckpt: str, seen: set, stop_after: int | None = None
) -> int:
    """Drain jobs from the checkpoint"""
    q = store.open("jobs")
    processed = 0
    offset = read_checkpoint(ckpt)
    while offset < q.next_offset:
        chunk = q.read(offset, offset + CHUNK)
        for raw in chunk:
            seen.add(json.loads(raw)["id"])
            processed += 1
            if stop_after is not None and processed >= stop_after:
                return processed  # crash: this chunk checkpoint never get's writen
        offset += len(chunk)
        write_checkpoint(ckpt, offset)  # commit only after a full chunk
    return processed


def main() -> None:
    with temp_store() as (root, store):
        ckpt = os.path.join(root, "worker.offset")

        # enqueue
        q = store.create("jobs", "application/json")
        with Timer() as t:
            append_range(
                q, 0, N, lambda i: jdump({"id": i, "task": f"resize-{i}"}), BATCH
            )
        print(f"enqueued {N:,} jobs in {t.s:.2f}s -> {N / t.s:,.0f} jobs/s")

        # worker runs
        seen: set = set()
        done = run_worker(store, ckpt, seen, stop_after=int(N * 0.6))
        print(
            f"worker CRASHED after {done:,} jobs; checkpoint @ {read_checkpoint(ckpt):,}"
        )

        # restart
        done += run_worker(store, ckpt, seen)
        print("worker restarted, resumed @ checkpoint, finished all")

        assert seen == set(range(N)), "some jobs were never processed"
        reprocessed = done - N
        print(
            f"processed {done:,} (unique {len(seen):,}), {reprocessed:,} reprocessed "
            f"after the crash -> at-least-once, no job lost."
        )


if __name__ == "__main__":
    main()
