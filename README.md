<p align="center">
  <img src="https://raw.githubusercontent.com/terminalkitten/durastream/main/docs/assets/dura-stream-logo.png" alt="dura.stream" width="180">
</p>

# dura.stream

Minimal durable streaming on local disk. Append-only, crash-safe, tailable streams
you import into any Python app, no server, no dependencies: stdlib only.

## Install

```bash
uv add durastream
```

Requires Python 3.12+. No runtime dependencies: pure Python, stdlib only.

## Quick start

```python
from durastream import Store

store = Store("./data")
stream = store.create("orders", content_type="text/plain")

stream.append(b"order-1")  # -> 1  (new next_offset)
stream.append(b"order-2")  # -> 2
stream.append_many([b"o-3", b"o-4"])  # -> 4  batch: one fsync for the whole list

stream.read(0)  # [b"order-1", b"order-2", b"o-3", b"o-4"]  all records from offset 0
stream.read(1)  # [b"order-2", b"o-3", b"o-4"]              from offset 1 to tail
stream.read(0, 1)  # [b"order-1"]                           half-open [start, end)

stream.next_offset  # 4                                     record count
stream.content_type  # "text/plain"
```

Durability is per-flush: `append()` writes a length+CRC-framed record and
`fsync`s before returning; `append_many()` writes the whole list in one `fsync`
(much faster for bulk ingest, same durability guarantee once it returns). After
a crash, reopening the store rebuilds state by scanning the log, a torn or
corrupt tail record is dropped, the intact prefix survives.

```python
store2 = Store("./data")
stream = store2.open("orders")
stream.read(0)  # [b"order-1", b"order-2", b"o-3", b"o-4"]  recovered from the log
```

## Tailing (`tail -f`)

`subscribe()` yields existing records from an offset, then blocks and yields new
ones as they're appended:

```python
import threading


def worker():
    for record in stream.subscribe(0):
        print("got", record)


threading.Thread(target=worker, daemon=True).start()
stream.append(b"live-1")
```

The iterator ends once the stream is closed and the consumer has caught up.

## Async

`AsyncStore` mirrors the sync API with the same names, awaitable:

```python
from durastream import AsyncStore

store = AsyncStore("./data")
stream = await store.create("chat")

await stream.append(b"hello ")
await stream.read(0)  # [b"hello "]

async for record in stream.subscribe(0):  # replay, then tail (pushed, no polling)
    print(record)
```

`subscribe` gets each record the moment it is appended in this process, from any
thread, handed over by the append itself (no polling, no disk read), so it stays
cheap with many clients per stream. `await stream.wait(offset)` is the same
wake-up for your own read loop (see `demos/fastapi_resume.py`). For LLM output, `AsyncBatchWriter` groups tokens
into one fsync per 20 ms window without weakening durability: readers only ever
see records that are already on disk.

```python
from durastream import AsyncBatchWriter

async with AsyncBatchWriter(stream) as w:
    async for token in llm_tokens():
        w.write(token)  # returns at once
# every token is durable here
await stream.close()
```


## Demo

`demos/bulk_stream.py` bulk-streams 100k JSON readings through one stream while a
second thread tails them live, then reopens the store from disk to prove the data
survived a restart:

```bash
make demo
```

Output (numbers vary by machine):

```
ingesting 100,000 readings in batches of 1,000 ...
  tailed  10,000     0.5 MB     409,514 rec/s
  ...
ingested 100,000 readings (5.1 MB) in 0.22s  ->  447,142 rec/s, 23 MB/s
reopened from disk: next_offset=100,000  (DS token 00000000000000100000)
resumed read at offset 50,000: [b'{"id": 50000, ...}', b'{"id": 50001, ...}']
durable OK - data survived the restart.
```

`demos/append_vs_batch.py` (`make demo-bench`) contrasts `append()` (one fsync per
record) with `append_many()` (one fsync per batch). Same durability, ~27x faster
here (more on platforms with a costlier `fsync`).

`demos/restart_stream.py` (`make demo-restart`) ingests 10M records across 5
stop/start sessions, resuming at the persisted offset each time. Proof the log
survives repeated restarts (10M in ~12s here). It also prints the reopen scan
time per session, which grows linearly with log size (the O(n) recovery cost a
persisted index would remove).

`demos/work_queue.py` (`make demo-queue`) is a durable job queue: a worker
consumes jobs, checkpoints its offset to a file, "crashes" at 60%, then restarts
and resumes from the checkpoint. Every job processed at least once, none lost.

`demos/ledger.py` (`make demo-ledger`) is an event-sourced bank account: it
appends deposit/withdraw events, then reopens and rebuilds the balance purely by
replaying the log (`read(0)`), including a point-in-time balance query. The log
is the source of truth; state is derived.

`demos/concurrent_users.py` (`make demo-concurrent`) runs 40 concurrent users on
one `AsyncStore` in a single process: first a stream per user (no cross-talk),
then all 40 writing into one shared stream while a consumer tails the interleaved
firehose (nothing lost). In-process concurrency is fully coordinated by the
per-stream lock; cross-process writes to the same stream need a single writer.

## Closing & deleting

```python
stream.close()  # no more appends; reads still work
stream.append(b"x")  # raises StreamClosed
stream.closed  # True (persisted)

store.delete("orders")  # removes the log file + metadata row
store.list()  # ["other-stream", ...]
```

Stream names are lowercase letters, digits, `.`, `_` and `-` (so two names can never
share a file on a case-insensitive disk). All errors derive from `DurastreamError`:
`StreamClosed`, `StreamLocked` (another process owns the stream) and `CorruptStream`
(bytes on disk fail their checksum).

## Offsets

Offset = logical record index (0-based). `next_offset` is the record count and the
position the next append lands at. Helpers convert to/from the DS wire token format:

```python
from durastream import to_token, from_token

to_token(1)  # "00000000000000000001"
from_token("-1", next_offset)  # 0            (start of stream)
from_token("now", next_offset)  # next_offset  (current tail)
from_token("00000000000000000003", next_offset)  # 3
```

## On-disk layout

```
data/
  meta.db                 SQLite: name, content_type, closed, created_at
  streams/
    orders.log            append-only frames: [u32 len][u32 crc32][payload]...
```

CRC is CRC-32/ISO-HDLC (the same as `zlib.crc32`). Writes to a stream are
serialized by an in-process lock; SQLite runs in WAL mode.

## Concurrency

dura.stream is a **single-process** engine. Within one process it is fully
concurrent: many threads or coroutines, many streams, or many writers into one
shared stream are all coordinated by a per-stream lock, so offsets stay
consistent and no data is lost (see `make demo-concurrent`). Use `AsyncStore`
from async code.

Across **separate processes** the rule is **one writer per stream**, and it is
enforced: the first process to open a stream takes an OS file lock (`flock`) on its
log. Other processes get a read-only view (`stream.writable` is `False`): reads
work, `append`/`close`/`delete` raise `StreamLocked`, and they never touch the
file. A read-only view is a snapshot; open it again from a new `Store` to see new
records. The lock is released when the writer closes the store or exits (even on a
crash). A forked child never writes through its parent's streams; open stores after
forking. On Windows there is no `flock`, so the rule is not enforced there.

## Develop

You need [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:terminalkitten/durastream.git && cd durastream
uv sync          # .venv + dev deps
```

Layout:

```
src/durastream/          the package: storage engine, async API, fan-out hub
tests/                   test suite
scripts/bench_duplex.py  LLM -> stream -> SSE clients benchmark
demos/                   runnable examples (make demo, make demo-serve, ...)
```

### Everyday commands

```bash
make check       # lint + test: what CI runs, run this before pushing
make full        # format, check, bench, build, in that order (edits files!)
```

### Test

```bash
make test           # pytest
make test-diskfull  # disk-full rollback against a real ENOSPC (needs Docker)
uv run pytest -q tests/test_sync.py::test_tail   # a single test
```

`tests/test_disk_full.py` fills a tiny file system so an append is half-written
when the disk runs out, and checks the log rolls back to its last intact record.
It skips unless `DURASTREAM_SMALL_FS` points at a small tmpfs; `make test-diskfull`
provides one in Docker, and CI mounts one on its Linux runners.

### Lint and format

```bash
make lint        # check only: ty, ruff check, ruff format --check
make format      # apply fixes: ruff --fix, ruff format
make typecheck   # ty only
```

### Benchmark

```bash
make bench                                     # ~1 min
uv run python scripts/bench_duplex.py --quick  # half the chats and tokens
```

Models an LLM/SSE app: chats append tokens at 50 tok/s while SSE-style clients tail
them with `subscribe`, some disconnecting and resuming. Latency is from token
produced to seen by a client. Apple M-series:

| scenario | p50 | p99 | CPU per 1k tokens |
|---|---|---|---|
| 1 chat | 0.4 ms | 1.6 ms | 670 ms |
| 50 chats | 1.0 ms | 8.2 ms | 179 ms |
| 200 chats × 3 clients | 4.8 ms | 13.8 ms | 128 ms |
| same, `AsyncBatchWriter` | 22.7 ms | 38.5 ms | 79 ms |

### Build

```bash
make build       # wheel + sdist into dist/
```

### CI and release

`ci.yml` runs on every push to `main` and every PR: `make lint`, and the tests on
Ubuntu, macOS and Windows with Python 3.12 and 3.14 (plus the disk-full test on
Ubuntu).

To release, bump `version` in `pyproject.toml` (the only version), commit, then:

```bash
git tag v<version> && git push origin main v<version>
```

`release.yml` builds the wheel and sdist, installs the wheel on Linux, macOS and
Windows and runs the test suite there, and only then publishes to PyPI via trusted
publishing (GitHub environment `pypi`). A version can only be published once, so
fix and bump rather than re-tag.

An experimental Rust engine for durastream (same API and on-disk format, ~10x
faster recovery and replay) was built and then set aside to keep this package pure
Python. It lives on in
[terminalkitten/durastream-core](https://github.com/terminalkitten/durastream-core).
