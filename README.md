<p align="center">
  <img src="https://raw.githubusercontent.com/terminalkitten/durastream/main/docs/assets/dura-stream-logo.png" alt="dura.stream" width="180">
</p>

# dura.stream

Minimal durable streaming on local disk. Append-only, crash-safe, tailable streams
you import into any Python app, no server, no runtime dependencies.

## Install

```bash
uv add durastream
```

Requires Python 3.12+. No runtime dependencies. Linux and macOS wheels include a
compiled Rust core for speed; everywhere else (Windows, PyPy) the same package runs in
pure Python. The API, behaviour and on-disk format are identical either way.

```python
import durastream

durastream.ENGINE  # "native" (Rust core) or "python"
```

Set `DURASTREAM_PURE=1` to force the pure-Python engine.

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

`subscribe` wakes the moment a record is appended in this process, from any
thread; `await stream.wait(offset)` is the same wake-up for your own read loop
(see `demos/fastapi_resume.py`). For LLM output, `AsyncBatchWriter` groups tokens
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

Output (numbers vary by machine and engine):

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

You need [uv](https://docs.astral.sh/uv/) and a Rust toolchain ([rustup](https://rustup.rs)).

```bash
git clone git@github.com:terminalkitten/durastream.git && cd durastream
uv sync          # .venv + dev deps, compiles the Rust core (release mode)
```

`uv run` / `uv sync` recompile the Rust core automatically when anything under `core/`
changes, so there is no separate build step while developing.

Layout:

```
src/durastream/   Python API + pure-Python engine; _engine.py picks the engine at import
core/             Rust core + PyO3 bindings, compiled to durastream/core.abi3.so
tests/            one test suite, run against both engines
scripts/bench.py  pure Python vs native benchmark
```

### Everyday commands

```bash
make check       # lint + test: what CI runs, run this before pushing
make full        # format, check, bench, build, in that order (edits files!)
```

### Test

Both engines must pass the same `tests/`; a behaviour difference is a bug.

```bash
make test         # all three below
make test-rust    # cargo test (core/tests)
make test-native  # pytest on the Rust core; fails if it didn't load
make test-pure    # pytest on the pure-Python engine (DURASTREAM_PURE=1)
make test-diskfull  # disk-full rollback on both engines, real ENOSPC (needs Docker)
```

`tests/test_disk_full.py` fills a tiny file system so an append is half-written
when the disk runs out, and checks the log rolls back to its last intact record.
It skips unless `DURASTREAM_SMALL_FS` points at a small tmpfs; `make test-diskfull`
provides one in Docker, and CI mounts one on its Linux runners.

Run a single test, per engine:

```bash
uv run pytest -q tests/test_sync.py::test_tail
DURASTREAM_PURE=1 uv run pytest -q tests/test_sync.py::test_tail
```

### Lint and format

```bash
make lint        # check only: ty, ruff check, ruff format --check, cargo fmt --check, clippy
make format      # apply fixes: ruff --fix, ruff format, cargo fmt
make typecheck   # ty only
```

### Benchmark

```bash
make bench                              # 100k records, median of 9 runs (~1 min)
uv run python scripts/bench.py --quick  # 20k records, 3 runs
make bench-duplex                       # LLM -> stream -> SSE clients, per engine (~1 min)
```

`make bench-duplex` models an LLM/SSE app: chats append tokens at 50 tok/s while
SSE-style clients tail them (some disconnect and resume). It reports token latency
(produced -> seen by a client), CPU per 1k tokens, event-loop lag and resume time.

It first checks that both engines read each other's files, then times each case on
both. Apple M-series:

| case | python | native | speedup |
|---|---|---|---|
| `append()` x1000 (fsync each) | 37.2ms | 34.6ms | 1.1x |
| `append()` 4 threads x500 | 41.8ms | 33.9ms | 1.2x |
| `append_many` small x100k | 37.0ms | 10.5ms | 3.5x |
| `append_many` 4KiB x10k | 22.2ms | 17.7ms | 1.3x |
| `read(0)` small x100k | 28.2ms | 3.3ms | 8.6x |
| `read(0)` 4KiB x10k | 11.9ms | 11.5ms | 1.0x |
| `read(o, o+10)` x10k | 50.5ms | 9.9ms | 5.1x |
| reopen + recover 100k | 62.3ms | 1.9ms | 33x |
| `subscribe` drain 100k | 31.6ms | 4.4ms | 7.1x |
| `subscribe` live 100k | 70.9ms | 11.2ms | 6.3x |

fsync-bound work runs at disk speed on both engines. The native engine wins on per-record work.

### Build

```bash
make build
```

writes to `dist/`:

| file | what |
|---|---|
| `durastream-<v>-cp312-abi3-<platform>.whl` | Rust core included, for this machine only |
| `durastream-<v>-py3-none-any.whl` | pure Python, the fallback for all other platforms |
| `durastream-<v>.tar.gz` | sdist; installing it compiles the Rust core |

Wheels for other platforms are built by CI on release. To try a built wheel in a clean venv:

```bash
# --no-cache: local rebuilds keep the same filename, uv would reuse a stale copy
uv venv --clear /tmp/ds && VIRTUAL_ENV=/tmp/ds uv pip install --no-cache --no-index --find-links dist durastream
/tmp/ds/bin/python -c "import durastream; print(durastream.ENGINE)"   # native
```

### CI and release

`ci.yml` runs on every push to `main` and every PR: `make lint` plus `cargo deny`,
`make test` on Ubuntu and macOS with Python 3.12 and 3.14 (plus the disk-full test on
Ubuntu), and the pure-Python wheel on Windows.

To release, bump `version` in `pyproject.toml` (the only version), commit, then:

```bash
git tag v<version> && git push origin main v<version>
```

`release.yml` then builds:

- native wheels for Linux (glibc and musl, x86_64 and aarch64) and macOS (x86_64 and arm64);
- the pure-Python wheel;
- the sdist.

It installs each wheel on its own platform and runs the test suite there. Only if all
pass does it publish to PyPI via trusted publishing (GitHub environment `pypi`). A
version can only be published once, so fix and bump rather than re-tag.
