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

Requires Python 3.12+. No runtime dependencies.

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
stream.read(0)  # [b"order-1", b"order-2"]  recovered from the log
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

The generator returns once the stream is closed and the consumer has caught up.

## Async

`AsyncStore` mirrors the sync API with the same names, awaitable:

```python
from durastream import AsyncStore

store = AsyncStore("./data")
stream = await store.create("chat")

await stream.append(b"hello ")
await stream.read(0)  # [b"hello "]

async for record in stream.subscribe(0):  # replay, then tail (poll-based)
    print(record)
```


## Demo

`demos/bulk_stream.py` bulk-streams 100k JSON readings through one stream while a
second thread tails them live, then reopens the store from disk to prove the data
survived a restart:

```bash
make demo
```

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

CRC is `zlib.crc32` (CRC-32/ISO-HDLC). One writer
per stream is serialized by an in-process lock; SQLite runs in WAL mode.

## Concurrency

dura.stream is a **single-process** engine. Within one process it is fully
concurrent: many threads or coroutines, many streams, or many writers into one
shared stream are all coordinated by a per-stream lock, so offsets stay
consistent and no data is lost (see `make demo-concurrent`). Use `AsyncStore`
from async code.

Across **separate processes** the rule is **one writer per stream**. The lock is
an in-memory `threading.Lock`, so it cannot coordinate two processes: if two
processes write the same stream they each keep their own offset index and the
offsets diverge silently. Route each stream to a single owning process. Concurrent readers in other processes are fine; they re-open to pick up new records.

## Develop

```bash
make test        # pytest (sync + async)
make lint        # ruff format + ty typecheck
```
