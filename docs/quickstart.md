# Quick start

## Install

```bash
uv add durastream
```

Requires Python 3.12+. No runtime dependencies.

## Write and read

```python
from durastream import Store

store = Store("./data")
stream = store.create("orders", content_type="text/plain")

stream.append(b"order-1")            # -> 1  (new next_offset)
stream.append_many([b"a", b"b"])     # -> 3  one fsync for the whole batch

stream.read(0)      # [b"order-1", b"a", b"b"]   from an offset
stream.read(1, 2)   # [b"a"]                     half-open [start, end)
stream.next_offset  # 3
```

`append()` frames the record with a length and CRC, then `fsync`s before it
returns, so the record is durable the moment the call completes. `append_many()`
does one `fsync` for the whole batch, which is much faster for bulk writes with
the same guarantee.

## Recover after a crash

Reopen the store and the log is rebuilt by scanning it. A torn or corrupt tail
record is dropped, the intact prefix survives.

```python
store2 = Store("./data")
stream = store2.open("orders")
stream.read(0)      # the records that were durably written
```

## Tail like `tail -f`

`subscribe()` yields existing records from an offset, then blocks and yields new
ones as they are appended.

```python
for record in stream.subscribe(0):   # replay, then follow live
    print(record)
```

## Async

`AsyncStore` mirrors the same names, awaitable. Every call runs the sync method
in `asyncio.to_thread`.

```python
from durastream import AsyncStore

store = AsyncStore("./data")
stream = await store.create("chat")

await stream.append(b"hello ")
await stream.read(0)

async for record in stream.subscribe(0):   # poll-based tail
    print(record)
```

## Producer and consumer

A common shape is one writer and one or more readers. The writer appends, the
reader tails from an offset. They can be threads in one process, or the reader can
be a separate run that starts from a saved offset.

```python
import threading

stream = store.create("events", "application/json")

def consumer():
    for record in stream.subscribe(0):   # replay, then follow live
        handle(record)

threading.Thread(target=consumer, daemon=True).start()

for event in source:
    stream.append(json.dumps(event).encode())
stream.close()                           # lets the consumer finish
```

The consumer returns once the stream is closed and it has caught up.

## A resumable worker

To survive restarts, a worker records the offset it has committed. On start it
resumes from there. Committing after each chunk gives at least once processing:
if it crashes mid chunk, that chunk is reprocessed, never skipped.

```python
def read_checkpoint(path):
    try:
        return int(open(path).read())
    except FileNotFoundError:
        return 0

queue = store.open("jobs")
offset = read_checkpoint("worker.offset")

while offset < queue.next_offset:
    chunk = queue.read(offset, offset + 1000)
    for job in chunk:
        handle(job)
    offset += len(chunk)
    with open("worker.offset", "w") as f:   # commit after the chunk
        f.write(str(offset))
```

For exactly the file plus fsync version, see the queue demo.

## Rebuild state by replaying the log

When the log is your source of truth, derive state by folding over the records.
Reading up to an offset gives a point in time value.

```python
def rebuild(stream):
    state = initial()
    for raw in stream.read(0):           # every record from the start
        state = apply(state, json.loads(raw))
    return state

# state right after offset k
def rebuild_at(stream, k):
    state = initial()
    for raw in stream.read(0, k):
        state = apply(state, json.loads(raw))
    return state
```

## Resume after a restart

`next_offset` is restored from the log on open, so a new process continues the
same stream. A reader remembers one integer to pick up where it left off.

```python
stream = store.open("events")     # fresh process, same disk
resume = stream.next_offset       # everything up to here is durable
records = stream.read(0, resume)  # replay what was written before
```

