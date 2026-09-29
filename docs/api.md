# API reference

```python
from durastream import Store, AsyncStore, to_token, from_token
```

Stream names are `[a-z0-9._-]+`: lowercase, so two names never share a file on a
case-insensitive disk.

## Store

The container of streams, backed by a root directory and a SQLite index.

| Method | Description |
|---|---|
| `Store(root)` | Open or create a store at `root`. |
| `create(name, content_type=None)` | Create a stream, or return the existing one. Raises `ValueError` for an invalid name, or if `content_type` is given and differs from the existing one. Defaults to `application/octet-stream` for a new stream. |
| `open(name)` | Open an existing stream; read-only if another process owns it. Raises `KeyError` if it does not exist. |
| `delete(name)` | Remove the log file and metadata row. Ends any `subscribe` on it. Raises `StreamLocked` if another process owns it. |
| `list()` | Names of all streams, sorted. |
| `close()` | Close file handles and the metadata database. |

## DurableStream

One append only stream. Get one from `Store.create` or `Store.open`.

| Member | Description |
|---|---|
| `append(payload) -> int` | Frame and fsync one record. Returns the new `next_offset`. Raises `StreamClosed` if closed, `StreamLocked` if read-only. |
| `append_many(payloads) -> int` | Frame and fsync a batch in one flush. Returns the new `next_offset`. |
| `read(offset=0, end=None) -> list[bytes]` | Record payloads for the half open range `[offset, end)`. Raises `CorruptStream` if bytes on disk fail their checksum. |
| `subscribe(offset=0)` | Iterator: yield records from `offset`, then block for new ones (in process). Ends once the stream is closed or deleted and drained. |
| `close()` | Mark the stream closed (persisted). Reads still work. Raises `StreamLocked` if read-only. |
| `next_offset` | Record count and the position the next append lands at. |
| `closed` | Whether the stream is closed, deleted, or disabled after a failed write. |
| `writable` | Whether this handle owns the log in this process (see [Concurrency](concurrency.md)). |
| `add_listener(callback) -> int` | Call `callback()` after every append, close or delete, from the thread that made the change, outside all locks. Keep it quick; exceptions are reported and ignored. Returns an id. |
| `remove_listener(id) -> bool` | Stop calling a listener. |
| `content_type` | The MIME type set at creation. |

## Errors

All derive from `DurastreamError`, so `except DurastreamError` catches every one.

| Exception | Raised when |
|---|---|
| `StreamClosed` | Appending to a closed or deleted stream, or one disabled after a failed write (reopen the store). |
| `StreamLocked` | Writing through a read-only handle: another process owns the stream. |
| `CorruptStream` | Bytes on disk no longer match their checksum. |
| `DurastreamError` | Itself: the store is closed, or the metadata database failed (e.g. `database is locked`). |

A failed write (disk full, I/O error) raises the `OSError` and is rolled back, so
the log never keeps a partial record.

## AsyncStore and AsyncDurableStream

The async mirror of the sync API, same names, awaitable. Every call runs the sync
method in `asyncio.to_thread`.

```python
store = AsyncStore("./data")
stream = await store.create("chat")
await stream.append(b"hi")
await stream.read(0)
await stream.close()

async for record in stream.subscribe(0):  # replay, then pushed tail
    ...
```

`subscribe` and `wait(offset)` are push based: an append wakes them directly
(through a change listener, from whichever thread appended), so latency is about
a millisecond and no worker thread is parked. Only appends made in this process
wake them; a read-only view of another process's stream never changes.

| Member | Description |
|---|---|
| `await wait(offset) -> bool` | Wait until a record past `offset` exists. `False` once the stream is closed (or deleted) with nothing past `offset`. |
| `subscribe(offset=0)` | Async iterator: replay from `offset`, then each new record as it is appended. Ends once closed and drained. |
| `await read(offset=0, end=None)` | Like the sync `read`; returns `[]` without a thread hop when there is nothing new. |

Each woken subscriber reads through `asyncio.to_thread`, so with hundreds of
clients per stream every record costs one thread hop per client; batching the
writer (below) cuts those wake-ups.

### AsyncBatchWriter

Groups many small appends, such as LLM tokens, into one `append_many` and one
fsync per `window` (default 20 ms).

```python
async with AsyncBatchWriter(stream, window=0.02) as w:
    for token in tokens:
        w.write(token)  # buffers, returns at once
# closed: everything written is durable
```

| Member | Description |
|---|---|
| `write(payload)` | Buffer one record; appended within `window` seconds. |
| `await flush() -> int` | Append everything written so far; returns `next_offset` once it is durable. |
| `await close()` | Flush and stop (does not close the stream). |

Durability is unchanged: readers only see records already on disk, and `flush()`
or `close()` returning means everything written so far is durable. A crash can
only lose records still buffered, which no reader has seen. If a flush fails
(e.g. disk full), its records stay buffered for the next flush and the error is
raised by the next `write`, `flush` or `close`.

## Offset tokens

Offsets are integers. These helpers convert to and from the zero padded wire
token format, and understand the `-1` (start) and `now` (tail) sentinels.

```python
to_token(1)  # "00000000000000000001"
from_token("-1", next_offset)  # 0
from_token("now", next_offset)  # next_offset
from_token("000...0003", next_offset)  # 3
```
