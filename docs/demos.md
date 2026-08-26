# Demos

Each demo is a runnable script that self verifies with asserts. The relevant
parts are shown below; run the full version with the `make` target named at the
end of each section.

## Durable telemetry ingest

A producer bulk appends batches while a second thread tails the stream live, then
the store is reopened from disk to prove the data survived.

```python
stream = store.create("telemetry", "application/json")

# producer: one fsync per batch
for base in range(0, N, 1000):
    stream.append_many([json.dumps(r).encode() for r in batch])
stream.close()

# consumer (another thread): replay then follow live
for record in stream.subscribe(0):
    handle(record)
```

`make demo`

## Batching the fsync

The durability cost is one `fsync` per flush. Writing records one at a time pays
it per record; batching pays it once for the whole group, which is much faster
for the same guarantee.

```python
for r in records:            # one fsync each
    stream.append(r)

stream.append_many(records)  # one fsync total, roughly 27x faster
```

`make demo-bench`

## Stop and restart

`next_offset` is rebuilt from the log on open, so a fresh process picks up exactly
where the last one stopped.

```python
store = Store(root)
stream = store.open("events")
resume = stream.next_offset      # where the previous run ended
stream.append_many(more)
store.close()                    # stop
# next run: reopen, next_offset is restored by scanning the log
```

`make demo-restart` (accepts `make demo-restart 100000 10`)

## A resumable worker

A worker checkpoints its offset to a small file after each chunk. If it crashes,
it restarts from the last committed offset, so every job is processed at least
once and nothing is lost.

```python
offset = read_checkpoint(path)        # last committed offset, or 0
while offset < queue.next_offset:
    chunk = queue.read(offset, offset + 1000)
    for job in chunk:
        handle(job)
    offset += len(chunk)
    write_checkpoint(path, offset)    # commit only after a full chunk
```

`make demo-queue`

## Event sourcing

The log is the source of truth. Append events, then rebuild any state by replaying
them. A point in time value is just a replay up to an offset.

```python
for ev in events:
    account.append_many([json.dumps(ev).encode()])

# rebuild the balance from the durable log
balance = 0
for raw in store.open("account").read(0):
    balance = apply(balance, json.loads(raw))
```

`make demo-ledger`

## Concurrent users

Many users on one store in a single process. A stream per user has no cross talk;
many users into one shared stream stay coordinated by the per stream lock, with
nothing lost. See the [Concurrency](concurrency.md) page.

```python
store = AsyncStore(root)

async def user(u):
    s = await store.create(f"chat.{u}")
    await asyncio.gather(produce(s), consume(s))

await asyncio.gather(*(user(u) for u in range(40)))
```

`make demo-concurrent`

## Resumable HTTP streaming

A FastAPI endpoint streams tokens over SSE, backed by a durable stream. The SSE
`id` on each event is the offset, so a client that drops can reconnect with its
last offset and receive the rest. Generation keeps running server side, so a page
refresh replays the transcript and continues.

```python
async def sse(key, offset):
    s = await store.open(key)
    while True:
        for i, rec in enumerate(await s.read(offset)):
            yield f"id: {to_token(offset + i)}\ndata: {rec.decode()}\n\n"
        offset = s.next_offset
        if s.closed:
            return
        await asyncio.sleep(0.05)
```

`make demo-serve`
