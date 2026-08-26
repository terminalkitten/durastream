# Concurrency

dura.stream is a **single process** engine. The distinction that matters is
threads and coroutines inside one process versus separate OS processes.

## Within one process: fully concurrent

Inside one process it is safe and coordinated. Many threads or coroutines, many
streams, or many writers into one shared stream are all serialized by a per
stream lock, so offsets stay consistent and no data is lost. Use `AsyncStore`
from async code.

The `make demo-concurrent` demo shows 40 concurrent users on one store: first a
stream per user with no cross talk, then all 40 writing into one shared stream
while a consumer tails the interleaved firehose with nothing lost.

## Across separate processes: one writer per stream

The lock is an in memory `threading.Lock`, so it cannot coordinate two
processes. Two processes writing the same stream each keep their own offset
index, and the offsets diverge silently. The individual frames stay intact
(append is atomic at the byte level and the CRC still holds), but the logical
offset numbering becomes inconsistent across writers.

The rule is **one writer per stream**. Route each stream to a single owning
process (this is the `activeStreamId` pattern). Concurrent readers in other
processes are fine; they reopen to pick up new records.

## SQLite metadata under many processes

Several processes sharing one store also share `meta.db`. WAL mode allows many
readers but one writer at a time, enforced by an OS file lock. dura.stream does
not set a `busy_timeout`, so a `create` or `close` racing another process can see
`database is locked` instead of waiting. Set a busy timeout if you run this shape.
