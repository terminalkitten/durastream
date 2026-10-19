---
title: Concurrency
description: Threads and coroutines in one process, and one writer per stream across processes.
sidebar:
  order: 3
---

dura.stream is a **single process** engine. The distinction that matters is
threads and coroutines inside one process versus separate OS processes.

## Within one process: fully concurrent

Inside one process it is safe and coordinated. Many threads or coroutines, many
streams, or many writers into one shared stream are all serialized by a per
stream lock, so offsets stay consistent and no data is lost. Use `AsyncStore`
from async code.

The [concurrent users demo](demos.md#concurrent-users) (`make demo-concurrent`) shows 40 concurrent users on one store: first a
stream per user with no cross talk, then all 40 writing into one shared stream
while a consumer tails the interleaved firehose with nothing lost.

## Across separate processes: one writer per stream

The rule is **one writer per stream**, and it is enforced. The first process to
open a stream takes an exclusive OS file lock (`flock`) on its log and becomes the
writer. Any other process (or a second `Store` in the same process) that opens the
stream gets a read-only view:

- `stream.writable` is `False`;
- `read` and `subscribe` work, on the snapshot: `subscribe` replays it and then
  waits for appends that never come, since only the writer's appends wake it;
- `append`, `close` and `store.delete` raise `StreamLocked`;
- the log file is never modified, so a writer mid-append is never disturbed.

A read-only view is a snapshot of the log at open time; open it again from a new
`Store` to pick up new records. The lock belongs to the writer's open file, so it
is released when the writer closes its store or exits, including a crash. The next
process to open the stream then becomes the writer and repairs a torn tail.

:::note[Forked processes]
A **forked** child (`multiprocessing` with the fork start method, `gunicorn
--preload`, ...) inherits the parent's open files, lock included, but only the
process that took the lock may write: in the child every stream is read-only. Open
stores after forking if the child should write. Note the lock is only released
once the parent *and* such children have closed the file or exited.
:::

A process without write access to a log (file permissions, read-only mount) also
gets a read-only view.

Route each stream to a single owning process (this is the `activeStreamId`
pattern).

:::caution[Windows]
Windows has no `flock`, so the one-writer rule is not enforced there: make sure only one process
writes to each stream.
:::

## SQLite metadata under many processes

Several processes sharing one store also share `meta.db`. WAL mode allows many
readers but one writer at a time, enforced by an OS file lock. A `create` or
`close` racing another process waits up to 5 seconds for it (SQLite busy timeout)
before failing with `DurastreamError: database is locked`.
