---
title: dura.stream
description: Tiny durable streaming on local disk, for Python.
sidebar:
  label: Introduction
---

<p align="center"><img src="img/dura-stream-logo.png" alt="dura.stream" width="220"></p>

dura.stream is a tiny, embedded durable streaming library for Python. It gives you an append-only
stream that survives crashes, can be replayed from any offset, and can be tailed live, without
Redis, Kafka or any other broker.

It is made for small applications, workers, agents, games and tools that need to remember what
happened and pick up where they left off, while keeping the whole system local, simple and easy to
embed.

## Install

::install[durastream]

:::note
Requires Python 3.12+. No runtime dependencies: only the standard library.
:::

```python
from durastream import Store

stream = Store("./data").create("orders")
stream.append(b"order-1")  # durable when it returns
stream.read(0)  # [b"order-1"]
```

::::cards
:::card[Quick start]{href="quickstart.md"}
Write, read, tail and recover, in a few lines each.
:::
:::card[Concepts]{href="concepts.md"}
Records, streams and the store, and where durability starts.
:::
:::card[Concurrency]{href="concurrency.md"}
Threads, coroutines and separate processes.
:::
:::card[Demos]{href="demos.md"}
Runnable scripts: telemetry, a work queue, event sourcing, SSE.
:::
:::card[API reference]{href="api.md"}
Every class, method and error.
:::
::::
