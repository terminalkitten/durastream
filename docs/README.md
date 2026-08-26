<p align="center">
  <img src="assets/dura-stream-logo.png" alt="dura.stream" width="180">
</p>

# dura.stream

dura.stream is a tiny, embedded durable streaming library for Python: it gives
you an append only stream that survives crashes, can be replayed from any offset,
and can be tailed live, without requiring Redis, Kafka, or other stream and
broker services. It is designed for small applications, workers, agents, games,
and tools that need to remember what happened and pick up where they left off,
while keeping the whole system local, simple, and easy to embed.

## Install

```bash
uv add durastream
```

Requires Python 3.12+. No runtime dependencies.

<p>
  <a href="#/quickstart" class="button">Quick start &rarr;</a>
</p>

<style>
.button {
  display: inline-block;
  padding: 8px 18px;
  border-radius: 8px;
  background: var(--theme-color, #42b983);
  color: #fff;
  font-weight: 600;
  text-decoration: none;
}
</style>
