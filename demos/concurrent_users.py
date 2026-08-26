import asyncio
import tempfile
from collections import Counter

from utils import Timer

from durastream import AsyncStore

N_USERS = 40
PER_USER = 50


async def isolated(store: AsyncStore) -> int:
    """Scenario A: a stream per user, produce + tail, verify no cross-talk."""

    async def user(u: int) -> None:
        s = await store.create(f"chat.{u}")

        async def produce() -> None:
            for i in range(PER_USER):
                await asyncio.sleep(0.0005)
                await s.append(f"u{u}-{i} ".encode())
            await s.close()

        got: list[bytes] = []

        async def consume() -> None:
            async for record in s.subscribe(0):
                got.append(record)

        await asyncio.gather(produce(), consume())
        assert len(got) == PER_USER
        assert got[0] == f"u{u}-0 ".encode()
        assert all(r.startswith(f"u{u}-".encode()) for r in got)  # no foreign records

    with Timer() as t:
        await asyncio.gather(*(user(u) for u in range(N_USERS)))
    total = N_USERS * PER_USER
    print(
        f"A: {N_USERS} users / {N_USERS} streams, {total:,} records in {t.s:.2f}s "
        f"-> {total / t.s:,.0f} rec/s, no cross-talk"
    )
    return total


async def fan_in(store: AsyncStore) -> int:
    """Scenario B: 40 writers into one shared stream, one consumer tails."""
    s = await store.create("firehose")
    got: list[bytes] = []

    async def consumer() -> None:
        async for record in s.subscribe(0):
            got.append(record)

    async def writer(u: int) -> None:
        for i in range(PER_USER):
            await asyncio.sleep(0.0005)
            await s.append(f"u{u}-{i} ".encode())

    with Timer() as t:
        task = asyncio.create_task(consumer())
        await asyncio.gather(*(writer(u) for u in range(N_USERS)))
        await s.close()
        await task

    total = N_USERS * PER_USER
    assert len(got) == total  # nothing lost under concurrent append
    per_user = Counter(r.split(b"-")[0] for r in got)
    assert len(per_user) == N_USERS and all(c == PER_USER for c in per_user.values())
    sample = b"".join(got[:8]).decode()
    print(
        f"B: {N_USERS} writers / 1 stream, {total:,} records in {t.s:.2f}s "
        f"-> {total / t.s:,.0f} rec/s"
    )
    print(f"   interleaved: {sample}...")
    return total


async def main() -> None:
    with tempfile.TemporaryDirectory() as root:
        store = AsyncStore(root)
        await isolated(store)
        await fan_in(store)
        await store.close()
        print("concurrent OK - one store, many users, offsets coordinated, no loss.")


if __name__ == "__main__":
    asyncio.run(main())
