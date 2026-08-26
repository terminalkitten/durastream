from utils import Timer, append_range, jdump, temp_store

N = 5_000
BATCH = 1_000


def record(i: int) -> bytes:
    return jdump({"id": i, "temp": 20 + (i % 100) / 10})


def main() -> None:
    with temp_store() as (_root, store):
        # one fsync per record
        one = store.create("one-by-one", "application/json")
        with Timer() as t_one:
            for i in range(N):
                one.append(record(i))

        # one fsync per BATCH records
        batched = store.create("batched", "application/json")
        with Timer() as t_batch:
            append_range(batched, 0, N, record, BATCH)

        assert one.next_offset == N == batched.next_offset

        print(f"append vs append_many  -  {N:,} records each\n")
        print("append()      one fsync per record")
        print(f"  {N:,} in {t_one.s:6.2f}s   ->   {N / t_one.s:>10,.0f} rec/s")
        print(f"append_many() one fsync per {BATCH:,}-record batch")
        print(f"  {N:,} in {t_batch.s:6.2f}s   ->   {N / t_batch.s:>10,.0f} rec/s")
        print(f"\nbatched is ~{t_one.s / t_batch.s:,.0f}x faster")


if __name__ == "__main__":
    main()
