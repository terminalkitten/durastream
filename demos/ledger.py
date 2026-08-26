import json

from utils import Timer, jdump, temp_store

from durastream import Store

N = 1_000_000
BATCH = 1_000


def event(i: int, balance: int) -> dict:
    # deterministic: deposit on evens, withdraw on odds when funds allow
    amount = 10 + (i % 90)
    if i % 2 == 0 or balance < amount:
        return {"op": "deposit", "amount": amount}
    return {"op": "withdraw", "amount": amount}


def apply(balance: int, ev: dict) -> int:
    return balance + ev["amount"] if ev["op"] == "deposit" else balance - ev["amount"]


def main() -> None:
    with temp_store() as (root, store):
        acct = store.create("account.123", "application/json")

        # append events
        live = 0
        mid_balance = 0
        batch = []
        with Timer() as t:
            for i in range(N):
                ev = event(i, live)
                live = apply(live, ev)
                if i == N // 2 - 1:
                    mid_balance = live  # balance right after offset N/2-1
                batch.append(jdump(ev))
                if len(batch) == BATCH:
                    acct.append_many(batch)
                    batch = []
            if batch:
                acct.append_many(batch)
        print(f"appended {N:,} events in {t.s:.2f}s -> {N / t.s:,.0f} ev/s")
        print(f"live balance: {live:,}")

        # restart: rebuild the balance from log
        acct2 = Store(root).open("account.123")
        rebuilt = 0
        for raw in acct2.read(0):
            rebuilt = apply(rebuilt, json.loads(raw))
        print(
            f"reopened, replayed {acct2.next_offset:,} events -> rebuilt balance: {rebuilt:,}"
        )
        assert rebuilt == live, "rebuilt balance != live balance"

        # point-in-time query: balance after the first N/2 events
        pit = 0
        for raw in acct2.read(0, N // 2):
            pit = apply(pit, json.loads(raw))
        print(f"point-in-time balance @ offset {N // 2:,}: {pit:,}")
        assert pit == mid_balance, "point-in-time replay mismatch"

        print("audit OK - balance is exactly the replay of the immutable log.")


if __name__ == "__main__":
    main()
