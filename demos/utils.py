import asyncio
import contextlib
import json
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from typing import Self

from durastream import DurableStream, Store


@contextlib.contextmanager
def temp_store() -> Iterator[tuple[str, Store]]:
    """A Store on a temp dir"""
    with tempfile.TemporaryDirectory() as root:
        store = Store(root)
        try:
            yield root, store
        finally:
            with contextlib.suppress(Exception):
                store.close()


def jdump(obj: object) -> bytes:
    """JSON-encode one record to bytes."""
    return json.dumps(obj).encode()


def append_range(
    stream: DurableStream,
    start: int,
    end: int,
    make: Callable[[int], bytes],
    batch: int = 1_000,
) -> None:
    """Append from start to end"""
    for base in range(start, end, batch):
        n = min(batch, end - base)
        stream.append_many([make(i) for i in range(base, base + n)])


class Timer:
    """Timer context manager for measuring elapsed time."""

    s: float = 0.0

    def __enter__(self) -> Self:
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.s = time.perf_counter() - self._t0


_TEXT = """\
The morning had dawned clear and cold, with a crispness that hinted at the end of \
summer. They set forth at daybreak to see a man beheaded, twenty in all, and Bran rode \
among them, nervous with excitement. This was the first time he had been deemed old \
enough to go with his lord father and his brothers to see the king's justice done. It \
was the ninth year of summer, and the seventh of Bran's life.

The man had been taken outside a small holdfast in the hills. Robb thought he was a \
wildling, his sword sworn to Mance Rayder, the King-beyond-the-Wall. It made Bran's \
skin prickle to think of it. He remembered the hearth tales Old Nan told them. The \
wildlings were cruel men, she said, slavers and slayers and thieves.

But the man they found bound hand and foot to the holdfast wall awaiting the king's \
justice was old and scrawny, not much taller than Robb. He had lost both ears and a \
finger to frostbite, and he dressed all in black, the same as a brother of the Night's \
Watch, except that his furs were ragged and greasy.

The breath of man and horse mingled, steaming, in the cold morning air as his lord \
father had the man cut down from the wall and dragged before them. A faint wind blew \
through the holdfast gate. Over their heads flapped the banner of the Starks of \
Winterfell: a grey direwolf racing across an ice-white field."""

TOKENS: list[str] = []
for _para in _TEXT.split("\n\n"):
    TOKENS += [w + " " for w in _para.split()]
    TOKENS.append("[[BR]]")


@dataclass
class Delta:
    content: str | None


@dataclass
class Choice:
    delta: Delta


@dataclass
class Chunk:
    choices: list[Choice]


class Completions:
    async def create(
        self,
        *,
        delay: float = 0.03,
    ) -> AsyncIterator[Chunk]:
        """Stream the corpus token by token."""
        for token in TOKENS:
            await asyncio.sleep(delay)
            yield Chunk(choices=[Choice(delta=Delta(content=token))])


class Chat:
    completions = Completions()


class FakeOpenAI:
    chat = Chat()
