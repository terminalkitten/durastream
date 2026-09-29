from importlib.metadata import version

from ._engine import ENGINE, DurableStream, Store, from_token, to_token
from .aio import AsyncDurableStream, AsyncStore
from .errors import CorruptStream, DurastreamError, StreamClosed, StreamLocked

__version__ = version("durastream")

__all__ = [
    "ENGINE",
    "AsyncDurableStream",
    "AsyncStore",
    "CorruptStream",
    "DurableStream",
    "DurastreamError",
    "Store",
    "StreamClosed",
    "StreamLocked",
    "__version__",
    "from_token",
    "to_token",
]
