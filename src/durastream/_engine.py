"""Pick the storage engine: native Rust engine (durastream.core) if built in, else pure Python.

Set DURASTREAM_PURE=1 to force the pure-Python engine.
"""

import os

ENGINE = "python"

if os.environ.get("DURASTREAM_PURE") != "1":
    try:
        from .core import DurableStream, Store, from_token, to_token

        ENGINE = "native"
    except ImportError:  # pure py3-none-any wheel (e.g. Windows, PyPy)
        pass

if ENGINE == "python":
    from .codec import from_token, to_token
    from .store import Store
    from .stream import DurableStream

__all__ = ["ENGINE", "DurableStream", "Store", "from_token", "to_token"]
