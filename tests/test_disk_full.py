"""Disk-full rollback, against a real ENOSPC.

Needs a tiny file system: set DURASTREAM_SMALL_FS to a size-limited tmpfs mount
(`make test-diskfull` runs this in Docker; CI mounts one on Linux).
"""

import errno
import os
import tempfile

import pytest

from durastream import Store

SMALL_FS = os.environ.get("DURASTREAM_SMALL_FS", "")
pytestmark = pytest.mark.skipif(
    not SMALL_FS, reason="set DURASTREAM_SMALL_FS to a tiny tmpfs mount"
)

PAGE = 4096


def fill(path: str, leave: int) -> None:
    """Write ballast until the file system is full, then free `leave` bytes."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        while True:
            os.write(fd, b"\0" * PAGE)
    except OSError as e:
        assert e.errno == errno.ENOSPC, e
    os.ftruncate(fd, max(os.fstat(fd).st_size - leave, 0))
    os.close(fd)


def test_disk_full_append_rolls_back():
    root = tempfile.mkdtemp(dir=SMALL_FS)
    ballast = os.path.join(SMALL_FS, f"ballast-{os.getpid()}")
    store = Store(root)
    s = store.create("t")
    assert s.append(b"a" * 100) == 1
    log = os.path.join(root, "streams", "t.log")
    size = os.path.getsize(log)

    # one free page: a 4-page record gets partially written, then ENOSPC
    fill(ballast, leave=PAGE)
    with pytest.raises(OSError) as exc:
        s.append(b"b" * (4 * PAGE))
    assert exc.value.errno == errno.ENOSPC
    assert os.path.getsize(log) == size  # partial frame rolled back
    assert s.next_offset == 1 and not s.closed  # stream still usable

    os.remove(ballast)  # space again: appends continue contiguously
    assert s.append(b"c") == 2
    store.close()
    assert Store(root).open("t").read(0) == [b"a" * 100, b"c"]
