import os
import tempfile
import threading
import time

from durastream import Store, StreamClosed, from_token, to_token


def test_roundtrip():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        for b in (b"a", b"b", b"c"):
            s.append(b)
        assert s.read(0) == [b"a", b"b", b"c"]
        assert s.read(1) == [b"b", b"c"]
        assert s.read(1, 2) == [b"b"]
        assert s.next_offset == 3


def test_append_many():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        assert s.append_many([b"a", b"b", b"c"]) == 3  # one fsync for the batch
        assert s.append_many([]) == 3  # empty batch is a no-op
        assert s.read(0) == [b"a", b"b", b"c"]
        assert s.next_offset == 3
        assert Store(root).open("t").read(0) == [b"a", b"b", b"c"]  # durable


def test_durability_reopen():
    with tempfile.TemporaryDirectory() as root:
        Store(root).create("t", "text/plain")
        s = Store(root).open("t")  # fresh Store, same disk
        s.append(b"x")
        s.append(b"y")
        s2 = Store(root).open("t")
        assert s2.read(0) == [b"x", b"y"]
        assert s2.next_offset == 2  # rebuilt from log, not SQLite


def test_recovery_torn_tail():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        s.append(b"aa")
        s.append(b"bb")
        path = os.path.join(root, "streams", "t.log")
        with open(path, "ab") as f:  # torn frame: garbage shorter than a header
            f.write(b"\x00\x00\x00\x09partial")
        s2 = Store(root).open("t")
        assert s2.read(0) == [b"aa", b"bb"]  # tail dropped, no exception
        assert s2.next_offset == 2
        s2.append(b"cc")  # appends stay contiguous after truncation
        assert s2.read(0) == [b"aa", b"bb", b"cc"]


def test_crc_guard():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        s.append(b"good")
        s.append(b"next")
        path = os.path.join(root, "streams", "t.log")
        with open(path, "rb") as f:
            data = bytearray(f.read())
        data[8] ^= 0xFF  # flip a payload byte of the first record
        with open(path, "wb") as f:
            f.write(data)
        s2 = Store(root).open("t")
        assert s2.read(0) == []  # corrupt record + everything after rejected
        assert s2.next_offset == 0


def test_closed():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        s.append(b"a")
        s.close()
        assert s.closed
        try:
            s.append(b"b")
            assert False, "append after close should raise"
        except StreamClosed:
            pass
        assert s.read(0) == [b"a"]
        assert Store(root).open("t").closed  # persisted


def test_tail():
    with tempfile.TemporaryDirectory() as root:
        s = Store(root).create("t", "text/plain")
        got = []
        sub = s.subscribe(0)

        def consume():
            for rec in sub:
                got.append(rec)
                if len(got) == 2:
                    return

        th = threading.Thread(target=consume)
        th.start()
        time.sleep(0.05)
        s.append(b"one")
        s.append(b"two")
        th.join(timeout=2)
        assert got == [b"one", b"two"]


def test_tokens():
    assert to_token(1) == "00000000000000000001"
    assert from_token("-1", 5) == 0
    assert from_token("now", 5) == 5
    assert from_token("00000000000000000003", 5) == 3
