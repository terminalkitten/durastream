import os
import sqlite3
import tempfile
import threading
import time

import pytest

from durastream import (
    CorruptStream,
    DurastreamError,
    Store,
    StreamClosed,
    StreamLocked,
    from_token,
    to_token,
)


def test_roundtrip():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        s = Store(root).create("t", "text/plain")
        for b in (b"a", b"b", b"c"):
            s.append(b)
        assert s.read(0) == [b"a", b"b", b"c"]
        assert s.read(1) == [b"b", b"c"]
        assert s.read(1, 2) == [b"b"]
        assert s.next_offset == 3


def test_append_many():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        s = Store(root).create("t", "text/plain")
        assert s.append_many([b"a", b"b", b"c"]) == 3  # one fsync for the batch
        assert s.append_many([]) == 3  # empty batch is a no-op
        assert s.read(0) == [b"a", b"b", b"c"]
        assert s.next_offset == 3
        assert Store(root).open("t").read(0) == [b"a", b"b", b"c"]  # durable


def test_durability_reopen():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        Store(root).create("t", "text/plain")
        s = Store(root).open("t")  # fresh Store, same disk
        s.append(b"x")
        s.append(b"y")
        s2 = Store(root).open("t")
        assert s2.read(0) == [b"x", b"y"]
        assert s2.next_offset == 2  # rebuilt from log, not SQLite


def test_recovery_torn_tail():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        s = store.create("t", "text/plain")
        s.append(b"aa")
        s.append(b"bb")
        path = os.path.join(root, "streams", "t.log")
        with open(path, "ab") as f:  # torn frame: garbage shorter than a header
            f.write(b"\x00\x00\x00\x09partial")
        store.close()  # the writer lets go of the log; the next opener repairs it
        s2 = Store(root).open("t")
        assert s2.read(0) == [b"aa", b"bb"]  # tail dropped, no exception
        assert s2.next_offset == 2
        s2.append(b"cc")  # appends stay contiguous after truncation
        assert s2.read(0) == [b"aa", b"bb", b"cc"]


def test_crc_guard():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
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
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
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
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
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


def test_content_type_mismatch():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        store.create("s", "text/plain")
        store.create("s", "text/plain")  # same type ok
        store.create("s")  # no type asserted, ok
        try:
            store.create("s", "application/json")
            assert False, "mismatch should raise"
        except ValueError:
            pass


def test_delete_and_reopen():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        store.create("s", "text/plain").append(b"x")
        store.delete("s")
        try:
            store.open("s")
            assert False, "open after delete should raise"
        except KeyError:
            pass
        assert store.create("s", "text/plain").read(0) == []  # recreated empty


def test_invalid_name():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        try:
            Store(root).create("bad/name")
            assert False, "invalid name should raise"
        except ValueError:
            pass


def test_concurrent_create_list():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        n = 40

        def make(i):
            store.create(f"s{i}", "text/plain")

        threads = [threading.Thread(target=make, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(store.list()) == n


@pytest.mark.skipif(
    os.name == "nt", reason="no flock on Windows: one writer is unenforced"
)
def test_second_store_is_read_only_and_never_truncates():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        s = Store(root).create("t")
        s.append(b"a")
        path = os.path.join(root, "streams", "t.log")
        with open(path, "ab") as f:  # half-written frame, as seen mid-append
            f.write(b"\x00\x00")
        size = os.path.getsize(path)
        other = Store(root)
        r = other.open("t")
        assert s.writable and not r.writable
        assert r.read(0) == [b"a"]
        for op in (lambda: r.append(b"x"), r.close, lambda: other.delete("t")):
            with pytest.raises(StreamLocked):
                op()
        assert os.path.getsize(path) == size  # a reader never truncates


def test_delete_ends_subscriber_and_closes_stream():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        s = store.create("t")
        th = threading.Thread(target=lambda: list(s.subscribe(0)))
        th.start()
        time.sleep(0.05)
        store.delete("t")
        th.join(timeout=2)
        assert not th.is_alive()
        with pytest.raises(StreamClosed):
            s.append(b"x")


def test_orphan_log_is_not_resurrected():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        store.create("t").append(b"old")
        store.close()
        # a delete that crashed after dropping the row but before removing the log
        db = sqlite3.connect(os.path.join(root, "meta.db"))
        db.execute("DELETE FROM streams")
        db.commit()
        db.close()
        assert Store(root).create("t").read(0) == []


def test_corruption_after_open_raises():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        s = Store(root).create("t")
        s.append_many([b"good", b"next"])
        path = os.path.join(root, "streams", "t.log")
        with open(path, "r+b") as f:
            f.seek(8)  # first payload byte
            f.write(b"X")
        with pytest.raises(CorruptStream):
            s.read(0)


def test_names_are_lowercase():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        for bad in ["Orders", "", "a/b", "a b", "a\n"]:
            with pytest.raises(ValueError):
                store.create(bad)
        store.create("orders.v2_x-1")


def test_exceptions_share_a_base():
    for exc in (StreamClosed, StreamLocked, CorruptStream):
        assert issubclass(exc, DurastreamError)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")  # fork with threads alive
def test_forked_child_is_not_a_writer():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        s = Store(root).create("t")
        s.append(b"parent-1")
        pid = os.fork()
        if pid == 0:  # child: shares the parent's lock, must still be refused
            code = 4  # any other exception
            try:
                s.append(b"child")
                code = 2
            except StreamLocked:
                code = 0 if not s.writable else 3
            finally:
                os._exit(code)  # never fall through into the rest of pytest
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert s.writable
        assert s.append(b"parent-2") == 2
        assert s.read(0) == [b"parent-1", b"parent-2"]


def test_delete_on_closed_store_touches_nothing():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        store.create("t").append(b"x")
        store.close()
        with pytest.raises(DurastreamError):
            store.delete("t")
        assert os.path.exists(os.path.join(root, "streams", "t.log"))


def test_database_errors_are_durastream_errors():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        db = sqlite3.connect(os.path.join(root, "meta.db"))
        db.execute("DROP TABLE streams")
        db.commit()
        db.close()
        with pytest.raises(DurastreamError):
            store.list()


@pytest.mark.skipif(
    os.name == "nt" or os.geteuid() == 0, reason="needs POSIX permissions, not root"
)
def test_read_only_file_opens_read_only():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        store = Store(root)
        store.create("t").append(b"x")
        store.close()
        os.chmod(os.path.join(root, "streams", "t.log"), 0o444)
        r = Store(root).open("t")
        assert not r.writable
        assert r.read(0) == [b"x"]
        with pytest.raises(StreamLocked):
            r.append(b"y")
