//! End-to-end tests of the public Rust API.

use std::io::Write;
use std::sync::Arc;
use std::thread;

use durastream_core::{Error, Store};

fn log_path(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    root.join("streams").join(format!("{name}.log"))
}

#[test]
fn roundtrip_recover_subscribe() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    let s = store.create("t", Some("text/plain")).unwrap();
    assert_eq!(s.append_many(&[b"a", b"b"]).unwrap(), 2);
    assert_eq!(s.append(b"c").unwrap(), 3);
    assert_eq!(s.read(1, Some(2)).unwrap(), [b"b"]);

    // torn tail is dropped on reopen, appends stay contiguous
    store.close();
    std::fs::OpenOptions::new()
        .append(true)
        .open(log_path(dir.path(), "t"))
        .unwrap()
        .write_all(b"\x00\x00\x00\x09partial")
        .unwrap();
    let store = Store::new(dir.path()).unwrap();
    let s = store.open("t").unwrap();
    assert_eq!(s.next_offset(), 3);

    let tail = {
        let s = Arc::clone(&s);
        thread::spawn(move || s.subscribe(1).map(Result::unwrap).collect::<Vec<_>>())
    };
    s.append(b"d").unwrap();
    s.close().unwrap();
    assert_eq!(tail.join().unwrap(), [b"b", b"c", b"d"]);
    assert!(matches!(s.append(b"e"), Err(Error::Closed(_))));
}

#[test]
fn second_store_is_read_only_and_never_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let writer = Store::new(dir.path()).unwrap();
    let w = writer.create("t", None).unwrap();
    w.append(b"a").unwrap();
    // a half-written frame, as a concurrent reader would see mid-append
    std::fs::OpenOptions::new()
        .append(true)
        .open(log_path(dir.path(), "t"))
        .unwrap()
        .write_all(b"\x00\x00")
        .unwrap();
    let size = std::fs::metadata(log_path(dir.path(), "t")).unwrap().len();

    let reader = Store::new(dir.path()).unwrap();
    let r = reader.open("t").unwrap();
    assert!(w.is_writer() && !r.is_writer());
    assert_eq!(r.read(0, None).unwrap(), [b"a"]);
    assert!(matches!(r.append(b"x"), Err(Error::Locked(_))));
    assert!(matches!(r.close(), Err(Error::Locked(_))));
    assert!(matches!(reader.delete("t"), Err(Error::Locked(_))));
    assert_eq!(
        std::fs::metadata(log_path(dir.path(), "t")).unwrap().len(),
        size
    );

    // once the writer lets go, the next opener takes over (and repairs the tail)
    writer.close();
    let again = Store::new(dir.path()).unwrap().open("t").unwrap();
    assert!(again.is_writer());
    assert_eq!(again.append(b"b").unwrap(), 2);
}

#[test]
fn delete_wakes_subscribers_and_closes_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    let s = store.create("t", None).unwrap();
    let tail = {
        let s = Arc::clone(&s);
        thread::spawn(move || s.subscribe(0).count())
    };
    store.delete("t").unwrap();
    assert_eq!(tail.join().unwrap(), 0);
    assert!(matches!(s.append(b"x"), Err(Error::Closed(_))));
}

#[test]
fn corruption_after_open_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    let s = store.create("t", None).unwrap();
    s.append_many(&[b"good", b"next"]).unwrap();
    let path = log_path(dir.path(), "t");
    let mut data = std::fs::read(&path).unwrap();
    data[8] ^= 0xFF; // first payload byte
    std::fs::write(&path, data).unwrap();
    assert!(matches!(s.read(0, None), Err(Error::Corrupt(_))));
}

#[test]
fn names_are_lowercase() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    for bad in ["Orders", "", "a/b", "a b"] {
        assert!(
            matches!(store.create(bad, None), Err(Error::Invalid(_))),
            "{bad:?}"
        );
    }
    store.create("orders.v2_x-1", None).unwrap();
}

#[test]
fn delete_on_a_closed_store_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    store.create("t", None).unwrap().append(b"x").unwrap();
    store.close();
    assert!(matches!(store.delete("t"), Err(Error::StoreClosed)));
    assert!(log_path(dir.path(), "t").exists());
}

#[test]
fn listeners_fire_on_every_change() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(dir.path()).unwrap();
    let s = store.create("t", None).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let id = {
        let calls = Arc::clone(&calls);
        s.add_listener(move || {
            calls.fetch_add(1, Ordering::SeqCst);
        })
    };
    let writer = {
        let s = Arc::clone(&s);
        thread::spawn(move || s.append(b"a").unwrap())
    };
    assert_eq!(writer.join().unwrap(), 1);
    assert_eq!(s.next_offset(), 1);
    s.close().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2); // append + close

    assert!(s.remove_listener(id));
    assert!(!s.remove_listener(id));
    store.delete("t").unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2); // removed: delete not seen
}
