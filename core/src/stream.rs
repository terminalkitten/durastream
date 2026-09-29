use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, BufReader, Read, Write};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::codec::{HEADER_SIZE, crc32, decode_header, iter_frames, push_frame};
use crate::store::{Db, with_db};
use crate::{Error, Result, fsync};

/// Append-only log file for one stream.
///
/// The first handle to open a log takes an exclusive `flock` on it and becomes the
/// writer. Handles in other processes (or other `Store`s) get a read-only view.
pub struct DurableStream {
    name: String,
    content_type: String,
    db: Db,
    closed: AtomicBool,
    state: Mutex<State>,
    cond: Condvar,
}

struct State {
    /// Byte offset of each record; the last entry is the end of the log. Never empty.
    index: Vec<u64>,
    /// `None` after `close_fds` (delete / store close).
    file: Option<Arc<File>>,
    /// Holds the log's flock, so it may append and truncate.
    owner: bool,
    /// Process that took the lock. A forked child shares the lock (flock belongs
    /// to the open file), so it must not act as the writer.
    pid: u32,
}

impl State {
    fn can_write(&self) -> bool {
        self.owner && self.pid == std::process::id()
    }

    fn next_offset(&self) -> u64 {
        self.index.len() as u64 - 1
    }

    fn end(&self) -> u64 {
        *self.index.last().expect("index is never empty")
    }
}

/// Records read from the log, verified against their checksums, in one buffer.
#[derive(Debug, Default)]
pub struct Frames {
    buf: Vec<u8>,
    spans: Vec<Range<usize>>,
}

impl Frames {
    /// Number of records.
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// True when there are no records.
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Payload of record `i`.
    pub fn get(&self, i: usize) -> Option<&[u8]> {
        self.spans.get(i).map(|r| &self.buf[r.clone()])
    }

    /// Payloads in order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.spans.iter().map(|r| &self.buf[r.clone()])
    }
}

impl DurableStream {
    pub(crate) fn open(
        db: Db,
        name: String,
        path: &Path,
        content_type: String,
        closed: bool,
    ) -> Result<Self> {
        let (file, owner) = match OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
        {
            Ok(file) => {
                let owner = try_lock(&file)?;
                (file, owner)
            }
            // no write access (permissions, read-only mount): a read-only view
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem
                ) =>
            {
                (File::open(path)?, false)
            }
            Err(e) => return Err(e.into()),
        };
        let index = recover(&file, owner)?;
        Ok(Self {
            name,
            content_type,
            db,
            closed: AtomicBool::new(closed),
            state: Mutex::new(State {
                index,
                file: Some(Arc::new(file)),
                owner,
                pid: std::process::id(),
            }),
            cond: Condvar::new(),
        })
    }

    // Poisoning is recovered, not propagated: every critical section leaves
    // `State` consistent before anything that can panic (only allocation can).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stream name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Content type set at creation.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Number of records; the offset the next append lands at.
    pub fn next_offset(&self) -> u64 {
        self.lock().next_offset()
    }

    /// True once closed, deleted, or disabled after a failed write.
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// True when this handle holds the log's lock in this process (can append).
    pub fn is_writer(&self) -> bool {
        self.lock().can_write()
    }

    /// Frame + fsync one record. Returns the new next_offset.
    pub fn append(&self, payload: &[u8]) -> Result<u64> {
        self.append_many(&[payload])
    }

    /// Frame + fsync a batch of records in one flush. Returns the new next_offset.
    pub fn append_many<P: AsRef<[u8]>>(&self, payloads: &[P]) -> Result<u64> {
        // Frame outside the lock; only write + fsync + index update are serialized.
        let size = payloads
            .iter()
            .map(|p| p.as_ref().len() + HEADER_SIZE)
            .sum();
        let mut buf = Vec::new();
        buf.try_reserve_exact(size)
            .map_err(|_| Error::OutOfMemory(size as u64))?;
        for p in payloads {
            push_frame(&mut buf, p.as_ref())?;
        }
        let mut st = self.lock();
        if self.closed() {
            return Err(Error::Closed(self.name.clone()));
        }
        if !st.can_write() {
            return Err(Error::Locked(self.name.clone()));
        }
        if payloads.is_empty() {
            return Ok(st.next_offset());
        }
        let file = st
            .file
            .clone()
            .ok_or_else(|| Error::Closed(self.name.clone()))?;
        let end = st.end();
        if let Err(e) = (&*file).write_all(&buf).and_then(|()| fsync(&*file)) {
            // A partial frame would shift every later record, and after a failed
            // fsync the page cache is in an unknown state ("fsyncgate"): cut back to
            // the last durable end. If even that fails, refuse further appends.
            if file.set_len(end).and_then(|()| fsync(&*file)).is_err() {
                self.closed.store(true, Ordering::Release);
                self.cond.notify_all();
            }
            return Err(e.into());
        }
        let mut pos = end;
        for p in payloads {
            pos += (HEADER_SIZE + p.as_ref().len()) as u64;
            st.index.push(pos);
        }
        self.cond.notify_all();
        Ok(st.next_offset())
    }

    /// Records [offset, end), checksum-verified. `end` is clamped to the tail.
    pub fn read_frames(&self, offset: u64, end: Option<u64>) -> Result<Frames> {
        let (file, start, stop, count) = {
            let st = self.lock();
            let last = st.next_offset();
            let end = end.map_or(last, |e| e.min(last));
            if offset >= end {
                return Ok(Frames::default());
            }
            let file = st
                .file
                .clone()
                .ok_or_else(|| Error::Closed(self.name.clone()))?;
            (
                file,
                st.index[offset as usize],
                st.index[end as usize],
                (end - offset) as usize,
            )
        };
        let mut buf = Vec::new();
        zeroed(&mut buf, stop - start)?;
        // Log is append-only, so bytes below `stop` are stable: pread without the lock.
        file.read_exact_at(&mut buf, start)
            .map_err(|e| match e.kind() {
                io::ErrorKind::UnexpectedEof => {
                    Error::Corrupt(format!("{}: log is shorter than its index", self.name))
                }
                _ => e.into(),
            })?;
        let spans: Vec<_> = iter_frames(&buf)
            .map(|(p, end)| end - p.len()..end)
            .collect();
        if spans.len() != count {
            return Err(Error::Corrupt(format!(
                "{}: record {} fails its checksum",
                self.name,
                offset + spans.len() as u64
            )));
        }
        Ok(Frames { buf, spans })
    }

    /// Record payloads for [offset, end).
    pub fn read(&self, offset: u64, end: Option<u64>) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .read_frames(offset, end)?
            .iter()
            .map(<[u8]>::to_vec)
            .collect())
    }

    /// Block until a record past `offset` exists (or `timeout` passes).
    /// Returns false once the stream is closed and nothing is left past `offset`.
    pub fn wait(&self, offset: u64, timeout: Option<Duration>) -> bool {
        let pending = |st: &mut State| st.next_offset() <= offset && !self.closed();
        let st = self.lock();
        let st = match timeout {
            Some(t) => {
                self.cond
                    .wait_timeout_while(st, t, pending)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0
            }
            None => self
                .cond
                .wait_while(st, pending)
                .unwrap_or_else(PoisonError::into_inner),
        };
        !(self.closed() && st.next_offset() <= offset)
    }

    /// Tail -f: yield records from `offset`, then block for new ones (in-process only).
    /// Ends when the stream is closed and drained; stops after the first error.
    pub fn subscribe(&self, mut offset: u64) -> impl Iterator<Item = Result<Vec<u8>>> + '_ {
        let mut batch = Vec::new().into_iter();
        let mut failed = false;
        std::iter::from_fn(move || {
            loop {
                if failed {
                    return None;
                }
                if let Some(rec) = batch.next() {
                    offset += 1;
                    return Some(Ok(rec));
                }
                match self.read(offset, None) {
                    Err(e) => {
                        failed = true;
                        return Some(Err(e));
                    }
                    Ok(b) if !b.is_empty() => batch = b.into_iter(),
                    Ok(_) if !self.wait(offset, None) => return None,
                    Ok(_) => {}
                }
            }
        })
    }

    /// Mark the stream closed (persisted). Reads still work.
    pub fn close(&self) -> Result<()> {
        let st = self.lock();
        if !st.can_write() {
            return Err(Error::Locked(self.name.clone()));
        }
        // persist first, so memory never claims a state the disk doesn't have
        with_db(&self.db, |db| {
            db.execute("UPDATE streams SET closed=1 WHERE name=?", [&self.name])
        })?;
        self.closed.store(true, Ordering::Release);
        self.cond.notify_all();
        Ok(())
    }

    /// Release the file handle (and the lock) on delete / store close; wakes subscribers.
    pub(crate) fn close_fds(&self) {
        let mut st = self.lock();
        st.file = None;
        st.owner = false;
        self.closed.store(true, Ordering::Release);
        self.cond.notify_all();
    }
}

/// Take the log's exclusive flock. `Ok(false)`: another handle owns it.
pub(crate) fn try_lock(file: &File) -> Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        // ponytail: file system without flock (e.g. some NFS setups): proceed as
        // writer, one-writer-per-stream is unenforced there, as on Windows.
        Err(TryLockError::Error(e)) if lock_unsupported(&e) => Ok(true),
        Err(TryLockError::Error(e)) => Err(e.into()),
    }
}

/// flock unavailable on this file system (same errno set as the Python engine).
fn lock_unsupported(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::Unsupported
        || e.raw_os_error().is_some_and(|code| {
            [libc::ENOLCK, libc::EOPNOTSUPP, libc::ENOTSUP, libc::ENOSYS].contains(&code)
        })
}

/// Scan + CRC-verify the log and build the index. The writer truncates a torn
/// tail so appends stay contiguous; readers leave the file alone.
fn recover(file: &File, owner: bool) -> Result<Vec<u64>> {
    let size = file.metadata()?.len();
    let index = scan(file, size)?;
    let good = *index.last().expect("index is never empty");
    if owner && good != size {
        file.set_len(good)?;
        fsync(file)?;
    }
    Ok(index)
}

/// Offsets of every intact frame, reading sequentially (no mmap: a concurrent
/// truncate by another process must not SIGBUS us).
fn scan(file: &File, size: u64) -> Result<Vec<u64>> {
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut index = vec![0];
    let mut pos = 0u64;
    let mut header = [0u8; HEADER_SIZE];
    let mut payload = Vec::new();
    while pos + HEADER_SIZE as u64 <= size {
        if !read_full(&mut reader, &mut header)? {
            break;
        }
        let (len, crc) = decode_header(header);
        let end = pos + (HEADER_SIZE + len) as u64;
        if end > size {
            break; // torn tail: header promises bytes we don't have
        }
        zeroed(&mut payload, len as u64)?; // a corrupt header can claim up to the rest of the file
        if !read_full(&mut reader, &mut payload)? || crc32(&payload) != crc {
            break; // file shrank under us, or corruption
        }
        index.push(end);
        pos = end;
    }
    Ok(index)
}

/// Resize `buf` to `len` zero bytes, as `OutOfMemory` instead of aborting the
/// process when the allocation fails.
fn zeroed(buf: &mut Vec<u8>, len: u64) -> Result<()> {
    let n = usize::try_from(len).map_err(|_| Error::OutOfMemory(len))?;
    buf.clear();
    buf.try_reserve_exact(n)
        .map_err(|_| Error::OutOfMemory(len))?;
    buf.resize(n, 0);
    Ok(())
}

/// `read_exact`, but EOF is `Ok(false)` instead of an error.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
    }
}
