//! Rust engine behind the `durastream` Python package (`durastream.core`).
//!
//! Same on-disk format as the pure-Python engine (`[len u32 BE][crc32 u32 BE][payload]`
//! frames in `streams/<name>.log`, metadata in `meta.db`) and the same `flock`
//! on each log, so both engines can open, and coordinate on, the same store.

#[cfg(not(unix))]
compile_error!(
    "durastream-core supports Unix (Linux, macOS) only; other platforms use the pure-Python engine"
);

mod codec;
#[cfg(feature = "python")]
mod python;
mod store;
mod stream;

pub use codec::{HEADER_SIZE, TOKEN_WIDTH, from_token, iter_frames, to_token};
pub use store::{DEFAULT_CONTENT_TYPE, Store, check_name};
pub use stream::{DurableStream, Frames};

use std::io;
use std::os::fd::AsRawFd;

/// Errors returned by the engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The stream is closed, deleted, or disabled after a failed write.
    #[error("stream closed: {0}")]
    Closed(String),
    /// Another process owns this stream's log (one writer per stream); this handle is read-only.
    #[error("stream is locked by another process: {0}")]
    Locked(String),
    /// No stream with this name.
    #[error("no such stream: {0}")]
    NotFound(String),
    /// Bytes on disk no longer match their checksum.
    #[error("corrupt stream: {0}")]
    Corrupt(String),
    /// The store was closed.
    #[error("store is closed")]
    StoreClosed,
    /// Bad argument: stream name, content-type mismatch, token, oversized payload.
    #[error("{0}")]
    Invalid(String),
    /// Could not allocate the buffer for a read.
    #[error("out of memory reading {0} bytes")]
    OutOfMemory(u64),
    /// File system error.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Metadata database error.
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// fsync matching Python's `getattr(os, "fdatasync", os.fsync)`.
// ponytail: plain fsync/fdatasync for parity with the Python engine. std's
// File::sync_data issues F_FULLFSYNC on macOS (stronger, ~10x slower); switch
// to it if you need power-loss durability on Apple hardware.
pub(crate) fn fsync(f: &impl AsRawFd) -> io::Result<()> {
    let fd = f.as_raw_fd();
    // SAFETY: `fd` comes from a live `AsRawFd` borrow, so it is open for the whole
    // call; fsync/fdatasync only take an fd and touch no Rust memory.
    #[cfg(target_os = "linux")]
    let r = unsafe { libc::fdatasync(fd) };
    // SAFETY: as above.
    #[cfg(not(target_os = "linux"))]
    let r = unsafe { libc::fsync(fd) };
    if r == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
