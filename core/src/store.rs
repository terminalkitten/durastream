use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension};

use crate::stream::{DurableStream, try_lock};
use crate::{Error, Result, fsync};

/// Content type used when `create` is given none.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Metadata DB shared by a store and its streams; `None` once the store is closed.
pub(crate) type Db = Arc<Mutex<Option<Connection>>>;

/// Validate a new stream name: `[a-z0-9._-]+`. Lowercase only, so two names can
/// never map to one file on a case-insensitive file system (macOS default).
pub fn check_name(name: &str) -> Result<&str> {
    let ok = !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
    if ok {
        Ok(name)
    } else {
        Err(Error::Invalid(format!(
            "invalid stream name {name:?}: use lowercase letters, digits, '.', '_' or '-'"
        )))
    }
}

/// Run `f` against the metadata DB; errors if the store was closed.
pub(crate) fn with_db<T>(
    db: &Mutex<Option<Connection>>,
    f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
) -> Result<T> {
    let db = db.lock().unwrap_or_else(PoisonError::into_inner);
    Ok(f(db.as_ref().ok_or(Error::StoreClosed)?)?)
}

/// A directory of streams: `meta.db` plus `streams/<name>.log`.
// Lock order: open map -> stream state -> db.
pub struct Store {
    root: PathBuf,
    streams_dir: PathBuf,
    db: Db,
    // ponytail: one lock for the whole map, held across a stream's recovery scan,
    // so opening a huge log blocks other opens; per-name slots if that matters.
    open: Mutex<HashMap<String, Arc<DurableStream>>>, // one DurableStream per name
}

impl Store {
    /// Open (or initialize) the store at `root`.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let streams_dir = root.join("streams");
        fs::create_dir_all(&streams_dir)?;
        let db = Connection::open(root.join("meta.db"))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS streams(\
               name TEXT PRIMARY KEY,\
               content_type TEXT NOT NULL,\
               closed INTEGER NOT NULL DEFAULT 0,\
               created_at REAL NOT NULL)",
            [],
        )?;
        Ok(Self {
            root,
            streams_dir,
            db: Arc::new(Mutex::new(Some(db))),
            open: Mutex::new(HashMap::new()),
        })
    }

    /// Directory this store lives in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, name: &str) -> PathBuf {
        self.streams_dir.join(format!("{name}.log"))
    }

    fn fsync_dir(&self) -> Result<()> {
        // persist dir entries (new/removed logs), so they survive a crash
        Ok(fsync(&File::open(&self.streams_dir)?)?)
    }

    fn open_map(&self) -> MutexGuard<'_, HashMap<String, Arc<DurableStream>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn row(&self, name: &str) -> Result<Option<(String, bool)>> {
        with_db(&self.db, |db| {
            db.query_row(
                "SELECT content_type, closed FROM streams WHERE name=?",
                [name],
                |r| Ok((r.get(0)?, r.get::<_, i64>(1)? != 0)),
            )
            .optional()
        })
    }

    /// Create a stream, or return the existing one (idempotent).
    /// Errors with `Invalid` if `content_type` is given and differs from the existing one.
    pub fn create(&self, name: &str, content_type: Option<&str>) -> Result<Arc<DurableStream>> {
        check_name(name)?;
        let mut open = self.open_map();
        if let Some(s) = open.get(name) {
            return match content_type {
                Some(ct) if ct != s.content_type() => Err(mismatch(name, s.content_type(), ct)),
                _ => Ok(s.clone()),
            };
        }
        match self.row(name)? {
            Some((existing, closed)) => match content_type {
                Some(ct) if ct != existing => Err(mismatch(name, &existing, ct)),
                _ => self.open_stream(&mut open, name, existing, closed),
            },
            None => {
                let ct = content_type.unwrap_or(DEFAULT_CONTENT_TYPE);
                // A log without a row is left over from a delete that crashed
                // midway: never resurrect its records into the new stream.
                let path = self.path(name);
                let guard = lock_for_removal(name, &path)?;
                remove_if_exists(&path)?;
                drop(guard);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                with_db(&self.db, |db| {
                    db.execute(
                        "INSERT INTO streams(name, content_type, closed, created_at) VALUES(?,?,0,?)",
                        rusqlite::params![name, ct, now],
                    )
                })?;
                let s = self.open_stream(&mut open, name, ct.to_owned(), false)?; // creates the log
                self.fsync_dir()?;
                Ok(s)
            }
        }
    }

    /// Open an existing stream. Read-only if another process owns its log.
    pub fn open(&self, name: &str) -> Result<Arc<DurableStream>> {
        let mut open = self.open_map();
        if let Some(s) = open.get(name) {
            return Ok(s.clone());
        }
        let (content_type, closed) = self
            .row(name)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        self.open_stream(&mut open, name, content_type, closed)
    }

    fn open_stream(
        &self,
        open: &mut HashMap<String, Arc<DurableStream>>,
        name: &str,
        content_type: String,
        closed: bool,
    ) -> Result<Arc<DurableStream>> {
        let s = Arc::new(DurableStream::open(
            self.db.clone(),
            name.into(),
            &self.path(name),
            content_type,
            closed,
        )?);
        open.insert(name.into(), s.clone());
        Ok(s)
    }

    /// Delete a stream: its log and its metadata. Fails with `Locked` if another
    /// process owns the log.
    pub fn delete(&self, name: &str) -> Result<()> {
        let mut open = self.open_map();
        with_db(&self.db, |_| Ok(()))?; // store must be open before any file is touched
        let path = self.path(name);
        // Hold the log's lock across the unlink (ours if we're the writer, else a
        // probe), so no other process can start writing a file about to vanish.
        let ours = open.get(name).is_some_and(|s| s.is_writer());
        let guard = if ours {
            None
        } else {
            lock_for_removal(name, &path)?
        };
        // file first: a crash after this leaves a row with no log, which reopens
        // empty; the reverse order could resurrect deleted records
        let removed = remove_if_exists(&path)?;
        if let Some(s) = open.remove(name) {
            s.close_fds();
        }
        drop(guard);
        if removed {
            self.fsync_dir()?;
        }
        with_db(&self.db, |db| {
            db.execute("DELETE FROM streams WHERE name=?", [name])
        })?;
        Ok(())
    }

    /// Stream names, sorted.
    pub fn list(&self) -> Result<Vec<String>> {
        with_db(&self.db, |db| {
            db.prepare("SELECT name FROM streams ORDER BY name")?
                .query_map([], |r| r.get(0))?
                .collect()
        })
    }

    /// Release every stream's file handle and close the metadata DB.
    pub fn close(&self) {
        let mut open = self.open_map();
        for s in open.values() {
            s.close_fds();
        }
        open.clear();
        *self.db.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// Lock the log before unlinking it; keep the returned file alive until the
/// unlink is done. Err(`Locked`) if another handle owns the log; `None` if
/// there is no log.
fn lock_for_removal(name: &str, path: &Path) -> Result<Option<File>> {
    match File::open(path) {
        Ok(f) if try_lock(&f)? => Ok(Some(f)),
        Ok(_) => Err(Error::Locked(name.into())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Remove `path`; `Ok(false)` if it didn't exist.
fn remove_if_exists(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn mismatch(name: &str, existing: &str, given: &str) -> Error {
    Error::Invalid(format!(
        "content_type mismatch for {name:?}: {existing:?} != {given:?}"
    ))
}
