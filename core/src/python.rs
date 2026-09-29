//! PyO3 bindings: `durastream.core`, API-compatible with the pure-Python engine.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyKeyError, PyMemoryError, PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList};

use crate::{DurableStream, Error, Frames, Store};

// One set of exception classes for both engines, defined in Python.
pyo3::import_exception!(durastream.errors, DurastreamError);
pyo3::import_exception!(durastream.errors, StreamClosed);
pyo3::import_exception!(durastream.errors, StreamLocked);
pyo3::import_exception!(durastream.errors, CorruptStream);

impl From<Error> for PyErr {
    fn from(e: Error) -> PyErr {
        let msg = e.to_string();
        match e {
            Error::Closed(n) => StreamClosed::new_err(n),
            Error::Locked(n) => StreamLocked::new_err(n),
            Error::NotFound(n) => PyKeyError::new_err(n),
            Error::Corrupt(m) => CorruptStream::new_err(m),
            Error::StoreClosed => DurastreamError::new_err(msg),
            Error::Invalid(m) => PyValueError::new_err(m),
            Error::OutOfMemory(_) => PyMemoryError::new_err(msg),
            Error::Io(e) => os_error(e),
            Error::Db(_) => DurastreamError::new_err(msg),
        }
    }
}

/// `OSError(errno, strerror)`, like Python's own I/O errors: keeps `e.errno`, and
/// Python picks the subclass (`FileNotFoundError`, ...). pyo3's default
/// conversion drops the errno.
fn os_error(e: std::io::Error) -> PyErr {
    match e.raw_os_error() {
        Some(code) => {
            let msg = e.to_string();
            let strerror = msg.trim_end_matches(&format!(" (os error {code})"));
            PyOSError::new_err((code, strerror.to_owned()))
        }
        None => e.into(),
    }
}

fn clamp(v: i64) -> u64 {
    v.max(0) as u64
}

#[pyclass(name = "Store", frozen)]
struct PyStore(Store);

#[pymethods]
impl PyStore {
    #[new]
    fn new(py: Python<'_>, root: PathBuf) -> PyResult<Self> {
        Ok(Self(py.detach(|| Store::new(root))?))
    }

    /// Lossless for non-UTF-8 paths (decoded like `os.fsdecode`).
    #[getter]
    fn root(&self) -> &OsStr {
        self.0.root().as_os_str()
    }

    #[pyo3(signature = (name, content_type=None))]
    fn create(&self, py: Python<'_>, name: &str, content_type: Option<&str>) -> PyResult<PyStream> {
        Ok(PyStream(py.detach(|| self.0.create(name, content_type))?))
    }

    fn open(&self, py: Python<'_>, name: &str) -> PyResult<PyStream> {
        Ok(PyStream(py.detach(|| self.0.open(name))?))
    }

    fn delete(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        Ok(py.detach(|| self.0.delete(name))?)
    }

    fn list(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        Ok(py.detach(|| self.0.list())?)
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| self.0.close())
    }
}

#[pyclass(name = "DurableStream", frozen)]
struct PyStream(Arc<DurableStream>);

#[pymethods]
impl PyStream {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[getter]
    fn content_type(&self) -> &str {
        self.0.content_type()
    }

    #[getter]
    fn next_offset(&self) -> u64 {
        self.0.next_offset()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.0.closed()
    }

    #[getter]
    fn writable(&self, py: Python<'_>) -> bool {
        // takes the stream lock, which an append holds across fsync: don't hold the GIL
        py.detach(|| self.0.is_writer())
    }

    fn append(&self, py: Python<'_>, payload: PyBackedBytes) -> PyResult<u64> {
        Ok(py.detach(|| self.0.append(&payload))?)
    }

    fn append_many(&self, py: Python<'_>, payloads: Vec<PyBackedBytes>) -> PyResult<u64> {
        Ok(py.detach(|| self.0.append_many(&payloads))?)
    }

    #[pyo3(signature = (offset=0, end=None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        offset: i64,
        end: Option<i64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let frames = py.detach(|| self.0.read_frames(clamp(offset), end.map(clamp)))?;
        PyList::new(py, frames.iter().map(|p| PyBytes::new(py, p)))
    }

    #[pyo3(signature = (offset=0))]
    fn subscribe(&self, offset: i64) -> Subscription {
        Subscription {
            stream: self.0.clone(),
            offset: clamp(offset),
            frames: Frames::default(),
            pos: 0,
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        Ok(py.detach(|| self.0.close())?)
    }

    /// Call `callback()` after every change (append, close, delete), from the
    /// writing thread. Exceptions go to `sys.unraisablehook`. Returns an id for
    /// `remove_listener`.
    fn add_listener(&self, callback: Py<PyAny>) -> u64 {
        self.0.add_listener(move || {
            // Runs in the writer's thread, which released the GIL for the I/O.
            // try_attach: skip, rather than hang or panic, during interpreter shutdown.
            Python::try_attach(|py| {
                if let Err(e) = callback.call0(py) {
                    e.write_unraisable(py, Some(callback.bind(py)));
                }
            });
        })
    }

    fn remove_listener(&self, id: u64) -> bool {
        self.0.remove_listener(id)
    }
}

/// Tail -f iterator. Buffers one verified read, hands out a record per `__next__`.
#[pyclass]
struct Subscription {
    stream: Arc<DurableStream>,
    offset: u64,
    frames: Frames,
    pos: usize,
}

#[pymethods]
impl Subscription {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        while self.pos >= self.frames.len() {
            let (s, off) = (&self.stream, self.offset);
            self.frames = py.detach(|| s.read_frames(off, None))?;
            self.pos = 0;
            if !self.frames.is_empty() {
                break;
            }
            // Short timed waits so Ctrl-C still reaches Python.
            if !py.detach(|| s.wait(off, Some(Duration::from_millis(100)))) {
                return Ok(None);
            }
            py.check_signals()?;
        }
        let rec = self.frames.get(self.pos).map(|p| PyBytes::new(py, p));
        self.pos += 1;
        self.offset += 1;
        Ok(rec)
    }
}

#[pyfunction]
fn to_token(offset: u64) -> String {
    crate::to_token(offset)
}

#[pyfunction]
fn from_token(token: &str, next_offset: u64) -> PyResult<i64> {
    Ok(crate::from_token(token, next_offset)?)
}

#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStore>()?;
    m.add_class::<PyStream>()?;
    m.add_function(wrap_pyfunction!(to_token, m)?)?;
    m.add_function(wrap_pyfunction!(from_token, m)?)?;
    Ok(())
}
