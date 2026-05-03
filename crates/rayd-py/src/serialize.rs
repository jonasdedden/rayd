//! Pickle helpers using `CPython`'s `_pickle` via `PyO3`.
//!
//! These wrappers are the only place we touch `pickle.dumps`/`pickle.loads`.
//! They keep the rest of the binding crate free of GIL-attached `pickle`
//! handles and let us evolve to pickle protocol-5 out-of-band buffers in a
//! later phase without rewriting callers.

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyModule};

static PICKLE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
static CLOUDPICKLE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

fn pickle_handle(py: Python<'_>) -> PyResult<Py<PyModule>> {
    let cached = PICKLE.get_or_try_init::<_, PyErr>(py, || Ok(py.import("pickle")?.unbind()))?;
    Ok(cached.clone_ref(py))
}

fn cloudpickle_handle(py: Python<'_>) -> PyResult<Py<PyModule>> {
    let cached =
        CLOUDPICKLE.get_or_try_init::<_, PyErr>(py, || Ok(py.import("cloudpickle")?.unbind()))?;
    Ok(cached.clone_ref(py))
}

/// Pickle a Python object to bytes (protocol 5).
pub(crate) fn dumps(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    let pickle = pickle_handle(py)?;
    let dumps = pickle.bind(py).getattr("dumps")?;
    let bytes_obj = dumps.call1((value,))?;
    let bytes = bytes_obj.cast::<PyBytes>()?;
    Ok(Bytes::copy_from_slice(bytes.as_bytes()))
}

/// Unpickle bytes back to a Python object.
pub(crate) fn loads(py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
    let pickle = pickle_handle(py)?;
    let loads = pickle.bind(py).getattr("loads")?;
    let py_bytes = PyBytes::new(py, data);
    let result = loads.call1((py_bytes,))?;
    Ok(result.unbind())
}

/// Cloudpickle a Python object. Use this for callables and task arguments
/// that may close over locals or live in test modules — `pickle` alone fails
/// for those, but `cloudpickle.loads` accepts both `cloudpickle.dumps` and
/// stdlib-`pickle.dumps` output.
pub(crate) fn cloudpickle_dumps(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    let cp = cloudpickle_handle(py)?;
    let dumps = cp.bind(py).getattr("dumps")?;
    let bytes_obj = dumps.call1((value,))?;
    let bytes = bytes_obj.cast::<PyBytes>()?;
    Ok(Bytes::copy_from_slice(bytes.as_bytes()))
}
