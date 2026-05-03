# Design: Python Bindings

The PyO3 layer is the **only** Python-visible entry point. All public API methods route through it. This document specifies module layout, exception hierarchy, stub generation, async bridge, and GIL discipline.

## Module shape

```
import rayd

# top-level functions
rayd.init(address: str | None = None) -> None
rayd.shutdown() -> None

# task / actor decorators
@rayd.remote
def f(x: int) -> int: ...

@rayd.remote
class MyActor:
    def __init__(self, n: int) -> None: ...
    def increment(self) -> int: ...

# put / get / wait
rayd.put(value: T, *, owner: ActorHandle | None = None) -> ObjectRef[T]
rayd.get(refs: ObjectRef[T] | list[ObjectRef[T]], *, timeout: float | None = None) -> T | list[T]
rayd.get_settled(refs: list[ObjectRef[T]], *, timeout: float | None = None) -> list[Result[T]]
rayd.wait(refs: list[ObjectRef[T]], *, num_returns: int = 1, timeout: float | None = None,
          fetch_local: bool = True) -> tuple[list[ObjectRef[T]], list[ObjectRef[T]]]
rayd.wait_with_states(refs: list[ObjectRef[T]], *, timeout: float | None = None,
                      fetch_local: bool = False) -> dict[ObjectRef[T], RefState]

rayd.cancel(ref: ObjectRef[T], *, force: bool = False) -> None
rayd.kill(actor: ActorHandle, *, no_restart: bool = True) -> None
```

The `rayd.<symbol>` form is a Python module that re-exports from `rayd._native` (the PyO3 extension). Pure-Python wrappers exist only for things PyO3 can't generate stubs for (mostly: generic `TypeVar` plumbing) — see the "Type variables and generics" section.

## Crate layout

```
crates/rayd-py/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # #[pymodule] root, registers everything
│   ├── core_worker.rs         # CoreWorker #[pyclass] (the singleton handle)
│   ├── object_ref.rs          # ObjectRef #[pyclass]
│   ├── actor_handle.rs        # ActorHandle #[pyclass]
│   ├── result.rs              # Result, Ok, Err, Pending #[pyclass] hierarchy
│   ├── error_info.rs          # ErrorInfo, ErrorCategory enum
│   ├── ref_state.rs           # RefState enum (#[pyclass] enum)
│   ├── exceptions.rs          # registered Python exception hierarchy
│   ├── serialization.rs       # pickle5 with out-of-band buffer callbacks
│   ├── runtime.rs             # tokio runtime + GIL coordination
│   └── stub_gen.rs            # bin: emits python/rayd/_native.pyi
└── ...
```

## `#[pymodule]` root

```rust
// crates/rayd-py/src/lib.rs
use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize the tokio runtime once.
    runtime::init(py)?;

    // Register pyclasses.
    m.add_class::<core_worker::CoreWorker>()?;
    m.add_class::<object_ref::ObjectRef>()?;
    m.add_class::<actor_handle::ActorHandle>()?;
    m.add_class::<result::Pending>()?;
    m.add_class::<result::Ok>()?;
    m.add_class::<result::Err>()?;
    m.add_class::<error_info::ErrorInfo>()?;
    m.add_class::<error_info::ErrorCategory>()?;
    m.add_class::<ref_state::RefState>()?;

    // Register exception hierarchy.
    exceptions::register(py, m)?;

    // Top-level functions.
    m.add_function(wrap_pyfunction!(top_level::init, m)?)?;
    m.add_function(wrap_pyfunction!(top_level::shutdown, m)?)?;
    // ... etc
    Ok(())
}

define_stub_info_gatherer!(stub_info);
```

## `ObjectRef` `#[pyclass]`

```rust
// crates/rayd-py/src/object_ref.rs
use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;

#[gen_stub_pyclass]
#[pyclass(name = "ObjectRef", frozen, module = "rayd._native")]
pub struct ObjectRef {
    inner: rayd_core::ObjectRef,
}

#[gen_stub_pymethods]
#[pymethods]
impl ObjectRef {
    #[getter]
    fn hex(&self) -> String { self.inner.id().hex() }

    #[getter]
    fn owner_address(&self) -> &str { self.inner.owner().as_str() }

    /// Cheap state inspection. Reads metadata only; does NOT deserialize the value.
    fn state(&self, py: Python<'_>) -> PyResult<RefState> {
        py.allow_threads(|| Ok(runtime::block_on(self.inner.state())))
    }

    /// Returns the error info (category, message, traceback) without deserializing
    /// the user payload. Returns None for refs that are pending or successful.
    fn peek_error(&self, py: Python<'_>) -> PyResult<Option<ErrorInfo>> {
        py.allow_threads(|| Ok(runtime::block_on(self.inner.peek_error())))
    }

    /// Returns the original Python exception. Heavier than peek_error; unpickles.
    fn exception(&self, py: Python<'_>) -> PyResult<Option<PyObject>> { ... }

    fn is_ready(&self, py: Python<'_>) -> PyResult<bool> { ... }
    fn is_failed(&self, py: Python<'_>) -> PyResult<bool> { ... }

    fn __repr__(&self) -> String { format!("ObjectRef({})", self.inner.id().hex()) }
    fn __hash__(&self) -> u64 { self.inner.id().hash64() }
    fn __eq__(&self, other: &Self) -> bool { self.inner == other.inner }

    /// Awaitable: returns the value (or raises) on completion.
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<PyObject> { ... }

    /// Custom reducer so ObjectRef survives pickling for cross-worker passing.
    fn __reduce__(slf: PyRef<'_, Self>) -> PyResult<(PyObject, PyObject)> { ... }
}
```

`#[pyclass(frozen)]` makes the inner field immutable from Python's side, lets PyO3 share `&self` access without `RefCell`, and enables `__hash__` because frozen pyclasses are hashable.

`#[gen_stub_pyclass]` and `#[gen_stub_pymethods]` are macros from `pyo3-stub-gen` that record the type information needed to emit `.pyi` stubs.

## Generated `.pyi` for `ObjectRef`

What `pyo3-stub-gen` will produce (after manual addition of generic parametrization — see below):

```python
# python/rayd/_native.pyi
from __future__ import annotations
from typing import Generic, TypeVar
import asyncio

T = TypeVar("T")

class ObjectRef(Generic[T]):
    @property
    def hex(self) -> str: ...
    @property
    def owner_address(self) -> str: ...

    def state(self) -> RefState: ...
    def peek_error(self) -> ErrorInfo | None: ...
    def exception(self) -> BaseException | None: ...
    def is_ready(self) -> bool: ...
    def is_failed(self) -> bool: ...

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __await__(self) -> "asyncio.Future[T]": ...
    def __reduce__(self) -> tuple[object, ...]: ...
```

## Type variables and generics

PyO3 cannot natively express `Generic[T]` parametrization. Two options:

1. **Hand-edit the generated `.pyi`** to add `Generic[T]` parametrization on `ObjectRef`, `Result`, etc. Acceptable — it's a single annotation fixup, drift-detected by `stubtest`. We do this.
2. **Wrap each binding class in a thin Python class** that adds the generic parameter. Cleaner but adds an indirection layer.

We pick **(1)** for v1: the binding class names are stable, the `Generic[T]` wrap is mechanical, and `stubtest` will catch any drift.

The fixup is a single sed-style transformation in `python/rayd/_native.pyi` (a few lines, applied by the `make stubs` target after `cargo run --bin stub_gen`). Documented in `crates/rayd-py/STUB_GEN.md`.

## Exception hierarchy

```python
# What users see
class RaydError(Exception): ...                 # root

class TaskException(RaydError):                 # task body raised
    cause: BaseException
    traceback: str

class WorkerCrashed(RaydError):                 # worker died mid-task
    worker_id: str

class ActorDied(RaydError):
    actor_id: str
    restart_count: int

class OwnerDied(RaydError):
    owner_address: str

class TaskCancelled(RaydError):
    task_id: str

class ObjectLost(RaydError):
    object_id: str

class ObjectUnreconstructable(ObjectLost):
    reason: str   # "lineage_evicted" | "max_attempts_exceeded" | "no_lineage"

class FetchTimeout(RaydError):
    object_id: str

class RuntimeEnvFailed(RaydError):
    detail: str

class Unschedulable(RaydError):
    reason: str

class ObjectStoreFull(RaydError):
    requested_bytes: int
    available_bytes: int
```

Implemented Rust-side as `#[pyclass(extends=PyException)]` for each leaf. PyO3 supports `extends=PyException` natively; the structured fields (`worker_id`, `actor_id`, etc.) are exposed as properties.

```rust
// crates/rayd-py/src/exceptions.rs
#[gen_stub_pyclass]
#[pyclass(extends=PyException, module="rayd._native")]
pub struct WorkerCrashed { worker_id: String }

#[gen_stub_pymethods]
#[pymethods]
impl WorkerCrashed {
    #[new]
    fn new(message: String, worker_id: String) -> (Self, PyException) {
        (Self { worker_id }, PyException::new_err(message).into())
    }
    #[getter]
    fn worker_id(&self) -> &str { &self.worker_id }
}
```

The `From<CoreError> for PyErr` impl (see `error.rs`) maps each Rust enum variant to the appropriate Python exception class with structured fields preserved.

## Result hierarchy (for `get_settled`)

```python
class Pending: ...
class Ok(Generic[T]):
    value: T
class Err:
    info: ErrorInfo

Result = Pending | Ok[T] | Err
```

Implemented as three `#[pyclass]` types. Pattern-matchable in Python via `isinstance` chains and (3.10+) `match` statements:

```python
match rayd.get_settled([r]):
    case [Ok(value=v)]: ...
    case [Err(info=info)]: ...
    case [Pending()]: ...
```

## Async bridge

`rayd.aio.get(refs)` and `await object_ref` both go through `pyo3-async-runtimes`:

```rust
use pyo3_async_runtimes::tokio::future_into_py;

#[pyfunction]
fn aio_get<'py>(py: Python<'py>, refs: Vec<PyRef<'_, ObjectRef>>)
    -> PyResult<Bound<'py, PyAny>>
{
    let inner_refs: Vec<rayd_core::ObjectRef> = refs.into_iter().map(|r| r.inner.clone()).collect();
    future_into_py(py, async move {
        let values = core_worker().get_settled(&inner_refs, None).await;
        Python::with_gil(|py| Ok(serialize_results(py, values)?))
    })
}
```

`future_into_py` releases the GIL across the `.await`, reacquires for the final result. Critical: never hold a `Bound<'_, PyAny>` across an `.await`.

## GIL discipline

Three rules:

1. **Always `Python::allow_threads(|| ...)` around blocking core_worker calls.** Even synchronous-looking calls (`object_ref.state()`) wrap their internals in `allow_threads` so concurrent Python threads can make progress.
2. **Never hold a `Py<PyAny>` across a `.await`.** Convert to owned Rust types before suspending.
3. **GIL acquisitions inside `tokio::spawn`'d tasks use `Python::with_gil`** — and only briefly, because they block the runtime thread.

The serialization layer is the only place this gets subtle. Pickling needs the GIL; the resulting bytes are released to plasma without the GIL. We carefully scope `Python::with_gil { let bytes = pickle.dumps(...)?; bytes.into_owned() }` so the lock is held only for the pickling itself, not for the plasma write.

## Serialization layer

```rust
// crates/rayd-py/src/serialization.rs
pub struct Serializer {
    pickle_module: Py<PyModule>,
    pickle_buffer_class: Py<PyType>,
}

impl Serializer {
    pub fn serialize(&self, py: Python<'_>, value: &Bound<'_, PyAny>)
        -> PyResult<SerializedRayObject>
    {
        // Use pickle protocol 5 with buffer_callback to capture out-of-band buffers.
        let buffers: Py<PyList> = PyList::empty(py).into();
        let buffer_callback = make_callback(py, buffers.clone_ref(py))?;
        let bytes = self.pickle_module
            .getattr(py, "dumps")?
            .call1(py, (value, py.None(), 5, buffer_callback))?;

        // bytes is the pickle stream (small for non-numpy objects).
        // buffers contains PickleBuffer objects whose .raw() points to large numpy memory.
        let nested_refs = scan_for_object_refs(py, value)?;

        Ok(SerializedRayObject {
            metadata: Metadata::Pickle5 { has_nested_refs: !nested_refs.is_empty() },
            data: bytes_concat_with_buffers(bytes, &buffers, py)?,
            nested_refs,
        })
    }
}
```

`SerializedRayObject` is the Rust counterpart of Ray's; it crosses into `rayd-core` for store insertion.

For zero-copy on numpy: the buffer callback writes directly into a plasma allocation if the total size exceeds the inline threshold. For inline objects, it concatenates into a single `Bytes` to ride the gRPC reply.

## Custom reducers

`ObjectRef`, `ActorHandle` need custom pickling so they round-trip across workers correctly. PyO3 supports `__reduce__` and `__reduce_ex__` natively:

```rust
#[pymethods]
impl ObjectRef {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(PyObject, PyObject)> {
        let constructor = py.import_bound("rayd._native")?.getattr("ObjectRef")?;
        let args = (self.inner.id().to_bytes(), self.inner.owner().to_bytes());
        Ok((constructor.into(), args.into_py(py)))
    }
}
```

`OBJECT_METADATA_TYPE_ACTOR_HANDLE` (the metadata tag for actor-handle objects) ensures that an `ActorHandle` round-tripped through `ray.put` deserializes correctly.

## Stub generation in CI

```makefile
stubs:
	cargo run --bin stub_gen --release
	python tools/fix_generic_stubs.py python/rayd/_native.pyi

check-stubs: stubs
	git diff --exit-code python/rayd/_native.pyi
	python -m mypy.stubtest rayd._native
```

CI runs `make check-stubs` on every PR. The `fix_generic_stubs.py` script is a tiny (≈30 LOC) post-processor that adds `Generic[T]` parametrization to a fixed list of classes (`ObjectRef`, `Ok`).

## What makes this typed end-to-end

Every step from user code to Rust core has a typed signature:

```
user code                rayd public API     PyO3 binding              Rust core
typed Python code  ──►   typed function  ──► #[pyfunction] sig    ──►  Rust fn signature
                                              with concrete types       with domain enums
```

No `Any`, no `object`, no untyped `**kwargs`. The exception hierarchy is a typed enum on both sides. The state and error inspection APIs return concrete classes the caller can `isinstance`-check. `mypy --strict` and `ruff check` pass over the whole tree without any inline disables.

The one place Python's type system stretches: `Generic[T]` on `ObjectRef`. PyO3 doesn't natively support it; we add the parametrization in a stub post-processor and document the boundary. `stubtest` ensures the typing matches the actual runtime classes.
