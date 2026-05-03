# Analysis: Cython Binding Layer Pain Points

Why `python/ray/_raylet.pyx` is hard to extend and what that means for a clean PyO3 rewrite.

## Where the layer lives

- `python/ray/_raylet.pyx` — single huge `.pyx` file (~6k LOC at last check, growing).
- `python/ray/_raylet.pxd` — companion declaration file.
- `python/ray/includes/*.pxd` — declarations of the C++ types it wraps: `common.pxd`, `libcoreworker.pxd`, `unique_ids.pxd`, `function_descriptor.pxd`, `object_ref.pxd`, etc.
- `python/ray/includes/*.pxi` — included Cython "header"-style fragments (e.g., `object_ref.pxi` defines the `ObjectRef` Python class).

It compiles to a single shared library `ray/_raylet.cpython-*.so` that statically links the C++ core_worker and dynamically links gRPC, Apache Arrow / plasma client, and protobuf.

## What it wraps

The Cython layer exposes `class CoreWorker:` as the Python-side handle for the C++ `CoreWorker` instance. Every public Ray API method lands on this class:

```python
class CoreWorker:
    def submit_task(self, language, function_descriptor, args, name, num_returns, ...): ...
    def submit_actor_task(self, ...): ...
    def create_actor(self, ...): ...
    def get_objects(self, object_refs, timeout_ms): ...
    def wait(self, object_refs, num_objects, timeout_ms, ...): ...
    def put_serialized_object(self, serialized, ...): ...
    def free_objects(self, object_ids, local_only): ...
    def get_actor_handle(self, actor_id): ...
    def add_object_ref_reference(self, object_ref): ...
    def remove_object_ref_reference(self, object_ref): ...
    # ... ~80 more methods
```

Each maps to a method on the C++ `CoreWorker` (`src/ray/core_worker/core_worker.cc`), with Cython doing argument marshaling, GIL handling, and ObjectRef construction.

## How Python data crosses into C++

The mechanism is heavy:

1. **Python args → cloudpickle → bytes**: arbitrary Python objects are pickled by `python/ray/_private/serialization.py` using cloudpickle (with custom reducers for `ObjectRef`, actor handles, numpy, torch tensors).
2. **Pickle protocol 5 out-of-band buffers**: numpy arrays go to the out-of-band path so they can be zero-copy mmapped from plasma. The pickler's `buffer_callback` collects each `PickleBuffer`; the bytes are written directly into a plasma allocation rather than copied into the pickle stream.
3. **Cython builds a C++ `RayObject`**: from `(metadata_bytes, data_buffer, contained_refs)` and either inserts into `MemoryStore` or seals into plasma.
4. **GIL handling**: blocking gRPC and plasma waits run with the GIL released via `with nogil:`. Callbacks from C++ into Python (e.g., on task arrival) reacquire the GIL.

The reverse direction (`get_objects`) is symmetrical: C++ returns `vector<shared_ptr<RayObject>>`, Cython wraps each into a `SerializedRayObject` named tuple, Python `serialization.py` deserializes via cloudpickle.loads.

## The friction points

Specific issues that bite engineers extending Ray Core:

### 1. Build complexity
- The build is Bazel + CMake-style C++ + Cython on top.
- `_raylet.pyx` pulls in roughly the entire C++ tree by transitive include.
- Building Ray from source requires Bazel and 8+ GB of RAM and 30+ minutes on a fresh checkout.
- Cython compile alone for `_raylet.pyx` is multi-minute (one `.pyx` → tens of MB of generated C++).
- Bumping Cython, gRPC, or protobuf versions regularly breaks the layer.

### 2. Cython is in maintenance mode
- Cython 3 stabilized in 2023 but adoption is slow; many Ray-specific patterns target Cython 0.29.x semantics.
- Type system is bespoke (`cdef`, `cpdef`, `cdef extern from`) and doesn't compose with Python's modern type-hint ecosystem.
- IDE support is poor: navigating from `.pyx` to C++ definitions requires hopping through `.pxd` files manually.
- No equivalent of `pyo3-stub-gen`: stubs (`.pyi`) for `_raylet` are partially hand-written under `python/ray/_raylet.pyi`, partially missing, and frequently drift from the actual `.pyx`.

### 3. Hard-to-type API surface
- Many `_raylet.pyx` methods accept or return `object` (Cython's untyped variant), which surfaces as `Any` in Python.
- `python/ray/_raylet.pyi` is incomplete — large swathes of methods either aren't declared or are typed loosely (`Any`).
- Downstream `python/ray/...` modules import `_raylet` symbols and propagate `Any` through their public API.
- `mypy --strict` against the Ray Python tree reports thousands of errors today.

### 4. Forking / asyncio sharp edges
- Because `_raylet.so` holds gRPC threads and plasma mmap regions, `os.fork()` after `ray.init()` is unsafe (deadlocks in gRPC). Ray detects and warns. Recurring class of issues with PyTorch DataLoader workers.
- Async actors run on a separate thread with its own asyncio loop; bridging exceptions between the C++ task receiver and the Python loop is a long-running source of bugs.

### 5. Numpy zero-copy pitfalls
- Objects fetched from plasma return numpy arrays *backed by the plasma mmap*, so they are read-only.
- Users frequently file bugs (`ValueError: assignment destination is read-only`) and the workaround is `np.array(arr, copy=True)`.
- The semantics aren't well-documented at the API surface.

### 6. Pickle-by-value of large globals
- Cloudpickle serializes captured globals into the task closure, which can balloon task sizes.
- Ray emits "task is too large" warnings; the canonical workaround is `ray.put` then capture the `ObjectRef`. But this is a learned skill, not surfaced by the type system.

### 7. Gradual API ossification
- Because `_raylet.pyx` is hard to change, additions tend to bolt on rather than refactor.
- The `submit_task` signature has a long argument list (16+ positional args at last count) — no kwargs, no struct, just positional. Adding a new feature means adding another positional arg and updating every call site.

## Why PyO3 is materially better here

The same things that make Cython painful are PyO3's strengths:

| Concern | Cython today | PyO3 in the new system |
|---|---|---|
| **Stub generation** | Hand-written, drifts | `pyo3-stub-gen` emits `.pyi` from macros; `stubtest` enforces consistency in CI |
| **Type system** | `cdef` separate world from Python types | Rust types map directly to Python types via `IntoPy`/`FromPyObject`; `mypy --strict` clean across the FFI |
| **Build** | Bazel + Cython + C++ | `cargo` + `maturin`; one build system end-to-end |
| **IDE** | Marginal `.pyx` support | First-class Rust LSP through rust-analyzer |
| **Refactors** | Cross-language refactors are manual | Rust's `cargo check` catches binding signature changes; PyO3 macros tie Rust signature to Python signature |
| **Error model** | Cython exceptions are weakly typed | PyO3 supports custom exception class registration with typed fields via `#[pyclass(extends=PyException)]` |
| **API stability** | Long positional arg lists | Rust's `#[pyclass]` structs enable kwargs and struct args via PyO3's `#[pyo3(signature = (...))]` |
| **Async bridge** | Custom thread + loop, exceptions can leak | `pyo3-async-runtimes` provides correct `tokio` ↔ `asyncio` bridge |

## What the rewrite preserves

The substance of what Cython does well in Ray today and what PyO3 must reproduce:

1. **GIL release on blocking calls**. PyO3 has `Python::allow_threads(|| ...)` which is the equivalent.
2. **Out-of-band pickle5 buffers**. PyO3 can call `pickle.dumps(obj, protocol=5, buffer_callback=cb)` from Rust and grab `PickleBuffer` objects; the callback can route their underlying memoryviews directly into mmap'd plasma allocations.
3. **Custom reducers for ObjectRef and actor handles**. Implemented Python-side via `__reduce__` hooks; the new system's `ObjectRef` and `ActorHandle` Python classes (which are `#[pyclass]` Rust structs exposed to Python) implement the same.
4. **Zero-copy numpy from plasma**. `numpy` crate provides typed array views over a Rust pointer + length; combined with mmap'd plasma buffers and `Python::allow_threads`, the round-trip is fundamentally the same — just cleaner to express.
5. **Synchronous, blocking core_worker API**. PyO3 functions that don't return futures look like plain Python functions to users. Async APIs use `pyo3-async-runtimes`.

## What the rewrite intentionally drops

- Cross-language workers (Java, C++). v1 is Python-only; multi-language can come later via additional bindings (or not at all).
- Streaming generators (`num_returns="streaming"`). v1 supports `num_returns: int >= 1` only.
- Runtime envs (pip/conda installation per worker). v1 assumes a homogeneous Python environment cluster-wide.
- Placement groups in their full generality. v1 supports CPU/GPU/memory + named custom resources only.

These cuts let v1 keep the binding layer surface area small enough to type strictly end-to-end.
