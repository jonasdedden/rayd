# Analysis: The `ObjectRef` State / Error API Gap

The user-facing pain point that motivates this rewrite. This document characterizes it precisely so the design in `../design/05-state-and-error-api.md` can target it surgically.

## The behavior

`ray.get(refs)` where `refs: list[ObjectRef]` returns `list[Any]` **only if every ref resolved successfully**. As soon as any one ref's task raised, was killed, lost its owner, or had its plasma copy lost, `ray.get` re-raises the corresponding exception. Successfully-resolved refs in the same call are discarded — there is no partial-success return path.

This holds in both **local mode** (single process) and via **Ray Client** (gRPC tunnel from a remote driver).

## Where the raise happens

The trace, anchored in source:

1. **`python/ray/_private/worker.py::Worker.get_objects`** is the entry point. It validates types and then calls into Cython:

   ```python
   serialized_objects: list[serialization.SerializedRayObject] = self.core_worker.get_objects(
       object_refs, timeout_ms,
   )
   ```

   `SerializedRayObject` is a tuple of `(data, metadata, transport)`. **One per ref. The C++ layer has already returned per-object metadata + data.**

2. The Python loop that turns metadata into exceptions:

   ```python
   if not return_exceptions:
       # Raise exceptions instead of returning them to the user.
       for value in values:
           if isinstance(value, RayError):
               if isinstance(value, ray.exceptions.ObjectLostError) and not isinstance(
                   value, ray.exceptions.OwnerDiedError
               ):
                   global_worker.core_worker.log_plasma_usage()
               if isinstance(value, RayTaskError):
                   raise value.as_instanceof_cause()
               else:
                   raise value
   ```

   This is the entire bug. **`raise` happens inside `for value in values:` on the first error.** Subsequent values are never returned to the user.

3. The C++ layer (`CoreWorker::Get`) doesn't itself raise — it returns a `vector<shared_ptr<RayObject>>` with each `RayObject` carrying its own `data` + `metadata`. The error is conveyed by metadata; the raise is purely a Python-side decision.

The interesting consequence: **the C++ layer already supports per-ref error reporting.** The single-error-aborts-batch behavior is a Python-side artifact of the serialization-context loop. A reimplementation that simply iterates the same C++-level `(data, metadata)` pairs without raising mid-loop solves the problem immediately.

## `ray.wait` doesn't help

Signature in `python/ray/_private/worker.py`:

```python
ray.wait(
    object_refs: list[ObjectRef],
    *,
    num_returns: int = 1,
    timeout: float | None = None,
    fetch_local: bool = True,
) -> tuple[list[ObjectRef], list[ObjectRef]]
```

Returns `(ready, not_ready)`. **A failed task counts as "ready"** — the result `RayObject` is materialized in the store with an error-typed `metadata`, so from the raylet's perspective it's available. `ray.wait` does *not* expose the metadata.

There is no `failed` bucket and no flag. The only way to discover failure given `ray.wait`'s output is to call `ray.get([ref])` per ref in a `try/except`, which:

- Forces a full deserialization (including the pickled exception payload) just to learn "did this fail?".
- Costs N round-trips into the core_worker for N refs instead of one.

`fetch_local=True` (default) makes `ray.wait` wait until the object exists *on the local node*. `fetch_local=False` returns "ready" once the object exists *anywhere* in the cluster. This is relevant for distinguishing local-ready from remote-ready in the new API.

## Existing user workarounds

### (a) Wrap every task body in try/except, return a tagged result

```python
@ray.remote
def safe_f(*args):
    try:
        return ("ok", real_f(*args))
    except Exception as e:
        return ("err", repr(e), traceback.format_exc())
```

`ray.get` then never raises. Cost: every task definition is now wrapper-aware; users lose native exception propagation; standard libraries can't be `@ray.remote`'d without a wrapper layer.

### (b) `ray.wait` with `num_returns=len(refs)`, then per-ref `ray.get`

```python
ready, _ = ray.wait(refs, num_returns=len(refs), timeout=timeout)
results: list[Result] = []
for r in ready:
    try:
        results.append(("ok", ray.get(r)))
    except Exception as e:
        results.append(("err", e))
```

The most common workaround, suggested across Ray's Discourse forum.

Cost:
- **N round-trips** into the core_worker (each `ray.get([r])` is a Cython call → C++ → memory store / plasma → Python decode). For N=1000 small tasks the dispatch overhead is order-of-magnitude (tens to hundreds of ms) of pure overhead, vs. a single batched `ray.get(refs)` of a few ms.
- **Forces full deserialization** even for objects you only want to know the *state* of. If a task returned a 100 MB array successfully but you only wanted to know "did it succeed", you paid for the unpickle + buffer copy.

### (c) Iterative `ray.wait` to stream results

```python
pending = list(refs)
while pending:
    done, pending = ray.wait(pending, num_returns=1)
    try:
        results.append(("ok", ray.get(done[0])))
    except Exception as e:
        results.append(("err", e))
```

O(N) `ray.wait` calls. Worse than (b) on dispatch overhead. The only benefit: lets you start downstream work before stragglers complete.

### (d) `asyncio.gather(*refs, return_exceptions=True)`

Each `ObjectRef` is awaitable (`__await__`). `asyncio.gather(*refs, return_exceptions=True)` returns a per-ref `value | Exception` list — closest existing approach to what we want.

Cost: still per-ref deserialization. Still no metadata-only inspection. Requires async context.

## The latent capability

The C++ layer already separates `data` from `metadata`. The plasma client already returns metadata+data via separate buffers. The in-memory store already holds them as separate fields on `RayObject`. The deserialization-context already does its first dispatch on the metadata buffer alone:

```python
def _deserialize_object(self, data, metadata, object_ref):
    if metadata:
        metadata_fields = metadata.split(b",")
        if metadata_fields[0] in [
            ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
            ray_constants.OBJECT_METADATA_TYPE_PYTHON,
            ray_constants.OBJECT_METADATA_TYPE_RAW,
        ]:
            return self._deserialize_msgpack_data(data, metadata_fields)
        # error metadata: parse the int code and raise the right type
        try:
            error_type = int(metadata_fields[0])
        except Exception:
            raise Exception(f"Can't deserialize object: {object_ref}, metadata: {metadata}")
        if error_type == ErrorType.WORKER_DIED:
            return WorkerCrashedError(...)
        # ...
```

Everything we need is already there. The missing piece is a public Python API that **stops at the metadata read** and never goes through the data deserializer.

## What `ObjectRef` exposes today

From `python/ray/includes/object_ref.pxi`:

```python
class ObjectRef(BaseID):
    def hex(self) -> str: ...
    def binary(self) -> bytes: ...
    def is_nil(self) -> bool: ...
    def task_id(self) -> TaskID: ...
    def job_id(self) -> JobID: ...
    def owner_address(self) -> bytes: ...
    def call_site(self) -> str: ...
    def tensor_transport(self) -> str | None: ...
    def future(self) -> concurrent.futures.Future: ...
    def __await__(self): ...                       # asyncio
    def as_future(self) -> asyncio.Future: ...
    def _on_completed(self, callback) -> ObjectRef: ...   # private
    @classmethod
    def nil(cls) -> ObjectRef: ...
    @classmethod
    def from_random(cls) -> ObjectRef: ...
```

**No `is_ready`. No `errored`. No `state`. No `peek_error`.** The closest thing is `_on_completed` (private) which fires a callback when the object is ready, but firing tells you nothing about whether it succeeded.

## What it would take to expose metadata-only state in Ray itself

(Not what we're doing — but useful to enumerate so we know we're not solving a problem someone else is about to solve.)

You'd need:

1. A new method on `CoreWorker` (C++) like `GetMetadataOnly(const std::vector<ObjectID>&)` that reads the in-memory store / plasma metadata buffer **without** copying or returning the data buffer.
2. Plumbing through `python/ray/_raylet.pyx` to surface it.
3. A public method on `ObjectRef` (e.g., `state()` / `peek_error()`).
4. An updated `ray.wait` variant or new `ray.wait_with_states` that returns the per-ref state instead of the binary ready/not-ready split.

This is mostly mechanical — the plasma client already separates metadata from data. The reason it doesn't exist isn't difficulty; it's that no one has driven the API change through the codebase. Several long-running issues and forum threads request something like this.

## Summary: the gap, in one sentence

> Ray's C++ layer already returns per-ref `(data, metadata)` pairs and the metadata is a small bytes buffer that fully encodes "ready vs errored vs which-error-category", but the Python public API offers no way to read that metadata without going through the deserialize-and-raise path of `ray.get`.

A reimplementation that exposes a typed, metadata-only state inspection API closes this gap by construction. The new public API is in `../design/05-state-and-error-api.md`.

## Failure-category enumeration for the new API

The new system distinguishes (mirroring Ray's `ErrorType`):

| New `ErrorCategory` | Maps from Ray `ErrorType` | Recoverable? |
|---|---|---|
| `task_exception` | `TASK_EXECUTION_EXCEPTION` | App-level (via `retry_exceptions`) |
| `worker_died` | `WORKER_DIED`, `LOCAL_RAYLET_DIED`, `NODE_DIED` | Retried per `max_retries` |
| `actor_died` | `ACTOR_DIED`, `ACTOR_CREATION_FAILED`, `ACTOR_UNAVAILABLE` | Per `max_restarts` |
| `owner_died` | `OWNER_DIED` | No |
| `task_cancelled` | `TASK_CANCELLED` | App-level |
| `object_lost` | `OBJECT_LOST`, `OBJECT_DELETED`, `OBJECT_FREED` | Sometimes by replay |
| `object_unreconstructable` | `OBJECT_UNRECONSTRUCTABLE`, `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`, `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` | No |
| `fetch_timeout` | `OBJECT_FETCH_TIMED_OUT` | Yes, by waiting longer |
| `runtime_env_failed` | `RUNTIME_ENV_SETUP_FAILED`, `WORKER_STARTUP_FAILED` | Yes, on a different node |
| `unschedulable` | `TASK_UNSCHEDULABLE_ERROR`, `ACTOR_UNSCHEDULABLE_ERROR`, `TASK_PLACEMENT_GROUP_REMOVED`, `ACTOR_PLACEMENT_GROUP_REMOVED` | Sometimes |
| `out_of_memory` | `OUT_OF_MEMORY`, `OUT_OF_DISK_ERROR` | App-level |

This collapses the 30+ Ray variants into 11 user-meaningful buckets while preserving the original `ErrorType` as a `raw_code` field on `ErrorInfo` for callers who need finer granularity.

## State enumeration

Four public states for an `ObjectRef`:

- **`pending`** — task hasn't completed; the result is not in any store.
- **`ready_local`** — result is in the calling worker's memory store *or* in this node's plasma. `get()` won't trigger a network fetch.
- **`ready_remote`** — result exists somewhere in the cluster but not on this node. `get()` will trigger a `Pull`.
- **`failed`** — result is in some store with error metadata. `get()` will raise.

The distinction between `ready_local` and `ready_remote` matters: a `state() == "ready"` answer that still triggers a 10-second cross-AZ fetch on `get()` is misleading. Ray's `ray.wait(fetch_local=...)` exposes the same dichotomy at the `wait` level but not at the per-ref level.
