# Design: State and Error Inspection API

The headline new API. This is the central differentiator from Ray. Read first: `../analysis/05-objectref-state-gap.md`.

## Goal restated

Make it cheap and ergonomic to:

1. Query the state of one or many `ObjectRef`s without deserializing payloads.
2. Recover the error category and message of a failed ref without unpickling the user exception.
3. Get the result of a list of `ObjectRef`s as a list of typed `Result`s — no first-error-aborts behavior.

All in one batched core_worker call per logical operation. No per-ref Cython hops, no per-ref RPCs.

## Public Python API

```python
# rayd/__init__.py

from typing import Generic, TypeVar, Literal, overload, final
from dataclasses import dataclass
from rayd._native import ObjectRef, ActorHandle  # PyO3 classes


T = TypeVar("T")


# ── State enum ────────────────────────────────────────────────────────

class RefState:  # PyO3 #[pyclass] enum
    """The lifecycle state of an ObjectRef.

    Values are class attributes. Compare with == or isinstance, not str.
    """
    PENDING:       "RefState"  # task not yet complete
    READY_LOCAL:   "RefState"  # value materialized on this node (memory or local plasma)
    READY_REMOTE:  "RefState"  # value materialized on the cluster but not this node
    FAILED:        "RefState"  # value is an error sentinel


# ── Error category enum (broad buckets) ────────────────────────────────

class ErrorCategory:  # PyO3 #[pyclass] enum
    TASK_EXCEPTION:           "ErrorCategory"
    WORKER_DIED:              "ErrorCategory"
    ACTOR_DIED:               "ErrorCategory"
    OWNER_DIED:               "ErrorCategory"
    TASK_CANCELLED:           "ErrorCategory"
    OBJECT_LOST:              "ErrorCategory"
    OBJECT_UNRECONSTRUCTABLE: "ErrorCategory"
    FETCH_TIMEOUT:            "ErrorCategory"
    RUNTIME_ENV_FAILED:       "ErrorCategory"
    UNSCHEDULABLE:            "ErrorCategory"
    OUT_OF_MEMORY:            "ErrorCategory"


# ── Error info (lightweight) ────────────────────────────────────────────

@dataclass(frozen=True)
@final
class ErrorInfo:
    category: ErrorCategory
    message: str
    traceback: str | None       # only for TASK_EXCEPTION; otherwise None
    raw_code: int               # the granular ErrorType integer; rarely needed


# ── Result hierarchy ────────────────────────────────────────────────────

@final
class Pending:
    pass

@final
class Ok(Generic[T]):
    __slots__ = ("value",)
    value: T

@final
class Err:
    __slots__ = ("info",)
    info: ErrorInfo

Result = Pending | Ok[T] | Err


# ── ObjectRef state inspection methods (added on the PyO3 class) ────────

# These appear as instance methods on ObjectRef. They never raise on
# already-failed refs; failures are reported via the return value.

class ObjectRefMethods(Generic[T]):
    """Type-only stub for documentation; actual class is ObjectRef[T]."""

    def state(self) -> RefState:
        """Cheap. Reads metadata only; no data deserialization, no network fetch.

        Result is a snapshot. PENDING and READY_REMOTE may transition; READY_LOCAL
        and FAILED are terminal once observed.
        """

    def peek_error(self) -> ErrorInfo | None:
        """Returns the error category + message + (for task exceptions) traceback,
        without unpickling the user exception payload.

        Returns None if the ref is pending or successful.
        """

    def exception(self) -> BaseException | None:
        """Returns the original Python exception. Heavier than peek_error: unpickles
        the user exception payload. Returns None if pending or successful.
        """

    def is_ready(self) -> bool:
        """Convenience for state() in (READY_LOCAL, READY_REMOTE, FAILED)."""

    def is_failed(self) -> bool:
        """Convenience for state() == FAILED."""


# ── Module-level batch APIs ─────────────────────────────────────────────

def state(refs: list[ObjectRef[T]]) -> dict[ObjectRef[T], RefState]:
    """One batched call. Returns the state of every ref. No deserialization."""

def get_settled(
    refs: list[ObjectRef[T]],
    *,
    timeout: float | None = None,
    fetch_local: bool = True,
) -> list[Result[T]]:
    """Returns one Result per input ref. Never raises on individual failures.

    PENDING refs (still running at timeout) are returned as `Pending` instances.
    Successful refs are `Ok(value)`. Failed refs are `Err(info)`.

    With fetch_local=False, refs that are READY_REMOTE (not yet local) are
    returned as Pending — same precedence as ray.wait(fetch_local=False).
    """

def wait_with_states(
    refs: list[ObjectRef[T]],
    *,
    timeout: float | None = None,
    fetch_local: bool = False,
) -> dict[ObjectRef[T], RefState]:
    """Waits up to `timeout` for any state changes; returns a snapshot.

    Cheaper than `state(refs)` if you want to block until something becomes
    READY_* or FAILED.
    """

# Backwards-compatible "raise on first failure" behavior, kept under
# the original name. Internally it's just get_settled + raise.
def get(
    refs: ObjectRef[T] | list[ObjectRef[T]],
    *,
    timeout: float | None = None,
) -> T | list[T]:
    """Returns the value(s). Raises on first failure.

    For partial-success behavior, use get_settled.
    """
```

The PyO3 implementations of these all route through the new `CoreWorker::state`/`peek_error`/`get_settled` Rust methods (see `02-object-store.md`).

## How it's cheap

```
state(refs) — what actually happens:

  Python: state([r1, r2, r3])
    │
    ▼  one PyO3 call, GIL released across body
  Rust: CoreWorker::state_batch(&[r1, r2, r3])
    │
    ▼  for each ref:
  MemoryStore::contains(&id) ─┬─► hit:   read metadata only → RefState
                              └─► miss:  PlasmaClient::contains(&id, owner)
                                          ├─► local hit:  metadata in reply → RefState
                                          ├─► owner says "remote node has it" → READY_REMOTE
                                          └─► nobody has it → PENDING
    │
    ▼  collect into HashMap<ObjectId, RefState>
  return to Python as dict[ObjectRef, RefState]
```

Cost in the common (already-local) case: **one hash-map lookup per ref + one 1–4 byte metadata parse per ref**. No data buffer copies. No deserialization. No network. No GIL roundtrips.

The plasma-resident case is one IPC per arena, and `PlasmaClient::contains` returns metadata-only — designed exactly for this.

The remote-but-not-yet-pulled case may or may not need a pubsub query to the owner; we cache the locations from the last `Get` and from incoming pubsub events, so the steady state is local-only.

## How it's correct

The metadata buffer's discriminator carries enough information to distinguish all four states unambiguously:

| Metadata enum variant | Mapped state |
|---|---|
| (no entry in store) | `PENDING` |
| `Pickle5 / Raw / ActorHandle` (any successful tag) and stored locally | `READY_LOCAL` |
| Same but only in remote plasma | `READY_REMOTE` |
| `Error { category, raw_code }` | `FAILED` |

There's no ambiguity, no race window where a ref appears as `READY_LOCAL` but actually triggers a remote fetch on `get()`. The plasma client's `contains` query is what tells us local-vs-remote.

### Snapshot semantics

`state()` is a snapshot. Between `state()` returning `PENDING` and a subsequent `get()`, the ref may have transitioned. Three guarantees we offer:

1. **`READY_LOCAL` is monotonic.** Once observed locally, it stays at least `READY_LOCAL` until explicit deletion.
2. **`FAILED` is monotonic.** Errors don't un-error.
3. **`PENDING → READY_*` and `PENDING → FAILED` are the only allowed transitions.** No mystery transitions.

`READY_REMOTE` can transition to `READY_LOCAL` after a `Pull`, or back to `PENDING` if all copies are lost (lineage reconstruction triggers).

## Example user code

### Pattern A: partial-success batch

```python
import rayd
from rayd import Ok, Err, Pending

@rayd.remote
def maybe_fail(i: int) -> int:
    if i == 5:
        raise ValueError("five is forbidden")
    return i * 2

refs = [maybe_fail.remote(i) for i in range(10)]
results = rayd.get_settled(refs)

successes = [r.value for r in results if isinstance(r, Ok)]
failures  = [r.info for r in results if isinstance(r, Err)]
print(f"got {len(successes)} successes, {len(failures)} failures")
for info in failures:
    print(f"  {info.category}: {info.message}")
```

`rayd.get` would have raised on the first failure, discarding the other nine values. `rayd.get_settled` returns all ten as a typed list.

### Pattern B: cheap progress dashboard

```python
import time
from rayd import RefState

# user submits 10000 tasks
refs = [my_task.remote(i) for i in range(10000)]

# loop without ever deserializing values
while True:
    states = rayd.state(refs)
    counts: dict[RefState, int] = {}
    for st in states.values():
        counts[st] = counts.get(st, 0) + 1
    print(f"pending={counts.get(RefState.PENDING, 0)} "
          f"ready={counts.get(RefState.READY_LOCAL, 0) + counts.get(RefState.READY_REMOTE, 0)} "
          f"failed={counts.get(RefState.FAILED, 0)}")
    if counts.get(RefState.PENDING, 0) == 0:
        break
    time.sleep(1)
```

Each loop iteration: one batched core_worker call, no payload bytes touched. Scales to millions of refs.

### Pattern C: structured error handling

```python
import rayd
from rayd import ErrorCategory

ref = my_task.remote()

# Wait without raising, peek the error.
rayd.wait_with_states([ref], timeout=10.0)
err = ref.peek_error()
if err is None:
    print(f"success: {ref.exception()}")  # actually returns None for success
elif err.category == ErrorCategory.WORKER_DIED:
    print("worker crashed; retrying with smaller batch...")
elif err.category == ErrorCategory.OUT_OF_MEMORY:
    print(f"OOM: {err.message}")
elif err.category == ErrorCategory.TASK_EXCEPTION:
    print("user code raised; full Python exception:")
    print(ref.exception())  # this one *does* unpickle
```

`peek_error` for the dispatch decision (cheap), `exception` only for the path that needs the full Python object.

### Pattern D: backwards-compat raise-on-first-failure

```python
# Identical to Ray's ray.get behavior. Kept for migration ergonomics.
values = rayd.get(refs)
```

Internally: `[r.unwrap() for r in get_settled(refs)]`, where `unwrap()` raises the appropriate `RaydError` subclass for `Err` results. `get_settled` is the primitive; `get` is the wrapper.

## Implementation notes

### Rust core

```rust
// rayd-core/src/core_worker/state_api.rs

impl CoreWorker {
    pub fn state(&self, r: &ObjectRef) -> RefState {
        if let Some(obj) = self.memory_store.get_if_exists(r.id()) {
            return match &obj.metadata {
                Metadata::Error { .. } => RefState::Failed,
                _ => RefState::ReadyLocal,
            };
        }
        match self.plasma.contains(r.id()) {
            Ok(ContainsReply { present: true, metadata: Some(m) }) => {
                match m { Metadata::Error { .. } => RefState::Failed, _ => RefState::ReadyLocal }
            }
            Ok(ContainsReply { present: false, .. }) => {
                if self.location_cache.has_remote_copy(r.id()) {
                    RefState::ReadyRemote
                } else {
                    RefState::Pending
                }
            }
            _ => RefState::Pending,
        }
    }

    pub fn state_batch(&self, refs: &[ObjectRef]) -> HashMap<ObjectRef, RefState> {
        // Same logic, but the plasma RPC accepts a vec and returns a vec —
        // saves IPC overhead for large batches.
        let local_hits = self.memory_store.snapshot_states(refs);
        let plasma_misses: Vec<_> = refs.iter()
            .filter(|r| !local_hits.contains_key(*r)).cloned().collect();
        let plasma_hits = self.plasma.contains_batch(&plasma_misses).unwrap_or_default();

        let mut out = local_hits;
        for (id, reply) in plasma_hits {
            // ... same dispatch as state()
        }
        out
    }

    pub fn peek_error(&self, r: &ObjectRef) -> Option<ErrorInfo> {
        let obj = self.memory_store.get_if_exists(r.id())
            .or_else(|| self.fetch_local_only(r))?;
        if let Metadata::Error { category, raw_code } = obj.metadata {
            // The data buffer holds the ErrorPayload. Decode the small proto;
            // do NOT unpickle pickled_python_exception (that's exception()'s job).
            let payload = ErrorPayload::decode(&obj.data).ok()?;
            Some(ErrorInfo {
                category,
                message: payload.message,
                traceback: payload.traceback,
                raw_code: raw_code as u32,
            })
        } else {
            None
        }
    }
}
```

### PyO3 binding

Just thin wrappers that take `&self` PyRefs, call Rust, and lift results into Python types. No GIL held during the actual work.

## Why this is hard to do in Ray today

The per-ref `state()` requires a `CoreWorker` method that returns *just metadata* (not data) for a list of `ObjectId`s. Ray's `get_objects` always returns `RayObject{data, metadata}` together. Adding a `get_metadata_only` method would require:

1. New C++ method on `CoreWorker` that calls `MemoryStore::contains` and `PlasmaStoreProvider::contains` and bundles the metadata.
2. Plasma protocol change: `Contains` reply doesn't include metadata today — it'd need a new flatbuffers field.
3. Cython glue.
4. Public Python API.

Each layer has its own friction; the changes haven't happened in Ray because no one has driven them through. We get them by building from scratch with this as a primary requirement.

## What we trade off

- **Two failure-encoding paths.** v1's metadata enum is incompatible with Ray's stringly-typed metadata. We don't interop with Ray clusters.
- **Slightly more state for the location cache.** To answer `READY_REMOTE` cheaply, the core_worker maintains an LRU cache of last-known locations per ObjectId. Bounded; configurable; ~32 bytes per entry.
- **Snapshot semantics, not strict consistency.** `state()` reflects "what we know right now"; the truth on the cluster could have moved on. For most use-cases (progress dashboards, error fanout) this is right.

## Test matrix

Every entry in `ErrorCategory` plus every value of `RefState` plus the four transition pairs:

| Test | Asserts |
|---|---|
| `test_state_pending_for_unsubmitted_ref` | `state() == PENDING` |
| `test_state_ready_local_after_inline_return` | After a small task completes, ref is `READY_LOCAL` |
| `test_state_ready_remote_for_remote_plasma_object` | Ref pinned on another node returns `READY_REMOTE` |
| `test_state_failed_for_each_error_category` | One test per `ErrorCategory` |
| `test_peek_error_no_unpickle` | Mock the unpickler; `peek_error()` doesn't call it |
| `test_get_settled_partial_success` | 10 refs, 3 fail → result has 7 `Ok` and 3 `Err` |
| `test_get_settled_with_pending` | timeout expired with some refs unfinished → corresponding `Pending` results |
| `test_state_monotonicity_ready_local` | Once `READY_LOCAL`, never goes back |
| `test_state_monotonicity_failed` | Once `FAILED`, never goes back |
| `test_wait_with_states_returns_failed_separately` | Failed refs are `FAILED`, not lumped with ready |

Each is a `pytest` integration test against a 2-node `rayd` cluster (single-process for `READY_LOCAL` paths, two-process for `READY_REMOTE`).
