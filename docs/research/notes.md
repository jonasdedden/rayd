# Research Notes (Working File)

Source-anchored notes from the analysis pass. Everything here is raw enough to need polish before going into a design doc, but it's the verbatim evidence that backs the analysis and design files.

## Verified-against-master Ray facts

These came from direct WebFetch against `raw.githubusercontent.com/ray-project/ray/master/...`:

### `src/ray/protobuf/common.proto`
- `TaskSpec` first three fields: `TaskType type = 1; string name = 2; Language language = 3;`
- `ErrorType` enum exists; ranges from `WORKER_DIED = 0` through `WORKER_STARTUP_FAILED = 33`. Variants confirmed: `TASK_EXECUTION_EXCEPTION`, `TASK_CANCELLED`, `ACTOR_DIED`, `ACTOR_CREATION_FAILED`, `ACTOR_UNAVAILABLE`, `OBJECT_IN_PLASMA`, `OBJECT_LOST`, `OBJECT_DELETED`, `OBJECT_FREED`, `OUT_OF_MEMORY`, `OUT_OF_DISK_ERROR`, `TASK_UNSCHEDULABLE_ERROR`, `ACTOR_UNSCHEDULABLE_ERROR`, `NODE_DIED`, `LOCAL_RAYLET_DIED`, `TASK_PLACEMENT_GROUP_REMOVED`, `ACTOR_PLACEMENT_GROUP_REMOVED`, multiple `OBJECT_UNRECONSTRUCTABLE_*` variants.
- `RayErrorInfo` fields: `oneof error` (with `ActorDeathCause`, `RuntimeEnvFailedContext`, `ActorUnavailableContext`); `string error_message`; `ErrorType error_type`.
- `TaskStatus` enum: `PENDING_ARGS_AVAIL`, `PENDING_NODE_ASSIGNMENT`, `PENDING_OBJ_STORE_MEM_AVAIL`, `PENDING_ARGS_FETCH`, `SUBMITTED_TO_WORKER`, `PENDING_ACTOR_TASK_ARGS_FETCH`, `PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY`, `RUNNING`, `RUNNING_IN_RAY_GET`, `RUNNING_IN_RAY_WAIT`, `GETTING_AND_PINNING_ARGS`, `FINISHED`, `FAILED`, `NIL`.
- `ObjectReference`: `bytes object_id; Address owner_address; string call_site; optional string tensor_transport;`
- `Address`: `bytes node_id; string ip_address; int32 port; bytes worker_id;`

### `python/ray/_private/worker.py::Worker.get_objects` (lines ~1054–1120)
```python
def get_objects(
    self,
    object_refs: list,
    timeout: Optional[float] = None,
    return_exceptions: bool = False,
    skip_deserialization: bool = False,
    use_object_store: bool = False,
) -> Tuple[List[serialization.SerializedRayObject], bytes]:
```
- Calls `self.core_worker.get_objects(object_refs, timeout_ms)` returning `List[SerializedRayObject]`.
- `SerializedRayObject = (data, metadata, transport)` named tuple.
- The raise loop:
```python
if not return_exceptions:
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
- `skip_deserialization=True` is an existing internal kwarg that returns the raw `SerializedRayObject` list — which is exactly the primitive a public `get_settled` could be built on.

### `python/ray/remote_function.py::RemoteFunction._remote()`
- Final dispatch:
```python
object_refs = worker.core_worker.submit_task(
    self._language, self._function_descriptor, list_args, name,
    num_returns, resources, max_retries, retry_exceptions,
    retry_exception_allowlist, scheduling_strategy,
    worker.debugger_breakpoint, serialized_runtime_env_info or "{}",
    generator_backpressure_num_objects, enable_task_events,
    labels, label_selector, fallback_strategy,
)
```
- 16+ positional arguments; demonstrates the Cython API stability problem (any signature change ripples everywhere).
- `num_returns="streaming"` maps to `STREAMING_GENERATOR_RETURN`; `"dynamic"` maps to `-1`. Default 1.
- Return: `ObjectRef` (1 return), `list[ObjectRef]` (N returns), or `ObjectRefGenerator` (streaming).

### `python/ray/actor.py::ActorClass._remote()`
- Around line 2200:
```python
actor_id = worker.core_worker.create_actor(
    meta.language, meta.actor_creation_function_descriptor, creation_args,
    max_restarts, max_task_retries, ...
)
```
- Per-method retry config (`method_max_task_retries`, `method_retry_exceptions`, `method_num_returns`) flows through `ActorHandle.__init__` → `_method_shells` → call-time options at `ActorMethod._remote`.
- `_actor_method_call` ends in `worker.core_worker.submit_actor_task(...)`.

### `src/ray/core_worker/store_provider/plasma_store_provider.h` public interface
```cpp
Status Put(const RayObject &object, const ObjectID &object_id, const rpc::Address &owner_address, bool *object_exists);
Status Get(const std::vector<ObjectID> &object_ids, const std::vector<rpc::Address> &owner_addresses, int64_t timeout_ms, absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);
Status GetIfLocal(const std::vector<ObjectID> &ids, absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);
Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size, const ObjectID &object_id, const rpc::Address &owner_address, std::shared_ptr<Buffer> *data, ...);
Status Seal(const ObjectID &object_id);
Status Contains(const ObjectID &object_id, bool *has_object);
Status Wait(const std::vector<ObjectID> &object_ids, const std::vector<rpc::Address> &owner_addresses, int num_objects, int64_t timeout_ms, const WorkerContext &ctx, absl::flat_hash_set<ObjectID> *ready);
Status Delete(const absl::flat_hash_set<ObjectID> &object_ids, bool local_only);
Status Release(const ObjectID &object_id);
absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> UsedObjectsList() const;
```
- `Contains` returns just a bool today. To support metadata-only state inspection, this would need a metadata field in the reply — confirming the API gap.

### `src/ray/core_worker/store_provider/memory_store/memory_store.h` public interface
```cpp
void Put(const RayObject &object, const ObjectID &object_id, bool has_reference);
Status Get(const std::vector<ObjectID> &ids, int num_objects, int64_t timeout_ms, const WorkerContext &ctx, std::vector<std::shared_ptr<RayObject>> *results);
Status Get(const absl::flat_hash_set<ObjectID> &ids, int64_t timeout_ms, const WorkerContext &ctx, absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results, bool *got_exception);
std::shared_ptr<RayObject> GetIfExists(const ObjectID &object_id);
void GetAsync(const ObjectID &object_id, std::function<void(std::shared_ptr<RayObject>)> callback);
Status Wait(const absl::flat_hash_set<ObjectID> &object_ids, int num_objects, int64_t timeout_ms, const WorkerContext &ctx, absl::flat_hash_set<ObjectID> *ready, absl::flat_hash_set<ObjectID> *plasma_object_ids);
void Delete(const absl::flat_hash_set<ObjectID> &object_ids, absl::flat_hash_set<ObjectID> *plasma_ids_to_delete);
void Delete(const std::vector<ObjectID> &object_ids);
bool Contains(const ObjectID &object_id, bool *in_plasma);
int Size();
uint64_t UsedMemory();
```
- `GetIfExists` and `Contains` already return without deserialization. The store has the data we need; the public API doesn't.
- `got_exception: bool*` out-parameter on the second `Get` overload — which means C++ already knows whether a fetched object is an exception. It's surfacing this to Python that's missing.

### `python/ray/_private/serialization.py` deserialization dispatch
- Metadata is parsed as comma-separated bytes:
```python
metadata_fields = metadata.split(b",")
```
- Type-tag dispatch: `metadata_fields[0]` matched against `OBJECT_METADATA_TYPE_CROSS_LANGUAGE` (`b"XLANG"`), `OBJECT_METADATA_TYPE_PYTHON` (`b"PYTHON"`), `OBJECT_METADATA_TYPE_RAW` (`b"RAW"`), `OBJECT_METADATA_TYPE_ACTOR_HANDLE` (`b"ACTOR_HANDLE"`).
- Error dispatch: `error_type = int(metadata_fields[0])` if it can be parsed as int.
- For task exceptions, `RayErrorInfo` proto is parsed from the data buffer.
- This is the file where the inversion would happen: today it raises mid-loop; a partial-success variant would just collect.

### `python/ray/_private/ray_constants.py`
- `OBJECT_METADATA_TYPE_CROSS_LANGUAGE = b"XLANG"`
- `OBJECT_METADATA_TYPE_PYTHON = b"PYTHON"`
- `OBJECT_METADATA_TYPE_RAW = b"RAW"`
- `OBJECT_METADATA_TYPE_ACTOR_HANDLE = b"ACTOR_HANDLE"`
- `OBJECT_METADATA_DEBUG_PREFIX = b"DEBUG:"`
- `DEFAULT_OBJECT_PREFIX = "ray_spilled_objects"`
- `PUT_OBJECT_LIMIT_BYTES`, `RAY_PUT_OBJECT_TIMEOUT_S`, `max_direct_call_object_size` are NOT in this file in the current master fetch — they're in `ray_config_def.h` (C++) only.

### `python/ray/includes/object_ref.pxi`
Verified `ObjectRef`'s public methods:
- `hex()`, `binary()`, `is_nil()`, `task_id()`, `job_id()`, `owner_address()`, `call_site()`, `tensor_transport()`
- `future()` (concurrent.futures), `as_future()` (asyncio.Future), `__await__`
- `_on_completed(callback)` (private)
- Class methods: `nil()`, `from_random()`, `size()`

**No state-inspection methods** — confirms the API gap.

### `src/ray/core_worker/task_manager.cc` responsibilities
- `submissible_tasks_` collection of pending tasks.
- `AddPendingTask()` records spec, retry limits (incl. OOM-specific), input refs.
- `CompletePendingTask()` → `HandleTaskReturn()` per return; routes to plasma or memory store.
- `FailOrRetryPendingTask()` decides retry vs permanent fail.
- `ResubmitTask()` for lineage reconstruction; calls `UpdateReferencesForResubmit()` and `async_retry_task_callback_`.
- `MapPlasmaPutStatusToErrorType()` maps storage errors to `ErrorType`.
- For streaming generators: `ObjectRefStream`, `MarkEndOfStream`, `HandleReportGeneratorItemReturns`, `TryDelObjectRefStream`.
- `total_lineage_footprint_bytes_` tracks lineage memory; clears on terminal completion.

## Things flagged for verification

These constants are claimed in the design but not freshly verified against current master:

- `max_direct_call_object_size = 100 * 1024` (claimed in `ray_config_def.h`).
- `object_manager_default_chunk_size ≈ 64 KiB`.
- `object_spilling_threshold ≈ 0.75`.
- `RAY_idle_worker_killing_time_threshold_ms` (default unverified).
- `RAY_raylet_report_resources_period_milliseconds = 100`.
- `ObjectID::Size() == 28`.
- Numeric assignments inside `ErrorType`.

If these matter for design parameters, re-check against master before fixing them in code.

## Open questions for the design

1. **`RAYD_INLINE_OBJECT_THRESHOLD_BYTES`** default — match Ray (100 KiB)? Or tune for our protocol?
2. **`RAYD_PLASMA_ARENA_SIZE_BYTES`** default — 1 GiB feels right, but should benchmark.
3. **`RAYD_SPILL_THRESHOLD`** default — match Ray's 0.75?
4. **Should `state()` ever be `async`?** Argument for: avoids any chance of blocking. Argument against: it should be cheap enough that sync is fine.
5. **Generic parametrization on `ObjectRef`** — hand-edit the stub or wrap in a Python class? Tentatively: hand-edit.
6. **Free-threaded CPython** — design choices today should not foreclose 3.13t/3.14t support. Specifically: don't rely on the GIL for invariants that should hold without it.

## Sources used

Files read via WebFetch on `raw.githubusercontent.com/ray-project/ray/master`:
- `src/ray/protobuf/common.proto`
- `python/ray/_private/worker.py`
- `python/ray/remote_function.py`
- `python/ray/actor.py`
- `src/ray/core_worker/task_manager.cc`
- `python/ray/_private/serialization.py`
- `python/ray/_private/ray_constants.py`
- `python/ray/includes/object_ref.pxi`
- `src/ray/object_manager/plasma/store.cc`
- `src/ray/core_worker/store_provider/plasma_store_provider.h`
- `src/ray/core_worker/store_provider/memory_store/memory_store.h`

Subagent reports (5 in total) supplied additional context on:
- Top-level architecture (general-purpose subagent)
- Plasma object store internals
- Tasks and actors lifecycle
- Rust ecosystem survey
- ObjectRef state and exception API gap

Three of the five subagents had limited tool permissions and produced reports from internal knowledge; their content was cross-checked against the WebFetch evidence above before being incorporated into the analysis docs.
