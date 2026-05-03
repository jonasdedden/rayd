# Analysis: Tasks and Actors

How Ray submits, executes, and recovers tasks and actor methods.

## 1. The `TaskSpec`

The wire-level representation of a task is the `TaskSpec` protobuf message in `src/ray/protobuf/common.proto`. Key fields (verified from current `master`):

```
TaskSpec {
    TaskType   type = 1;       // NORMAL_TASK, ACTOR_CREATION_TASK, ACTOR_TASK, DRIVER_TASK
    string     name = 2;
    Language   language = 3;
    FunctionDescriptor function_descriptor;  // module + class + function + (sometimes) source-hash
    JobID      job_id;
    TaskID     task_id;
    int64      attempt_number;
    repeated TaskArg args;          // by-value (inlined bytes) or by-reference (ObjectRef)
    int64      num_returns;
    map<string, double> required_resources;          // CPU/GPU/memory/custom
    map<string, double> required_placement_resources;
    int32      max_retries;
    bool       retry_exceptions;
    string     retry_exception_allowlist;            // serialized allowlist
    SchedulingStrategy scheduling_strategy;          // DEFAULT/SPREAD/PACK/...
    Address    caller_address;                       // the OWNER's address — this is critical
    string     serialized_runtime_env_info;
    int64      generator_backpressure_num_objects;
    bool       enable_task_events;
    map<string, string> labels;
    LabelSelector label_selector;
    FallbackStrategy fallback_strategy;
    optional ActorTaskSpec  actor_task_spec;
    optional ActorCreationTaskSpec actor_creation_task_spec;
}
```

The `caller_address` field is the owner address described in `02-ownership-and-references.md`. Borrower workers and the executor read this to know where to report results, refcount updates, and locations.

## 2. Function descriptor and remote-function registration

A Python callable is identified across the cluster by its `PythonFunctionDescriptor`:

```
PythonFunctionDescriptor {
    string module_name;
    string class_name;       // empty for top-level functions
    string function_name;
    string function_hash;    // hash of source / pickle bytes
}
```

Two strategies coexist for resolving a descriptor on a worker:

1. **Import-by-name**: the worker imports `module_name`, looks up `function_name` (and optionally `class_name`). Works if the module is on the worker's `PYTHONPATH`. Cheap, but requires sync'd code on every node.
2. **Cloudpickled function shipped with the task**: when the function isn't importable on the worker, Ray pickles the function (`cloudpickle.dumps`) and includes the bytes in `args` or in an internal kv entry. Workers de-pickle it. Slower but lets you define `@remote` functions in REPLs / notebooks.

The default for `@ray.remote` decorated module-level functions is import-by-name. For closures and nested defs, cloudpickle.

## 3. Task submission path (Python → C++ → raylet → worker)

The path of `f.remote(x)`:

1. **`RemoteFunction.remote(*args)`** in `python/ray/remote_function.py`. This wraps the call in `_remote(args, **task_options)`.
2. **`_remote(...)`** resolves task options (overriding defaults from the `@ray.remote(num_cpus=..., max_retries=...)` decorator), validates resource specs, builds the runtime env spec, and finally calls:
   ```
   object_refs = worker.core_worker.submit_task(
       self._language,
       self._function_descriptor,
       list_args,
       name,
       num_returns,
       resources,
       max_retries,
       retry_exceptions,
       retry_exception_allowlist,
       scheduling_strategy,
       worker.debugger_breakpoint,
       serialized_runtime_env_info or "{}",
       generator_backpressure_num_objects,
       enable_task_events,
       labels,
       label_selector,
       fallback_strategy,
   )
   ```
3. **`CoreWorker.submit_task(...)`** in `python/ray/_raylet.pyx` builds the C++ `TaskSpec`, deterministically computes the `ObjectID`s for each return value (so it can return them to Python *before* the task runs), and calls `CoreWorker::SubmitTask` in C++ with the GIL released (`with nogil:`).
4. **`CoreWorker::SubmitTask`** in `src/ray/core_worker/core_worker.cc` registers the task in its local `TaskManager` (which records it for retries and lineage), then dispatches to either:
   - The **direct task transport** (`src/ray/core_worker/transport/direct_task_transport.cc`) for normal tasks: it sends `RequestWorkerLease` to the local raylet, waits for a worker grant or spillback, then sends `PushTask` directly to the leased worker's `CoreWorkerService`.
   - The **direct actor transport** for actor methods: sends `PushTask` directly to the actor's worker, with per-actor in-order guarantees (sequence numbers).
5. **The leased worker** receives `PushTask` on its `CoreWorkerService`, queues the task on a worker thread, executes the user function, and reports back via the task reply.

The deterministic preallocation of `ObjectID`s in step 3 is a non-obvious but important property: the submitter can hand the user `ObjectRef`s synchronously, and the user can chain `f.remote(g.remote())` *before* `g`'s task has even been scheduled. The system pretends the future ID is the value's name from the moment of submission.

## 4. Task return values and `num_returns`

Three modes:

- `num_returns=1` (default): one `ObjectRef` returned, holds the function's single return value.
- `num_returns=N` (fixed > 1): function should return a tuple of N values; N `ObjectRef`s returned, one per element. IDs are deterministic from `(task_id, return_index)`.
- `num_returns="streaming"` (generator): function is a generator/async-generator; an `ObjectRefGenerator` returned that yields refs as the generator produces values. Backpressure governed by `generator_backpressure_num_objects` so producers don't outpace consumers. Internals: see `src/ray/core_worker/task_manager.cc` `ObjectRefStream`, `MarkEndOfStream`, `HandleReportGeneratorItemReturns`. **Out of scope for v1 of the reimplementation.**

## 5. Retry semantics

Configured per-call via `@ray.remote(max_retries=..., retry_exceptions=...)` or `f.options(...).remote()`. Implemented in `TaskManager::FailOrRetryPendingTask` (`src/ray/core_worker/task_manager.cc`):

- **Transient/system failures** (worker crashed, OOM, lost lease) trigger automatic retries up to `max_retries`. Default `max_retries=3` for tasks. OOM has its own budget (`max_oom_retries`).
- **Application errors** (user code raised an exception): retry only if `retry_exceptions=True` *and* the exception type matches the `retry_exception_allowlist`. Otherwise, terminal failure: write a `RayObject` with `metadata=TASK_EXECUTION_EXCEPTION` and `data=pickled_RayTaskError(...)` into the in-memory store on the caller, decrement submit-dependency counts, surface to user.
- **Lineage-driven retries**: distinct from above. Triggered by `TaskManager::ResubmitTask` when a `Get` finds no plasma copies and lineage reconstruction is enabled. Uses the same retry-attempt budget.

Each retry increments `attempt_number` in the `TaskSpec`. On retry, `SetupTaskEntryForResubmit` updates input refs' refcounts via `UpdateReferencesForResubmit` (the inputs may have themselves been GC'd; we need to bump them back).

## 6. Actor creation

`ActorClass.remote(*args)` in `python/ray/actor.py`. Path:

1. Validate name, handle the get-or-create pattern for named actors.
2. Build the actor creation task spec (an `ActorCreationTaskSpec` inside a normal `TaskSpec` of type `ACTOR_CREATION_TASK`).
3. Call `worker.core_worker.create_actor(meta.language, meta.actor_creation_function_descriptor, creation_args, max_restarts, max_task_retries, ...)`.
4. The C++ side registers the actor with GCS via `RegisterActor` / `CreateActor` RPCs on `ActorInfoGcsService` (the GCS owns the actor table for cluster-wide visibility).
5. The actor creation task is scheduled like any other task; the leased worker is "frozen" to host this actor and won't be returned to the worker pool.
6. `ActorHandle()` is constructed and returned synchronously to the caller, holding the `ActorID`, the (eventually-resolved) actor's worker address, and per-method metadata (`method_max_task_retries`, `method_retry_exceptions`, `method_num_returns`).

Named actors: GCS holds a `name → ActorID` map. `ray.get_actor("name")` queries GCS once, then talks to the actor's worker directly.
Detached actors: lifetime independent of the creating job; survive driver exit.

## 7. Actor method dispatch

`actor.method.remote(args)`:

1. `ActorMethod._remote(...)` resolves per-call options, then calls `actor_handle._actor_method_call(...)`.
2. `_actor_method_call` builds a `TaskSpec` of type `ACTOR_TASK` with the actor's ID, the method's function descriptor, and a per-actor sequence number (for in-order delivery).
3. `worker.core_worker.submit_actor_task(...)` dispatches.
4. C++ side sends `PushTask` directly to the actor's worker via `CoreWorkerService` (no raylet involvement on the data path).
5. The actor's worker has a per-actor task queue; it executes tasks in submission order. For async actors (`max_concurrency > 1`), tasks are dispatched to a thread pool but still respect the in-order semantics via per-coroutine sequencing.

Reply path: the actor's worker writes the return `RayObject` into its caller's in-memory store via the task reply (small returns) or into local plasma (large returns) and reports the location to the caller.

## 8. Worker pool

`src/ray/raylet/worker_pool.cc`. The raylet manages a pool of pre-warmed Python workers, keyed by `(language, runtime_env_hash, job_id)`. A few sharp edges:

- **Prestart**: when the raylet starts, it can pre-fork some workers per the configured `num_cpus`. Lazy on-demand startup is the default for non-prestarted resources.
- **Reuse**: after a non-actor task finishes, the worker returns to the idle pool. After an actor task finishes, the worker is bound to that actor and stays.
- **Idle eviction**: idle workers killed after `RAY_idle_worker_killing_time_threshold_ms`.
- **Runtime env**: each worker materialized with a runtime env (pip/conda/working_dir) by the runtime-env agent; mismatched envs spawn new workers.

## 9. Fault model summary

| Failure | Detection | Recovery |
|---|---|---|
| Worker dies mid-task | Raylet's child-monitor detects exit; reports to GCS; GCS pubsub notifies owner | Owner retries task per `max_retries` |
| Raylet dies | GCS heartbeat timeout marks node dead; pubsub fanout to all workers | All objects on that node treated as lost → owners trigger lineage reconstruction or surface `OBJECT_LOST` |
| GCS dies (no Redis backup) | Workers lose pubsub; cluster pauses | Cluster effectively down; restart required |
| GCS dies (Redis-backed) | Same detection; new GCS rebuilds from Redis | Workers reconnect, replay in-flight task specs from local TaskManager state |
| Owner of a ref dies | Borrower's `WaitForRefRemoved` RPC fails / pubsub silence | Borrower marks ref as `OWNER_DIED`; subsequent `Get` raises `OwnerDiedError` |
| Actor dies | Actor's worker exits or crashes; raylet reports | If `max_restarts > 0`: GCS replays actor creation on a new worker; per-method retry per `max_task_retries` |

## 10. Task and actor invariants the reimplementation must preserve

1. **Deterministic ObjectID preallocation.** `f.remote(...)` returns `ObjectRef`s synchronously. Their IDs are `H(parent_task_id, return_index)`. The submitter can hand them out before the task is scheduled. Don't postpone ID minting.
2. **Submitter is owner.** No exceptions. Do not allow constructions that produce orphaned refs.
3. **`caller_address` is in the wire `TaskSpec`.** Every executor and every raylet reads this to know where to report.
4. **In-order actor dispatch within a single handle.** Sequence numbers per `(actor_id, caller_id)`. Out-of-order delivery is observable user-visible behavior; many actor patterns assume FIFO.
5. **Direct call bypass.** Once a worker is leased, the data path is core_worker → leased_worker. The raylet is not in the loop. Plan this fast path from day one.
6. **Retry retains the same `task_id` and increments `attempt_number`.** Re-submit doesn't mint a new task; it reuses the spec. This keeps refcounts and lineage consistent.
7. **Actor handles are themselves serializable as object refs.** When a Python program does `actor_ref = some_function.remote(actor_handle)`, the handle gets pickled with custom reducers. The new system needs the same property: actor handles must round-trip through the serializer cleanly. Use the same metadata-tagging trick (`OBJECT_METADATA_TYPE_ACTOR_HANDLE`) so deserialization can reconstruct the handle without inspecting payload.
8. **Generator support is *not* in v1.** Generators (`num_returns="streaming"`) add real complexity (`ObjectRefStream`, backpressure protocol). Plan to skip them in v1 and revisit in a later phase.

## 11. Things that look like they belong here but don't

- Placement groups: orchestrated by GCS via `gcs_placement_group_scheduler.cc`. Out of scope for v1.
- Runtime envs (pip/conda materialization): handled by the dashboard agent, not core_worker. Out of scope for v1; the new system can require homogeneous Python environments across the cluster initially.
- Cross-language workers (Java, C++): each language has its own core_worker binding. Out of scope for v1.
