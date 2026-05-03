# Design: Tasks and Actors

How the new system runs `@remote` callables and stateful actors. Read first: `../analysis/03-tasks-and-actors.md` (Ray's approach).

## Type-level overview

```rust
// rayd-core/src/id.rs
pub struct JobId([u8; 16]);
pub struct TaskId([u8; 24]);     // 16 bytes job + 8 bytes counter
pub struct ObjectId([u8; 28]);   // 24 bytes task_id + 4 bytes return_index
pub struct WorkerId([u8; 16]);
pub struct ActorId([u8; 16]);

// rayd-core/src/task.rs
pub struct TaskSpec {
    pub kind: TaskKind,
    pub task_id: TaskId,
    pub job_id: JobId,
    pub attempt_number: u32,
    pub language: Language,                       // v1: Python only
    pub function: FunctionDescriptor,
    pub args: Vec<TaskArg>,
    pub num_returns: u32,
    pub required_resources: ResourceMap,
    pub max_retries: u32,
    pub retry_exceptions: RetryExceptionPolicy,
    pub scheduling_strategy: SchedulingStrategy,
    pub caller_address: Address,                  // OWNER address
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub label_selector: LabelSelector,
}

pub enum TaskKind {
    Normal,
    ActorCreation { spec: ActorCreationSpec },
    ActorTask { spec: ActorTaskSpec },
}

pub enum TaskArg {
    Value { metadata: Metadata, data: Bytes, contained_refs: Vec<ObjectRef> },
    ByRef { object_ref: ObjectRef },
}

pub struct FunctionDescriptor {
    pub module: String,
    pub class_name: Option<String>,
    pub function_name: String,
    pub source_hash: [u8; 16],            // sha-256 truncated
}

pub enum RetryExceptionPolicy {
    None,
    All { max_retries: u32 },
    Allowlist { types: Vec<PickledException>, max_retries: u32 },
}
```

The wire form is a `prost`-generated protobuf in `proto/rayd.proto`. The Rust struct above is the hand-written domain type the rest of the code uses; `prost` types are converted at the gRPC boundary only.

## Task submission path

End-to-end for `f.remote(x)`:

```
Python user code
    │  rayd.remote(f)(x)
    ▼
RemoteFunction (Python wrapper)
    │  builds args list, options
    ▼
CoreWorker (PyO3 #[pyclass]) on the binding side
    │  GIL-released call to:
    ▼
CoreWorker (Rust, in rayd-core)
    │  - allocate ObjectIDs deterministically: H(task_id, return_index) for i in 0..num_returns
    │  - register in TaskManager (for retries / lineage)
    │  - push into the LeaseTransport channel
    ▼
LeaseTransport (rayd-core/src/transport/lease.rs)
    │  RequestWorkerLease RPC to the local raylet
    ▼
Raylet (NodeManagerService)
    │  pop a worker from the WorkerPool, return its address
    │  OR: spillback hint to a different node's raylet
    ▼
LeaseTransport (back in caller)
    │  PushTask RPC directly to the leased worker's CoreWorkerService
    ▼
Leased Worker (CoreWorkerService::push_task)
    │  queue task on a worker thread
    │  execute user function (under GIL, via PyO3)
    │  capture return value via the serialization layer
    │  for each return: SealReturnObject {inline if small, plasma if large}
    │  reply to caller with the per-return RayObject
    ▼
Caller's TaskManager: HandleTaskReply
    │  for each return:
    │    if inline: write into MemoryStore
    │    if plasma: record location pointer in MemoryStore
    │  decrement submit-dependency counters on input refs
    │  notify any waiters / async futures
```

Determinism of `ObjectId` from `(task_id, return_index)` is preserved: callers can hand the user `ObjectRef`s synchronously before the task is even leased.

## Direct-call vs raylet-mediated

For v1, **all task submission goes through the raylet for the lease**, then PushTask is direct to the leased worker. Same as Ray's "direct call" path. We don't implement Ray's older raylet-mediated argument plumbing.

Actor methods skip the lease step entirely: the actor's worker is already known (from the actor handle), so PushTask is direct.

## Task replies and inline returns

For the inline path (return ≤ inline threshold):

```protobuf
message PushTaskReply {
  TaskId task_id = 1;
  uint32 attempt_number = 2;
  repeated InlineReturn inline_returns = 3;  // up to num_returns of these
  TaskStatus status = 4;
  RayErrorInfo error = 5;     // present iff status != FINISHED
}

message InlineReturn {
  ObjectId object_id = 1;
  bytes metadata = 2;       // 1-4 bytes
  bytes data = 3;
  repeated ObjectReference nested_refs = 4;  // owner-address propagation
}
```

Caller's `HandleTaskReply` writes each `InlineReturn` into its own `MemoryStore`. Refcount entries are created for `nested_refs` (the caller becomes a borrower of those).

For plasma path: the executor writes into local plasma, the reply contains only `ObjectId + size + plasma_node_id`. The caller's `MemoryStore` records a `PlasmaLocation` pointer; on `get()`, `MemoryStore::get` consults the plasma client.

## Retry semantics

```rust
impl TaskManager {
    pub async fn handle_task_failure(
        &self,
        task_id: TaskId,
        cause: TaskFailureCause,
    ) -> Result<RetryDecision> {
        let entry = self.entries.write().get_mut(&task_id)?;
        let policy = entry.spec.retry_exceptions.clone();

        let decision = match cause {
            TaskFailureCause::WorkerCrashed | TaskFailureCause::Timeout => {
                if entry.attempts_remaining > 0 {
                    RetryDecision::Retry
                } else {
                    RetryDecision::Fail(ErrorCategory::WorkerDied)
                }
            }
            TaskFailureCause::OutOfMemory => {
                if entry.oom_attempts_remaining > 0 {
                    entry.oom_attempts_remaining -= 1;
                    RetryDecision::Retry
                } else {
                    RetryDecision::Fail(ErrorCategory::OutOfMemory)
                }
            }
            TaskFailureCause::Application { exception_type, .. } => {
                match policy {
                    RetryExceptionPolicy::None => RetryDecision::Fail(ErrorCategory::TaskException),
                    RetryExceptionPolicy::All { .. } if entry.attempts_remaining > 0 => RetryDecision::Retry,
                    RetryExceptionPolicy::Allowlist { types, .. }
                        if entry.attempts_remaining > 0
                        && types.iter().any(|t| t.matches(&exception_type)) =>
                    {
                        RetryDecision::Retry
                    }
                    _ => RetryDecision::Fail(ErrorCategory::TaskException),
                }
            }
            TaskFailureCause::Cancelled => RetryDecision::Fail(ErrorCategory::TaskCancelled),
        };

        match decision {
            RetryDecision::Retry => {
                entry.attempts_remaining -= 1;
                entry.spec.attempt_number += 1;
                self.resubmit(entry.spec.clone()).await?;
            }
            RetryDecision::Fail(cat) => {
                self.write_failed_returns(&entry.spec, cat).await?;
            }
        }
        Ok(decision)
    }
}
```

Failed returns are written into the caller's `MemoryStore` as `RayObject{metadata: Error{category, raw_code}, data: encode(ErrorPayload)}`.

## Actor lifecycle

```rust
pub struct Actor {
    pub id: ActorId,
    pub worker: WorkerId,             // bound for actor's lifetime
    pub class: FunctionDescriptor,
    pub max_restarts: u32,
    pub max_task_retries: u32,
    pub state: ActorState,
}

pub enum ActorState {
    Pending,                           // creation task scheduled
    Alive { restart_count: u32 },
    Restarting { since: Instant },
    Dead,                              // after max_restarts exhausted
}

pub struct ActorHandle {
    pub actor_id: ActorId,
    pub address: Address,             // current worker address (mutable across restarts)
    pub class_name: String,
    pub method_descriptors: Arc<HashMap<String, MethodDescriptor>>,
}
```

### Creation
1. `ActorClass.remote(*args)` builds an `ActorCreationTaskSpec`, dispatches via the normal task path with `kind=ActorCreation`.
2. The lease grant binds a worker to the actor permanently; the worker won't be returned to the pool.
3. Actor creation runs `__init__` on the worker.
4. GCS is informed via `RegisterActor` for cluster-wide naming; this is the only thing GCS does for actors in v1.

### Method dispatch
- `actor.method.remote(args)` builds an `ActorTaskSpec` with the actor's address (from the handle).
- Submitted directly to the actor's worker (no lease, no raylet involvement).
- In-order delivery enforced by per-`(actor_id, caller_id)` sequence numbers in the `ActorTaskSpec`.
- Actor's worker has a `tokio::sync::mpsc::UnboundedReceiver<ActorTask>` per actor; the task loop consumes in order, executes, replies.

### Restart on death
- Actor's worker dies → raylet detects → notifies GCS via `ActorDied` event.
- GCS pubsub fans out to all `ActorHandle` borrowers: "this actor is restarting".
- If `restart_count < max_restarts`: schedule a new lease, run the actor creation task again, update the handle's `address` field.
- During restart, in-flight method calls fail with `ActorUnavailable`; if `max_task_retries > 0` they're replayed once the actor is back.
- After exhaustion: handle's state becomes `Dead`; subsequent methods return `ActorDied`.

### In-order semantics across restart

A subtlety Ray gets right and we must too: in-order delivery is per-`(actor, caller)` and is *eventually* in-order across restarts. After a restart, the actor's worker resumes at sequence number 1 (the actor's state was lost; in-order resumes from a fresh start). This is the Ray default and is documented behavior.

If a user wants stronger guarantees (replay all queued tasks against the new instance), they need `max_task_retries > 0` *and* deterministic actor state — which is the user's problem, not the framework's.

## Worker pool

```rust
pub struct WorkerPool {
    nodes: HashMap<NodeId, NodeWorkerPool>,
    runtime_env_hashes: HashSet<RuntimeEnvHash>,
}

pub struct NodeWorkerPool {
    /// Pool of idle workers, keyed by (language, runtime_env_hash, job_id, resource_shape).
    idle: HashMap<WorkerKey, VecDeque<WorkerHandle>>,
    /// Workers leased out and currently running tasks.
    leased: HashMap<WorkerId, LeaseInfo>,
    /// Actor-bound workers, never returned to idle.
    actor_bound: HashMap<ActorId, WorkerId>,
}

impl NodeWorkerPool {
    pub fn pop(&mut self, key: &WorkerKey) -> Option<WorkerHandle>;
    pub fn push(&mut self, handle: WorkerHandle);          // return to idle
    pub fn bind(&mut self, actor_id: ActorId, worker: WorkerId);  // pin to actor
    pub fn evict_idle(&mut self, older_than: Duration);    // periodic cleanup
}
```

### Spawning workers

A worker is `python -m rayd._worker --raylet-uds=/path --core-port=N --runtime-env=...`. The Python entry point loads a tiny boot script that:

1. Imports `rayd._native`.
2. Constructs a `CoreWorker` instance, connects to the local raylet UDS.
3. Loops on the worker's task receiver, executing tasks.

The Rust raylet `spawn`s these via `tokio::process::Command` and uses `prctl(PR_SET_PDEATHSIG, SIGKILL)` (Linux) so the worker dies if the raylet dies.

### Worker-key matching

`WorkerKey = (Language::Python, runtime_env_hash, job_id)`. Ray also keys on the leased resource bundle in newer versions; we add this in v1.5 if profiling shows pool fragmentation hurts.

## Scheduling

v1 implements a minimal but correct scheduling policy:

1. Lease request arrives at local raylet.
2. If local node has the resources, lease locally.
3. Otherwise, query GCS for cluster-wide resource availability (refreshed via heartbeats every 100 ms).
4. Pick the best remote node by: hard resource feasibility, then *random* tie-breaking among feasible nodes (no locality optimization in v1).
5. Reply with spillback hint; caller re-issues the lease to the chosen node.

Locality awareness (preferring nodes that already have the task's argument objects) is post-v1.

`SchedulingStrategy` enum supports `DEFAULT` only in v1; `SPREAD`, `PACK`, `STRICT_*`, `NodeAffinity` deferred.

## Failure model — what happens when

| Scenario | Detection | Action |
|---|---|---|
| Worker dies mid-task | Raylet's `wait4` on the child PID; reports to caller via `WorkerCrashed` event | Caller's TaskManager applies retry policy; retries on a fresh worker |
| Raylet dies | GCS heartbeat timeout | All workers on that node treated as dead; tasks they ran retry; objects in their plasma are lost (lineage reconstruction may rebuild) |
| GCS dies | Workers' GCS pubsub silent | v1: cluster pauses, restart needed (no GCS HA) |
| Owner of a ref dies | Borrower's pubsub reports owner death | Borrower marks ref as `OwnerDied`; subsequent `state()` returns `Failed(OwnerDied)`; `get()` raises `OwnerDiedError` |
| Actor dies | Actor's worker exits | Per `max_restarts`: restart on a new worker; in-flight method calls retry per `max_task_retries` |
| Object lost from plasma everywhere | Owner notices location set empty | If lineage available: resubmit creating task; otherwise write `ObjectUnreconstructable` sentinel |

Each scenario is a named integration test in `python/rayd/tests/test_failure_modes/`.

## Invariants

1. **`ObjectId` deterministic from `(task_id, return_index)`.** Callers can preallocate refs.
2. **Submitter is always owner.** No exceptions.
3. **`caller_address` is in the wire `TaskSpec`.** Every executor and raylet reads this.
4. **In-order actor dispatch within a single handle.** Sequence numbers in `ActorTaskSpec`.
5. **Direct-call data path.** Once leased, executor talks directly to caller. Raylet is not in the loop.
6. **Retry preserves `task_id`, increments `attempt_number`.** Refcount and lineage stay consistent.
7. **All public entry points type-check end to end.** No `Any` leaks across the FFI.
