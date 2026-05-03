# Analysis: Ownership and Reference Counting

The single most important architectural concept in Ray Core. Read this before any other design decisions.

## Canonical reference

Stephanie Wang et al., **"Ownership: A Distributed Futures System for Fine-Grained Tasks"**, NSDI 2021. <https://www.usenix.org/conference/nsdi21/presentation/wang-stephanie>.

Companion implementation in <https://github.com/ray-project/ray>:
- `src/ray/core_worker/reference_count.h` / `.cc`
- `src/ray/core_worker/task_manager.h` / `.cc`
- `src/ray/object_manager/ownership_based_object_directory.h` / `.cc`

## The core idea

When worker `A` calls `f.remote(...)`, **`A` becomes the owner of the resulting `ObjectRef`**. The owner is *not* the worker that produces the value — it is the worker that submitted the task.

This is the inverse of the obvious choice and it is what unlocks everything else:

- The owner (`A`) holds the **task spec** that produced the object, locally in its `ReferenceCounter`. So the owner can re-execute the task to recover a lost object — **lineage reconstruction is purely local to the owner.**
- The owner holds the **set of locations** where the value is stored. Other workers query the owner directly (`GetObjectLocationsOwner` RPC) instead of consulting GCS. **GCS is not on the object-location data path.**
- The owner holds the **distributed reference count** for that object. When local count + outstanding borrows hits zero, the owner tells the raylets holding copies to free the bytes (`FreeObjects` RPC).

There is no central refcount table. There is no central object directory. The cluster's metadata load is sharded across owners, and each ObjectRef's metadata travels with the ref.

## The two-part `ObjectRef`

An `ObjectRef` everywhere in Ray carries two pieces:

```
ObjectRef = (object_id: 28 bytes, owner_address: rpc::Address)
```

- `object_id` is a deterministic hash of `(parent_task_id, return_index)`. Layout in `src/ray/common/id.h`. Determinism is important: the submitter can predict the ObjectIDs of a task's return values *before* the task runs, so it can return them to the user immediately and propagate them onward without waiting.
- `owner_address` is the worker address (IP, port, worker_id) of the submitter. Encoded as `rpc::Address` in `src/ray/protobuf/common.proto`, fields: `node_id`, `ip_address`, `port`, `worker_id`.

When an `ObjectRef` is serialized — passed as a task argument, returned from a task, stored inside another value — both parts travel together. Any holder can talk directly to the owner without consulting any central service.

## The `ReferenceCounter` data structure

In `src/ray/core_worker/reference_count.h`. Per object the owner manages, conceptually:

```
struct OwnedRef {
    TaskSpec creating_task;            // for lineage reconstruction
    Set<NodeID> locations;              // every node that has a copy
    int local_count;                    // ObjectRef handles in this process
    Set<WorkerID> borrowers;            // other workers currently holding the ref
    Set<ObjectID> nested_in;            // refs nested inside other objects
    int submit_dependency_count;        // pending tasks waiting on this as input
    optional<SpilledLocation> spilled;  // if present, where the bytes are on disk
}
```

Plus a parallel structure for *borrowed* refs (refs this worker holds but does not own).

## Propagation rules

These are subtle. The borrower handshake is one of the trickiest parts of Ray's protocol.

### Returned from a task
The executor's task reply lists every `ObjectRef` contained anywhere in the return value's serialized form. The owner of those contained refs adds the *caller* (the worker receiving the return value) to its borrower set.

### Stored inside another object
On `ray.put({"a": some_ref})`, the contained `ObjectRef`s are recorded in `ContainedInObjectId` mappings. The owner of the containing object becomes a "nested holder" of the contained object. This is the "reference contained in object" mechanism.

### Passed as a task argument
The submitter increments a submit-dependency count on the owner; when the task completes (success or failure), the count drops.

### When a borrower's last local handle drops
The borrower notifies the owner via `WaitForRefRemoved`. The owner removes that worker from the borrower set. When the borrower set + nested-in set + submit-dep count + local count all hit zero, the owner triggers `FreeObjects`.

## Lineage reconstruction

Triggered when:

1. A `Get` finds the object in no plasma store anywhere (location set went empty).
2. The owner is alive.
3. The task that produced the object is still resubmittable: `attempts_remaining > 0`, lineage not evicted (`max_lineage_bytes` budget), and the task's input refs are themselves reconstructable (recursive).

Implementation: `TaskManager::ResubmitTask` in `src/ray/core_worker/task_manager.cc`. The owner had been pinning the task's input refs (preventing them from being GC'd in their respective owners) precisely to enable this. After `max_retries` exhausted, the owner gives up and writes an `OBJECT_LOST` / `OBJECT_UNRECONSTRUCTABLE` sentinel into the in-memory store; subsequent `Get`s raise `ObjectReconstructionFailedError` (or a more specific subtype).

For actor tasks, lineage reconstruction is more limited because actor state is not deterministic by default; you need `max_task_retries > 0` *and* the actor's `max_restarts > 0` to make replay possible, and replays still execute against potentially different actor state.

## Failure semantics

Each failure category has a distinct code path. Mirror them in any reimplementation; collapsing them loses information users need:

| Failure | What happens | Recoverable? |
|---|---|---|
| **Task raised a Python exception** | Task spec retried if `retry_exceptions` matches and `max_retries > 0`; otherwise terminal. Object materialized with metadata = `TASK_EXECUTION_EXCEPTION` and data = pickled `RayTaskError`. | Application-level decision via `retry_exceptions`. |
| **Worker died mid-task** | Distinct from raise — no traceback. Retried up to `max_retries`. Metadata = `WORKER_DIED`. | Yes, by re-running. |
| **Owner died** | All objects owned by that worker become unreconstructable forever. Metadata = `OWNER_DIED`. | No. |
| **Actor died** | `RayActorError`. Subsumes worker-death for actor tasks. Metadata = `ACTOR_DIED`. | If `max_restarts > 0`: actor restarted, task replayed if `max_task_retries > 0`. |
| **Task cancelled** | Explicit `ray.cancel`. Metadata = `TASK_CANCELLED`. Distinguishable from death. | App-level decision. |
| **Object lost** | The bytes were evicted from plasma (or the holding node is gone) and lineage reconstruction failed. Metadata = `OBJECT_LOST` or `OBJECT_UNRECONSTRUCTABLE_*`. | Sometimes by replay, otherwise terminal. |
| **Runtime-env / placement-group / scheduling failure** | Task never ran. Metadata = `RUNTIME_ENV_SETUP_FAILED` / `TASK_PLACEMENT_GROUP_REMOVED` / `TASK_UNSCHEDULABLE_ERROR`. | Usually retriable on a different node. |
| **Fetch timeout** | Object alive somewhere but couldn't be pulled in time. Metadata = `OBJECT_FETCH_TIMED_OUT`. | Yes, by waiting longer. |

Full enum in `src/ray/protobuf/common.proto::ErrorType`.

## Why the ownership model wins over centralized refcount

1. **No central bottleneck.** GCS isn't on the refcount data path; refcount-update RPCs flow worker-to-worker. Ray 0.x put refcounts in Redis and didn't scale; the rewrite was the central thesis of the NSDI'21 paper.
2. **Bounded GCS state.** GCS no longer needs to remember every object that ever existed; it only tracks node/actor/placement-group state.
3. **Failure domain is the owner.** This is a deliberate trade-off: cheap and decentralized in the common case, but if the owner dies its objects are lost. For Python drivers this means "if your driver crashes, the work it scheduled evaporates." Acceptable for most workloads.
4. **Lineage reconstruction is local.** Each owner remembers the task specs of just the objects it owns. No global lineage graph.

## Implications for the reimplementation

1. **The ownership model is non-optional.** Adopt it as-is. Anything else — centralized refcount table in a kv store, GCS-resident object directory — will rebuild Ray 0.x and inherit its scaling problems.
2. **`ObjectRef = (ObjectID, OwnerAddress)`** must be the wire-level identity, not just `ObjectID`. Carry the owner address in every place a ref appears.
3. **Borrower handshake is non-trivial but tractable.** Plan to use `tonic` bidi streaming for the `WaitForRefRemoved` / `PubsubLongPolling` analog. Loom-test the racing-borrowers / racing-owner-death scenarios before relying on the protocol in production.
4. **Lineage budget must be explicit and bounded.** Owners pin task specs to enable replay; without a bound this becomes an OOM source. Enforce an LRU `LineageBudget` per owner.
5. **Each error category must remain distinguishable** in the public API. Don't collapse them into a single generic `RayError`. The `ErrorType` enum is the right shape; mirror it as a Rust enum and a typed Python exception hierarchy (see `../design/05-state-and-error-api.md`).
6. **Owner death must not corrupt other owners' state.** When worker `B` learns that worker `A` died, `B` cleans up references it borrowed from `A` and surfaces `OWNER_DIED` to any user code that touches those refs. Encode this in the protocol so a single owner crash does not cascade.

## Sequence diagram: `f.remote(g.remote())` lifetime

```
Caller (owner A)        Executor for f          Executor for g       Owner of g's result
       │                        │                        │                       │
       │ submit g.remote()      │                        │                       │
       ├─────────────────────────────────────────────────┼──────────────────────►│ (A becomes owner of g's result)
       │                        │                        │       create owned-ref entry
       │                        │                        │                       │
       │ submit f.remote(g_ref) │                        │                       │
       │  bumps submit-dep+1 on g_ref's owner (A itself) │                       │
       │                        │                        │                       │
       │  (g's worker reports completion)                │                       │
       │                        │                        ├──────────────────────►│ result materialized;
       │                        │                        │                       │ A pins for lineage
       │ pushes f task to its leased worker              │                       │
       ├───────────────────────►│                        │                       │
       │                        │ Pull g's value from    │                       │
       │                        │ plasma (zero-copy or   │                       │
       │                        │ cross-node fetch)      │                       │
       │                        │                        │                       │
       │                        │ executes f, returns    │                       │
       │                        │ value to A             │                       │
       │◄───────────────────────┤                        │                       │
       │  decrements submit-dep on g_ref to 0            │                       │
       │  if no other holders, schedules FreeObjects     │                       │
       └─────────────────────────────────────────────────┴───────────────────────┘
```

The diagram condenses many details (lease RPCs, raylet involvement, plasma seal, location reports) but captures the key invariant: every ref has a single owner and every refcount-changing event flows to that owner.
