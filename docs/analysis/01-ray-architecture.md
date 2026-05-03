# Analysis: Ray Core Architecture

A map of the components, processes, and gRPC services that together form Ray Core. Citations point at file paths in <https://github.com/ray-project/ray>.

## 1. Top-level component map

| Component | Process | Owns | Key gRPC service |
|---|---|---|---|
| **GCS** (Global Control Service) | `gcs_server` (singleton, head node) | Node membership, actor table, placement groups, job table, named-actor registry, internal kv | `GcsService` and split sub-services (`NodeInfoGcsService`, `ActorInfoGcsService`, `PlacementGroupInfoGcsService`, `JobInfoGcsService`, `WorkerInfoGcsService`, `InternalKVGcsService`, `InternalPubSubGcsService`) |
| **Raylet** | `raylet` (one per node) | Local scheduler (`NodeManager`), object transport (`ObjectManager`), embedded **plasma store**, local worker pool | `NodeManagerService`, `ObjectManagerService` |
| **core_worker** | C++ library, linked into every Python worker and the driver | Per-worker task submission, in-process and plasma stores, the `ReferenceCounter`, the direct-call task receiver, gRPC connection to the local raylet | `CoreWorkerService` |
| **Python driver / worker** | `python` (driver = whoever called `ray.init()`; worker = `python -m ray._private.workers.default_worker`) | User code execution, holds a `CoreWorker` via Cython | n/a (uses `CoreWorker`) |
| **Plasma object store** | **Embedded inside `raylet`** as a thread/component (since ~Ray 1.x). Uses `/dev/shm` mmap regions for the data plane. | Shared-memory blobs accessed by all local workers | Custom flatbuffers protocol over a UDS, **not** gRPC |
| **Dashboard agent** | `python -m ray.dashboard.agent` (one per node) | Per-node metrics, runtime-env materialisation, log monitor sidecar | `reporter.proto`, `runtime_env_agent.proto` |

Source pointers:

- GCS: `src/ray/gcs/gcs_server/gcs_server.cc`, `gcs_server_main.cc`
- Raylet: `src/ray/raylet/main.cc`, `src/ray/raylet/raylet.cc`, `src/ray/raylet/node_manager.cc`
- ObjectManager: `src/ray/object_manager/object_manager.cc`
- core_worker: `src/ray/core_worker/core_worker.cc` plus `reference_count.cc`, `task_manager.cc`, `actor_manager.cc`, `transport/`, `store_provider/`
- Plasma: `src/ray/object_manager/plasma/store.cc` (server), `client.cc` (client used inside core_worker)
- Cython binding: `python/ray/_raylet.pyx` and `python/ray/includes/*.pxd`

## 2. Process model on a single node

`ray start --head` (or `ray.init()` on a fresh node) spawns, in this order — code in `python/ray/_private/services.py` and `python/ray/_private/node.py`:

1. `gcs_server` (head only).
2. `raylet`, started with command-line flags pointing at the GCS address, the plasma UDS path, the raylet UDS path, the assigned node IP, the resource description, and the Python worker command-template.
3. The dashboard agent (`python -m ray.dashboard.agent`) and, on the head, the dashboard head (`python -m ray.dashboard.dashboard`).
4. The log monitor (`python -m ray._private.log_monitor`) and runtime-env agent.
5. A worker pool of pre-warmed Python workers, started lazily by the raylet's `WorkerPool` (`src/ray/raylet/worker_pool.cc`) when tasks are dispatched.

**Supervision**: the raylet is the local supervisor. It tracks worker liveness and notifies the owners of those workers' tasks/objects via the GCS pub-sub channel when one dies. If the raylet itself dies, GCS marks the node dead; all owners of refs on that node treat them as lost and (depending on `max_retries`) trigger lineage reconstruction. Children typically use `prctl(PR_SET_PDEATHSIG, SIGKILL)` on Linux to die when their supervisor dies.

**Worker reuse**: Python workers are not one-shot — after a task finishes, the worker returns to the raylet's idle pool keyed by `(language, runtime_env_hash, job_id)` and can be re-dispatched. Actors are different: an actor is bound to a specific worker process for its lifetime. Idle non-actor workers are killed after a configurable timeout (`RAY_idle_worker_killing_time_threshold_ms`).

**IPC**:
- Plasma data plane: mmap regions in `/dev/shm` (or a configured `--plasma-directory`).
- Plasma control plane: UDS at `<session_dir>/sockets/plasma_store`, custom flatbuffers, with `SCM_RIGHTS` ancillary data on `sendmsg(2)` to pass the memfd to clients.
- Raylet control plane: UDS at `<session_dir>/sockets/raylet`.
- Inter-node: TCP/gRPC for everything (GCS heartbeats, lease requests, cross-node object pull/push, direct-call task RPCs).

## 3. gRPC service surface

Proto files live under `src/ray/protobuf/`. The principal services and their hot RPCs:

### `GcsService` and per-table services (`gcs_service.proto`)
- `RegisterNode`, `DrainNode`, `GetAllNodeInfo` — node membership.
- `RegisterActor`, `CreateActor`, `GetActorInfo`, `KillActor` — actor lifecycle.
- `AddJob`, `MarkJobFinished`, `GetAllJobInfo` — driver/job tracking.
- `CreatePlacementGroup`, `RemovePlacementGroup`, `WaitPlacementGroupUntilReady`.
- `InternalKVGet/Put/Del/Exists/Keys` — generic kv used by Ray internals (e.g., for cluster config).
- `GcsPublish`, `GcsSubscriberPoll` — pub-sub over gRPC, replacing the old Redis pub-sub.

### `NodeManagerService` (`node_manager.proto`)
- `RequestWorkerLease` — the heart of distributed scheduling. A core_worker asks a raylet to lease a worker meeting a resource spec; the raylet replies with either a granted worker (address+port) or a *spillback* hint to try another node.
- `ReturnWorker`, `ReleaseUnusedWorkers`, `CancelWorkerLease`.
- `PinObjectIDs` — pin objects in plasma against eviction (used during task execution to keep arguments live).
- `RequestObjectSpillage`, `ReleaseUnusedBundles` — spilling / placement-group plumbing.
- `GetNodeStats`, `GetSystemConfig`, `ShutdownRaylet`.

### `ObjectManagerService` (`object_manager.proto`)
- `Push`, `Pull` — chunked object transfers between nodes. Default chunk size around 64 KiB (configurable via `object_manager_default_chunk_size`).
- `FreeObjects` — the GC fanout: when an owner's local refcount + borrower set hits zero, it tells every raylet that ever held a copy to free it.

### `CoreWorkerService` (`core_worker.proto`)
- `PushTask` — direct task submission. Used for actor calls and direct task calls bypassing the raylet's argument plumbing (the "direct call" optimization).
- `DirectActorCallArgWaitComplete`.
- `GetObjectStatus`, `WaitForActorOutOfScope`.
- `PubsubLongPolling`, `PubsubCommandBatch` — owner pubsub for the borrower handshake.
- `AddObjectLocationOwner`, `RemoveObjectLocationOwner`, `GetObjectLocationsOwner`, `AssignObjectOwner` — the gRPC manifestation of the ownership model.
- `KillActor`, `CancelTask`, `RemoteCancelTask`, `Exit`.
- `LocalGC`, `SpillObjects`, `RestoreSpilledObjects`, `DeleteSpilledObjects`.

### Other proto files
- `runtime_env_agent.proto`, `reporter.proto` — dashboard agent surface.
- `autoscaler.proto`, `gcs.proto` — autoscaler ↔ GCS contracts.
- `common.proto` — shared messages (`TaskSpec`, `Address`, `ObjectReference`, `RayErrorInfo`, the `ErrorType` enum, the `TaskStatus` enum).

## 4. Bootstrap and cluster join

`ray.init()` (`python/ray/_private/worker.py::init` → `python/ray/_private/services.py::canonicalize_bootstrap_address_or_die`) resolves the head node via, in order:

1. Explicit `address=` kwarg.
2. `RAY_ADDRESS` env var.
3. The file `/tmp/ray/ray_current_cluster` written by `ray start --head`.
4. Otherwise, start a fresh local cluster in-process.

Once an address is resolved, the connecting process:

1. Connects to GCS over gRPC, registers itself as a driver via `GcsClient::AddJob`.
2. Reads cluster-wide config via `GetSystemConfig` from the local raylet.
3. Discovers the local plasma and raylet UDS paths from the session-directory convention (`/tmp/ray/session_<id>/sockets/...`).
4. Instantiates a C++ `CoreWorker` through Cython, which connects to the local raylet UDS and the local plasma UDS.

GCS storage backend: as of Ray 2.x, GCS persists state to an internal in-memory store by default. With `--redis-address` GCS is fault-tolerant — on restart it rebuilds in-memory tables from Redis and re-establishes connections to all live raylets, which then re-register. In-flight tasks survive thanks to the ownership model: each worker retains the task specs of the objects it owns, so it can replay on reconnect. See `src/ray/gcs/gcs_server/gcs_server.cc` and `gcs_storage_client.cc`.

## 5. Scheduling

Two-tier, mostly in `src/ray/raylet/scheduling/cluster_task_manager.cc` and `cluster_resource_scheduler.cc`. The flow:

1. A core_worker calls `f.remote(args)`. The Python side resolves the args' `ObjectRef`s, builds a `TaskSpec`, and calls `RequestWorkerLease` on its **local** raylet. (Direct-actor calls go straight to the actor's worker via `CoreWorkerService.PushTask`, bypassing raylets entirely — see `src/ray/core_worker/transport/direct_actor_transport.cc`.)
2. The local raylet's `ClusterTaskManager` checks if the task's resource demand can be satisfied locally. If yes, it pops a worker from the `WorkerPool` and returns its address. The core_worker then sends `PushTask` directly to the leased worker.
3. If the local raylet can't satisfy the request, it picks a remote node via `ClusterResourceScheduler::GetBestSchedulableNode`, considering hard resource feasibility (CPU/GPU/memory/custom resources), the scheduling strategy (`DEFAULT/SPREAD/PACK/STRICT_SPREAD/STRICT_PACK/NodeAffinity`), and data-locality hints.
4. The local raylet returns a *spillback* reply naming the chosen remote raylet. The core_worker re-sends `RequestWorkerLease` to that node.

Spillback is core_worker-driven, not raylet-to-raylet forwarded — keeps each raylet stateless about remote leases.

Resource accounting uses fixed-point representation (`src/ray/common/scheduling/fixed_point.h`) to avoid float drift on fractional resources. Custom resources are arbitrary string keys with numeric values declared per node via `ray start --resources='{"TPU": 4}'` and matched by string equality.

Each raylet broadcasts its local view of resources to GCS via heartbeats every `RAY_raylet_report_resources_period_milliseconds` (default ~100 ms); GCS aggregates and rebroadcasts to all raylets on the `RESOURCES_BATCH` pubsub channel.

## 6. Bootstrapping order summary

```
ray.init()
   │
   ├─► resolve head address
   ├─► gcs_server gRPC: AddJob, GetClusterId
   ├─► find local plasma + raylet UDS
   ├─► instantiate CoreWorker (C++ via Cython)
   │      │
   │      ├─► connect to plasma UDS (custom flatbuffers proto)
   │      ├─► connect to raylet UDS (gRPC over UDS for leases)
   │      ├─► register with GCS as a worker
   │      └─► start own gRPC server (CoreWorkerService) for direct calls
   │
   └─► return; user can now call ray.remote / ray.put / ray.get
```

## 7. Component-graph diagram

```
                  ┌─────────────────────────────────┐
                  │  GCS (gcs_server)               │
                  │  - node membership              │
                  │  - actor registry               │
                  │  - placement groups             │
                  │  - internal kv                  │
                  │  - pubsub                       │
                  └────────────▲────────────────────┘
                               │ heartbeats / reports
                               │ pubsub
        ┌──────────────────────┼─────────────────────────┐
        │                      │                         │
        ▼                      ▼                         ▼
  Node 1 (head)          Node 2                    Node 3
  ┌─────────────┐       ┌─────────────┐           ┌─────────────┐
  │   raylet    │       │   raylet    │  pull/push│   raylet    │
  │  ┌─plasma─┐ │◄─────►│  ┌─plasma─┐ │◄─────────►│  ┌─plasma─┐ │
  │  └────────┘ │       │  └────────┘ │           │  └────────┘ │
  └──▲──────────┘       └──▲──────────┘           └──▲──────────┘
     │ UDS                 │ UDS                     │ UDS
     │                     │                         │
  ┌──┴──────────────┐   ┌──┴──────────────┐       ┌──┴──────────────┐
  │ Python driver / │   │ Python workers  │       │ Python workers  │
  │ workers         │   │ (each: core_-   │       │ (each: core_-   │
  │ (each: core_-   │   │  worker C++ lib │       │  worker C++ lib │
  │  worker C++ lib │   │  via Cython)    │       │  via Cython)    │
  │  via Cython)    │   └─────────────────┘       └─────────────────┘
  └─────────────────┘
```

Direct-call task RPCs (between core_workers, for actor methods and post-lease task pushes) and ownership pubsub (`PubsubLongPolling`) flow worker-to-worker, bypassing the raylet on the data path.

## Key insights for a reimplementation

These are picked up and elaborated in `../design/`:

1. **Ownership is the load-bearing wall.** Decentralized refcount, lineage, no GCS on the data path — all flow from "the submitter owns the future". Don't compromise this.
2. **The raylet's hot path is `RequestWorkerLease` + `PushTask`.** Once a worker is leased, the data path is core_worker → leased_worker direct gRPC; the raylet is not in the loop.
3. **Plasma's protocol is custom flatbuffers + `SCM_RIGHTS` fd-passing**, not gRPC. Replicating this gives true zero-copy from any local worker.
4. **`RayObject = (data, metadata, nested_refs)`** with metadata as a small separate buffer is the design that makes O(1) state inspection possible. Keep this layout — it's the foundation of the new state-API.
5. **gRPC has real overhead on small tasks.** Ray mitigates with direct-call paths, batched pubsub, UDS for local hops. Plan an analogous fast path early.
6. **The Cython binding is also Ray's API stability layer** — many "Ray internals" fixes are actually fixes to `_raylet.pyx`. Plan the PyO3 module as the single Python-visible entry point and route all public Python API through it.
7. **Failure semantics are subtle and inconsistent across owner-died / worker-died / task-raised / object-lost categories.** The new design enumerates these from day one (see `../analysis/02-ownership-and-references.md` and `../design/05-state-and-error-api.md`).

> ⚠️ **Constants to verify against current Ray master before relying on them as design parameters**:
> `max_direct_call_object_size` (≈ 100 KiB), default object-manager chunk size (≈ 64 KiB), `RAY_raylet_report_resources_period_milliseconds` (≈ 100 ms), `idle_worker_killing_time_threshold_ms`. Names are stable; numeric values have drifted historically.
