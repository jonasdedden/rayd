# Analysis: The Distributed Object Store

How Ray stores task return values and `ray.put` payloads, how it splits them between an in-process store and shared-memory plasma, how it transports them across nodes, and — critically — how it encodes the metadata that lets a `Get` distinguish a successful value from an exception in O(1) without touching the data buffer.

## 1. The `RayObject` triple

The fundamental in-memory unit. Defined in `src/ray/common/ray_object.h`:

```
RayObject {
    Buffer            data;          // payload (pickled value or pickled exception)
    Buffer            metadata;      // small (≈ a few bytes); type tag or error code
    Vec<ObjectRef>    nested_refs;   // ObjectRefs contained in `data` (for refcount propagation)
}
```

The split between `data` and `metadata` is *the* design that makes cheap state inspection possible. The metadata buffer is typically 3–20 bytes. The data buffer is the user payload. They're stored separately in both stores (memory and plasma).

## 2. Two-tier store

A core_worker writes a `RayObject` into one of two places, decided by size:

- **`CoreWorkerMemoryStore`** (`src/ray/core_worker/store_provider/memory_store/memory_store.h/.cc`): an in-process per-worker `absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>>`. Used for **small objects (≤ ~100 KiB)**, exception markers, and inlined task replies.
- **`CoreWorkerPlasmaStoreProvider`** (`src/ray/core_worker/store_provider/plasma_store_provider.h/.cc`): a thin client onto the node-local plasma store, used for **large objects**.

The threshold is `RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)` in `src/ray/common/ray_config_def.h`. Verify the current default — it has held at 100 KiB for many releases but is configurable.

The decision is made at task-return time in `CoreWorker::SealReturnObject` (and equivalents on the `Put` path):

- ≤ threshold → inline into the task reply RPC, caller writes to its own `MemoryStore` on receipt.
- \> threshold → executor writes into local plasma, caller receives only the `ObjectRef` + location, fetches lazily.

Errors are almost always small, so they ride the inline path.

### Public interface of `CoreWorkerMemoryStore`

```cpp
void Put(const RayObject &object, const ObjectID &object_id, bool has_reference);
Status Get(const std::vector<ObjectID> &ids, int num_objects, int64_t timeout_ms,
           const WorkerContext &ctx, std::vector<std::shared_ptr<RayObject>> *results);
std::shared_ptr<RayObject> GetIfExists(const ObjectID &id);
void GetAsync(const ObjectID &id, std::function<void(std::shared_ptr<RayObject>)> cb);
Status Wait(const absl::flat_hash_set<ObjectID> &ids, int num_objects, int64_t timeout_ms,
            const WorkerContext &ctx,
            absl::flat_hash_set<ObjectID> *ready,
            absl::flat_hash_set<ObjectID> *plasma_object_ids);
void Delete(const absl::flat_hash_set<ObjectID> &ids,
            absl::flat_hash_set<ObjectID> *plasma_ids_to_delete);
bool Contains(const ObjectID &id, bool *in_plasma);
int Size();
uint64_t UsedMemory();
```

Note `GetIfExists` and `Contains` already provide a non-blocking presence check that does not deserialize.

### Public interface of `CoreWorkerPlasmaStoreProvider`

```cpp
Status Put(const RayObject &object, const ObjectID &id, const rpc::Address &owner, bool *exists);
Status Create(const std::shared_ptr<Buffer> &metadata, size_t data_size, const ObjectID &id,
              const rpc::Address &owner, std::shared_ptr<Buffer> *data, ...);
Status Seal(const ObjectID &id);
Status Get(const std::vector<ObjectID> &ids, const std::vector<rpc::Address> &owners,
           int64_t timeout_ms,
           absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);
Status GetIfLocal(const std::vector<ObjectID> &ids,
                  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);
Status Contains(const ObjectID &id, bool *has_object);
Status Wait(const std::vector<ObjectID> &ids, const std::vector<rpc::Address> &owners,
            int num_objects, int64_t timeout_ms, const WorkerContext &ctx,
            absl::flat_hash_set<ObjectID> *ready);
Status Release(const ObjectID &id);
Status Delete(const absl::flat_hash_set<ObjectID> &ids, bool local_only);
```

`GetIfLocal` is the no-fetch version of `Get` — it returns objects only if they're already on the local node.

## 3. Plasma client/server protocol

Plasma originally lived in Apache Arrow, was upstreamed back into Ray, and now sits at `src/ray/object_manager/plasma/`. Key files:

- `plasma.fbs` — flatbuffers schema for the wire protocol.
- `store.cc` — server (runs as a thread inside the raylet process).
- `client.cc` — client (used inside `CoreWorkerPlasmaStoreProvider`).
- `plasma_allocator.cc` — `dlmalloc`-based arena over an mmap'd backing region.

### Transport
Unix Domain Socket at `<session_dir>/sockets/plasma_store`. Messages are flatbuffers: `PlasmaCreateRequest`, `PlasmaCreateReply`, `PlasmaGetRequest`, `PlasmaGetReply`, `PlasmaSealRequest`, `PlasmaReleaseRequest`, `PlasmaContainsRequest`, `PlasmaAbortRequest`, `PlasmaDeleteRequest`.

### Memory-fd handoff
The reply to `Create` and `Get` carries the **memfd** of the underlying mmap region via `SCM_RIGHTS` ancillary data on the UDS. Each client mmaps each region once and caches it in a per-client `client_mmap_table_` keyed by memfd id. After that, all reads are direct mmap loads — no further IPC for the same region.

This is the crucial property: data is **truly zero-copy** on the local node. A 1 GB numpy array put by worker A is read by worker B as a pointer into the same physical pages.

### Lifecycle of `ray.put(big_obj)`

```
client                                          server (in raylet)
  │                                                     │
  ├── CreateRequest{id, data_size, metadata_size, owner}─►
  │                                                     │ allocate (data_size + metadata_size)
  │                                                     │ via PlasmaAllocator (dlmalloc over mmap)
  │                                                     │ record ObjectTableEntry{state=CREATED, refcount=1}
  │   ◄── CreateReply{memfd, offset, size, ...} (with SCM_RIGHTS fd)
  │                                                     │
  │ mmap memfd if not cached, return writable Buffer    │
  │ user serializes payload directly into the buffer    │
  │                                                     │
  ├── SealRequest{id} ──────────────────────────────────►
  │                                                     │ flip state to SEALED
  │                                                     │ broadcast notifications to local raylet
  │                                                     │ (which propagates location to owner)
  │   ◄── SealReply{} ──────────────────────────────────
```

`Get` is symmetrical: a `GetRequest{[ids], timeout}` returns memfd+offset+size for each found object; the client mmaps and returns read-only `Buffer`s. All reads are lock-free; sealed objects are immutable.

### Two refcount layers (don't conflate)

- **Plasma-internal refcount**: per-(client, object) `Get`/`Release` count. When the last client releases, the object becomes evictable (LRU under allocator pressure). Code: `PlasmaStore::ReleaseObject`, `EvictObjects`.
- **Ray distributed refcount**: handled by the *owner's* `ReferenceCounter`. When owner-side count → 0, the owner sends `FreeObjects` to all raylets that ever held a copy, which calls `Delete` on plasma globally.

## 4. The metadata format

The metadata `Buffer` is a small bytes string. Its leading bytes encode the object's type:

### Type tags (Python successful values)

Defined in `python/ray/_private/ray_constants.py`:

| Constant | Bytes | Meaning |
|---|---|---|
| `OBJECT_METADATA_TYPE_RAW` | `b"RAW"` | Raw bytes (e.g., `ray.put(b"hello")`); no deserialization |
| `OBJECT_METADATA_TYPE_PYTHON` | `b"PYTHON"` | pickle5 / cloudpickle blob (the common case for Python returns) |
| `OBJECT_METADATA_TYPE_CROSS_LANGUAGE` | `b"XLANG"` | msgpack-encoded cross-language value |
| `OBJECT_METADATA_TYPE_ACTOR_HANDLE` | `b"ACTOR_HANDLE"` | Pickled actor handle reducer payload |
| `OBJECT_METADATA_DEBUG_PREFIX` | `b"DEBUG:"` | Debug annotation prefix (rare) |

Actual format is comma-separated: `metadata_fields = metadata.split(b",")`. The first field is the type tag; subsequent fields carry mode-specific extras (e.g., a numpy dtype hint).

### Error codes

When a task fails, the metadata holds the integer string of an `ErrorType` enum value (from `src/ray/protobuf/common.proto`). For example `b"3"` for `TASK_EXECUTION_EXCEPTION`. The Python deserializer (`_deserialize_object` in `python/ray/_private/serialization.py`) does:

```python
try:
    error_type = int(metadata_fields[0])
except Exception:
    raise Exception(f"Can't deserialize object: {object_ref}, metadata: {metadata}")
# pattern-match against ErrorType.Value() names
if error_type == ErrorType.TASK_EXECUTION_EXCEPTION:
    ray_error_info = RayErrorInfo()
    ray_error_info.ParseFromString(pb_bytes)  # data buffer holds the proto
    return RayTaskError(...)  # eventually raised by the caller
elif error_type == ErrorType.WORKER_DIED: ...
elif error_type == ErrorType.OWNER_DIED: ...
# etc.
```

Full `ErrorType` enum (verified from `src/ray/protobuf/common.proto`):

```
WORKER_DIED, ACTOR_DIED, OBJECT_LOST, TASK_EXECUTION_EXCEPTION,
OBJECT_DELETED, OWNER_DIED, OBJECT_UNRECONSTRUCTABLE,
OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED,
OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED,
RUNTIME_ENV_SETUP_FAILED, OBJECT_FETCH_TIMED_OUT,
TASK_PLACEMENT_GROUP_REMOVED, ACTOR_PLACEMENT_GROUP_REMOVED,
TASK_UNSCHEDULABLE_ERROR, ACTOR_UNSCHEDULABLE_ERROR,
LOCAL_RAYLET_DIED, TASK_CANCELLED, ACTOR_CREATION_FAILED,
ACTOR_UNAVAILABLE, NODE_DIED, OUT_OF_MEMORY, OUT_OF_DISK_ERROR,
END_OF_STREAMING_GENERATOR, WORKER_STARTUP_FAILED, OBJECT_FREED, ...
```

(Total ~33 variants. Numeric assignments may have shifted across Ray versions.)

The companion `RayErrorInfo` proto carries a backtrace, `error_message`, and `error_type` for richer reconstruction.

### Why this enables cheap state inspection

For the *vast majority* of error cases, the metadata buffer alone tells you "this is errored, here's the category" without touching `data`. Only `TASK_EXECUTION_EXCEPTION` needs the data buffer to recover the original Python exception (it holds the pickled `RayTaskError` with traceback).

A `state(ref) → Pending | Ready | Failed(ErrorCategory)` API costs a hash-map lookup (memory store) or a single `Contains` IPC (plasma store) plus a metadata-buffer parse. **Zero deserialization, zero data-buffer copy.**

This is the foundation of the new public API in `../design/05-state-and-error-api.md`.

## 5. Cross-node object transfer

When worker A on node N1 needs an object owned/located on N2:

1. Caller's `CoreWorker::GetObjects` checks local memory store and local plasma; misses both.
2. Issues a `Pull` request to its **local raylet**.
3. The raylet's `ObjectManager` (`src/ray/object_manager/object_manager.cc`) consults the **owner-based object directory** (`ownership_based_object_directory.cc`) for object locations.
4. The directory queries the *owner worker* (per the address embedded in the `ObjectRef`) via `GetObjectLocationsOwner` RPC; the owner returns the node-set that has reported having a copy.
5. ObjectManager picks a peer node and issues `Pull`. The peer's ObjectManager replies by `Push`ing chunks.
6. Chunked transfer (default chunk size ~64 KiB; configurable via `object_manager_default_chunk_size`). Each chunk is a unary gRPC `Push` carrying chunk index, offset, total size, and bytes. Code: `ObjectManager::PushObject`, `ObjectManager::ReceiveObjectChunk`, `PullManager`/`PushManager`.
7. Receiver allocates a plasma buffer of full size on the first chunk, writes chunks directly in, and `Seal`s once complete. It then notifies the directory ("I now have a copy").

Key supporting classes:

- `PullManager` (`src/ray/object_manager/pull_manager.cc`): quota-aware queue; decides which pulls to activate based on memory budget; supports **pull bundles** so a task's argument set is fetched atomically — either all of the task's args become local or none do, preventing partial-delivery deadlocks.
- `PushManager` (`src/ray/object_manager/push_manager.cc`): sender-side chunk scheduling, throttles concurrent transfers per peer.
- `OwnershipBasedObjectDirectory`: replaces the old GCS-based directory. Owners are the source of truth.

## 6. Spilling

When plasma's `object_store_memory` budget fills up *and* the objects have nonzero distributed refcount (so they can't just be evicted), Ray spills them to disk:

- `src/ray/raylet/local_object_manager.cc` orchestrates spilling on each node.
- `python/ray/_private/external_storage.py` does the actual write/read. Default backend: `FileSystemStorage` writing to `/tmp/ray/session_*/...`. Pluggable: S3, GCS, arbitrary URIs configured via `system_config["object_spilling_config"]`.

**Policy**: LRU among spillable objects; pinned objects (those currently arguments to executing tasks) cannot spill. Spilling triggers automatically at `RayConfig::object_spilling_threshold`.

**File format**: custom binary, multiple objects packed per file for batched I/O efficiency. The exact framing is internal to `external_storage.py` and has shifted across releases — design an alternative for the new system that's versioned and self-describing.

**Refcount interaction**: spilling does not change the distributed refcount; it just moves bytes from shared memory to disk. The owner records `spilled_url`. On a subsequent `Get`, the raylet either restores the object back into plasma (`RestoreSpilledObjects`) or streams directly. When refcount → 0, the spilled file is deleted as part of `FreeObjects`.

The raylet exposes spill URLs back to the owner via `ReportObjectSpilled` so other nodes can fetch from the spill location instead of forcing a re-execute.

## 7. Lineage reconstruction (briefly here; full treatment in `02-ownership-and-references.md`)

When a `Get` finds *no* plasma copies anywhere and the object isn't spilled, the owner can re-execute its creating task — provided `attempts_remaining > 0`, lineage isn't evicted, and the inputs are themselves reconstructable. Triggered by `TaskManager::ResubmitTask`.

If reconstruction fails after exhausting retries: write a sentinel `RayObject{metadata=OBJECT_UNRECONSTRUCTABLE_*, data=empty}` into the in-memory store; subsequent `Get`s raise `ObjectReconstructionFailedError`.

## 8. Known weaknesses

1. **`ray.get(list)` raises on first failure.** Treated in `05-objectref-state-gap.md`.
2. **`ray.wait` does not distinguish errored from succeeded.** Both are "ready". Treated in `05-objectref-state-gap.md`.
3. **Object-store-full / OOM errors are opaque.** When plasma fills up and spilling can't keep up, users get `ObjectStoreFullError` and the worker-killer subsystem starts evicting workers in ways that are hard to debug.
4. **Refcount leaks** in cross-language interop (Java/C++/Python) and during racy worker-death-mid-handshake scenarios. Symptom: objects pinned forever, plasma fills up.
5. **Spilling perf cliffs** with the default `FileSystemStorage`; users often need NVMe or S3.
6. **Hugepages support is finicky** on cloud VMs.
7. **`OBJECT_LOST` / `OWNER_DIED` errors during normal autoscaling.** Even healthy scale-down can race with object refs.

## 9. Design implications for the reimplementation

These flow into `../design/02-object-store.md`:

1. **Mirror the `RayObject` triple** — separate `data` and `metadata` buffers; track nested refs.
2. **Two-tier store with a configurable threshold** (~100 KiB default).
3. **Eagerly populate the caller's in-memory store with `(data, metadata)` on inline returns** — by the time user code calls `get_state()` or `peek_error()`, the bytes are local and the check is O(1).
4. **Plasma-equivalent over UDS + flatbuffers + `SCM_RIGHTS` fd-passing.** Replicate Ray's mechanical sympathy. In Rust, `nix` provides `sendmsg(2)` with ancillary data; `memmap2` provides the mmap layer.
5. **Use a typed metadata enum**, not stringified integers. A single `u8` discriminator + optional payload field is plenty and survives forward-compat better than `b"3"` vs `b"WORKER_DIED"` ambiguity Ray has battled.
6. **Owner-based directory.** No GCS in the location data path; ObjectRef carries the owner address.
7. **Lineage budget per-owner with explicit eviction policy.** Avoid Ray's implicit pinning model.
8. **Spilling pluggable from day one** with a versioned, self-describing on-disk format.
9. **Differentiate `Ready-success` from `Ready-with-error`** in the public Wait API. This is a public-API correctness bug in Ray; don't replicate it.
