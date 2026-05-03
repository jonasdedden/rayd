# Design: Distributed Object Store

The object store is the load-bearing infrastructure for the new system. This document specifies its layout, protocols, and APIs in enough detail to implement.

Read first: `../analysis/04-object-store.md` (how Ray does it).

## Goals

1. **O(1) per-`ObjectRef` state inspection** without deserializing the data buffer. The metadata-vs-data separation makes this achievable.
2. **Zero-copy reads on the local node** for any worker, via `mmap` + `SCM_RIGHTS` fd handoff.
3. **Cross-node transfer** via owner-directed pull, chunked over gRPC.
4. **Pluggable spilling** with a versioned, self-describing on-disk format from day one.
5. **Strict typing across the FFI**: all metadata is a typed enum on the Rust side, exposed to Python as a typed enum. No stringly-typed integer bytes.

## Two-tier store

Same shape as Ray:

- **In-process memory store** (per-worker): a `tokio::sync::RwLock<HashMap<ObjectId, Arc<RayObject>>>` in `rayd-core`. Holds small returns (≤ 100 KiB by default), exception markers, and inlined task replies.
- **Plasma store** (per-node, embedded in raylet): shared-memory store accessed by all local workers via UDS.

Threshold configurable via `RAYD_INLINE_OBJECT_THRESHOLD_BYTES`, default 100 KiB.

The split decision happens at task-return time inside the executor's core_worker:

```rust
fn seal_return(&self, id: ObjectId, payload: ReturnPayload) -> Result<()> {
    if payload.total_size() <= self.inline_threshold {
        // ride the gRPC PushTaskReply inline; caller writes into its memory store
        self.task_reply.add_inline_return(id, payload.metadata, payload.data, payload.nested_refs);
    } else {
        // allocate in plasma, hand caller only the ref + size
        let plasma_handle = self.plasma.create(id, payload.metadata.len(), payload.data.len(), self.owner_address)?;
        plasma_handle.write_metadata(&payload.metadata);
        plasma_handle.write_data(&payload.data);
        plasma_handle.seal()?;
    }
    Ok(())
}
```

## The `RayObject` type

```rust
// rayd-core/src/store/ray_object.rs
pub struct RayObject {
    pub metadata: Metadata,        // typed, NOT raw bytes
    pub data: Bytes,               // payload
    pub nested_refs: Vec<ObjectRef>,
}
```

### `Metadata`: typed discriminator

Replaces Ray's stringly-typed metadata bytes. A single Rust enum, serialized to a fixed-size header on the wire:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum Metadata {
    /// Successful return: pickled Python object
    Pickle5 { has_nested_refs: bool } = 1,
    /// Successful return: raw bytes (e.g., `rayd.put(b"...")`)
    Raw = 2,
    /// Successful return: actor handle reducer payload
    ActorHandle = 3,
    /// Failed: see ErrorCategory and optional ErrorPayload
    Error { category: ErrorCategory, raw_code: u16 } = 16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum ErrorCategory {
    TaskException = 1,
    WorkerDied = 2,
    ActorDied = 3,
    OwnerDied = 4,
    TaskCancelled = 5,
    ObjectLost = 6,
    ObjectUnreconstructable = 7,
    FetchTimeout = 8,
    RuntimeEnvFailed = 9,
    Unschedulable = 10,
    OutOfMemory = 11,
}
```

`raw_code` carries the underlying granular error code (analog of Ray's `ErrorType`) for callers who need finer detail; `category` is the user-friendly bucket.

**Serialized form** (the bytes that go in the metadata buffer):

```
[1 byte: discriminator]
  if Pickle5: [1 byte: has_nested_refs flag]
  if Error:   [1 byte: category] [2 bytes: raw_code LE]
  Raw, ActorHandle: empty payload
```

So metadata is 1–4 bytes for everything. Reading metadata costs a single load from the in-memory store hash map. There is no string parsing, no integer-byte conversion, and no ambiguity about format versions.

For `Error` objects, the `data` buffer holds an optional `ErrorPayload` (see below) that's only deserialized if the user calls `peek_error()` or `get()`.

### `ErrorPayload` (in `data` for errored objects)

```rust
#[derive(Serialize, Deserialize)]
pub struct ErrorPayload {
    pub message: String,                   // always present
    pub traceback: Option<String>,         // present for task_exception
    pub raw_code: u16,                     // duplicated from metadata for self-contained payload
    pub pickled_python_exception: Option<Bytes>,  // present only for task_exception
}
```

Serialized via `prost` (same protobuf machinery as the rest of the wire protocol). `peek_error()` decodes everything except `pickled_python_exception` (which is opaque bytes); `exception()` decodes the full thing including unpickling the exception via CPython.

## Memory store (per-worker)

```rust
pub struct MemoryStore {
    objects: RwLock<HashMap<ObjectId, Arc<RayObject>>>,
    waiters: RwLock<HashMap<ObjectId, Vec<oneshot::Sender<Arc<RayObject>>>>>,
    plasma_pointers: RwLock<HashMap<ObjectId, PlasmaLocation>>,  // refs that live in plasma; just pointers
}

impl MemoryStore {
    pub fn put(&self, id: ObjectId, obj: Arc<RayObject>);
    pub fn get_if_exists(&self, id: &ObjectId) -> Option<Arc<RayObject>>;
    pub fn contains(&self, id: &ObjectId) -> bool;          // used by state()

    pub async fn get(&self, id: ObjectId, timeout: Option<Duration>) -> Result<Arc<RayObject>, GetError>;
    pub async fn wait(&self, ids: &[ObjectId], num_objects: usize, timeout: Option<Duration>)
        -> WaitResult;

    // The new API: return per-id state without raising.
    pub fn state_snapshot(&self, ids: &[ObjectId]) -> HashMap<ObjectId, RefState>;

    pub fn delete(&self, ids: &[ObjectId]);
    pub fn used_memory(&self) -> u64;
}
```

`state_snapshot` is the workhorse for the new `ObjectRef.state()` and `wait_with_states()` Python APIs. It's pure metadata reads; no data deserialization.

## Plasma store (per-node)

A standalone process: `rayd-plasma`. Embedded inside `rayd-raylet` for v1 (matches Ray's choice and avoids one IPC hop).

### Wire protocol

UDS at `<session_dir>/sockets/plasma`. Frame format: 4-byte length prefix, then a `prost`-encoded `PlasmaRequest` / `PlasmaResponse` message. No flatbuffers — `prost` is what we already use for gRPC, one less serializer in the build.

```protobuf
message PlasmaRequest {
  oneof body {
    CreateRequest   create   = 1;
    SealRequest     seal     = 2;
    AbortRequest    abort    = 3;
    GetRequest      get      = 4;
    ContainsRequest contains = 5;  // metadata-only; returns category w/o data
    ReleaseRequest  release  = 6;
    DeleteRequest   delete   = 7;
    ListRequest     list     = 8;  // diagnostic
  }
}

message CreateRequest {
  bytes object_id      = 1;       // 28 bytes
  uint64 metadata_size = 2;
  uint64 data_size     = 3;
  Address owner        = 4;       // for refcount queries from other workers
}

message CreateReply {
  uint64 mmap_id   = 1;
  uint64 metadata_offset = 2;
  uint64 data_offset     = 3;
  // The memfd is sent OOB via SCM_RIGHTS on the same UDS connection
}

message ContainsReply {
  bool present              = 1;
  optional Metadata metadata = 2;  // present iff `present` is true
}
```

`ContainsReply` carries the metadata. This is the plasma-side support for cheap state inspection: a worker can ask "is this ref ready, and if so what category?" without triggering any data transfer.

### Memfd handoff

On `CreateReply` and `GetReply`, the plasma server passes the `memfd` of the underlying mmap region via `SCM_RIGHTS` ancillary data on the UDS. The Rust client uses `nix::sys::socket::sendmsg`/`recvmsg` with `ControlMessage::ScmRights`. Each client mmaps each region once and caches the mapping in a per-client `client_mmap_table_: HashMap<u64, MmapMut>` keyed by `mmap_id`.

After the first handoff for a given region, all reads are direct mmap loads. Plasma server is not in the data path.

### Allocator

Per-arena bump allocator over an mmap'd backing region. Each arena is a contiguous region (default 1 GiB; configurable via `RAYD_PLASMA_ARENA_SIZE_BYTES`). When all objects in an arena have refcount 0, the entire arena resets — far simpler than a general-purpose heap. Multiple arenas are pooled.

Falls back to a slab allocator inside an arena when objects vary widely in size and bump fragmentation hurts. Keep the slab implementation behind a feature flag (`feature = "slab-allocator"`) and pick at startup based on workload.

### Per-arena layout

```
+-------------------------+ <- mmap base
| arena header (page 0)   |   - magic, version, total size, bump_offset, refcount
+-------------------------+
| object 0: metadata bytes | <- aligned to 16
| object 0: data bytes     |
+-------------------------+
| object 1: metadata bytes |
| object 1: data bytes     |
+-------------------------+
| ... etc                  |
+-------------------------+
```

Each object's `(metadata_offset, metadata_size, data_offset, data_size)` is recorded in the plasma server's in-memory `ObjectTableEntry`. The arena itself contains no object index; the server is the source of truth.

### Two refcount layers (kept distinct)

- **Plasma client refcount**: per-(client, object) Get/Release count. When the last client releases, the object becomes evictable. Code: `PlasmaServer::release_object`, `evict_objects`.
- **Distributed refcount**: handled by the owner's `ReferenceCounter` in `rayd-core`. When owner-side count → 0, owner sends `FreeObjects` to all raylets that ever held a copy.

Don't merge these. They have different lifetimes and different failure modes.

## Cross-node transfer

Object owner is the source of truth for locations. Flow when worker A on node 1 needs object owned by worker on node 2:

1. A's core_worker calls `MemoryStore::get_if_exists` → miss.
2. Calls `PlasmaClient::contains` → miss.
3. Sends `Pull{object_id, owner_address}` to local raylet's `ObjectManagerService`.
4. Raylet's `ObjectManager` calls `GetObjectLocations` on the owner via `CoreWorkerService` (gRPC). Owner replies with the set of node addresses that have a copy.
5. ObjectManager picks a peer (closest, least-loaded, or simple round-robin in v1) and sends `Pull{object_id}` to that peer's `ObjectManagerService`.
6. Peer streams `Push{chunk_index, total_chunks, offset, bytes}` chunks back via a server-streaming RPC. Default chunk size 64 KiB; configurable.
7. Receiver allocates a plasma buffer of full size on first chunk, writes chunks directly in (no Rust-level intermediate copy: each chunk is `unsafe { std::ptr::copy_nonoverlapping(...) }` into the mmap'd region after a length check). Once last chunk arrives, calls `Seal`.
8. Receiver notifies the owner ("I have a copy") via `AddObjectLocation`.

### `PullManager` and bundle semantics

A task with K argument refs needs all K available before execution. Atomic bundle pulls prevent partial-delivery deadlocks:

```rust
pub struct PullBundle {
    pub task_id: TaskId,
    pub objects: Vec<(ObjectId, u64)>,  // (id, expected size)
}

impl PullManager {
    /// Activates the bundle if and only if the entire set fits in the budget.
    pub async fn activate_bundle(&self, bundle: PullBundle) -> Result<(), PullError>;
    /// Cancels in-flight chunks for a bundle (e.g., task cancelled).
    pub fn cancel_bundle(&self, task_id: TaskId);
}
```

Quotas: each pull manager has a memory budget (configurable) and prioritizes bundles oldest-first. Single-object pulls are a degenerate bundle with K=1.

## Spilling

Pluggable from day one:

```rust
#[async_trait]
pub trait SpillBackend: Send + Sync {
    async fn spill(&self, ids: &[ObjectId], objects: &[Arc<RayObject>]) -> Result<Vec<SpillUrl>>;
    async fn restore(&self, url: &SpillUrl) -> Result<RayObject>;
    async fn delete(&self, urls: &[SpillUrl]) -> Result<()>;
}
```

v1 ships:
- `LocalFsBackend`: writes to `<session_dir>/spilled/`.
- (Optional) `S3Backend`: uses `aws-sdk-s3`, gated behind `feature = "s3"`.

Other backends (GCS, Azure Blob) deferred to user contributions.

### On-disk format

Self-describing, versioned:

```
[8 bytes magic: "RAYD\1\0\0\0"]   // RAYD + version 1
[8 bytes total_length LE]
[8 bytes count: number of packed objects]
for each object:
  [28 bytes object_id]
  [4 bytes metadata_length]
  [metadata_length bytes metadata]
  [8 bytes data_length]
  [data_length bytes data]
  [4 bytes nested_refs_count]
  [nested_refs_count * 56 bytes (28 id + 28 owner address)]
[4 bytes CRC32 of payload]
```

Multiple objects per file batched for efficient I/O. Format documented and stable for all minor releases of v1.

### Eviction policy

LRU among spillable objects. Pinned objects (currently arguments to executing tasks) cannot spill. Triggers automatically at 75 % memory pressure (configurable `RAYD_SPILL_THRESHOLD`). The raylet's `LocalObjectManager` orchestrates.

When refcount → 0 for a spilled object, the raylet deletes the spill file as part of `FreeObjects` fanout.

## Lineage reconstruction

Cross-cuts here, tasks, and refcount. Outline:

```rust
impl TaskManager {
    pub fn resubmit_task(&self, task_id: TaskId) -> Result<()> {
        let entry = self.tasks.read().get(&task_id).ok_or(NotFound)?;
        if entry.attempts_remaining == 0 {
            self.fail_task(task_id, ErrorCategory::ObjectUnreconstructable, "max attempts exhausted");
            return Err(LineageExhausted);
        }
        let mut spec = entry.spec.clone();
        spec.attempt_number += 1;
        // bump refcounts of input refs (which may have been GC'd in their owners)
        self.bump_input_refs(&spec).await?;
        self.submit(spec).await
    }
}
```

The lineage budget is a per-owner `LineageBudget` with explicit eviction:

```rust
pub struct LineageBudget {
    max_bytes: u64,
    current_bytes: u64,
    pinned_specs: lru::LruCache<TaskId, PinnedSpec>,
}
```

On budget pressure, evict oldest unpinned specs (LRU); the corresponding `OwnedRef` is downgraded to "lineage_evicted" and any future loss results in `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`.

## API summary (cross-cutting)

The new system's public store API in `rayd-core`:

```rust
impl CoreWorker {
    // Put
    pub async fn put(&self, value: PythonValue, owner: Option<Address>) -> Result<ObjectRef>;

    // Get with new failure semantics
    pub async fn get_settled(&self, refs: &[ObjectRef], timeout: Option<Duration>)
        -> Vec<RefResult>;
    pub async fn get_or_raise(&self, refs: &[ObjectRef], timeout: Option<Duration>)
        -> Result<Vec<PythonValue>, GetError>;

    // Cheap state inspection
    pub fn state(&self, r: &ObjectRef) -> RefState;
    pub fn state_batch(&self, refs: &[ObjectRef]) -> HashMap<ObjectRef, RefState>;
    pub fn peek_error(&self, r: &ObjectRef) -> Option<ErrorInfo>;

    // Wait
    pub async fn wait(&self, refs: &[ObjectRef], num_returns: usize,
                      timeout: Option<Duration>, fetch_local: bool)
        -> WaitOutcome;
    pub async fn wait_with_states(&self, refs: &[ObjectRef], timeout: Option<Duration>,
                                  fetch_local: bool)
        -> HashMap<ObjectRef, RefState>;

    pub fn free(&self, refs: &[ObjectRef]);
}
```

These map 1:1 to PyO3-exposed methods on the Python `CoreWorker` class — see `04-python-bindings.md`.
