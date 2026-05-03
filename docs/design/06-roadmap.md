# Design: Implementation Roadmap

Phased plan from empty repo to a v1 release that hosts a real workload. Each phase has concrete entry/exit criteria and is sized to be implementable.

## Phase 0 — Skeleton and harness (target: 1 week)

Goal: scaffold builds, lints, and CI before any feature work.

**Deliverables**
- Cargo workspace with all crate stubs (`rayd-core`, `rayd-plasma`, `rayd-gcs`, `rayd-raylet`, `rayd-py`, `rayd-cli`).
- `pyproject.toml` + `maturin` configured to build `rayd._native`.
- `proto/rayd.proto` with empty service definitions (just enough to compile).
- CI: Linux x86_64 + aarch64, macOS arm64, Windows amd64. Builds wheel, runs `cargo test`, `cargo clippy -D warnings`, `cargo fmt --check`, `mypy --strict`, `ruff check`, `pytest`, `stubtest`.
- `make stubs` target wired up; `pyo3-stub-gen` emits stubs for a single sample `#[pyfunction]`.
- `rayd.init()` and `rayd.shutdown()` are no-ops that start/stop a tokio runtime.
- Empty `ObjectRef` `#[pyclass]` with just `hex()` and `__repr__`.

**Exit**: `pip install -e .` succeeds in a fresh venv. CI green. `import rayd; rayd.init(); rayd.shutdown()` runs.

## Phase 1 — Single-process: tasks (target: 3 weeks)

Goal: tasks run in-process. No actors, no plasma, no cross-node. Just a worker pool of subprocess Python workers and a memory store.

**Deliverables**
- `MemoryStore` complete: `put`, `get_if_exists`, `wait`, `state_snapshot`, `delete`. Loom-tested.
- `Metadata` enum + protobuf encoding.
- `RayObject`, `ErrorPayload`, `ErrorCategory`.
- `WorkerPool` (`rayd-core/src/worker_pool.rs`) that spawns Python workers via `tokio::process::Command`.
- Python worker entrypoint (`python -m rayd._worker`): connects via UDS, receives task descriptors, executes them, writes results.
- `TaskManager` for retry/lineage tracking (lineage skeleton; full reconstruction in phase 4).
- `@rayd.remote` decorator: builds `RemoteFunction` Python class, dispatches `_remote()` through PyO3 to `CoreWorker::submit_task`.
- `rayd.put`, `rayd.get`, `rayd.get_settled`, `rayd.wait`, `rayd.state`, `rayd.wait_with_states`.
- `ObjectRef.state()`, `peek_error()`, `exception()`, `is_ready()`, `is_failed()`.
- `ErrorCategory.TASK_EXCEPTION` (every other category is stubbed out for now).
- pickle5 serialization with out-of-band buffers (small objects only — large objects defer to phase 2 with plasma).

**Exit criteria**:
- `pytest python/rayd/tests/test_tasks.py` passes:
  - `f.remote(x)` returns an `ObjectRef`; `rayd.get(ref) == f(x)`.
  - `rayd.get_settled([f.remote(i) for i in range(10)])` with one raising task returns 9 `Ok` and 1 `Err`.
  - `rayd.state([ref])` returns `PENDING` then `READY_LOCAL` then never `FAILED`-on-success.
- `mypy --strict`, `ruff check`, `cargo clippy -D warnings` clean.
- Sample throughput: 10k trivial tasks (`f = lambda: 1`) submitted/retrieved in under 5 seconds on a laptop.

## Phase 2 — Plasma object store (target: 4 weeks)

Goal: large objects stored in shared memory, zero-copy reads.

**Deliverables**
- `rayd-plasma` crate: server + client.
  - UDS listener with `prost`-encoded request/reply frames.
  - `SCM_RIGHTS` memfd handoff (using `nix::sys::socket::sendmsg`).
  - `bumpalo`-per-arena allocator over `memmap2` regions.
  - `Create`, `Seal`, `Get`, `Contains` (with metadata in reply!), `Release`, `Delete`, `List`.
- `PlasmaStoreProvider` in `rayd-core` that switches between memory and plasma based on size.
- `CoreWorker::seal_return` chooses inline vs. plasma based on `RAYD_INLINE_OBJECT_THRESHOLD_BYTES`.
- numpy zero-copy: pickle5 buffer callback writes large numpy arrays directly into a plasma allocation; deserialization mmap-reads them out.
- `cargo-test`: loom on the bump allocator's free-list. Property tests on the wire codec.
- `pytest` end-to-end test putting a 100 MB numpy array, getting it back, verifying byte-equality and that the Rust process used `mmap` (verifiable via `/proc/self/maps`).

**Exit criteria**:
- `rayd.put(np.zeros(10**8))` then `arr = rayd.get(ref)` round-trips at multi-GB/s on a single machine.
- Watching `/proc/<pid>/maps` shows the plasma region mapped into both the worker and the driver.
- All Phase 1 tests still pass.

## Phase 3 — Multi-node: GCS, raylet, cross-node transfer (target: 6 weeks)

Phase 3 is split into incremental sub-phases to keep each landing shippable.

### Phase 3.1 — Multi-process plasma + clean shutdown (✅ shipped)

Goal: enable a single plasma server to be shared by multiple driver
processes on one host, and fix the GIL-teardown noise from the per-task
thread spawn model.

**Deliverables**
- `crates/rayd-cli/` binary: `rayd version`, `rayd plasma-server <socket>`.
- `RAYD_PLASMA_SOCKET` env var → `rayd.init()` connects to an external
  plasma server instead of auto-spawning one.
- Managed thread pool (`crates/rayd-py/src/pool.rs`): replaces the
  per-task `std::thread::spawn` model. `rayd.shutdown()` releases the GIL
  and joins workers so finalize-time `PyGILState_Release` errors are gone.
- Multi-driver tests confirming two Python processes can independently
  put/get against the same shared plasma server.

### Phase 3.2 — Worker subprocess pool (✅ shipped)

Goal: tasks run in dedicated worker processes spawned from the driver,
giving real (non-GIL-bound) parallelism on a single host.

**Deliverables**
- `python -m rayd._worker` entry point: connects to the driver's UDS,
  receives task descriptors (cloudpickled callable + args + return ids),
  runs them, writes results to the shared plasma server.
- `crates/rayd-py/src/dispatcher.rs` — driver-side `Dispatcher` that
  spawns N (default 4) worker subprocesses, accepts their `worker_ready`
  greetings, and routes queued tasks to idle workers.
- `crates/rayd-py/src/wire.rs` — length-prefixed pickled-dict frames over
  UDS (driver ↔ worker dispatch protocol).
- `cloudpickle` runtime dependency for callables/args so closures and
  test-module functions round-trip across the process boundary.
- Phase 3.1's in-process `ThreadPool` is fully replaced; the dispatcher
  takes over task execution end-to-end.
- Tests in `python/rayd/tests/test_subprocess_pool.py` confirm:
  tasks run in distinct PIDs, 4 parallel sleeps finish in ~one sleep
  (real parallelism), exceptions propagate with their original Python
  type via cloudpickle, and partial-failure semantics still hold.

**Phase 3.2b (deferred) optimisation backlog**
- Inline-result fast path: small task results travel back through the
  dispatch socket instead of always going through plasma.
- Quiet-mode worker stderr (`RAYD_WORKER_QUIET=1`) for cleaner test output.
- Per-worker plasma client cached across tasks (rather than the worker's
  CoreWorker re-using a single client behind a Mutex).

### Phase 3.3 — GCS + node registration (✅ shipped)

**Phase 3.3a (✅ shipped):**
- `crates/rayd-gcs/`: tonic gRPC service `NodeRegistry` with
  `Register` / `Drain` / `List` / `Heartbeat` RPCs over a strongly-typed
  client/server pair. In-memory `HashMap<node_id, NodeInfo>` registry,
  16-byte server-assigned node ids, fresh `cluster_session_id` per
  startup so callers can detect a GCS bounce.
- `proto/node_info.proto` (`tonic-build` codegen via `build.rs`).
- `rayd gcs --bind <addr>` CLI subcommand: runs the server until SIGINT,
  with proper async shutdown.
- 3 integration tests: register/list/drain round-trip across two clients,
  drain-of-unknown-id returns NotFound, register-with-empty-host returns
  InvalidArgument.

**Phase 3.3b (✅ shipped this turn):**
- `JobRegistry` gRPC service alongside `NodeRegistry`: `AddJob`,
  `MarkJobFinished`, `List` (`proto/job_info.proto`). One server binary
  multiplexes both services on the same TCP port.
- Driver-side `GcsBinding` in `rayd-py`: when `RAYD_GCS_ADDRESS` is set,
  `rayd.init()` spins up a tokio runtime, registers as a node + adds a
  job linked to that node, and stores the assigned ids on the session.
  `rayd.shutdown()` calls `mark_job_finished` + `drain` before tearing
  the runtime down. Worker subprocesses inherit `RAYD_PLASMA_SOCKET` but
  NOT `RAYD_GCS_ADDRESS` — only the driver registers, not the pool.
- New Python surface: `rayd._native.list_nodes() / list_jobs()`,
  `node_id() / job_id() / cluster_session_id()`, and the
  `Resources` / `NodeInfo` / `JobInfo` pyclasses.
- `rayd start --head`: composes `gcs` + `plasma-server` in one process
  and prints the env vars a driver needs to attach. Phase 3.3b's
  stand-in for the eventual per-node `rayd-raylet`.
- 6 new pytest cases (`test_gcs.py`): driver registers node + job,
  shutdown drains node + finishes job, accessors return None when no
  GCS env, init fails on unreachable / malformed `RAYD_GCS_ADDRESS`.
- 2 new tonic tests in `rayd-gcs/tests/end_to_end.rs`:
  add/list/finish-job round-trip, empty-driver-host rejected.

**Phase 3.3c (✅ shipped this turn):**
- Server-side sweeper task that flips `Alive` nodes to `Dead` when their
  `last_heartbeat_unix_ms` is older than `heartbeat_timeout` (default
  10 s). `Draining` and already-`Dead` nodes are left alone — drain is a
  deliberate state and we don't second-guess it from the sweeper.
  `GcsServerConfig` exposes `heartbeat_timeout` and `sweep_interval`.
  CLI flag: `rayd gcs --heartbeat-timeout-ms <ms>` (0 disables expiry).
- Driver-side heartbeat task on the per-session tokio runtime: pings
  every `RAYD_HEARTBEAT_INTERVAL_MS` (default 2 s) until shutdown
  cancels it via oneshot. Errors are logged and swallowed —
  reconnect-on-Dead lands with raylet.
- 3 new tonic tests (missed-heartbeats-mark-dead, periodic-heartbeats-
  keep-alive, drain-not-resurrected-as-dead) plus 2 Python integration
  tests using `rayd gcs --heartbeat-timeout-ms` to exercise both paths
  without slow real-time waits.

**Deferred to Phase 3.4:**
- Internal pubsub (server-streaming for job/node lifecycle changes).
- `rayd start --address=<head>:<port>` (registers a node) — needs the
  per-node `rayd-raylet` binary.
- `RequestWorkerLease` RPC: caller's core_worker → local raylet → either
  a local lease or a spillback hint, then re-issue to the chosen remote
  raylet. Also needs `rayd-raylet`.
- Driver reconnect after being marked `Dead` (re-register + new
  heartbeat task on transient GCS partition).

### Phase 3.4 — Cross-node object transfer (in progress)

**Phase 3.4a (✅ shipped this turn) — raylet skeleton:**
- New `rayd-raylet` crate with `proto/object_transport.proto` defining
  the `ObjectTransport` service (Pull / Push / GetObjectLocations).
  Phase 3.4a serves the proto with `Unimplemented` stubs — Pull/Push
  streaming and the directory answer land in 3.4b/c.
- `Raylet::start(RayletConfig)` boots a raylet: TCP-binds the
  `ObjectTransport` server, registers with the GCS as a node, spawns
  a heartbeat task, and returns a `RayletHandle`. Graceful
  `shutdown()` cancels heartbeats, drains the node, and stops the
  gRPC server.
- `rayd start --address=<gcs-addr>` CLI subcommand: launches a raylet
  attached to an existing head's GCS. Mutually exclusive with
  `--head`. Exposed knobs: `--advertise-host`, `--raylet-bind`,
  `--plasma-socket`.
- 3 new crate tests (`crates/rayd-raylet/tests/end_to_end.rs`):
  raylet registers + heartbeats keep it Alive across multiple sweeper
  cycles + transitions to Draining on shutdown; ObjectTransport stubs
  return `Unimplemented`; two raylets share one GCS with distinct ids
  and the same cluster_session_id.
- 1 new Python integration test (`test_driver_sees_raylet_in_node_list`):
  a driver attached to a head GCS sees both itself and a separately-
  launched raylet in `list_nodes()`, all `alive`.

**Phase 3.4b (✅ shipped this turn) — Pull from local plasma + directory:**
- The raylet now opens a `PlasmaClient` against the configured socket
  at startup; `Raylet::start` fails fast (`Plasma(...)`) if the
  socket isn't reachable.
- New `RegisterObject` RPC plus `GetObjectLocations`: backed by an
  in-memory `ObjectDirectory` (`HashMap<ObjectId, HashSet<NodeId>>`,
  `parking_lot::Mutex`). Idempotent registration; empty list for
  unknown ids (no `NotFound` — locations of "I don't know" are an
  empty set, not an error).
- `Pull` streaming RPC: reads metadata + data out of the local
  plasma store on a blocking task (so the tonic worker thread isn't
  pinned on plasma I/O), then streams a single `ObjectMetadata`
  frame followed by 64 KiB `Data` frames. Plasma's `NotFound` /
  `NotSealed` map to gRPC `NotFound` / `FailedPrecondition`.
- `ObjectTransportClient` gains `pull()` (reassembles bytes,
  validates `data_size` matches the metadata header) and
  `register_object()`.
- `rayd start --address` now spins up its own plasma server (so a
  worker node is fully self-contained — `--plasma-capacity-mb`
  controls the arena size).
- 4 new tonic tests (`pull_round_trips_a_sealed_object`,
  `pull_unknown_object_returns_not_found`,
  `pull_handles_zero_byte_object`,
  `register_then_get_object_locations_round_trip`,
  `register_object_rejects_wrong_byte_lengths`) plus 3 directory
  unit tests.

**Phase 3.4c (✅ partially shipped this turn) — driver hosts a local raylet:**
- `rayd.init()` (with `RAYD_GCS_ADDRESS` set) now starts a `Raylet`
  in-process, replacing the driver's old direct `Register` +
  heartbeat. The driver's `NodeInfo` advertised to peers carries the
  raylet's actual host/port — peers can dial it for `Pull`.
- New pyfunctions exposing the raylet's wire surface:
  - `_native.local_raylet_address() -> (host, port) | None`
  - `_native.register_object(object_id, holder_node_id) -> None`
  - `_native.get_object_locations(object_id) -> list[bytes]`
  - `_native.pull_object(host, port, object_id) -> (metadata, data)`
- 3 new Python integration tests (`test_driver_node_info_carries_raylet_address`,
  `test_pull_object_round_trips_via_local_raylet`,
  `test_pull_object_unknown_id_returns_runtime_error`).

**Phase 3.4c (rest, ✅ shipped this turn) — auto-register + cross-process fetch:**
- `rayd.put()` now forces the plasma path when GCS is attached
  (regardless of size — bypasses the 100 KiB inline threshold so
  every put is fetchable cross-node) and auto-calls
  `register_object` at the local raylet.
- New `_native.fetch_object(object_id, owner_node_id)` pyfunction:
  resolves owner → list_nodes → owner's raylet addr →
  `GetObjectLocations` → picks a non-self holder (falls back to
  any) → resolves holder addr → `Pull` → `seal_value_to_plasma`
  (idempotent on `AlreadyExists`) → `RegisterObject` at the owner
  to record the new replica.
- Driver raylet now binds to `0.0.0.0:0` instead of `127.0.0.1:0`
  so peers reaching us via `gethostname()` (which resolves to
  `127.0.1.1` on Debian-style hosts, or to a LAN IP elsewhere)
  can dial through.
- 2 new Python integration tests:
  `test_fetch_object_self_round_trips` (single-process loop) and
  `test_fetch_object_pulls_from_peer_process` (two driver
  processes, one GCS — the second process pulls the first's bytes
  through the wire and verifies them).

**Phase 3.4d (✅ shipped this turn) — auto-fetch in `rayd.get` + ObjectRef pickling:**
- `ObjectRef` gains an `Option<[u8; 16]> owner_node_id` field (in
  rayd-core). `rayd.put()` stamps it from the local GCS binding;
  refs created without GCS leave it `None`.
- Pickle support across the wire: `ObjectId`, `Address`, and
  `ObjectRef` all implement `__reduce__`, so `pickle.dumps(ref)` →
  `pickle.loads(blob)` round-trips through stdlib pickle without
  a custom serializer (e.g. over `multiprocessing.Queue`).
- Python `rayd.get` (and `get_settled`) auto-call `fetch_object`
  on every ref whose `owner_node_id` differs from the local
  node id. Idempotent; no extra cost when the ref is already
  local.
- 2 new pytest cases: `test_object_ref_round_trips_through_pickle`
  (single-process pickle smoke) and
  `test_rayd_get_auto_fetches_remote_ref_through_pickle` (the
  pay-off — Process A puts → pickles → ships hex over stdout;
  Process B unpickles and calls `rayd.get(ref)` with no manual
  `fetch_object`; bytes round-trip).
- `tools/fix_stubs.py` gained a rule for nested bare `tuple` in
  generic args (so `__reduce__`'s `tuple[object, tuple]` becomes
  `tuple[object, tuple[object, ...]]` for mypy --strict).

**Phase 3.4 polish (✅ shipped this turn):**
- **Channel pooling** — `RayletConnPool` (`crates/rayd-py/src/raylet_pool.rs`)
  caches `addr → tonic::Channel` per session. `pull_from`,
  `register_object_at`, `get_object_locations_at` now reuse the
  same HTTP/2 multiplexer, turning the second-and-onwards call
  to a peer raylet from "TCP+H2 handshake" into "send a frame on
  an open stream". `ObjectTransportClient` exposes
  `from_channel(Channel)` and `build_channel(addr)` for callers
  that want to share a tonic Channel.
- **`ReadyRemote` state surface** — `ObjectRef.state()` now
  returns `ReadyRemote` for unfetched refs whose
  `owner_node_id` differs from the local node id. Surfaces the
  signal "we know the bytes exist on a peer raylet, just not
  here yet" without any RPC. After `rayd.get` fetches and seals
  locally, the state flips to `ReadyLocal`. 3 new pytest cases
  cover the transitions.
- **`Push` streaming RPC** — server-side handler in the raylet:
  reads a `Header` frame, accumulates `Data` frames into a
  buffer (validating `data_size`), then seals into local plasma
  on a `spawn_blocking` task (idempotent on `AlreadyExists`).
  Client wrapper (`ObjectTransportClient::push`) chunks the
  payload in 64 KiB frames over a `tokio::sync::mpsc` channel.
  3 new tonic tests: round-trip, idempotency on already-sealed,
  zero-byte object.

**Phase 3.4 polish-of-polish (✅ shipped this turn):**
- **Pool eviction on `Unavailable`** — `RayletConnPool::evict(addr)`
  drops the cached channel; `pull_from`/`get_object_locations_at`/
  `register_object_at`/`push_to` all classify their errors via
  `should_evict`, so a remote raylet restart mid-session no
  longer leaves a stale channel reused forever. Re-exported
  `tonic::Code as RpcCode` from rayd-raylet so the pool can
  match on `RpcCode::Unavailable` without pulling tonic into
  rayd-py's deps.
- **`_native.push_object(host, port, object_id, metadata, data)`**
  — Python surface for the Push RPC. Goes through the channel
  pool (with eviction). Idempotent. New
  `test_push_object_round_trips_via_local_raylet` covers the
  same-process push + pull roundtrip.
- **Zero-copy-ish Pull** — the server now slices the plasma
  mmap one chunk at a time instead of `data().to_vec()`-ing the
  whole object up front. The `ReadHandle` (with its
  `Arc<MmapMut>`) is moved into the streaming task and dropped
  on completion or early caller-side close. Per-chunk copies
  to owned `Vec<u8>` are still required for tonic's wire frames,
  but the upfront full-object allocation is gone — meaningful
  for big objects.

**Exit criteria for Phase 3.4 (whole phase):**
- 2-node cluster (separate processes; can run on same host) executes tasks across nodes.
- Test `test_cross_node_get`: put on node A, get on node B; bytes round-trip in under 100 ms for a 10 MB object.
- Test `test_state_remote_to_local`: ref starts `READY_REMOTE` on borrower; `state()` switches to `READY_LOCAL` after `get()`.

## Phase 4 — Ownership and lineage (in progress)

Goal: distributed reference counting works correctly under concurrent borrowers, owner death is observable, lineage reconstruction recovers lost objects.

### Phase 4.1 (✅ shipped this turn) — owner-side `RefCounter` data structure

- New `crates/rayd-core/src/ref_counter.rs`:
  - `OwnerEntry { local_count, borrowers: HashSet<WorkerId>, submit_dep_count }`.
  - `RefCounter` (a `parking_lot::Mutex<HashMap<ObjectId, OwnerEntry>>`)
    with `add_owned`, `inc_local`, `dec_local`, `add_borrower`,
    `remove_borrower`, `add_submit_dep`, `complete_submit_dep`,
    `snapshot`. `is_unpinned` flips only when *all three* counters
    are zero — borrowers and in-flight tasks both pin the object
    past local-count → 0.
- 10 unit tests covering the invariants: idempotent borrower add,
  unrelated-borrower drop is harmless, saturating-subtract on
  stray drops, three-counter unpin gating.

### Phase 4.2 (✅ shipped this turn) — wire `RefCounter` into `CoreWorker`

- `CoreWorker` owns an `Arc<RefCounter>`. `seal_value` /
  `seal_value_to_plasma` both call `add_owned` on success.
- New methods: `inc_local_ref(id)` for `ObjectRef::clone`,
  `dec_local_ref(id)` for `ObjectRef::drop`. The drop path frees
  the local store entry AND removes the object from plasma when
  the entry transitions to fully unpinned (no borrowers, no
  submit-deps).
- 5 new integration tests: seal registers an entry; dec frees
  inline + plasma objects; clone+inc requires N decs; borrower
  pin survives owner-side dec.

### Phase 4.3.1 (✅ shipped this turn) — Python `ObjectRef.__del__` → `dec_local_ref`

- `PyObjectRef` now implements `Drop` that calls
  `worker.dec_local_ref(id)`. Manual `Clone` sets `owns_count =
  false` so Rust-side clones (e.g. `extract_ref`'s
  `borrow().clone()`) don't double-decrement. Manual `Hash` /
  `Eq` ignore the flag — two refs to the same id still compare
  equal regardless of which one "owns the count".
- Drop is a no-op when the runtime isn't initialised (e.g. a ref
  outlives `rayd.shutdown()` because Python kept it alive).
- Two new pytest cases: dropping the last `ObjectRef` for a
  `put()` clears its plasma entry; Python aliases (`b = a`)
  share one underlying `PyObjectRef`, so the dec only fires
  when the last alias goes away.

### Phase 4.3.2 (✅ shipped this turn) — `WaitForRefRemoved` RPC + cross-process drop

- New RPC: `WaitForRefRemoved(object_id, node_id) → ()` on the
  raylet's `ObjectTransport` service. Borrower sends to the
  owner-raylet on its last `ObjectRef` drop.
- New `OwnerSink` trait in `rayd-raylet` (with `add_borrower` /
  `remove_borrower`). The raylet's `RegisterObject` and
  `WaitForRefRemoved` handlers call into the sink, so the
  driver-side `RefCounter` actually tracks peer borrowers and
  frees the local plasma when all pins clear.
- `WorkerOwnerSink` in `rayd-py` wraps the local `CoreWorker`'s
  `RefCounter`. `GcsBinding::connect_and_register` builds one
  during `rayd.init()` and hands it to the local `Raylet`.
- `PyObjectRef::Drop` was extended: when `owner_node_id` differs
  from local, send `WaitForRefRemoved` to the owner-raylet
  (via the channel pool, with eviction on `Unavailable`).
- New `ObjectDirectory::remove(oid, node_id)` method drops
  empty entries.
- New pytest case: producer A puts, consumer B fetches (now
  registered as a borrower), B drops the ref → producer's
  directory no longer lists B. Confirms the wire works
  end-to-end across processes.

### Phase 4.3.3a (✅ shipped this turn) — owner-self-deregister on local free

- New `FreeCallback = Arc<dyn Fn(ObjectId) + Send + Sync>` in
  rayd-core, installable via `CoreWorker::set_free_callback`.
  Invoked at the end of every successful `free_unpinned` (after
  the store + plasma cleanup).
- `RayletHandle::register_self(oid)` and `deregister_self(oid)`:
  direct directory access bypassing the gRPC + sink path. The
  driver's `put` now uses `register_self_local` instead of
  `register_object_local`, so self-registration no longer creates
  a self-pin in the owner's `RefCounter.borrowers`.
- `runtime::install` wires the callback to call
  `binding.deregister_self(oid)` after the GcsBinding is built,
  closing the loop: `dec_local_ref` → `free_unpinned` → store +
  plasma + directory all cleared.
- New `test_owner_self_deregisters_on_local_free` covers the
  in-process happy path; the existing cross-process
  `test_borrower_drop_notifies_owner_and_frees_object` was
  tightened from "consumer entry removed" to "directory fully
  empty after both drop" — proves the full Phase 4 lifecycle.

### Phase 4.3.3b (✅ shipped this turn) — surface OwnerDied via GCS liveness

- New Python exception `rayd.OwnerDiedError`. Raised by
  `rayd.get` (and `get_settled`) when a remote ref's owner is
  not `alive` in the GCS — covers the graceful-drain case
  (status `draining`, set by `rayd.shutdown`'s drain RPC) and
  the hard-crash case (`dead`, eventually flipped by the GCS
  sweeper after heartbeat timeout). Lazy lookup: only one
  `list_nodes()` call per `get` invocation, even with many
  remote refs.
- Crucially, this avoids the doomed Pull RPC to a gone owner —
  the borrower learns of the death via the GCS heartbeat
  signal that's already in place rather than a transport
  timeout. No new RPC needed; reuses the Phase 3.3c sweeper.
- New `test_get_raises_owner_died_when_owner_has_drained`:
  producer puts → exits cleanly (drains GCS) → consumer's
  `rayd.get` raises `OwnerDiedError` instead of hanging /
  failing with an opaque transport error.

### Phase 4.4 (✅ shipped this turn) — lineage reconstruction MVP

- New `TaskManager` (`crates/rayd-py/src/task_manager.rs`) keyed
  by output `ObjectId`. `submit_task` now stamps every output
  with the (cloudpickled callable, args, kwargs) blobs plus a
  retry budget (default 3). `try_resubmit` rebuilds the same
  `DispatchJob` with the same `task_id`, so the worker's seal
  lands at the original plasma id and any in-flight `ObjectRef`
  resolves on the second attempt.
- New `_native.try_resubmit_for_lineage(object_id) -> bool`
  Python hook: `True` when a resubmit fired, `False` when no
  record / budget exhausted.
- New `_native._evict_local(object_id)` test helper: drops both
  the local memory store entry AND the plasma object without
  touching the refcount or firing the free-callback. Lets
  tests simulate object loss between gets.
- `CoreWorker::evict_local` is the public sibling of the
  private `free_unpinned` — same store + plasma cleanup,
  bypasses callbacks/refcount.
- 4 new TaskManager unit tests + 3 Python integration tests:
  end-to-end reconstruction (put → get → evict → resubmit →
  get returns identical bytes); unknown-id returns False;
  retry-budget exhaustion.

### Phase 4.4b (✅ shipped this turn) — auto-resubmit + ObjectUnreconstructable

- `TaskRecord` gained a `was_completed: bool` flag set by the
  dispatcher's completion handler. `try_resubmit` now requires
  `was_completed && retries_remaining > 0`, AND resets
  `was_completed = false` on a successful resubmit so a
  concurrent caller (e.g. `rayd.get`'s auto path) sees the new
  attempt as in-flight rather than double-submitting it.
- New `LineageStatus` enum and `_native._lineage_status_str(oid)`
  pyfunction returning one of `not_recorded`, `not_yet_completed`,
  `ready`, `exhausted`.
- `Dispatcher` holds an `Option<Arc<TaskManager>>`; the completion
  handler fires `tasks.mark_completed(job.task_id)` after every
  successful seal.
- New Python exception `rayd.ObjectUnreconstructableError`. The
  `rayd.get` auto-resubmit path:
  - Local-Pending ref + status `ready` → `try_resubmit_for_lineage`
  - Local-Pending ref + status `exhausted` → raise
    `ObjectUnreconstructableError`
  - All other cases → fall through (`_native.get` blocks)
- 2 new pytest cases:
  - `test_rayd_get_auto_resubmits_lost_object`: no manual
    resubmit call, `rayd.get` transparently replays.
  - `test_rayd_get_raises_object_unreconstructable_after_budget`:
    drains the budget, asserts the typed exception.

### Phase 4.6 (✅ shipped this turn) — proptest for RefCounter invariants

- Added `proptest = "1"` as a dev-dep on rayd-core.
- New `ref_counter::proptests` module driving the counter with
  random sequences from a 4-id × 3-worker space:
  - `snapshotted_entry_is_never_unpinned`: every snapshot has
    at least one non-zero pin (else the counter would have
    removed it).
  - `agrees_with_reference_model`: replays the same op stream
    on a parallel `HashMap` model and asserts state matches
    after each op (presence, counter values, borrower set).
  - `extra_decs_after_sequence_never_panic`: regression-guards
    the saturating-sub on `dec_local` / `complete_submit_dep`.
- Each property runs proptest's default 256 randomized cases.

### Phase 4.3.3c (✅ infrastructure shipped this turn) — pubsub + Evict + loom

Three load-bearing pieces of the long-deferred 4.3.3c, plus the loom
tests that were called out next to it.

**WatchNodes streaming RPC** on the GCS:
- New `rpc WatchNodes(WatchNodesRequest) returns (stream NodeEvent)`
  on `NodeRegistry` (`crates/rayd-gcs/proto/node_info.proto`).
- Server: `tokio::sync::broadcast::Sender<NodeEvent>` plus a 1024-event
  ring buffer for resume-after-disconnect. Subscribers pass their
  highest-seen sequence; out-of-range gaps return `OUT_OF_RANGE` so
  the client falls back to a fresh snapshot. `last_seen=0` triggers
  a snapshot of currently-known nodes followed by the live tail.
- Publish hooks wired into `Register`, `Drain`, and the heartbeat
  sweeper (`Alive → Dead` flips). Heartbeat itself doesn't publish —
  no state change.
- Client: `GcsClient::watch_nodes(last_seen) -> tonic::Streaming<NodeEvent>`.
- 3 new integration tests in `crates/rayd-gcs/tests/end_to_end.rs`:
  snapshot-then-live, drain+dead transitions, resume-skips-stale.

**Raylet subscriber + NodeIndex** (`crates/rayd-raylet`):
- New `node_index.rs`: `RwLock<HashMap<NodeId, (NodeStatus, SocketAddr)>>`
  with a `StatusTransition` return on `apply_event` so callers can
  distinguish first-sight inserts from genuine `Alive→Dead` flips.
- New `watch_nodes.rs`: long-lived task that opens the stream, applies
  events to the index, and forwards Dead transitions to
  `OwnerSink::on_owner_died`. Reconnect-with-backoff (200 ms → 10 s)
  on transient errors; reset-resume on `OUT_OF_RANGE`; clean shutdown
  via `oneshot`.
- `Raylet::start` now spawns the subscriber alongside the existing
  heartbeat task; `RayletHandle::shutdown` joins it before the gRPC
  server teardown.
- New `OwnerSink::on_owner_died(node_id)` trait method with a default
  no-op impl — keeps the existing `WorkerOwnerSink` in rayd-py
  source-compatible while the borrower-side propagation lands later.

**Evict RPC infrastructure** (`crates/rayd-raylet`):
- New `rpc Evict(EvictRequest) returns (EvictReply)` on
  `ObjectTransport`. Server handler is idempotent (plasma delete +
  spill forget, both swallow `NotFound`).
- Client method `ObjectTransportClient::evict(object_ids)`.
- **Owner-side automatic fanout from `free_callback` is NOT wired.**
  Discovery during implementation: `OwnerEntry::is_unpinned`
  requires `borrowers.is_empty()`, so by the time the free hook
  fires, the borrower set is empty by construction — there's no
  one to fan out to. The Evict mechanism is in place for a future
  use case (force-evict on owner-death-from-borrower-side, or a
  lineage-driven force free) where the trigger has a non-empty
  target list. Roadmap entry adjusted accordingly.

**Loom tests for the borrower handshake** (`crates/rayd-core`):
- New cfg-gated `mod loom_tests` in `ref_counter.rs`. Compiles only
  with `RUSTFLAGS="--cfg loom"`; declared as an expected cfg in the
  workspace `[workspace.lints.rust]` to avoid false-positive warns.
- A parallel mini-counter built with `loom::sync::{Arc, Mutex}` and
  `loom::thread` — needed because loom can only see operations that
  go through its own primitives, and the real `RefCounter` uses
  `parking_lot::Mutex`.
- 3 invariants checked under exhaustive interleaving:
  - `loom_concurrent_dec_local_and_add_borrower_no_panic` —
    documents the resurrection race (dec frees, add_borrower
    recreates) and asserts the resulting state is well-formed.
  - `loom_concurrent_two_borrowers_independent` — two threads
    each add+remove their own borrower id, never lose either's
    contribution.
  - `loom_dec_and_remove_race_exactly_one_unpin` — under a race
    between `dec_local` and `remove_borrower(self)` clearing the
    last two pins, exactly one observes `freed=true`.
- Loom run completes in ~10 ms after the (one-time) build:
  `RUSTFLAGS="--cfg loom" cargo test --release loom_ -p rayd-core --lib`.

**Compared to my pre-implementation recommendation**: kept the pubsub
shape (single `WatchNodes` stream, raylet-level subscriber). Dropped
the "directed fanout via OwnerEntry.borrowers" — the borrower set is
empty at free time, so directed fanout has no targets, and
broadcast-to-live-peers from `NodeIndex` is the natural shape if the
trigger ever lands. The infrastructure is built such that swapping
in either trigger is local to the rayd-py free callback; no proto
change required.

**Phase 4.3.3c-E (follow-on this turn)** — wired the `WatchNodes`
infrastructure into the existing Phase 5.4e owner-liveness gate:
- New `RayletHandle::node_status(node_id) -> Option<NodeStatus>`,
  `GcsBinding::node_status`, and `_native.node_status_local`
  pyfunction. Reads the local `NodeIndex` directly — no RPC.
- Python `_resolve_remote_ref` now consults the fast path first;
  only falls back to the synchronous `list_nodes()` RPC when the
  cache hasn't observed `node_id` yet. At steady state, every
  `rayd.get(remote_ref)` skips the GCS round-trip entirely.
- All 23 `test_gcs.py` cases still pass — including
  `test_get_raises_owner_died_when_owner_has_drained` which
  exercises the gate end-to-end. Stubs regenerated; mypy/ruff clean.

**Phase 4.3.3c-F (follow-on this turn)** — observability for the new
pubsub + fast-path so a silent regression of either is detectable:
- `rayd_node_index_status_lookups_total{outcome="hit"|"miss"}` on the
  raylet (`IntCounterVec`). Both label values are pre-instantiated
  so a Prometheus hit-ratio query is well-defined before the first
  miss. Bumped from `RayletHandle::node_status`.
- `rayd_gcs_watch_events_published_total` on the GCS — incremented
  on every `Registry::publish` (Register, Drain, sweeper-driven
  Alive→Dead). Not bumped by Heartbeat (which doesn't change state
  and doesn't publish).
- 1 new test on each side: `raylet_metrics_record_node_status_lookup_outcomes`
  primes the cache, asserts both label values appear in `/metrics`;
  the existing GCS metrics test gained a `watch_events_published_total`
  assertion.
- `RayletHandle` now stores the `Metrics` bag (separate from the
  HTTP server handle) so `node_status` can bump without re-checking
  whether the endpoint is bound.

### Phase 4 still-TODO (deferred)

**Phase 4.3.3c (still pending)**:
- Borrower-side `OwnerDied` propagation — the `OwnerSink::on_owner_died`
  default impl is empty. Filling it in requires a borrower-side
  index of "objects I have whose owner is X," which doesn't exist
  in the current ref-counting model.
- `FreeObjects`/`Evict` trigger wiring — the RPC mechanism is built;
  a concrete use case (force-evict during owner-death recovery, or
  a lineage decision to drop a reconstructable object) needs to
  drive the fanout caller.

**Phase 4.4 — owner-death surfacing**:
- Borrower's pubsub stream errors out → mark all borrowed refs
  from that owner as `OwnerDied`.

**Phase 4.5 — lineage reconstruction**:
- `TaskManager::resubmit_task` triggers when `MemoryStore::get_if_exists`
  misses and no plasma copies remain. Bounded by `LineageBudget`.
- All `ErrorCategory` variants surface correctly; one integration
  test per category.

**Phase 4.6 — property + concurrency tests**:
- Loom-test the borrower handshake: every interleaving of (borrower
  drops local count, owner pulls, owner dies) is sound.
- Property test in `proptest`: random sequence of submissions,
  gets, drops, owner deaths; assert refcount invariants hold.

**Exit criteria**:
- `test_owner_death`: kill the owner mid-execution; borrowers' refs surface `OwnerDied` within 1 s.
- `test_lineage_reconstruction`: lose a plasma copy of an object; subsequent `get()` succeeds because the task is replayed.
- `test_max_retries_exhausted`: kill the worker `max_retries+1` times; ref surfaces `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED` (as `ObjectUnreconstructable`).
- Loom checks pass on the borrower handshake.

## Phase 5 — Actors (in progress)

Goal: stateful actors with method dispatch and restart.

### Phase 5.1 (✅ shipped this turn) — in-process actor MVP

- Two decorators, strictly typed: `@rayd.remote` for functions
  (`Callable[P, R] → RemoteFunction[P, R]`) and `@rayd.actor`
  for classes (`type[T] → ActorClass[T]`). Splitting them
  sidesteps the `type[T]` ↔ `Callable[..., T]` overload
  ambiguity that mypy can't always resolve. Note: mypy's
  class-decorator inference is itself limited (python/mypy#3135),
  so for fully-typed code the docstring recommends
  `MyActor = rayd.actor(_MyImpl)` over `@rayd.actor`.
- `ActorClass.remote(*args, **kwargs)` instantiates the class on
  a fresh daemon thread inside the driver and returns an
  `ActorHandle`. `handle.method_name.remote(*args)` queues a
  call onto that thread (FIFO via `queue.Queue`); the result
  is sealed into the local store via the existing
  `_native._worker_seal` shim.
- Method exceptions land as `Error` metadata so `rayd.get`
  re-raises the original exception in the caller.
- New `_native._mint_actor_result_ref` pyfunction allocates a
  fresh `ObjectRef` per method call (uses the same task-id
  allocator as `submit_task`, so actor results sit in the same
  deterministic id space).
- `ActorHandle.terminate(timeout=...)` drains and joins the
  worker thread; idempotent.
- 7 new pytest cases:
  - 1k sequential `Counter.increment()` calls preserve order
  - args + kwargs round-trip
  - method exceptions surface through `rayd.get`
  - per-instance state isolation
  - `ActorClass` / `ActorHandle` / `terminate` smoke tests

### Phase 5.2 (✅ shipped this turn) — per-actor worker subprocess

- `ActorClass.remote(*args)` now spawns a dedicated subprocess
  (`python -m rayd._actor_worker`) with its own UDS connection
  to the driver — no more in-driver thread. CPU-bound or
  GIL-grabbing actor methods don't block the driver loop.
- Wire protocol: `actor_ready` greeting (subprocess → driver)
  → `actor_spawn` (driver → subprocess; cloudpickled
  `(class, args, kwargs)`) → loop on `actor_call` /
  `actor_call_complete`. `actor_shutdown` ends the loop.
- Subprocess seals method results into shared plasma via the
  existing `_native._worker_seal` shim. New
  `_native._record_plasma_seal(oid, metadata, data_size)`
  pyfunction lets the driver register the matching
  `PlasmaIndex` entry on its side once it observes a
  completion frame.
- New `_native._plasma_socket_path()` exposes the active
  session's plasma path so the driver can spawn the actor
  subprocess pointing at the same store.
- cloudpickle is configured with `register_pickle_by_value`
  on the actor class's source module, so classes defined in
  pytest test modules (or REPL/notebook contexts) round-trip
  to the subprocess without needing the source on its
  `sys.path`.
- Reader thread on the driver consumes completion frames
  asynchronously; method calls never block the user's thread
  beyond the socket write.
- 2 new pytest cases:
  - `test_actor_runs_in_separate_process`: driver pid ≠
    `handle.pid`, and a method-reported `os.getpid()`
    matches `handle.pid`.
  - `test_distinct_actors_get_distinct_subprocesses`: two
    `.remote()` calls produce two subprocesses with
    different pids.

### Phase 5.3 (✅ shipped this turn) — restart + ActorDied surface

- `rayd.actor(cls, *, max_restarts=3)` now configures the
  per-actor restart budget. Default `3`. `max_restarts=0`
  disables restart so a single crash marks the actor dead.
- New `rayd.ActorDiedError` exception, exported from the
  package. Raised in two places:
  - `rayd.get(ref)` on a method whose subprocess died
    mid-call (the driver seals the in-flight `ObjectRef`
    with this error so callers don't hang).
  - `actor.method.remote(...)` after the restart budget is
    exhausted.
- `_ActorSubprocess` now tracks in-flight oids; when the
  reader thread sees socket EOF before `terminate()`, it
  enters the crash-handler path: seals every in-flight oid
  as `ActorDied`, reaps the dead subprocess, and either
  spawns a fresh one (same class + ctor args) or marks the
  actor dead.
- Two-lock design: `_state_lock` for in-flight tracking +
  terminated/dead flags, `_send_lock` for serializing
  outgoing frames. They never overlap — load testing with 1k
  rapid `submit()` calls demonstrated that putting send
  inside the state lock deadlocks under socket-buffer
  pressure (subprocess can't send completions back because
  reader needs the same lock).
- Old socket fds are closed eagerly when a reader thread
  exits, so pytest's `--strict` filterwarnings doesn't
  catch unraisable `ResourceWarning`s.
- 3 new pytest cases:
  - `test_actor_subprocess_crash_seals_in_flight_call_with_actor_died`
  - `test_actor_restarts_after_crash_when_budget_remains`
  - `test_actor_dies_when_max_restarts_exhausted`

### Phase 5.4a (✅ shipped this turn) — `ActorHandle` pickling within one driver

- Each `_ActorSubprocess` is now stamped with a 16-byte
  `actor_id` (uuid). A process-wide
  `dict[actor_id, _ActorSubprocess]` registry (with a lock)
  tracks live actors. Registration happens in
  `__init__`; deregistration happens in `terminate()` and
  when an actor exhausts its restart budget.
- `ActorHandle.__reduce__` emits
  `(rebuild_actor_handle, (actor_id,))`. Within the same
  driver process, `pickle.dumps(handle)` →
  `pickle.loads(blob)` returns a fresh handle that wraps the
  SAME live `_ActorSubprocess` — both handles share state.
  Cross-driver unpickling raises `LookupError` with a
  pointer to the planned 5.4b fix.
- 2 new pytest cases:
  - `test_actor_handle_round_trips_through_pickle`: bumps
    state on one handle, unpickles a twin, observes the
    same state, mutates through the twin, observes the
    update through the original.
  - `test_unpickling_handle_after_terminate_raises_lookup_error`:
    pickle a handle → terminate → unpickle → `LookupError`.

### Phase 5.4b (✅ shipped this turn) — GCS named-actor directory

- New `ActorRegistry` gRPC service in `rayd-gcs`:
  `RegisterActor`, `GetActor`, `UnregisterActor`, `List`.
  Name → `ActorInfo { name, actor_id, owner_node_id,
  owner_pid, registered_at_unix_ms }`. Names are unique;
  re-registering a taken name returns `AlreadyExists`.
  `UnregisterActor` rejects mismatched `actor_id` to
  prevent stale handles from clobbering a freshly-
  registered slot.
- `ActorClass.options(name="foo")` returns a copy whose
  `.remote(...)` reserves the name in the GCS *before*
  spawning the subprocess (so a clash surfaces
  synchronously, no orphaned subprocess). `terminate()`
  unregisters the name; same on the post-`max_restarts`
  dead-actor path.
- `rayd.get_actor(name)` looks the name up in the GCS,
  resolves the `actor_id` against the local
  `_ACTOR_REGISTRY`, and returns a fresh `ActorHandle`
  wrapping the same `_ActorSubprocess`. Cross-driver
  lookups (the `actor_id` isn't local) raise
  `RuntimeError` pointing at 5.4c.
- 7 new pytest cases in `test_actor_registry.py`:
  GCS-listing, same-driver round-trip, terminate frees
  the name, name reuse, duplicate-name rejection,
  unknown-name lookup, cross-driver clear error.

### Phase 5.4c (✅ shipped this turn) — cross-driver actor method calls

- Per-driver TCP listener
  (`python/rayd/_actor_rpc.py::_DriverActorRpcServer`) on
  `127.0.0.1:0`, started lazily by `rayd.init()` when GCS
  is attached, stopped by `rayd.shutdown()`. One server
  multiplexes for every owned actor; accept loop polls a
  short-timeout `accept` so it can wind down cleanly.
- Wire protocol over the existing length-framed transport:
  `actor_invoke {actor_id, method, args_blob, kwargs_blob,
  result_oid}` → `actor_invoke_ack` / `actor_invoke_reject
  {reason: "malformed" | "unknown_actor_id" | <exception
  str>}`.
- `_record_plasma_seal` now also registers the seal at
  the local raylet's directory when GCS is attached, so
  cross-node `Pull` resolves actor results.
- `_mint_actor_result_ref(owner_node_id=None)` stamps the
  ref's owner; the cross-driver caller passes the actor
  driver's node id so its `rayd.get` triggers the existing
  cross-node fetch path.
- ActorInfo proto extended with `driver_actor_host` /
  `driver_actor_port`; the owner driver advertises its
  RPC-listener address on `RegisterActor`.
- `rayd.get_actor(name)` now returns:
  - `ActorHandle` when the actor lives on the calling
    driver (same as before),
  - `_RemoteActorHandle` for cross-driver — the
    `.method.remote()` surface dials the owner driver's
    listener and produces a properly-stamped `ObjectRef`
    whose bytes the caller fetches via the existing
    cross-node path.
- `_ActorSubprocess.submit` was refactored into a public
  `dispatch_call_with_oid(method, args_blob, kwargs_blob,
  oid_bytes)` helper so both same-driver and cross-driver
  paths share the in-flight tracking + lock discipline.
- 11 new pytest cases:
  `test_actor_rpc.py` (lifecycle: no-GCS, address shape,
  shutdown, happy-path TCP round-trip, unknown actor_id,
  malformed frame, args+kwargs forwarding) +
  `test_actor_registry.py::test_cross_driver_get_actor_round_trips_via_rpc`
  (parent driver invokes a method on a child-driver-owned
  actor, fetches the result through the cross-node path).

### Phase 5.4d (✅ shipped this turn) — ActorHandle pickling + remote-actor failure modes

- `ActorHandle.__reduce__` and the symmetric
  `_RemoteActorHandle.__reduce__` now embed `(actor_id,
  owner_node_id, host, port)`. The shared
  `_rebuild_actor_handle` factory dispatches: registry hit
  → `ActorHandle`; foreign owner with a non-empty address
  → `_RemoteActorHandle`; otherwise `LookupError`. Same-
  driver pickle-after-terminate still raises
  `LookupError` (the dispatch distinguishes "stale local"
  from "remote" via local-vs-embedded `node_id`).
- `rayd.put(handle)` + `rayd.get(ref)` round-trips an
  `ActorHandle` through plasma — no special metadata type
  needed because the bytes serialize via `__reduce__`.
- Cross-driver pickle: pickled bytes ship to a peer
  driver (e.g. via `cloudpickle.dumps`) and rehydrate
  there as `_RemoteActorHandle`.
- Remote-actor crash mid-call surfaces `ActorDiedError`
  on the caller's `rayd.get`. The owner driver's reader
  thread seals the in-flight oid as ActorDied; the seal
  now also registers at the local raylet directory
  (`_worker_seal` extended to call `register_self_local`
  when GCS is attached), so the cross-node fetch path
  can locate the bytes.
- `_RemoteActorHandle._submit` maps owner replies of
  `actor_invoke_reject{reason="unknown_actor_id"}` to
  `ActorDiedError`, so a post-terminate / post-budget
  call from a peer driver raises the same exception
  user code already handles for same-driver crashes.
- `_resolve_remote_ref` retries `fetch_object` with
  short backoff when the owner-side seal hasn't landed
  yet (caller-side race between `actor_invoke_ack` and
  the owner's reader thread observing a crash). Other
  failures propagate immediately.
- 4 new pytest cases:
  `test_actor_handle_round_trips_through_put_get`,
  `test_actor_handle_pickle_rehydrates_remote_in_other_driver`,
  `test_remote_actor_crash_mid_call_raises_actor_died`,
  `test_remote_actor_terminate_then_invoke_raises_actor_died`.

### Phase 5.4e (✅ owner-died mapping shipped this turn)

- `_RemoteActorHandle._submit` wraps the `socket.create_connection`
  in `try/except OSError`. On failure, it consults the GCS via
  `list_nodes()` and — if the owner-driver's node status is
  `dead`/`draining`/absent — re-raises as `OwnerDiedError`
  chained from the original. Anything else (transient
  network error while the owner is still alive) propagates
  as the original `OSError`.
- `_spawn_gcs` test helper now accepts extra args so the
  GCS can be brought up with a short `--heartbeat-timeout-ms`
  for the kill test.
- 1 new test:
  `test_remote_actor_invoke_after_owner_killed_raises_owner_died`
  — spawns GCS with 1s heartbeat timeout, registers an actor in a
  child driver, SIGKILLs the child, polls the GCS until the dead
  node is marked, then dispatches a method through the cached
  remote handle and asserts `OwnerDiedError`. End-to-end runtime
  ~3s.

### Phase 5.4f (✅ closed this turn) — actor RPC transport unification

**Decision: keep the per-driver Python TCP listener.**

We considered migrating actor invocation onto a new
`InvokeActor` RPC on the raylet's gRPC `ObjectTransport`
so each node carries one transport instead of two. We
rejected the migration:

1. `_ActorSubprocess.dispatch_call_with_oid` lives in
   Python (cloudpickle dump, `_in_flight` set, per-actor
   UDS to the worker subprocess). A Rust gRPC handler
   would have to call back into Python via a thread/queue
   channel, so the dispatch surface doesn't actually
   move — it just renames.
2. Actor results MUST land on the owner driver's reader
   thread because that's where `_record_plasma_seal`
   registers the seal at the owner's local raylet
   directory. Without that registration, subsequent
   cross-node `Pull` requests fail. So the
   owner-driver-as-broker is intrinsic — bypassing it
   breaks the directory invariant.
3. Net wire-transport count goes 2 → 1, but internal-IPC
   goes 0 → 1 and the architectural duplication shifts
   location instead of disappearing.

Rationale pinned at the top of `python/rayd/_actor_rpc.py`
so future readers don't redo the analysis. Revisit only
if/when actor management itself moves into Rust — a much
larger project, since cloudpickle is Python-only.

Phase 5 is now fully shipped — no remaining TODOs.

(The original Phase 5 spec carried forward; tests below
are still the exit criteria.)

**Exit criteria**:
- `test_counter_actor`: 1k method calls in order; final state correct.
- `test_actor_restart`: kill actor mid-call; with `max_restarts=1, max_task_retries=1`, the call retries on the new instance.
- `test_named_actor`: `rayd.get_actor("foo")` from another driver finds and uses the actor.

### Phase 6 audit pass (✅ this turn) — punchlist applied

After Phase 6.7 a code review surfaced 5 items; all were
addressed in one pass. Notable: the new
`concurrent_re_spill_of_same_oid_is_safe` unit test
(`object_manager.rs`) caught a real concurrency hole —
`LocalFsBackend` was using a shared `<hex>.spill.tmp`
filename, so two threads racing to re-spill the same id
truncated each other's in-flight bytes before the
rename.

Fixes:
- `LocalFsBackend::spill` now stamps a process-wide
  atomic counter into the temp filename
  (`spill.tmp.<seq>`), so each writer's temp path is
  unique and the rename is the linearization point.
- `_RemoteActorHandle._submit` calls `sock.settimeout`
  after connect so a stuck owner-driver RPC thread
  surfaces as a timeout instead of an indefinite hang.
- Removed `CoreWorker::set_inline_threshold` (no-op stub
  with apologetic comment, zero callers).
- Clarified `_spill_object` docstring re. ambiguous
  `False` (covers re-spill / inline-only / race).
- Added a doc-comment to `recover_and_reseal` explaining
  the trust assumption (no re-read after `create_and_seal`).

### Phase 6.7 (✅ shipped this turn) — automatic spill-on-pressure

- New `SpillPolicy { budget_bytes, threshold }` in
  rayd-core, plus `DEFAULT_SPILL_BUDGET_BYTES` (1 GiB)
  and `DEFAULT_SPILL_THRESHOLD` (0.75) constants.
- `MemoryStore::plasma_entries()` snapshots all
  `StoredEntry::Plasma` entries as
  `Vec<(ObjectId, PlasmaIndex)>` so the spill policy
  can iterate without holding the store lock.
- `CoreWorker` gained `set_spill_policy(budget,
  threshold)` (clamps threshold to `(0, 1]`) and
  `maybe_spill_for_pressure() -> usize` which:
  walks plasma entries → sums `data_size` →
  if `total > budget * threshold`, spills via
  `spill_to_recoverer` until the estimated total
  drops back under. Returns the number of successful
  spills. Transient spill errors are swallowed; the
  next seal retries.
- `seal_value_to_plasma` calls
  `maybe_spill_for_pressure` after every successful
  seal — automatic eviction on pressure with no
  background thread needed.
- rayd-py reads `RAYD_SPILL_BUDGET_BYTES` and
  `RAYD_SPILL_THRESHOLD` env vars on init and applies
  the policy to `CoreWorker` BEFORE the recoverer is
  wired (so a too-aggressive setting can't fire
  before the recoverer is in place — the policy is a
  no-op without a recoverer).
- 2 new pytest cases:
  `test_seal_above_threshold_triggers_automatic_spill`
  uses budget=1, threshold=1.0 to force eviction on
  the very first put; the ref still resolves through
  restore-on-local-Get.
  `test_eviction_keeps_user_visible_refs_alive` puts
  10 lists under aggressive eviction and confirms each
  is individually readable.

### Phase 6.6 (✅ shipped this turn) — refcount-zero free hook cleans up spill files

- The free-callback wired in `runtime::install` now also
  calls `binding.object_manager().forget(oid)` after the
  raylet-directory `deregister_self`. Errors from
  `forget` log via `tracing::warn!` and don't stall the
  unpin flow. Both calls run inside the same `with_gcs`
  closure so they share the GCS-binding lookup.
- New diagnostic pyfunction `_native._is_spilled(oid) ->
  bool` so tests can observe the directory state. Returns
  `False` cleanly when no GCS is attached.
- 2 new pytest cases:
  `test_drop_last_ref_removes_spill_entry` puts an
  object, spills it, asserts it's spilled, drops the
  last `ObjectRef`, and asserts the spill record is
  gone. `test_drop_unspilled_ref_is_a_no_op_on_spill_directory`
  confirms the new `forget` step is idempotent for
  refs that were never spilled.

### Phase 6.5 (✅ shipped this turn) — restore-on-local-Get + Python spill helper

- New `crates/rayd-core/src/recovery.rs`: `ObjectRecoverer`
  trait + `RecoveredObject` + `RecoveryError`. Trait
  lives in rayd-core to avoid a dep cycle (rayd-raylet
  already imports rayd-core for ObjectId). Two methods:
  `recover(id) -> Option<RecoveredObject>` for the
  restore path, `store(id, metadata, data)` for the
  spill path.
- `LocalObjectManager` (rayd-raylet) now implements
  `ObjectRecoverer`. Wraps the existing `restore` /
  `spill` methods; preserves `Option<...>` semantics for
  "not present" rather than turning it into an error.
- `CoreWorker` gained `recoverer: Mutex<Option<Arc<dyn
  ObjectRecoverer>>>` + `set_recoverer(...)`. New
  `CoreError::Recovery { object_id, reason }` for
  terminal recovery failures.
- `CoreWorker::resolve_entry`: on plasma `NotFound` for
  a `StoredEntry::Plasma` index, consults the recoverer,
  reseals the bytes back into plasma (treats
  `AlreadyExists` as benign so concurrent restorers
  don't race-fail), and returns the resolved object.
  `Ok(None)` from the recoverer surfaces as a synthetic
  plasma `NotFound` with a clear message.
- New `CoreWorker::spill_to_recoverer(id) -> Result<bool,
  CoreError>`: reads bytes from plasma, hands to
  recoverer's `store` hook, deletes plasma copy. Local
  `MemoryStore` index entry preserved so the next
  resolve triggers recover-and-reseal.
- rayd-py wires the recoverer in `runtime::install`:
  after the GCS binding comes up, `worker.set_recoverer`
  receives the `Arc<dyn ObjectRecoverer>` view of the
  per-session `LocalObjectManager`.
- New pyfunction `_native._spill_object(object_id) ->
  bool`: drives the spill path. Returns `True` if a
  spill happened, `False` if the object wasn't in
  plasma. Test-only API for now; the
  spill-on-pressure policy (Phase 6.6) will use the
  same backing method.
- 4 new Python tests in `test_spill.py`: end-to-end
  `rayd.put → spill → rayd.get` round-trip;
  double-spill is idempotent; repeated `get` after
  spill works (proves reseal stuck); spill without GCS
  raises `RuntimeError("no recoverer registered")`.

### Phase 6.4 (✅ shipped this turn) — manager wired into driver glue + evict-and-restore integration test

- `crates/rayd-py/src/gcs.rs` constructs a default
  `LocalObjectManager` rooted in a `tempfile::TempDir`
  during `connect_and_register`, and passes it to
  `RayletConfig::object_manager`. Tempdir lifetime is
  tied to `GcsBinding` so spilled files are cleaned up
  on session shutdown. Errors from spill setup surface
  as `GcsBindingError::{Spill, SpillTempDir}`.
- New Rust integration test
  `pull_restores_after_evicting_from_plasma`: seals an
  object in plasma, reads its bytes, calls
  `manager.spill`, deletes from plasma (asserting the
  delete took effect), then issues a `Pull` and
  confirms the spill-aware handler restores the bytes
  AND reseals into plasma so subsequent direct plasma
  `get`s succeed.
- The `object_manager()` accessor on `GcsBinding` is
  marked `#[allow(dead_code)]` until the Python-side
  helpers (Phase 6.5) start driving it.

### Phase 6.3 (✅ shipped this turn) — spill-aware `Pull`

- `RayletConfig` gains an optional
  `object_manager: Option<Arc<LocalObjectManager>>`
  field. `None` keeps pre-Phase-6 behavior — the raylet
  surfaces plasma-miss as `Status::not_found` exactly as
  before.
- `Raylet::start` threads the manager through to
  `ObjectTransportService`.
- New `plasma_get_with_restore` helper in `service.rs`
  centralizes the read path: try plasma → on `NotFound`
  consult the manager → restore from backend → seal back
  into plasma (`AlreadyExists` is benign — somebody
  raced) → re-open the read handle. Errors during the
  spill restore surface as `Status::internal`; an absent
  spill entry stays `Status::not_found`.
- 2 new integration tests:
  `pull_restores_spilled_object_when_plasma_misses`
  pre-spills 200 KB through the manager (never touches
  plasma) and confirms `Pull` round-trips, plus a second
  `Pull` to verify the reseal is idempotent;
  `pull_unknown_object_with_spill_manager_still_returns_not_found`
  asserts the configured-but-empty manager doesn't muddy
  the not-found path.
- The driver-side glue (`crates/rayd-py/src/gcs.rs`)
  builds a `RayletConfig` with `object_manager: None`
  for now — the spill manager is observable only when
  callers opt in. Wiring it on by default is Phase 6.4.

### Phase 6.2 (✅ shipped this turn) — `LocalObjectManager` skeleton

- New `crates/rayd-raylet/src/object_manager.rs`:
  `LocalObjectManager` owns an `Arc<dyn SpillBackend>` and
  a `Mutex<HashMap<ObjectIdBytes, SpillUrl>>` directory.
  Lock is short-lived — backend I/O runs *outside* the
  lock so a slow disk doesn't stall lookup.
- API: `spill(oid, metadata, data) -> SpillUrl`,
  `restore(oid) -> Option<RestoredObject>` (None when not
  spilled, errors propagate, self-heals stale entries on
  `NotFound`/`Corrupt`), `forget(oid)` (idempotent),
  plus `is_spilled` / `spill_url` / `spilled_count`
  read-only helpers.
- 8 unit tests: empty manager, spill/restore round-trip,
  unknown-oid restore, forget drops directory + backend,
  forget on unknown is no-op, re-spill overwrites,
  multi-object independence, self-healing on stale
  `NotFound`.
- Re-exported from `rayd_raylet::LocalObjectManager` for
  the next chunk's wiring.

### Phase 6.1 (✅ shipped this turn) — `SpillBackend` + `LocalFsBackend`

- New `crates/rayd-raylet/src/spill/` module:
  - `SpillBackend` sync trait: `spill(object_id, metadata,
    data) -> SpillUrl`, `restore(url) -> RestoredObject`,
    `remove(url)`. Caller wraps in `spawn_blocking` from
    async context. No `async_trait` dep introduced.
  - `LocalFsBackend` impl: one file per object under a base
    directory, length-prefixed
    `[u32 metadata_len][metadata][u64 data_len][data]`
    layout, atomic write-then-rename via `.spill.tmp`,
    canonicalized root, path-traversal guard on the url.
- 9 unit tests: round-trip, empty payloads, re-spill
  overwrite, idempotent remove, `NotFound` on missing
  url, path-traversal rejection, truncation detection,
  persistence across reopen.
- Re-exported from `rayd_raylet` so the next phase
  (`LocalObjectManager`) can plug it in without crossing
  another module boundary.

## Phase 6 — Spilling and resource limits (target: 3 weeks)

Goal: object store fills up gracefully; spilled objects can be retrieved.

**Deliverables**
- `LocalObjectManager` in `rayd-raylet`: spilling orchestration.
- `LocalFsBackend` impl of `SpillBackend`.
- Spill-on-pressure (`RAYD_SPILL_THRESHOLD`, default 0.75).
- Restore on `Get`: raylet detects spilled URL, restores into plasma.
- `FreeObjects` deletes spilled files when refcount → 0.
- (Optional, behind `feature = "s3"`) `S3Backend`.

**Exit criteria**:
- `test_spill_and_restore`: pin enough objects to exceed plasma capacity; verify some are spilled, then `get()` restores them transparently.
- `test_spill_delete_on_gc`: spilled object's distributed refcount → 0; spill file is deleted within 5 s.

### Phase 7.4d (✅ shipped this turn) — driver-side `/metrics`

- New `crates/rayd-py/src/driver_metrics.rs`. Same `prometheus
  + axum` shape as the GCS / raylet, but hosted by the rayd
  Python driver. Five `IntCounter` handles
  (`rayd_driver_tasks_submitted_total`,
  `rayd_driver_tasks_completed_total`,
  `rayd_driver_tasks_failed_total`,
  `rayd_driver_puts_total`,
  `rayd_driver_gets_total`) plus one custom-collector gauge
  `rayd_driver_refs_alive` that reads `RefCounter::len()` at
  scrape time so ref-drop call sites stay free of metric
  bookkeeping.
- Bumpers wired in `lib.rs` (put/get/get_settled/submit_task)
  and `dispatcher.rs::handle_completion` (branches on
  `metadata.is_error()` for completed-vs-failed).
- Process-global `RwLock<Option<Arc<DriverMetrics>>>` (not
  `OnceLock`) so `rayd.shutdown() → rayd.init()` reinstalls a
  fresh registry. With `OnceLock` the second init's HTTP
  server would scrape an empty registry while bumps orphaned
  into the first session's stale counters — surfaced
  immediately by the test suite.
- Dedicated single-thread tokio runtime owned by
  `MetricsServerHandle`. Independent of GCS — the endpoint
  comes up whether or not `RAYD_GCS_ADDRESS` is set.
- Worker subprocesses `env_remove("RAYD_METRICS_BIND")` at
  spawn time so they don't all race to bind the same port.
- New `RAYD_METRICS_BIND=host:port` env var (unset = off).
  Bound address logged at INFO so users who pass `:0` can
  discover the actual port.
- 7 new Python smoke tests in `test_driver_metrics.py`:
  unset-disables-server, all-six-metrics-registered, puts
  bump, gets bump, refs_alive tracks live RefCounter (incl.
  return-to-baseline after `del`), tasks submitted+completed
  on real tasks, tasks_failed on `RuntimeError`.
- README env-var table + Observability bullet updated.

### Phase 7.7 (✅ shipped this turn) — single-node reference benchmarks

- New `python/rayd/benches/` package (no pytest collection
  via the existing `testpaths` config). Three end-to-end
  scripts + an in-house stats helper:
  - `bench_task_throughput.py` — submit M=10 000 trivial
    tasks, measure tasks/sec across 3 runs.
  - `bench_task_latency.py` — submit-and-get one trivial
    task at a time × 1 000 iterations after 50-iteration
    warmup; report p50/p95/p99/min/max.
  - `bench_put_get.py` — `rayd.put` + `rayd.get` for sizes
    100 B → 10 MB (powers of 10), 5 iterations each, refs
    dropped between iterations so the 128 MiB plasma arena
    doesn't fragment.
  - `_stats.py` — sorted-list percentile, mean, stddev. No
    scipy/numpy dep so benchmarks run on a bare
    `pip install rayd` install.
- New `make bench` target wires the three together.
- New per-file ruff ignore for `python/rayd/benches/*.py`
  (`T201` print is the CLI surface; `PLR2004` magic
  numbers like 1024/4 are obvious; `S311` pseudo-random
  bytes are not crypto).

Reference numbers from a current dev laptop (single
process, default 4 dispatcher workers):
- Task latency: p50 ≈ 1.2 ms, p99 ≈ 1.9 ms.
- Task throughput: ~3 800 tasks/sec end-to-end.
- Put/get bandwidth peaks ~2 GB/s at 10 KB; ~1 GB/s
  at 100 KB; ~600 MB/s at 1 MB; ~700 MB/s at 10 MB.

Multi-node and qualification-gate benchmarks deferred —
they're the natural next category, but require either
an actual multi-host setup or careful subprocess
choreography that's its own iteration's work.

### Phase 7.4c — plasma `/metrics` (✅ shipped this turn)

- New `crates/rayd-plasma/src/metrics.rs`: same six-handle
  `Metrics` shape as the GCS / raylet, but the HTTP server
  is a hand-rolled std::thread + std::net::TcpListener
  responder rather than axum. Plasma stays free of the
  tokio dep tree — its accept loop is sync UDS, and the
  scrape endpoint is a 60-LOC HTTP/1.0 responder that
  parses just `GET /metrics`.
- Six metrics:
  `rayd_plasma_arena_bytes_total` (gauge, set once),
  `rayd_plasma_arena_bytes_used` (gauge, refreshed on
  create/delete), `rayd_plasma_objects_total` (gauge),
  `rayd_plasma_create_total` (counter),
  `rayd_plasma_get_total` (counter),
  `rayd_plasma_delete_total` (counter).
- New `PlasmaServer::start_with_metrics(socket, capacity,
  Option<SocketAddr>)`. The original `start(...)` thin-
  forwards with `metrics_bind: None` so existing callers
  are unaffected.
- New `PlasmaError::Metrics(_)` variant + From impl.
- New `rayd plasma-server --metrics-bind=...` CLI flag.
  Startup line surfaces the metrics URL when enabled.
- 1 new unit test
  `metrics_endpoint_counts_create_get_delete`: drives 2
  `create_and_seal` + 1 `get` + 1 `delete`, scrapes /metrics
  via raw `TcpStream` HTTP/1.0 request, asserts every
  counter and the surviving-objects gauge.

Driver-side `/metrics` shipped as 7.4d in a follow-up
iteration; the tokio runtime + multi-init ordering it
needs were enough extra surface to deserve their own
phase.

### Phase 7.4b (✅ shipped this turn) — Prometheus `/metrics` on the raylet

- New `crates/rayd-raylet/src/metrics.rs` mirrors the
  GCS pattern: `Metrics` bag of six handles on a private
  `prometheus::Registry`, axum scraper on a configurable
  bind addr.
- Six metrics: `rayd_raylet_pull_total`,
  `rayd_raylet_push_total`,
  `rayd_raylet_register_object_total`,
  `rayd_raylet_get_object_locations_total`,
  `rayd_raylet_spill_restore_total`,
  `rayd_raylet_directory_entries` (gauge).
- `RayletConfig.metrics_bind: Option<SocketAddr>` (None
  is the default, disables both counters and bind).
  `RayletHandle::metrics_addr()` accessor reports the
  bound port.
- Counters bumped from `pull` / `push` / `register_object`
  / `get_object_locations` handlers; `spill_restore_total`
  bumps inside `plasma_get_with_restore` whenever the
  manager actually returns bytes; `directory_entries`
  gauge re-set after every `RegisterObject` (using the
  new `ObjectDirectory::len()`).
- 1 new integration test: drives 2 RegisterObject + 1
  Pull, scrapes /metrics, asserts the counter and gauge
  values match.

Defers: plasma server `/metrics` (its only "interesting"
state today is the arena byte counter, which the existing
plasma protocol doesn't expose to the raylet), and
driver-side metrics (no HTTP server in rayd-py yet —
would need to add one and route Prometheus through it).

### Phase 7.5 (✅ shipped this turn) — Health RPC on GCS and raylet

- New workspace dep `tonic-health = "0.12"`. Adds the
  standard `grpc.health.v1.Health` service to both
  components.
- GCS server marks each per-service slot
  (`rayd.gcs.node_info.v1.NodeRegistry`,
  `rayd.gcs.job_info.v1.JobRegistry`,
  `rayd.gcs.actor_info.v1.ActorRegistry`) plus the
  empty-key overall slot as `SERVING` for the duration
  of the server's lifetime. Reporter is dropped after
  setup; the service continues reporting the last set
  status until the gRPC server itself shuts down.
- Raylet does the same for its `ObjectTransport`
  service.
- 2 new integration tests: `health_check_reports_serving`
  (GCS) and `raylet_health_check_reports_serving`
  (raylet) connect a `tonic_health::pb::health_client::
  HealthClient` and assert the `Check` RPC returns
  `SERVING` for both the empty-key overall slot and a
  named per-service slot.

Use cases unlocked: k8s liveness/readiness probes,
load-balancer health checks, cluster-wide readiness
gating before sending real RPCs.

### Phase 7.4 (✅ shipped this turn) — Prometheus `/metrics` on the GCS

- New `crates/rayd-gcs/src/metrics.rs`:
  `Metrics` bag of `IntCounter`/`IntGauge` handles wired
  on a private `prometheus::Registry` so we don't clash
  with anything else in the host process. Six handles
  for V1: `rayd_gcs_register_node_total`,
  `rayd_gcs_heartbeat_received_total`,
  `rayd_gcs_nodes_alive`, `rayd_gcs_nodes_total`,
  `rayd_gcs_jobs_running`, `rayd_gcs_actors_total`.
- `MetricsServerHandle` runs an axum app on a
  configurable bind addr serving `/metrics` via
  `prometheus::TextEncoder`. Graceful shutdown on
  drop or explicit `.shutdown().await`.
- `GcsServerConfig` gains `metrics_bind: Option<SocketAddr>`.
  When `None`, no counters update and no port binds.
  When `Some`, all three RPC services receive an
  `Option<Metrics>` and bump counters/gauges on
  `Register`/`Drain`/`Heartbeat`/`AddJob`/
  `MarkJobFinished`/`RegisterActor`/`UnregisterActor`.
  The sweeper decrements `nodes_alive` for every node
  it flips from `Alive` to `Dead`.
- New `rayd gcs --metrics-bind=127.0.0.1:9100` CLI flag.
- New `MetricsStartError` propagated through
  `GcsServerStartError::Metrics(_)`.
- 1 new integration test
  `metrics_endpoint_serves_register_and_heartbeat_counters`:
  spawns GCS with metrics on, registers a node,
  sends 3 heartbeats, scrapes `/metrics` via a raw
  `tokio::net::TcpStream` HTTP request, asserts
  `register_node_total 1`, `heartbeat_received_total 3`,
  `nodes_alive 1`, `nodes_total 1` are present in the
  text-format response.

Defers: raylet/plasma `/metrics` endpoints (7.4b),
driver-side metrics (7.4c). Same architectural shape
will apply — concrete metric types, axum scraper, opt-in
bind addr.

### Phase 7.3 (✅ shipped this turn) — Python `logging` bridge

- New `rayd_core::EventHandler` trait + global
  `OnceLock<Arc<dyn EventHandler>>` slot. `set_event_handler`
  registers an impl; first call wins.
- `init_default_subscriber`'s registry now always includes a
  `DispatchLayer` that, on each event, looks up the global
  handler and forwards a `(level, target, message)` tuple.
  Cheap when no handler is set — just a `OnceLock::get`.
- New `crates/rayd-py/src/python_log.rs::PythonLogBridge`:
  impls `EventHandler`. On each event acquires the GIL via
  `Python::attach`, looks up `logging.getLogger("rayd")`
  (cached in a `OnceLock<Py<PyAny>>` so subsequent events
  skip the import), and calls `logger.log(level, msg)`.
  Level mapping: TRACE/DEBUG → 10, INFO → 20, WARN → 30,
  ERROR → 40.
- Off by default. `register_if_enabled()` checks
  `RAYD_LOG_FORWARD=1` and registers the bridge BEFORE
  `init_default_subscriber` so the very first event flows
  through it.
- `runtime::install` emits a deterministic `tracing::info!(...)`
  "rayd-py: init complete" event right after subscriber
  install — useful as both an init-success diagnostic and a
  reliable test anchor (events from before the deferred init
  fall on the floor by design).
- 2 new pytest cases:
  `test_log_forward_default_off_does_not_emit_to_python_logging`
  asserts the Python logger receives nothing when the env var
  is unset; `test_log_forward_enabled_routes_rust_events_to_python_logging`
  asserts at least one INFO+ record arrives at
  `logging.getLogger("rayd")` when the bridge is on.

### Phase 7.2b (✅ shipped this turn) — OTLP wired in rayd-py

- `runtime::install` now defers the
  `init_default_subscriber` call until after the GCS
  binding is up, so the install runs inside the
  binding's tokio runtime context (via
  `binding.runtime_handle().enter()`). The OTLP
  exporter's tonic gRPC client now constructs cleanly.
  Trade-off: very-early plasma + dispatcher tracing
  events are dropped, since no subscriber is installed
  yet — accepted because the load-bearing events all
  fire after init.
- `GcsBinding::runtime_handle()` accessor added to
  expose the per-session runtime to the init helper.
- Worker subprocesses (`_worker.py`,
  `_actor_worker.py`) clear `OTEL_EXPORTER_OTLP_ENDPOINT`
  before calling `rayd.init()` so they don't log a
  spurious "no runtime" warning on every spawn.
  Workers don't need their own exporter — they emit
  through driver-side call sites.
- Verified end-to-end: with GCS attached and
  `OTEL_EXPORTER_OTLP_ENDPOINT` set, no fallback warning
  surfaces in either driver or workers.

### Phase 7.2 (✅ shipped this turn) — OTLP span exporter

- `init_default_subscriber` refactored to use the
  `Registry + Layer` pattern (always installs a stderr
  fmt layer; conditionally adds OTLP).
- New `otlp` Cargo feature on rayd-core, default-on.
  Pulls in `opentelemetry`, `opentelemetry_sdk`,
  `opentelemetry-otlp` (with `grpc-tonic`), and
  `tracing-opentelemetry`. `--no-default-features`
  produces a slim build without the OTel dep tree.
- OTLP layer gates on `OTEL_EXPORTER_OTLP_ENDPOINT` at
  runtime. Service name from `OTEL_SERVICE_NAME`
  (default `rayd`). Global tracer provider installed so
  library code can also `opentelemetry::global::tracer(...)`.
- Graceful fallback when no tokio runtime is in scope:
  the tonic-based OTLP exporter requires a current
  runtime, so we detect via `Handle::try_current()` and
  log a one-line diagnostic instead of panicking deep
  inside hyper. The fmt layer still comes up.
- rayd-cli's subscriber init moved from `main()` into
  each subcommand's `runtime.block_on` so OTLP can
  attach in practice on the long-running services
  (`gcs`, `start --head`, `start --address=...`). The
  `version` subcommand has no init (nothing to log);
  `plasma-server` initializes outside any runtime so
  OTLP falls back gracefully there.
- Verified end-to-end: with `OTEL_EXPORTER_OTLP_ENDPOINT`
  set, the exporter constructs cleanly inside
  `tokio.block_on`. Without it, no warning, just stderr
  output as before.

### Phase 7.1 (✅ shipped this turn) — tracing subscriber init

- New `crates/rayd-core/src/log.rs::init_default_subscriber()`:
  installs a `tracing_subscriber::fmt` formatter that
  writes structured events to stderr. Filter from
  `RAYD_LOG` env var (RUST_LOG syntax); default
  `rayd=info,warn`. Idempotent via `OnceLock` so worker
  subprocesses re-entering `rayd.init()` see a no-op.
- Wired into `rayd-py::runtime::install` (before plasma
  setup so failures during init are visible) and
  `rayd-cli::main` (before `clap::parse` so a flag
  panic still surfaces).
- Existing `tracing::info!`/`warn!` call sites across
  rayd-core, rayd-gcs, rayd-raylet, rayd-plasma, rayd-py
  are now user-visible. Sample output:
  `INFO rayd_gcs::server: rayd-gcs: NodeRegistry +
  JobRegistry + ActorRegistry listening
  local_addr=127.0.0.1:44563 heartbeat_timeout_ms=10000`.
- 1 new unit test: `init_is_idempotent` confirms the
  subscriber install is replay-safe.

Defers: OTLP exporter (7.2), Python `logging` bridge
(7.3), Prometheus `/metrics` endpoint (7.4).

## Phase 7 — Production polish (target: 4 weeks)

Goal: ready to host a real workload at modest scale (10s of nodes, 10s of thousands of tasks per second).

**Deliverables**
- `tracing` + `tracing-opentelemetry` exports to OTLP.
- Bridge to Python `logging` (off by default; on via `RAYD_LOG_FORWARD=1`).
- Prometheus `/metrics` endpoint on each component.
- Backpressure on task submission rate.
- Health checks: each component exposes a `Health` RPC.
- Documentation (user-facing): tutorials for tasks, actors, the new state API.
- Reference benchmarks in `benches/`: throughput, latency, memory, end-to-end-failure-recovery time.

**Exit criteria**:
- 10-node cluster runs a workload of 100k tasks across 1 hour; metrics show stable memory, no leaks; one node killed mid-run, work continues with `< 5 %` failure rate.
- All design-doc examples (Patterns A–D in `05-state-and-error-api.md`) work as documented.
- README.md and `docs/` are publishable; a new user can go from `pip install rayd` to a working hello-world in 5 minutes.

## Risks and gating decisions

| Decision point | When | Trigger |
|---|---|---|
| Switch from `bumpalo` to slab allocator | After Phase 6 | Production workload shows >30 % fragmentation |
| Add GCS HA via Redis | Post-v1 | Real users complain about cluster restarts on GCS crashes |
| Add free-threaded CPython support | Post-v1 | PyO3 stabilizes the API; benchmarks show GIL is bottleneck |
| Add streaming generators | Post-v1 | At least 3 user requests for it |
| Re-add `arrow-flight` for cross-node transfer | Post-v1 | Profiling shows our chunked gRPC is bandwidth-bound |
| Drop direct-call optimization | Never | This is core to perf; if we hit issues we fix them |

## Out-of-scope (no plan to implement, ever)

- Full Ray feature parity. We're a focused subset.
- Migration tools from Ray clusters. Different protocols, different semantics.
- Cross-language workers (Java, C++).
- Workflows / Tune / Train / Serve / Data / RLlib equivalents.
- A web dashboard.
- Built-in autoscaling. Operator-managed cluster size.

## Definition of done for v1

- All phases 0–7 complete.
- All hard constraints met: `cargo clippy -D warnings`, `cargo fmt --check`, `mypy --strict` everywhere, `ruff check` everywhere, `stubtest` clean, no `Any` / `object` / `cast` / `# type: ignore`.
- Documented Python API stable; the new state/error inspection API is the headline feature.
- A real internal workload at DeepL runs on it for a sustained period (the user-facing acceptance test).
