# Design: Overview

Read after `../analysis/`. This document fixes scope, principles, and structure for the Rust+PyO3 reimplementation.

## Working name

`rayd` (placeholder; the project is `ray-replacement` and the import path is open).

## Goals (in priority order)

1. **Correctness on the failure-handling pain point.** The public API exposes per-`ObjectRef` state and error inspection without forcing deserialization. `ray.get(list)` analog returns `list[Result[T, RefError]]` and never raises on partial failure.
2. **End-to-end strict typing.** No `Any`, no `object`, no `cast`, no `# type: ignore` outside generated `.pyi` files. `mypy --strict` is clean. `ruff check` with all rules is clean. The Rust side passes `cargo clippy -- -D warnings`.
3. **Architectural fidelity to Ray's ownership model.** Decentralized refcount, lineage reconstruction, owner-based directory. Same essential design as Ray Core (because it's correct), just better implemented.
4. **Zero-copy where it matters.** Shared-memory object store with mmap + `SCM_RIGHTS` fd-passing for local workers. `numpy` and `bytes` arguments round-trip without an extra copy.
5. **A small, sharp implementation.** Aim for a few thousand lines of Rust + a thin Python facade. Resist the urge to reproduce every feature of Ray.

## Non-goals (v1)

- Cross-language workers (Java, C++ user code).
- Streaming generators (`num_returns="streaming"`, `ObjectRefGenerator`).
- Runtime env materialization (pip/conda installation per worker). Assume homogeneous environment.
- Placement groups in full generality. v1 supports basic CPU/GPU/memory + custom named resources via the scheduling RPC; group bundling is post-v1.
- Autoscaling. Cluster size is operator-managed.
- A web dashboard. Prometheus/OpenTelemetry export is enough.
- GCS HA via Redis. v1 has a single GCS process per cluster; failure means cluster restart.
- Workflow / DAG-level constructs (Ray Workflows, Ray Tune, Ray Train, Ray Serve, Ray Data, RLlib).

## Design principles

- **Typed by construction.** Every Python-visible function has a typed signature. Every Rust ↔ Python boundary uses concrete `#[pyclass]` types or primitive scalars; never `PyAny` at the public surface.
- **Boring is good.** Choose mature, well-supported crates (`tokio`, `tonic`, `prost`, `memmap2`, `arrow-rs`, `pyo3`) over clever-but-narrow alternatives.
- **One way to do each thing.** No legacy paths, no compatibility shims. v1 ships the right shape and we evolve it forward.
- **Failure categories are user-visible.** A typed Python exception hierarchy mirrors the Rust `enum`. Tests exercise each category explicitly.
- **No untyped seams.** `pyo3-stub-gen` enforces stub generation; `stubtest` enforces consistency. CI fails on drift.

## Component structure

```
rayd/                                    # workspace root
├── crates/
│   ├── rayd-core/                       # the brain: tasks, actors, refcount, scheduling
│   │   ├── src/
│   │   │   ├── id.rs                    # ObjectId, TaskId, JobId, WorkerId, ActorId
│   │   │   ├── ref/                     # ObjectRef, ReferenceCounter, borrower handshake
│   │   │   ├── store/                   # MemoryStore + PlasmaClient
│   │   │   ├── task.rs                  # TaskSpec, TaskManager
│   │   │   ├── actor.rs                 # Actor lifecycle, handle, dispatch queue
│   │   │   ├── worker_pool.rs           # Per-node worker pool
│   │   │   ├── scheduler.rs             # Lease, spillback
│   │   │   └── error.rs                 # CoreError enum
│   │   └── proto/
│   │       └── rayd.proto               # all RPCs (worker-lease, push-task, owner-pubsub, ...)
│   ├── rayd-plasma/                     # standalone shared-memory object store
│   │   ├── src/
│   │   │   ├── server.rs                # UDS server, fd handoff
│   │   │   ├── client.rs                # client used by rayd-core
│   │   │   ├── allocator.rs             # arena allocator over mmap region
│   │   │   ├── proto/                   # custom flatbuffers (or postcard) frames
│   │   │   └── spill.rs                 # pluggable spill backend trait
│   │   └── ...
│   ├── rayd-gcs/                        # cluster control plane: node registry, actor registry
│   ├── rayd-raylet/                     # per-node daemon binary
│   ├── rayd-py/                         # PyO3 bindings (the only Python entry point)
│   │   ├── src/
│   │   │   ├── lib.rs                   # #[pymodule] root
│   │   │   ├── object_ref.rs            # ObjectRef #[pyclass]
│   │   │   ├── actor_handle.rs
│   │   │   ├── core_worker.rs           # the entrypoint #[pyclass] that drives everything
│   │   │   ├── exceptions.rs            # registered Python exception hierarchy
│   │   │   └── serialization.rs         # pickle5 with out-of-band buffer callbacks
│   │   └── stub_gen.rs                  # pyo3-stub-gen binary target
│   └── rayd-cli/                        # `rayd start --head`, `rayd stop`, etc.
├── python/
│   └── rayd/
│       ├── __init__.py                  # public Python API: rayd.remote, rayd.get, rayd.put, ...
│       ├── _native.pyi                  # generated by pyo3-stub-gen
│       ├── py.typed
│       └── tests/
└── proto/
    └── *.proto                          # canonical protobuf source (referenced from build.rs)
```

This separation lets `rayd-core` be tested without Python, lets `rayd-plasma` be reused / replaced, and keeps `rayd-py` as a single Python-visible surface.

## Concrete document map

The rest of the design docs:

1. **[01-rust-stack.md](01-rust-stack.md)** — pinned crate selections, build configuration, distribution.
2. **[02-object-store.md](02-object-store.md)** — memory store + plasma design, metadata layout, transfer protocol.
3. **[03-tasks-and-actors.md](03-tasks-and-actors.md)** — task lifecycle, actor lifecycle, ownership protocol implementation.
4. **[04-python-bindings.md](04-python-bindings.md)** — PyO3 module layout, stub generation, exception hierarchy, async bridge, GIL discipline.
5. **[05-state-and-error-api.md](05-state-and-error-api.md)** — the headline new API: `ObjectRef.state()`, `peek_error()`, `get_settled()`, `wait_with_states()`.
6. **[06-roadmap.md](06-roadmap.md)** — phased implementation plan with concrete milestones.

## Hard constraints (recap)

These are not negotiable:

- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `cargo fmt -- --check` clean.
- `mypy --strict` clean over **everything**: bindings, client, tests. No `Any`, no `object`, no `cast`, no `# type: ignore` outside `*.pyi`.
- `ruff check` with all rules enabled, no inline disables outside `*.pyi`.
- `.pyi` stubs come from `pyo3-stub-gen`. `stubtest` runs in CI.
- `cargo test`, `pytest`, and `stubtest` all green for any merge.

## Risks and how we'll know we're in trouble

| Risk | Indicator | Mitigation |
|---|---|---|
| Ownership protocol races | Loom finds an interleaving where refcount drops below zero | Loom-test the borrower handshake in isolation before integrating |
| Plasma allocator fragmentation | Synthetic workload OOMs the store at <70 % nominal capacity | Start with `bumpalo`-per-region; switch to slab when needed; benchmark with the same scenarios Ray uses |
| PyO3 GIL contention on hot path | Profiler shows `Python::with_gil` dominates `submit_task` | Move serialization to a dedicated thread; release GIL across all blocking C++ equivalents |
| Stub generator can't express a public API | `mypy --strict` requires a hand-edited `.pyi` | Hide untypeable internals behind a typed wrapper; if truly impossible, accept it but document the boundary |
| Failure-mode coverage gaps | Integration test reveals a category Ray handles but we don't | The fault-model table in `../analysis/02-ownership-and-references.md` is the source of truth; every category gets an integration test in v1 |

## Out of scope but worth flagging for later

- Direct interop with Ray clusters / Ray Client. Could be useful for migration but adds enormous surface area; explicitly post-v2.
- A Rust-native client (writing tasks in Rust, executing in Rust workers). Architecturally compatible — the binding layer is just one frontend — but not v1.
- Free-threaded CPython (3.13t / 3.14t) support. Will become attractive once PyO3 fully supports it; design choices today should not foreclose it (e.g., don't rely on the GIL for invariants that should hold on the no-GIL build).
