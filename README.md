# rayd

A from-scratch reimplementation of the **core** of [Ray](https://github.com/ray-project/ray) in Rust + PyO3.

**Status: alpha.** Phases 1–7 of the [roadmap](docs/design/06-roadmap.md) are shipped: tasks, actors, distributed object store with shared-memory plasma + cross-node Pull/Push, lineage reconstruction, GCS-backed cluster control, observability (tracing/OTLP/Prometheus/health). 130+ Python tests + 110+ Rust tests, full strict-typing pass, all linters clean. End-to-end benchmarks in [`python/rayd/benches/`](python/rayd/benches/).

The scope of this project is deliberately narrow:

1. **Tasks** — distributed execution of `@rayd.remote` Python callables.
2. **Actors** — distributed stateful workers with method dispatch and cross-driver invocation.
3. **Distributed object storage** — shared-memory, zero-copy, with cross-node transfer + spill-to-disk.

Everything else Ray ships (Serve, Train, Tune, Data, RLlib, the autoscaler, the dashboard, runtime envs, placement groups in their full generality) is explicitly out of scope.

## Why a rewrite

Ray is a remarkable piece of engineering, but it carries baggage that bites in production:

- **Cython binding layer is brittle.** Every cross-language change touches `python/ray/_raylet.pyx` and the C++ tree, and Cython itself is in maintenance mode.
- **Loose typing in the Python layer.** A lot of `python/ray/...` code is untyped or weakly typed.
- **`ray.get(list_of_refs)` raises on first failure.** There is no efficient way to inspect the per-ref state without deserializing the full payload, and no public API to peek at *just* the exception of a failed `ObjectRef`.

rayd addresses all three at the architectural level — typed Python bindings end to end, generated `.pyi` stubs via `pyo3-stub-gen`, and a first-class state/error inspection API on `ObjectRef`.

## Quickstart

### Install

Requires Rust 1.80+ and Python 3.12+.

```bash
make venv     # create .venv + install dev deps
make build    # cargo + maturin build, install rayd as editable
```

### Hello, world: tasks

```python
import rayd

rayd.init()

@rayd.remote
def square(x: int) -> int:
    return x * x

ref = square.remote(7)
print(rayd.get(ref))  # 49

# Fan out:
refs = [square.remote(i) for i in range(10)]
print(rayd.get(refs))  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

rayd.shutdown()
```

### Actors

```python
class _CounterImpl:
    def __init__(self, start: int = 0) -> None:
        self.x = start
    def increment(self) -> int:
        self.x += 1
        return self.x

# Assignment form preserves static typing through mypy --strict;
# the @rayd.actor decorator works at runtime but mypy can't propagate
# class-decorator return types (python/mypy#3135).
Counter = rayd.actor(_CounterImpl)

handle = Counter.remote(100)
print(rayd.get(handle.increment.remote()))  # 101
print(rayd.get(handle.increment.remote()))  # 102
handle.terminate()
```

### State-and-error inspection (the central API improvement)

```python
import rayd
from rayd import Ok, Err, Pending

@rayd.remote
def maybe_fail(x: int) -> int:
    if x < 0:
        msg = "x must be non-negative"
        raise ValueError(msg)
    return x * 2

refs = [maybe_fail.remote(i) for i in [1, -1, 3]]

# Per-ref settled state, no exception:
results = rayd.get_settled(refs, timeout=2.0)
for ref, r in zip(refs, results):
    match r:
        case Ok(value=v):
            print(f"{ref.hex}: ok = {v}")
        case Err(info=info):
            print(f"{ref.hex}: failed: {info.category} — {info.message}")
        case Pending():
            print(f"{ref.hex}: still running")
```

vs Ray, where `ray.get([ref1, ref2_failed, ref3])` raises on the second ref before you can see the others' results, and inspecting `info.message` requires unpickling the exception.

### Cross-node clusters

Same API, just point the driver at a GCS:

```bash
# Terminal 1 — head node:
rayd start --head

# Terminal 2 — driver:
export RAYD_GCS_ADDRESS=127.0.0.1:60000
export RAYD_PLASMA_SOCKET=/tmp/rayd-head/plasma.sock
python -c 'import rayd; rayd.init(); ...'

# Terminal 3 — additional worker node:
rayd start --address=127.0.0.1:60000
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Python driver process                                        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ rayd (Python)                                          │  │
│  │   put / get / remote / actor / get_settled             │  │
│  └────────────────────┬───────────────────────────────────┘  │
│                       │ PyO3 boundary                        │
│  ┌────────────────────┴───────────────────────────────────┐  │
│  │ rayd-py (Rust): runtime, serialise, dispatcher         │  │
│  └─┬──────────────┬──────────────┬───────────────┬────────┘  │
│    │              │              │               │           │
└────┼──────────────┼──────────────┼───────────────┼───────────┘
     │ UDS          │ UDS          │ UDS           │ gRPC
     ▼              ▼              ▼               ▼
┌──────────┐  ┌──────────┐  ┌──────────┐    ┌──────────┐
│ rayd     │  │ rayd     │  │ rayd     │    │ rayd-    │
│ -plasma  │  │ -worker  │  │ -actor-  │    │ raylet   │
│  store   │  │ subproc  │  │  worker  │    │ (per-    │
│ (mmap)   │  │ pool     │  │ subproc  │    │  node)   │
└──────────┘  └──────────┘  └──────────┘    └────┬─────┘
                                                 │ gRPC
                                                 ▼
                                            ┌──────────┐
                                            │ rayd-gcs │
                                            │  (head)  │
                                            └──────────┘
```

Crates:

- **`rayd-core`** — domain types: `ObjectId`, `ObjectRef`, `Metadata`, `RefCounter`, `MemoryStore`, `CoreWorker`. The `ObjectRecoverer` trait that bridges spill into `resolve_entry`. The shared `tracing-subscriber` initialiser.
- **`rayd-plasma`** — shared-memory object store. UDS protocol, `PlasmaServer` + `PlasmaClient`, mmap-backed buffers.
- **`rayd-gcs`** — Global Control Service. gRPC `NodeRegistry`, `JobRegistry`, `ActorRegistry`. Heartbeat sweeper, optional Prometheus `/metrics` and `grpc.health.v1.Health`.
- **`rayd-raylet`** — per-node daemon. gRPC `ObjectTransport` (`Pull`/`Push`/`RegisterObject`/`GetObjectLocations`/`WaitForRefRemoved`). `LocalObjectManager` for spill, `LocalFsBackend` impl. Optional Prometheus `/metrics` and Health.
- **`rayd-py`** — Python module. PyO3 bindings, dispatcher pool, GCS binding, actor RPC server, OTLP/logging-bridge wiring.
- **`rayd-cli`** — `rayd` binary: `gcs`, `start --head`, `start --address=...`, `plasma-server`, `version`.

## Configuration

Driver-side env vars (read on `rayd.init()`):

| Env var | Default | Purpose |
|---|---|---|
| `RAYD_GCS_ADDRESS` | — | When set, the driver registers with this `host:port` GCS. Unset = local-only single-driver mode. |
| `RAYD_PLASMA_SOCKET` | auto-spawned | Path to an existing plasma server's UDS. Unset = auto-spawn one. |
| `RAYD_NUM_WORKERS` | 4 | Number of dispatch worker subprocesses. |
| `RAYD_HEARTBEAT_INTERVAL_MS` | 2000 | Driver→GCS heartbeat cadence. |
| `RAYD_SPILL_BUDGET_BYTES` | 1 GiB | Plasma-pressure budget before spill-on-pressure fires. |
| `RAYD_SPILL_THRESHOLD` | 0.75 | Fraction of `RAYD_SPILL_BUDGET_BYTES` that triggers eviction. |
| `RAYD_LOG` | `rayd=info,warn` | `tracing_subscriber::EnvFilter` syntax (same as `RUST_LOG`). |
| `RAYD_LOG_FORWARD` | unset | `1` to forward Rust tracing events into Python's `logging.getLogger("rayd")`. |
| `RAYD_METRICS_BIND` | unset | When set to `host:port`, the driver hosts a Prometheus text-format `/metrics` endpoint with task/put/get counters and a live `refs_alive` gauge. Independent of GCS. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | When set, ships spans to an OTLP/gRPC collector (requires the default `otlp` Cargo feature). |
| `OTEL_SERVICE_NAME` | `rayd` | OTel service name attached to exported spans. |

CLI flags worth knowing:

```bash
rayd gcs --bind 0.0.0.0:60000 --metrics-bind 127.0.0.1:9100
rayd start --head --plasma-capacity-mb 2048
rayd start --address=remote-head:60000
```

## Observability

Five independent surfaces, all opt-in:

- **Tracing** (`tracing` crate). Default subscriber writes structured events to stderr, filtered by `RAYD_LOG`. Spans/events land at every meaningful entry point — registration, RPC, spill, actor lifecycle.
- **OTLP** (default-on Cargo feature). Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317` and span data flows to your tracing backend (Jaeger/Tempo/Honeycomb/etc). Slim builds: `cargo build --no-default-features`.
- **Prometheus `/metrics`** on the GCS, raylet, plasma server, and driver. `rayd gcs --metrics-bind=127.0.0.1:9100` opens the GCS endpoint; `RAYD_METRICS_BIND=127.0.0.1:9101 python …` opens the driver's. Counters/gauges cover nodes/jobs/actors and `WatchNodes` event publish rate (GCS), pulls/pushes/directory size/spill restores plus the `NodeIndex` fast-path hit ratio (raylet), object count and bytes (plasma), and tasks submitted/completed/failed plus puts/gets and live ref count (driver). A ready-to-import Grafana dashboard with all 26 metrics is at [`dashboards/rayd-overview.json`](dashboards/rayd-overview.json).
- **Python `logging` bridge**. `RAYD_LOG_FORWARD=1` routes Rust tracing events into `logging.getLogger("rayd")` so existing Python log handlers see them.
- **`grpc.health.v1.Health`** RPC on the GCS and raylet. K8s liveness probes, load-balancer health checks.

## Performance

End-to-end benchmarks live under [`python/rayd/benches/`](python/rayd/benches/). Reference numbers from a current dev laptop with default config:

| Workload | rayd |
|---|---|
| Single-task latency (submit + get) | p50 1.2 ms, p99 1.9 ms |
| Trivial-task throughput | ~3 900 tasks/sec |
| 10 KB put/get bandwidth | ~2.0 GB/s put, ~1.7 GB/s get |
| 1 MB put/get bandwidth | ~330 MB/s put, ~615 MB/s get |
| 10 MB put/get bandwidth | ~690 MB/s put, ~1.0 GB/s get |

Run them yourself with `make bench`.

For comparison against upstream Ray's microbenchmark suite (different hardware so not directly comparable) see [docs/design/06-roadmap.md](docs/design/06-roadmap.md) Phase 7.7.

## Development

```bash
make build   # cargo + maturin develop
make stubs   # regenerate _native.pyi via pyo3-stub-gen + tools/fix_stubs.py
make test    # cargo test --workspace + uv run pytest
make lint    # cargo fmt --check + cargo clippy + ruff format/check + mypy --strict
make check   # lint + stubs + test
make bench   # the three benchmark scripts
```

Quality bar (non-negotiable):

- Rust: `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt -- --check`.
- Python: `mypy --strict` over **all** binding code, client code, and test code. No `Any`, no `object`, no `# type: ignore` outside generated `.pyi` files, no `typing.cast`.
- Python: `ruff` with all rules selected and a small documented per-file allowlist (`pyproject.toml`).
- Type stubs generated by [`pyo3-stub-gen`](https://github.com/Jij-Inc/pyo3-stub-gen), not hand-written.

## Repository layout

```
crates/
  rayd-core/      # ObjectId, ObjectRef, RefCounter, MemoryStore, CoreWorker
  rayd-plasma/    # shared-memory object store (UDS protocol)
  rayd-gcs/       # gRPC NodeRegistry/JobRegistry/ActorRegistry
  rayd-raylet/    # per-node daemon: ObjectTransport gRPC + spill
  rayd-py/        # PyO3 bindings, dispatcher, GCS binding, actor RPC
  rayd-cli/       # `rayd` CLI binary

python/rayd/
  __init__.py     # typed Python facade
  _actor.py       # actor handle, subprocess, registry pickling
  _actor_rpc.py   # cross-driver TCP listener
  _actor_worker.py
  _worker.py
  _native.pyi     # generated PyO3 stubs (do not edit; run `make stubs`)
  tests/          # pytest suite
  benches/        # end-to-end benchmarks (excluded from pytest)

docs/
  analysis/       # how Ray Core works internally (research output)
  design/         # how the reimplementation is structured + roadmap
  research/       # raw notes, citations, source excerpts

tools/
  fix_stubs.py    # post-process generated stubs
```

## Where to start reading

If you want to use rayd, work through the tutorials in order:

1. [docs/tutorials/01-tasks.md](docs/tutorials/01-tasks.md) — defining remote functions, fan-out, error propagation, lineage.
2. [docs/tutorials/02-actors.md](docs/tutorials/02-actors.md) — stateful workers, FIFO method dispatch, restart-on-crash, named actors, cross-driver invocation.
3. [docs/tutorials/03-state-and-errors.md](docs/tutorials/03-state-and-errors.md) — the central API improvement: typed `Ok`/`Err`/`Pending` results, `ErrorCategory` taxonomy, partial-success patterns.

For the comprehensive API in executable-spec form: [`python/rayd/tests/`](python/rayd/tests/).

If you want to understand the design:
1. [docs/design/00-overview.md](docs/design/00-overview.md) — goals, non-goals, phasing.
2. [docs/design/05-state-and-error-api.md](docs/design/05-state-and-error-api.md) — the central API improvement.
3. [docs/design/06-roadmap.md](docs/design/06-roadmap.md) — phased implementation plan + what's shipped.

If you want to understand Ray's internals (the rationale for this rewrite):
- [docs/analysis/00-overview.md](docs/analysis/00-overview.md) — guided tour of Ray Core.
- [docs/analysis/05-objectref-state-gap.md](docs/analysis/05-objectref-state-gap.md) — the central pain point this project addresses.

## License

Apache-2.0.
