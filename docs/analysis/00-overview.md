# Analysis: Ray Core Internals — Overview

This directory documents how Ray Core actually works under the hood. The findings inform the design in `../design/`.

## Reading order

1. **[01-ray-architecture.md](01-ray-architecture.md)** — Top-level component map (GCS, raylet, core_worker, plasma, dashboard), process model on a single node, gRPC service surface, cluster bootstrap, scheduling.
2. **[02-ownership-and-references.md](02-ownership-and-references.md)** — Ray's ownership-based reference counting, the `ReferenceCounter` data structure, lineage reconstruction, owner-vs-borrower handshake. The single most important architectural concept in Ray Core.
3. **[03-tasks-and-actors.md](03-tasks-and-actors.md)** — Task submission path (Python → Cython → C++ → raylet → worker → back), `TaskSpec`, retry semantics, actor creation and method dispatch, worker pool, fault model.
4. **[04-object-store.md](04-object-store.md)** — In-process memory store vs Plasma, the 100 KiB threshold, the `RayObject = (data, metadata, nested_refs)` triple, the metadata format that encodes both successful-value tags and `ErrorType` codes, plasma protocol over Unix sockets with `SCM_RIGHTS` fd-passing, cross-node `Pull`/`Push`, spilling.
5. **[05-objectref-state-gap.md](05-objectref-state-gap.md)** — The user-facing problem: `ray.get(list)` raises on first failure; no public API to inspect ref state cheaply. Where in the codebase the first-error-aborts loop lives, and why it's a Python-side artifact rather than a fundamental design choice.
6. **[06-cython-pain-points.md](06-cython-pain-points.md)** — Why the Cython binding layer is hard to extend, what specifically breaks when you try, and what it means for a rewrite.

## TL;DR for the reader in a hurry

- **Components**: `gcs_server` (one per cluster, head node), `raylet` (one per node, contains the embedded plasma store and the local scheduler), `core_worker` (a C++ library linked into every Python worker via Cython), Python driver/worker, dashboard agent.
- **Ownership**: when worker A calls `f.remote(...)`, **A owns the ObjectRef**. Reference counting is decentralized — A keeps the count locally, no central refcount table. The owner is also responsible for lineage reconstruction.
- **Object store**: two-tier. Small returns (≤ ~100 KiB) flow inline through gRPC into a per-worker in-process hash map. Large returns go through plasma — a shared-memory store embedded in the raylet, with mmap regions and `SCM_RIGHTS`-based fd passing for true zero-copy access from any local worker.
- **`RayObject`** is `(data: Bytes, metadata: Bytes, nested_refs: Vec<ObjectRef>)`. The `metadata` byte string is the key to cheap state inspection: it's typically a few bytes (`b"RAW"`, `b"PYTHON"`, `b"3"` for `TASK_EXECUTION_EXCEPTION`, etc.) and is stored separately from `data`. Reading it does *not* require deserializing the value.
- **The pain point in three sentences**: when Ray's Python deserializer encounters a failed ref in a list, it raises immediately inside a `for` loop in `_deserialize_object`, so successors are never processed. The C++ layer already returns per-object `(data, metadata)` pairs. There is no public Python API today to ask "is this ref ready / errored / pending" without going through the deserializer's raise path.

## What this analysis is and isn't

It is: a structured map of Ray's architecture, anchored to specific source paths under `src/ray/...` and `python/ray/...`, biased toward the parts that matter for a reimplementation of *just* tasks, actors, and the object store.

It isn't: a Ray user manual, a perf benchmark, an accurate snapshot of Ray master at any specific commit, or a critique. Where the architecture is a reasonable choice that I'm reproducing, I say so plainly; where it has known sharp edges (the partial-failure problem, the `ray.wait` failure-vs-success ambiguity) I document them in `05-objectref-state-gap.md`.

## Sources used in this analysis

- The Ray repository: <https://github.com/ray-project/ray>. Specific files cited inline in each subdocument.
- Wang et al., **"Ownership: A Distributed Futures System for Fine-Grained Tasks"**, NSDI 2021. <https://www.usenix.org/conference/nsdi21/presentation/wang-stephanie>. The canonical reference for Ray's ownership model.
- Moritz et al., **"Ray: A Distributed Framework for Emerging AI Applications"**, OSDI 2018. <https://www.usenix.org/conference/osdi18/presentation/moritz>. The original Ray paper; useful for context on what's been kept vs. rewritten since.
- Ray architecture whitepaper: <https://docs.ray.io/en/latest/ray-contribute/whitepaper.html>.
- Specific Python/C++ source files cited per-document.

Constants and exact numeric thresholds (e.g., 100 KiB inline cutoff, 64 KiB chunk size) are flagged inline where the value should be re-verified against the current Ray master before being reused as a design parameter.
