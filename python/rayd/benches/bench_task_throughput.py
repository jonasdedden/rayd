"""Task-throughput benchmark.

Submits M trivial tasks back-to-back, then `rayd.get`s all of them in
one batch. Reports end-to-end tasks-per-second over 3 runs.

What's measured:
- Submission overhead (Python → cloudpickle → dispatch UDS)
- Worker pickup + execution + plasma seal
- Driver-side reader thread observing completions
- Final batch get over all refs

What's NOT measured (covered by other benches):
- Per-task latency tail (use `bench_task_latency`).
- Bytes/sec for non-trivial payloads (use `bench_put_get`).

Usage:
    python -m rayd.benches.bench_task_throughput
"""

from __future__ import annotations

import time

import rayd
from rayd.benches._stats import Stats

# Number of tasks per run. Each task returns an `int`, so plasma usage
# stays under a few MiB even at M=10_000.
DEFAULT_TASKS = 10_000
# Three runs; the first one warms the worker pool. Reported number is
# the median of all three (warmup typically sets a floor).
DEFAULT_RUNS = 3


@rayd.remote
def _trivial(x: int) -> int:
    return x + 1


def run_one(num_tasks: int) -> float:
    """Submit + get `num_tasks` tasks. Return wall-clock seconds."""
    start = time.monotonic()
    refs = [_trivial.remote(i) for i in range(num_tasks)]
    rayd.get(refs)
    return time.monotonic() - start


def main() -> None:
    rayd.init()
    try:
        # Warm the workers.
        _ = run_one(num_tasks=64)
        elapsed = [run_one(DEFAULT_TASKS) for _ in range(DEFAULT_RUNS)]
    finally:
        rayd.shutdown()

    rates = [DEFAULT_TASKS / t for t in elapsed]
    stats = Stats.from_samples(rates)
    print(f"task throughput ({DEFAULT_TASKS} tasks x {DEFAULT_RUNS} runs):")
    print(f"  median tasks/sec = {stats.p50:>9_.0f}")
    print(f"  min    tasks/sec = {stats.min:>9_.0f}")
    print(f"  max    tasks/sec = {stats.max:>9_.0f}")
    print(f"  raw elapsed (s)  = [{', '.join(f'{t:.3f}' for t in elapsed)}]")


if __name__ == "__main__":
    main()
