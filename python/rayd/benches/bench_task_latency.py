"""Per-task latency benchmark.

Submits one trivial task at a time, then immediately `rayd.get`s it.
Drops the ref between iterations to keep memory flat. Reports median /
p95 / p99 / min / max over N iterations.

Why one-at-a-time: this isolates the per-task latency *tail*, not
amortised throughput. Pipelining hides the worst case.

Usage:
    python -m rayd.benches.bench_task_latency
"""

from __future__ import annotations

import time

import rayd
from rayd.benches._stats import Stats

# Number of submit-and-get iterations. ~1k is enough for stable p99
# without taking forever.
DEFAULT_ITERATIONS = 1_000
# Burn-in before measuring so JIT/page-cache/worker-warmup don't
# pollute the early samples.
WARMUP_ITERATIONS = 50


@rayd.remote
def _trivial(x: int) -> int:
    return x + 1


def main() -> None:
    rayd.init()
    try:
        for _ in range(WARMUP_ITERATIONS):
            ref = _trivial.remote(0)
            rayd.get(ref)
            del ref

        latencies = []
        for i in range(DEFAULT_ITERATIONS):
            t0 = time.monotonic()
            ref = _trivial.remote(i)
            rayd.get(ref)
            latencies.append(time.monotonic() - t0)
            del ref
    finally:
        rayd.shutdown()

    stats = Stats.from_samples(latencies)
    print(f"task latency (n={stats.n}):")
    print(f"  p50  = {stats.p50 * 1e6:>9.1f} µs")
    print(f"  p95  = {stats.p95 * 1e6:>9.1f} µs")
    print(f"  p99  = {stats.p99 * 1e6:>9.1f} µs")
    print(f"  mean = {stats.mean * 1e6:>9.1f} µs (±{stats.stddev * 1e6:.1f})")
    print(f"  min  = {stats.min * 1e6:>9.1f} µs")
    print(f"  max  = {stats.max * 1e6:>9.1f} µs")


if __name__ == "__main__":
    main()
