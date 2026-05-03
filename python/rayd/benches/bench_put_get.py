"""`rayd.put` + `rayd.get` bandwidth benchmark.

Round-trips bytes through plasma for object sizes 100 B → 64 MB.
Measures put time, get time, and inferred bandwidth (MB/s) for each.
Drops refs each iteration so plasma doesn't accumulate — important
because the auto-spawned plasma server defaults to a 128 MiB arena.

What's measured:
- Pickle overhead in `_native.put` for the bytes payload
- Plasma create/seal for the byte buffer
- Driver-side store + refcount bookkeeping
- `rayd.get` going through the typed Python facade

Usage:
    python -m rayd.benches.bench_put_get
"""

from __future__ import annotations

import os
import time

import rayd
from rayd.benches._stats import Stats

# Sizes are powers of 10 from 100 B to 10 MB. The auto-spawned plasma
# arena is 128 MiB and the allocator fragments under repeated cycles
# of large objects, so we keep the max comfortably below ~10% of the
# arena even with brief overlap.
SIZES_BYTES: list[int] = [
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
]
# 5 round-trips per size; report median.
ITERATIONS_PER_SIZE = 5


def _format_size(b: int) -> str:
    fb: float = float(b)
    for unit in ("B", "KB", "MB", "GB"):
        if fb < 1024:
            return f"{fb:>6.1f} {unit}"
        fb /= 1024
    return f"{fb:>6.1f} TB"


def _format_rate(bytes_per_s: float) -> str:
    mb_per_s = bytes_per_s / (1024 * 1024)
    if mb_per_s >= 1.0:
        return f"{mb_per_s:>7.1f} MB/s"
    kb_per_s = bytes_per_s / 1024
    return f"{kb_per_s:>7.1f} KB/s"


def measure_one(size: int) -> tuple[Stats, Stats]:
    """Run ITERATIONS_PER_SIZE put+get round-trips at the given size.

    Returns (put_latencies, get_latencies) as Stats. Object payload is
    deterministic-but-non-trivial (pseudo-random bytes derived from
    `os.urandom`) so dictionary compression in any stage of the
    pipeline can't game the result.
    """
    payload = os.urandom(size)
    put_lat: list[float] = []
    get_lat: list[float] = []
    for _ in range(ITERATIONS_PER_SIZE):
        t0 = time.monotonic()
        ref = rayd.put(payload)
        put_lat.append(time.monotonic() - t0)
        t0 = time.monotonic()
        got = rayd.get(ref)
        get_lat.append(time.monotonic() - t0)
        # Sanity check that the round trip is faithful — without this
        # the `get` could be returning the *cached* inline copy and
        # we'd be measuring zero work for the small sizes.
        if got != payload:
            msg = f"round-trip mismatch at size {size}"
            raise RuntimeError(msg)
        del ref, got
    return Stats.from_samples(put_lat), Stats.from_samples(get_lat)


def main() -> None:
    rayd.init()
    try:
        # Warmup with one round-trip so the workers/plasma don't
        # pollute the smallest-size measurements.
        _ = measure_one(100)

        rows: list[tuple[int, Stats, Stats]] = []
        for size in SIZES_BYTES:
            put_stats, get_stats = measure_one(size)
            rows.append((size, put_stats, get_stats))
    finally:
        rayd.shutdown()

    header = (
        f"{'size':>10}  {'put p50':>10}  {'put rate':>14}  "
        f"{'get p50':>10}  {'get rate':>14}"
    )
    print(header)
    print("-" * len(header))
    for size, put, get in rows:
        put_rate = size / put.p50 if put.p50 > 0 else float("inf")
        get_rate = size / get.p50 if get.p50 > 0 else float("inf")
        print(
            f"{_format_size(size)}  "
            f"{put.p50 * 1e6:>7.1f} µs  {_format_rate(put_rate)}  "
            f"{get.p50 * 1e6:>7.1f} µs  {_format_rate(get_rate)}"
        )


if __name__ == "__main__":
    main()
