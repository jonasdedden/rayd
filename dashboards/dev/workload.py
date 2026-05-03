"""Steady workload generator for the Grafana dev dashboard.

Connects to the local rayd cluster brought up by `run.sh`, then loops
over puts, gets, and remote-task submissions so every panel on the
dashboard shows non-zero rates.

Run in a second terminal once `./run.sh` is up:

    uv run python dashboards/dev/workload.py
    # ...or, with rayd already in the venv:
    python dashboards/dev/workload.py

Press Ctrl-C to stop.
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
from types import FrameType

import rayd

# Burst sizes — kept small so a laptop runs the loop comfortably.
_TASKS_PER_BURST = 16
_PUTS_PER_BURST = 4
_GETS_PER_BURST = 4
_BURST_PERIOD_S = 0.5

# Approximate task work — enough to be visible in Pull/seal traffic
# without flooding plasma. ~1 KB serialised payload per return.
_PAYLOAD_BYTES = 1024


def _check_env() -> None:
    """Print a friendly message if RAYD_GCS_ADDRESS isn't set."""
    if not os.environ.get("RAYD_GCS_ADDRESS"):
        print(
            "RAYD_GCS_ADDRESS is unset — start the dev cluster first via "
            "`dashboards/dev/run.sh`. Aborting.",
            file=sys.stderr,
        )
        sys.exit(2)


@rayd.remote
def _produce(seed: int, size_bytes: int = _PAYLOAD_BYTES) -> bytes:
    """Trivial task: return `size_bytes` of pseudo-random data."""
    rng = random.Random(seed)
    return rng.randbytes(size_bytes)


@rayd.remote
def _maybe_fail(p_fail: float, seed: int) -> int:
    """Sometimes fails — exercises the `tasks_failed_total` counter."""
    if random.Random(seed).random() < p_fail:
        msg = f"intentional failure (seed={seed})"
        raise RuntimeError(msg)
    return seed * seed


def _do_burst(burst: int) -> None:
    """One iteration: tasks + puts + gets."""
    # Submit tasks; the dispatcher will complete them asynchronously.
    refs = [_produce.remote(burst * 1000 + i) for i in range(_TASKS_PER_BURST)]
    # Add a few that intentionally fail so the failure-ratio panel shows life.
    refs.extend(_maybe_fail.remote(0.10, burst * 1000 + 9000 + i) for i in range(2))

    # Local puts/gets for the driver-side counters.
    for j in range(_PUTS_PER_BURST):
        ref = rayd.put({"burst": burst, "i": j, "data": b"x" * _PAYLOAD_BYTES})
        for _ in range(_GETS_PER_BURST // _PUTS_PER_BURST):
            _ = rayd.get(ref)

    # Drain the failed task refs (raises) and the rest (succeeds) so
    # they don't accumulate as "submitted but never completed" — that
    # would skew the task-lifecycle panel.
    settled = rayd.get_settled(refs, timeout=5.0)
    ok_count = sum(1 for r in settled if isinstance(r, rayd.Ok))
    err_count = sum(1 for r in settled if isinstance(r, rayd.Err))
    pending_count = sum(1 for r in settled if isinstance(r, rayd.Pending))
    print(
        f"burst {burst:>4}: submitted={len(refs)} ok={ok_count} "
        f"err={err_count} pending={pending_count}"
    )


def _install_signal_handlers() -> None:
    """Make Ctrl-C exit cleanly without dumping a stack trace."""

    def _on_signal(_sig: int, _frame: FrameType | None) -> None:
        print("\nworkload: stopping", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)


def main() -> int:
    _check_env()
    _install_signal_handlers()
    rayd.init()
    print(
        "workload: connected. submitting bursts every "
        f"{_BURST_PERIOD_S}s. ctrl-c to stop.",
        flush=True,
    )
    try:
        burst = 0
        while True:
            start = time.monotonic()
            _do_burst(burst)
            burst += 1
            elapsed = time.monotonic() - start
            if elapsed < _BURST_PERIOD_S:
                time.sleep(_BURST_PERIOD_S - elapsed)
    finally:
        rayd.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
