"""Tiny stats helper for benchmarks.

Sorted-list percentiles, mean, stddev. No `scipy`/`numpy` dep — keeps
benchmarks runnable from a bare `pip install rayd` install.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class Stats:
    """Summary of a sample list. All times in seconds; sizes in bytes."""

    n: int
    mean: float
    stddev: float
    p50: float
    p95: float
    p99: float
    min: float
    max: float

    @classmethod
    def from_samples(cls, samples: Sequence[float]) -> Stats:
        if not samples:
            msg = "Stats.from_samples requires at least one sample"
            raise ValueError(msg)
        sorted_s = sorted(samples)
        n = len(sorted_s)
        mean = sum(sorted_s) / n
        var = sum((x - mean) ** 2 for x in sorted_s) / n
        stddev = math.sqrt(var)
        return cls(
            n=n,
            mean=mean,
            stddev=stddev,
            p50=_percentile(sorted_s, 0.50),
            p95=_percentile(sorted_s, 0.95),
            p99=_percentile(sorted_s, 0.99),
            min=sorted_s[0],
            max=sorted_s[-1],
        )


def _percentile(sorted_samples: Sequence[float], q: float) -> float:
    """Linear-interpolation percentile on a pre-sorted sample list.

    Equivalent to numpy's `np.percentile(s, q*100, method="linear")`.
    """
    if not sorted_samples:
        msg = "_percentile requires non-empty samples"
        raise ValueError(msg)
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    pos = q * (len(sorted_samples) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_samples[lo]
    frac = pos - lo
    return sorted_samples[lo] * (1.0 - frac) + sorted_samples[hi] * frac
