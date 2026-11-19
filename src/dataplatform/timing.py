"""End-to-end and hop-by-hop delay (ADR-034, B4): every hop stamps its own time; this turns the stamps into the
50th, 95th and 99th percentile per hop and end to end, each labelled measured or modeled.

Batch hops on a silver row: source commit (_committed_at, change feeds) -> bronze (_bronze_at, the lander) -> silver
(_silver_loaded_at). A hop a run did not stamp is left out, never guessed. In the simulator, hops the platform runs are
measured; a hop the laptop cannot run (a cloud service's own lag) is added as modeled, and the report says so.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

BATCH_HOPS = (("commit->bronze", "_committed_at", "_bronze_at"),
              ("bronze->silver", "_bronze_at", "_silver_loaded_at"),
              ("commit->silver", "_committed_at", "_silver_loaded_at"))
PERCENTILES = (50, 95, 99)


@dataclass(frozen=True)
class HopDelay:
    hop: str
    n: int
    p50: float
    p95: float
    p99: float
    basis: str = "measured"  # measured | modeled


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of already sorted values (0 for none)."""
    if not sorted_values:
        return 0.0
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return float(sorted_values[rank - 1])


def hop_delay(hop: str, seconds: list[float], basis: str = "measured") -> HopDelay:
    s = sorted(seconds)
    return HopDelay(hop, len(s), *(percentile(s, p) for p in PERCENTILES), basis=basis)


def breaches(delays: list[HopDelay], promises: dict[str, float], at: str = "p95") -> list[str]:
    """The hops whose delay at `at` is over its promise (seconds), each named, with its basis (ADR-034: a breach points
    at its hop, and a modeled number is never passed off as measured)."""
    out = []
    for d in delays:
        limit = promises.get(d.hop)
        value = getattr(d, at)
        if limit is not None and d.n and value > limit:
            out.append(f"{d.hop}: {at} {value:.0f}s over the {limit:.0f}s promise ({d.basis})")
    return out
