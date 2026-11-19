"""Hop-by-hop delay (ADR-034): percentiles, breaches named by hop and basis; the engine's numbers match the kernel's."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dataplatform.timing import HopDelay, breaches, hop_delay, percentile

PCT_CASES = [
    ("none", [], 50, 0.0),
    ("one value", [7.0], 99, 7.0),
    ("median of ten", [float(i) for i in range(1, 11)], 50, 5.0),
    ("p95 of a hundred", [float(i) for i in range(1, 101)], 95, 95.0),
]


@pytest.mark.parametrize(("case", "values", "p", "want"), PCT_CASES, ids=[c[0] for c in PCT_CASES])
def test_percentile(case: str, values: list[float], p: float, want: float) -> None:
    assert percentile(sorted(values), p) == want


def test_breach_names_the_hop_and_basis() -> None:
    delays = [hop_delay("commit->bronze", [60.0] * 99 + [4000.0]),
              HopDelay("bronze->silver", 10, 30, 40, 50, "modeled")]
    assert breaches(delays, {"commit->bronze": 300, "bronze->silver": 35}) == [
        "bronze->silver: p95 40s over the 35s promise (modeled)"]
    assert breaches(delays, {"commit->bronze": 300}, at="p99") == []  # one slow row in a hundred: p99 still 60s
    slow = [hop_delay("commit->bronze", [60.0] * 98 + [4000.0] * 2)]
    assert breaches(slow, {"commit->bronze": 300}, at="p99") == [
        "commit->bronze: p99 4000s over the 300s promise (measured)"]


def test_engine_delays_match_the_kernel(spark: Any) -> None:
    from dataplatform.adapters.spark_silver import hop_delays_frame

    t0 = datetime(2026, 11, 9, 10, tzinfo=UTC)
    rows = [(t0, t0 + timedelta(seconds=60 * i), t0 + timedelta(seconds=60 * i + 30)) for i in range(1, 11)]
    df = spark.createDataFrame(rows, "_committed_at timestamp, _bronze_at timestamp, _silver_loaded_at timestamp")
    got = {d.hop: d for d in hop_delays_frame(df)}
    assert got["bronze->silver"].p50 == 30.0
    assert got["commit->bronze"].n == 10
    assert abs(got["commit->silver"].p95 - hop_delay("x", [60.0 * i + 30 for i in range(1, 11)]).p95) <= 60
