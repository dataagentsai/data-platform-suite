"""The engine's bucket totals equal the kernel's on the same rows (DPC-0052): both sides must agree on buckets."""

from __future__ import annotations

from typing import Any

from dataplatform.reconcile import bucket_totals


def test_engine_buckets_match_the_kernel(spark: Any) -> None:
    from dataplatform.adapters.spark_silver import bucket_totals_frame

    rows = [(f"SO{i}", i % 3, float(i), True, i == 5) for i in range(30)]
    df = spark.createDataFrame(rows, "order_id string, line_no long, net_value double, _is_current boolean, "
                                     "_is_deleted boolean")
    measures = {"lines": None, "net_value": "net_value"}
    live = [{"order_id": o, "line_no": n, "net_value": v} for o, n, v, _, d in rows if not d]
    assert bucket_totals_frame(df, ("order_id", "line_no"), measures, 8) == bucket_totals(
        live, ("order_id", "line_no"), measures, 8)
