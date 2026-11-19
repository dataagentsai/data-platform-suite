"""Lineage before the engine's catalog (X7, LIN-01): file-log records to OpenLineage-shaped run events, table-driven."""

from __future__ import annotations

import pytest

from dataplatform.meta.port import FileRecord
from dataplatform.obs.lineage import events_from_files


def rec(status: str, rows: int = 5) -> FileRecord:
    return FileRecord("ingest-7", "erp", "sales_order_line", "k1", "sol_2026-11-09.parquet", "2026-11-09", "abc", 5,
                      rows, status, "" if status == "loaded" else "why")


CASES = [
    ("a loaded batch: complete, source in, bronze out", "loaded", "COMPLETE", 1),
    ("a held batch: fail, nothing out", "held_mass_delete", "FAIL", 0),
    ("a rejected batch: fail, nothing out", "rejected_contract", "FAIL", 0),
]


@pytest.mark.parametrize(("case", "status", "event_type", "outputs"), CASES, ids=[c[0] for c in CASES])
def test_events(case: str, status: str, event_type: str, outputs: int) -> None:
    [e] = events_from_files([rec(status)], {"erp": "erp_db"}, "dev", "2026-11-09T10:00:00+00:00")
    assert e["eventType"] == event_type
    assert e["run"]["runId"] == "ingest-7"  # the platform's run id: joins the run log and the engine's lineage
    assert e["inputs"][0]["namespace"] == "erp_db"
    assert len(e["outputs"]) == outputs
    if outputs:
        assert e["outputs"][0]["name"] == "dev_bronze.erp.sales_order_line"


def test_a_skipped_duplicate_moves_nothing() -> None:
    assert events_from_files([rec("skipped_duplicate", 0)], {}, "dev", "t") == []
