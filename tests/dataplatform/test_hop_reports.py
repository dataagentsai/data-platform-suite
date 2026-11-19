"""Hop reports as plain data: what a hop writes is exactly what the judge reads back, through JSON."""

from __future__ import annotations

import json
from typing import Any

import pytest

from dataplatform.gold.harness import GoldReport, GoldTable, Restatement
from dataplatform.hop_reports import HopReportError, dump, kind_of, load
from dataplatform.ingest.runner import DatasetReport, RunReport
from dataplatform.silver.runner import SilverDataset, SilverReport

INGEST = RunReport("ingest-2026-11-09", "succeeded", [
    DatasetReport("erp", "sales_order_line", "succeeded", {"read": 900, "appended": 900}, {"DUP-01": 3}, ["3 resent"]),
    DatasetReport("dealer_api", "dealer", "held_mass_delete", {"read": 12}, {}),
])
SILVER = SilverReport("silver-2026-11-09", "failed", [
    SilverDataset("erp", "sales_order_line", "succeeded", {"written": 897, "quarantined": 3}, 2, "", 12.5),
    SilverDataset("mes", "production", "skipped", reason="nothing new"),
])
GOLD = GoldReport("gold-2026-11-09", "2026-11-09", "succeeded", [
    GoldTable("fact_otif", "succeeded", {"rows": 410}, ["2026-11-08"], ["2026-11-09"],
              [Restatement("fact_otif", "2026-11-08", "LATE-01", {"otif": 0.91}, {"otif": 0.89})], ["plan-1"], "", 4.0),
    GoldTable("dim_dealer", "skipped", reason="inputs unchanged"),
])
EMPTY = RunReport("ingest-empty", "succeeded", [])

ROUND_TRIP = [
    ("ingest with notices and reasons", "ingest", INGEST),
    ("ingest with no datasets", "ingest", EMPTY),
    ("silver with defaults left unset", "silver", SILVER),
    ("gold with a restatement", "gold", GOLD),
]


@pytest.mark.parametrize(("case", "kind", "report"), ROUND_TRIP, ids=[c[0] for c in ROUND_TRIP])
def test_round_trip_through_json(case: str, kind: str, report: Any) -> None:
    assert kind_of(report) == kind
    back = load(kind, json.loads(json.dumps(dump(report))))
    assert back == report
    assert type(back) is type(report)


def test_methods_still_work_after_loading() -> None:
    back = load("gold", json.loads(json.dumps(dump(GOLD))))
    assert back.by_name()["fact_otif"].restated[0].detector == "LATE-01"


REFUSED = [
    ("unknown hop", "bronze", dump(INGEST), "unknown hop 'bronze'"),
    ("the other hop's report", "silver", dump(INGEST), "asked for a silver report, found 'ingest'"),
    ("a newer format", "ingest", {**dump(INGEST), "version": 2}, "version 2 is not 1"),
    ("a field missing", "ingest", {"kind": "ingest", "version": 1, "report": {"run_id": "x"}}, "expected shape"),
    ("an unknown field on a dataset", "silver",
     {"kind": "silver", "version": 1, "report": {"run_id": "x", "status": "succeeded", "datasets": [{"nope": 1}]}},
     "expected shape"),
]


@pytest.mark.parametrize(("case", "kind", "data", "message"), REFUSED, ids=[c[0] for c in REFUSED])
def test_refused(case: str, kind: str, data: dict[str, Any], message: str) -> None:
    with pytest.raises(HopReportError, match=message):
        load(kind, data)


def test_not_a_report_is_refused() -> None:
    with pytest.raises(HopReportError, match="not a hop report: dict"):
        dump({"run_id": "x"})
