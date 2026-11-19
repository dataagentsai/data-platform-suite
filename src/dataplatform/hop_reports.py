"""A hop's report as plain data, so a hop that runs as its own job can hand its report to whoever judges it later.

On the laptop one process runs every hop and passes the report objects along. Deployed, ingestion, silver and gold
are separate jobs; each writes its report next to the run, and the proof driver reads it back to judge the day.
`dump` and `load` are exact inverses for the three report kinds: load(kind, dump(report)) == report.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from dataplatform.gold.harness import GoldReport, GoldTable, Restatement
from dataplatform.ingest.runner import DatasetReport, RunReport
from dataplatform.silver.runner import SilverDataset, SilverReport

KINDS = ("ingest", "silver", "gold")
VERSION = 1


class HopReportError(ValueError):
    pass


def kind_of(report: Any) -> str:
    if isinstance(report, RunReport):
        return "ingest"
    if isinstance(report, SilverReport):
        return "silver"
    if isinstance(report, GoldReport):
        return "gold"
    raise HopReportError(f"not a hop report: {type(report).__name__}")


def dump(report: Any) -> dict[str, Any]:
    """The report as JSON-ready data, with its kind and a format version."""
    return {"kind": kind_of(report), "version": VERSION, "report": asdict(report)}


def _ingest(body: dict[str, Any]) -> RunReport:
    return RunReport(body["run_id"], body["status"], [DatasetReport(**d) for d in body["datasets"]])


def _silver(body: dict[str, Any]) -> SilverReport:
    return SilverReport(body["run_id"], body["status"], [SilverDataset(**d) for d in body["datasets"]])


def _gold(body: dict[str, Any]) -> GoldReport:
    tables = [GoldTable(**{**t, "restated": [Restatement(**r) for r in t.get("restated", [])]})
              for t in body["tables"]]
    return GoldReport(body["run_id"], body["as_of"], body["status"], tables)


_LOADERS = {"ingest": _ingest, "silver": _silver, "gold": _gold}


def load(kind: str, data: dict[str, Any]) -> Any:
    """The report object back from `dump`'s data. The kind asked for must be the kind that was written."""
    if kind not in _LOADERS:
        raise HopReportError(f"unknown hop '{kind}'; known: {', '.join(KINDS)}")
    if data.get("kind") != kind:
        raise HopReportError(f"asked for a {kind} report, found '{data.get('kind')}'")
    if data.get("version") != VERSION:
        raise HopReportError(f"hop report version {data.get('version')} is not {VERSION}")
    try:
        return _LOADERS[kind](data["report"])
    except (KeyError, TypeError) as exc:
        raise HopReportError(f"{kind} report is not in the expected shape: {exc}") from exc
