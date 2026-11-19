"""Gold per world day: after the day's silver run, one gold run for the day's business date, then gold is compared
with the oracle's gold for the day and the incidents gold owns are judged.

The comparison and the judges are the domain's (a `GoldJudge`, e.g. world/domains/tyre_gold.py): they read published
gold through the GoldReader port and the oracle's gold numbers, and say which numbers differ. This module is
domain-free and product-free: it runs the harness, keeps the days, and hands each judge the run's report and the
days before it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from dataplatform.gold.harness import GoldReport, run_gold
from dataplatform.gold.ports import GoldEngine, GoldReader, GoldTables
from dataplatform.ingest.daybyday import Verdict
from dataplatform.meta.port import MetadataStore

TOLERANCE = 0.01


@dataclass
class GoldDay:
    day: int
    date: str
    report: GoldReport
    measures: dict[str, dict[str, tuple[float, float]]]  # table -> measure -> (gold, oracle)
    verdicts: list[Verdict] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)  # what the judge keeps for later days (and the report)

    @property
    def differences(self) -> dict[str, dict[str, tuple[float, float]]]:
        return {t: bad for t, ms in self.measures.items()
                if (bad := {k: v for k, v in ms.items() if abs(v[0] - v[1]) > TOLERANCE})}

    @property
    def ok(self) -> bool:
        return not self.differences and self.report.status == "succeeded"


class GoldJudge(Protocol):
    def compare(self, oracle: dict[str, Any], reader: GoldReader, report: GoldReport) -> tuple[
            dict[str, dict[str, tuple[float, float]]], dict[str, Any]]:
        """(gold against the oracle per table and measure, notes kept for later days)."""

    def judge(self, n: int, oracle: dict[str, Any], today: GoldDay, before: list[GoldDay],
              reader: GoldReader) -> list[Verdict]:
        """Verdicts for the incidents gold owns that the day closes (some are judged days after they are planted)."""


def gold_day(n: int, oracle: dict[str, Any], store: MetadataStore, tables: GoldTables, engine: GoldEngine,
             reader: GoldReader, judge: GoldJudge, domain: str, env: str, tz: str, before: list[GoldDay],
             prefix: str = "gold") -> GoldDay:
    """One gold run for the day, then gold against the oracle and the day's gold incidents."""
    date = str(oracle["date"])
    report = run_gold(store, tables, engine, domain, env, f"{prefix}-{date}", date, tz)
    return judge_gold(n, oracle, report, reader, judge, before)


def judge_gold(n: int, oracle: dict[str, Any], report: GoldReport, reader: GoldReader, judge: GoldJudge,
               before: list[GoldDay]) -> GoldDay:
    """The day's gold run judged from its report; the run itself may have happened in another process."""
    measures, notes = judge.compare(oracle, reader, report)
    day = GoldDay(n, str(oracle["date"]), report, measures, notes=notes)
    day.verdicts = judge.judge(n, oracle, day, before, reader)
    return day
