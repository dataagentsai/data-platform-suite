"""The world and the platform, day by day: a world day is written into the mock sources, the push movers fill landing,
one ingestion run takes everything new to bronze, and bronze is compared with the oracle's bronze totals for that day.

Each planted incident is then judged by the DAC detector the world names, as ingestion can see it:

    handled   ingestion does what the obligation asks (a resend skipped, a renamed column stopped, a mass delete held)
    flagged   ingestion notices it and records it; the layer that applies it is later (silver orders, drops, deletes)
    deferred  not an ingestion detector: gold or reconciliation (W1c-W1e) must catch it; ingestion only loads the rows

Domain-free: it knows DAC ids, not tyres.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from dataplatform.ingest.pipelines import Plan, Sources
from dataplatform.ingest.ports import BronzeWriter, CurrentKeys, Tokenizer
from dataplatform.ingest.runner import DatasetReport, RunReport, ingest
from dataplatform.meta.port import MetadataStore

NOTICED = {
    "DEL-01": "delete rows from the change feed counted; silver marks them deleted (W1c)",
    "SCD-02": "change older than one already seen counted; silver orders history by sequence (W1c)",
    "LATE-03": "readings behind the newest counted; silver places them by event time (W1c)",
    "DUP-04": "replayed reading ids counted; bronze keeps them as delivered, silver drops them (W1c)",
}
DEFERRED = {
    "LATE-01": "file loaded on arrival; the month's restatement is gold's (W1d)",
    "HIST-05": "corrections recorded in the store's corrections table; the backfill plan is gold's (W1d)",
    "LATE-02": "orders loaded; the unknown dealer member is gold's (W1d)",
    "REC-05": "the feed skipped rows silently; the live source against silver judges it after the silver run",
}


class DayWorld(Protocol):
    def prepare(self) -> None: ...

    def step(self, day: int) -> dict[str, Any]: ...

    def finish(self) -> None: ...


@dataclass
class Verdict:
    day: int
    incident: str
    detector: str
    dataset: str
    verdict: str  # handled | flagged | deferred | missed
    evidence: str


@dataclass
class Day:
    day: int
    date: str
    run: RunReport
    bronze: dict[str, tuple[int, int]]  # dataset -> (bronze rows this run, oracle bronze)
    verdicts: list[Verdict] = field(default_factory=list)
    oracle: dict[str, Any] = field(default_factory=dict)  # the world's oracle for the day (silver and gold check it)

    @property
    def bronze_ok(self) -> bool:
        return all(got == want for got, want in self.bronze.values())


def judge(day: int, finding: dict[str, Any], d: DatasetReport | None) -> Verdict:
    det, ds = finding["detector"], finding["dataset"]
    c, noticed = (d.counts, d.noticed) if d else ({}, {})

    def v(verdict: str, evidence: str) -> Verdict:
        return Verdict(day, finding["incident"], det, ds, verdict, evidence)

    if det == "DUP-01":
        ok = c.get("skipped_duplicate", 0) > 0
        return v("handled" if ok else "missed", f"{c.get('skipped_duplicate', 0)} batch(es) skipped as duplicates")
    if det == "SCH-02":
        col = str(finding["detail"].get("missing_column", ""))
        named = d is not None and any(col in r for r in d.reasons)
        return v(
            "handled" if c.get("rejected_contract", 0) and named else "missed",
            f"{c.get('rejected_contract', 0)} file(s) stopped naming {col}; others loaded ({c.get('loaded', 0)})",
        )
    if det == "DEL-04":
        return v(
            "handled" if c.get("held_mass_delete", 0) else "missed", "; ".join(d.reasons) if d else "dataset not run"
        )
    if det == "DEL-01" and finding["detail"].get("via") == "full_copy":
        return v("deferred", "the next full copy lacks the key; silver's key comparison marks it deleted (W1c)")
    if det == "SCD-02" and "field" in finding["detail"]:
        return v("deferred", "a master record changed value; silver closes the old version and opens the new (W1c)")
    if det in NOTICED:
        return v("flagged" if noticed.get(det, 0) else "missed", f"{noticed.get(det, 0)} {NOTICED[det]}")
    if det in DEFERRED:
        return v("deferred", DEFERRED[det])
    return v("missed", "no ingestion rule for this detector")


def run_days(
    world: DayWorld,
    days: int,
    store: MetadataStore,
    writer: BronzeWriter,
    tokenizer: Tokenizer,
    sources: Sources,
    move: Callable[[int, str], None],
    domain: str,
    env: str = "dev",
    prefix: str = "ingest",
    on_day: Callable[[Day], None] | None = None,
    silver: CurrentKeys | None = None,
    run_ingest: Callable[[str, str], RunReport] | None = None,
) -> list[Day]:
    """`run_ingest(run_id, window)` runs the day's ingestion somewhere else (its own process or job) and returns its
    report; without it the ingestion runs here."""
    world.prepare()
    out = []
    for n in range(1, days + 1):
        oracle = world.step(n)
        window = str(oracle["date"])
        move(n, window)
        run_id = f"{prefix}-{window}"
        if run_ingest is not None:
            report = run_ingest(run_id, window)
        else:
            report = ingest(store, writer, tokenizer, sources, domain, env, run_id, Plan("normal", window),
                            silver=silver)
        by = report.by_name()
        bronze = {
            ds: (int(writer.fingerprint(ds, {"_run_id": run_id})["row_count"]), int(want))
            for ds, want in sorted(oracle["bronze"].items())
        }
        day = Day(n, window, report, bronze, [judge(n, f, by.get(f["dataset"])) for f in oracle["findings"]], oracle)
        out.append(day)
        if on_day:
            on_day(day)
    world.finish()
    return out
