"""ops_restatements (RSTM-01): every published gold window that changed, one row per measure, from the harness's
gold.restatement steps. Table-driven."""

from datetime import UTC, datetime

import pytest

from dataplatform.obs.ops import COLUMNS, restatements_table
from dataplatform.obs.runlog import RunLogSnapshot, StepRow

AT = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)


def step(name, counts, message):
    return StepRow("gold-3", "sales", "fact_otif", name, "warned", counts, message, AT)


@pytest.mark.parametrize(("case", "steps", "want"), [
    ("late data moved a published day",
     [step("gold.restatement", {"rows.before": 10, "rows.after": 12, "qty.before": 100.0, "qty.after": 130.5},
           "window=2026-11-10; reason=late data; detector=LATE-01")],
     [("2026-11-10", "late data", "qty", 100.0, 130.5, 30.5), ("2026-11-10", "late data", "rows", 10.0, 12.0, 2.0)]),
    ("a correction plan", [step("gold.restatement", {"rows.before": 5, "rows.after": 5},
                                "window=2026-11-02; reason=correction plan P-7; detector=HIST-05")],
     [("2026-11-02", "correction plan P-7", "rows", 5.0, 5.0, 0.0)]),
    ("other steps are not restatements", [step("gold.write", {"rows": 3}, "")], []),
])
def test_restatements_table(case, steps, want):
    got = restatements_table(RunLogSnapshot(steps=steps), "Asia/Kolkata")
    assert [(r["window"], r["reason"], r["measure"], r["before"], r["after"], r["change"]) for r in got] == want
    for r in got:
        assert list(r) == [c for c, _ in COLUMNS["ops_restatements"]]
        assert (r["table"], r["day"]) == ("fact_otif", "2026-11-12")
