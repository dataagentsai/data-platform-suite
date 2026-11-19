"""The local ops page (B10): pure rendering of the ops tables; table-driven."""

from __future__ import annotations

import pytest

from dataplatform.obs.page import render, table

CASES = [
    ("no rows says so", [], "Nothing recorded."),
    ("a failed status is marked", [{"status": "failed"}], 'class="bad"'),
    ("a success is marked", [{"status": "succeeded"}], 'class="ok"'),
    ("values are escaped", [{"reason": "<script>x</script>"}], "&lt;script&gt;"),
]


@pytest.mark.parametrize(("case", "rows", "want"), CASES, ids=[c[0] for c in CASES])
def test_table(case: str, rows: list[dict[str, object]], want: str) -> None:
    assert want in table(rows)


def test_long_tables_say_they_are_cut() -> None:
    assert "5 more rows not shown" in table([{"n": i} for i in range(10)], limit=5)


def test_page_tiles_and_sections() -> None:
    tables = {"ops_runs": [{"run_id": "r1", "status": "succeeded"}],
              "ops_dataset_runs": [{"dataset": "erp.orders", "status": "failed"}],
              "ops_step_durations": [{"step": "silver", "duration_s": 4.0}, {"step": "silver", "duration_s": 2.0}]}
    html = render(tables, "T", "sub", [{"day": 1, "layer": "bronze", "equals_oracle": True}],
                  [{"day": 1, "layer": "bronze", "incident": "x", "verdict": "missed"}])
    assert "<title>T</title>" in html
    assert ">0/1</b>" in html  # one planted incident, none caught
    assert ">6.0</td>" in html  # silver's seconds summed
    assert "background:#fff" in html  # a white page
    assert "<script" not in html
