"""The platform explains itself in plain words, deterministically (obs/explain.py, obs/explain_meta.py). Table-driven.

No engine, no model, no network: a small report or manifest goes in, a text comes out, and the same input gives the same
text. A sentence is checked by the words that carry its meaning, not by the whole line, so wording can be polished.
"""

import copy

import pytest

from dataplatform.obs.explain import (
    bronze_differences,
    differences,
    explain,
    facts,
    gold_lines,
    ingest_lines,
    silver_lines,
)
from dataplatform.obs.explain_meta import explain_cicd, explain_dataset, listed


def report(**over):
    base = {
        "days": 1,
        "scale": 0.1,
        "engine": "duckdb",
        "bronze_equals_oracle": True,
        "silver_equals_oracle": True,
        "gold_equals_oracle": True,
        "per_day": [
            {
                "day": 1,
                "date": "2026-11-09",
                "run": "succeeded",
                "bronze": {"erp.material": {"bronze": 3, "oracle": 3}},
                "datasets": [
                    {"dataset": "erp.material", "status": "succeeded", "counts": {"loaded": 1, "rows_loaded": 3}}
                ],
                "incidents": [],
            }
        ],
        "silver": [
            {
                "day": 1,
                "date": "2026-11-09",
                "measures": {"erp.material": {"row_count": {"silver": 3, "oracle": 3}}},
                "datasets": [
                    {
                        "source": "erp",
                        "dataset": "material",
                        "status": "succeeded",
                        "seconds": 1.5,
                        "counts": {"batches": 1, "rows_in": 3, "rows_written": 3},
                    }
                ],
                "incidents": [],
            }
        ],
        "gold": [
            {
                "day": 1,
                "date": "2026-11-09",
                "measures": {"fact_x": {"rows": {"gold": 3, "oracle": 3}}},
                "tables": {"fact_x": {"status": "succeeded", "counts": {"rows": 3}, "restated": []}},
                "incidents": [],
            }
        ],
    }
    base.update(over)
    return base


@pytest.mark.parametrize(
    ("case", "counts", "table", "want"),
    [
        ("nothing recorded says so", {}, [("loaded", "{n} loaded")], "nothing to do"),
        ("zero counts say nothing", {"loaded": 0}, [("loaded", "{n} loaded")], "nothing to do"),
        ("a count is formatted with its thousands", {"loaded": 62208}, [("loaded", "{n} loaded")], "62,208 loaded"),
        ("facts keep the table's order", {"b": 2, "a": 1}, [("a", "{n} a"), ("b", "{n} b")], "1 a, 2 b"),
    ],
)
def test_facts(case, counts, table, want):
    assert facts(counts, table) == want, case


@pytest.mark.parametrize(
    ("case", "edit", "must_have", "must_not_have"),
    [
        (
            "an equal run says every layer equals the truth",
            lambda r: None,
            ["Equal to the truth."],
            ["Differs from the truth"],
        ),
        (
            "a silver difference is named with both numbers",
            lambda r: r["silver"][0]["measures"]["erp.material"]["row_count"].update(silver=2),
            ["erp.material.row_count: 2 against the truth's 3"],
            [],
        ),
        (
            "a bronze difference is named",
            lambda r: r["per_day"][0]["bronze"]["erp.material"].update(bronze=1),
            ["erp.material: 1 against the truth's 3"],
            [],
        ),
        ("a layer the run did not judge is said so", lambda r: r.update(gold=[]), ["This run did not judge gold."], []),
        (
            "a verdict that missed is shouted",
            lambda r: r["silver"][0]["incidents"].append(
                {
                    "incident": "x",
                    "detector": "DEL-01",
                    "dataset": "erp.material",
                    "verdict": "missed",
                    "evidence": "none seen",
                }
            ),
            ["**MISSED**"],
            [],
        ),
        ("held files are listed on their day", lambda r: None, [], ["held:"]),
    ],
)
def test_the_run_narrative(case, edit, must_have, must_not_have):
    r = report()
    edit(r)
    text = explain(r, {})
    for w in must_have:
        assert w in text, case
    for w in must_not_have:
        assert w not in text, case


def test_held_files_come_from_the_ops_tables():
    tables = {
        "ops_files_held": {
            "columns": [
                ["run_id", "text"],
                ["day", "text"],
                ["source", "text"],
                ["dataset", "text"],
                ["file_name", "text"],
                ["status", "text"],
                ["rows", "integer"],
                ["reason", "text"],
            ],
            "rows": [
                ["r", "2026-11-09", "erp", "erp.material", "copy.json", "held_mass_delete", 0, "720 keys missing"]
            ],
        }
    }
    assert "held: copy.json of erp.material (held_mass_delete): 720 keys missing" in explain(report(), tables)


@pytest.mark.parametrize(
    ("case", "build", "want"),
    [
        (
            "ingestion shows the way a dataset arrives",
            lambda: ingest_lines(report()["per_day"][0], {"erp.material": "pull/full_copy"}),
            "erp.material [pull/full_copy]",
        ),
        ("ingestion counts rows into bronze", lambda: ingest_lines(report()["per_day"][0], {}), "3 row(s) into bronze"),
        ("silver says how long it took", lambda: silver_lines(report()["silver"][0]), "in 1.5 s"),
        (
            "gold names a restated window with its detector",
            lambda: gold_lines(
                {
                    "tables": {
                        "f": {
                            "status": "succeeded",
                            "counts": {"rows": 1},
                            "restated": [{"window": "2026-10", "detector": "LATE-01"}],
                        }
                    }
                }
            ),
            "restated 2026-10 (LATE-01)",
        ),
    ],
)
def test_layer_lines(case, build, want):
    assert want in "\n".join(build()), case


def test_the_same_report_gives_the_same_text():
    r = report()
    assert explain(copy.deepcopy(r), {}) == explain(copy.deepcopy(r), {})


@pytest.mark.parametrize(
    ("case", "fn", "arg", "want"),
    [
        ("no differences", differences, {"a": {"m": {"x": 1.0, "oracle": 1.0}}}, []),
        ("a difference", differences, {"a": {"m": {"x": 2.0, "oracle": 1.0}}}, ["a.m: 2 against the truth's 1"]),
    ],
)
def test_differences(case, fn, arg, want):
    assert fn(arg, "x") == want, case


def test_bronze_differences_ignore_equal_datasets():
    assert bronze_differences({"bronze": {"a": {"bronze": 1, "oracle": 1}}}) == []


# ----------------------------------------------------------------------------------------------- the metadata
MANIFEST = {
    "source_name": "erp",
    "dataset_name": "sales_order_line",
    "kind": "dataset",
    "config": {
        "schedule_trigger": "cron",
        "schedule_cron": "*/15 * * * *",
        "schedule_timezone": "Asia/Kolkata",
        "watermark_column": "changed_at",
        "load_type": "INCREMENTAL",
        "sla_freshness_hours": "1",
        "sla_freshness_error_hours": "4",
    },
    "ingest": {"mode": "push", "read": "change_feed"},
    "history": {"kind": "transaction", "type": "scd2", "track_columns": ["promised_date", "ordered_qty"]},
    "dedup": {"mode": "business_key_latest", "business_keys": ["order_id", "line_no"], "order_by": "change_seq"},
    "load": {
        "history_order_by": "change_seq",
        "deletes": {"detect": "change_feed", "apply": "mark_deleted"},
        "late_lookback_days": 35,
        "new_columns": "hold_in_bronze",
        "quarantine_stop_share": 0.01,
    },
    "quality": {"actions": {"major": {"rows": "flag", "dataset": "alert_owner", "page": False}}},
    "reconciliation": {
        "hops": ["sources", "bronze", "silver"],
        "measures": [{"name": "row_count"}],
        "tolerance": {"row_count": 0.001},
    },
    "owners": {"dataset": "orders-data", "alerts": "orders-rota"},
}


@pytest.mark.parametrize(
    ("case", "edit", "words"),
    [
        (
            "a change feed is taken from change capture",
            lambda m: None,
            ["change capture writes change files to landing"],
        ),
        ("the schedule is stated with its zone", lambda m: None, ["`*/15 * * * *`", "Asia/Kolkata"]),
        ("freshness has a warning and an error", lambda m: None, ["at most 1 hour(s) old", "error at 4 hour(s)"]),
        (
            "type 2 keeps every version",
            lambda m: None,
            ["keeps every version of a key", "`promised_date` and `ordered_qty`"],
        ),
        (
            "order is the source's, not arrival",
            lambda m: None,
            ["order of `change_seq`", "never in the order they arrived"],
        ),
        (
            "deletes are detected and applied",
            lambda m: None,
            ["change feed says so with a delete row", "flagged deleted"],
        ),
        (
            "a transaction with no stated type gets type 1",
            lambda m: m["history"].pop("type"),
            ["keeps only the latest state of a key (type 1)"],
        ),
        (
            "a master with no stated type gets type 2",
            lambda m: m.update(history={"kind": "master"}),
            ["keeps every version of a key"],
        ),
        (
            "a full copy is read whole",
            lambda m: m.update(ingest={"mode": "pull", "read": "full_copy"}),
            ["reads the whole table from the source on every run"],
        ),
        (
            "a stream stops at the end of what arrived",
            lambda m: m.update(ingest={"mode": "stream", "read": "events"}),
            ["then stops. It never runs continuously"],
        ),
        (
            "an unknown ingestion combination is said, not guessed",
            lambda m: m.update(ingest={"mode": "pull", "read": "magic"}),
            ["no template for this combination"],
        ),
        ("a held batch share is a percentage", lambda m: None, ["more than 1% of a batch is set aside"]),
        ("bad-row actions are listed by severity", lambda m: None, ["A major rule failing: rows are **flag**"]),
        ("owners are named", lambda m: None, ["alerts: orders-rota", "dataset: orders-data"]),
        ("the code is pointed at", lambda m: None, ["stages/history.py: scd2", "silver/plan.py: plan_from_view"]),
    ],
)
def test_the_dataset_narrative(case, edit, words):
    m = copy.deepcopy(MANIFEST)
    edit(m)
    text = explain_dataset(m)
    for w in words:
        assert w in text, f"{case}: {w!r} not in\n{text}"


def test_a_contract_adds_its_columns_and_key():
    contract = {
        "schema": [
            {
                "properties": [
                    {"name": "order_id", "logicalType": "string", "primaryKey": True, "primaryKeyPosition": 1},
                    {"name": "qty", "logicalType": "integer"},
                ]
            }
        ]
    }
    text = explain_dataset(MANIFEST, contract)
    assert "lists 2 column(s)" in text
    assert "`order_id`" in text


@pytest.mark.parametrize(
    ("items", "want"), [([], ""), (["a"], "a"), (["a", "b"], "a and b"), (["a", "b", "c"], "a, b and c")]
)
def test_listed(items, want):
    assert listed(items) == want


# ----------------------------------------------------------------------------------------------- the checks
@pytest.mark.parametrize(
    ("case", "helps", "hooks", "workflows", "words"),
    [
        ("make targets are listed", ["make test     pytest"], [], {}, ["`make test     pytest`"]),
        ("hooks are listed with their source", [], [("https://x/ruff", "ruff")], {}, ["`ruff` from https://x/ruff"]),
        ("no workflows is said", [], [], {}, ["no workflow files are in this checkout"]),
        (
            "a workflow's jobs and steps are told",
            [],
            [],
            {
                "ci.yml": {
                    "name": "CI",
                    "on": {"push": None, "pull_request": None},
                    "jobs": {"lint": {"name": "Lint", "steps": [{"name": "ruff"}, {"run": "mypy --strict src"}]}},
                }
            },
            ["### CI (`ci.yml`), on pull_request and push", "**Lint**: ruff; mypy --strict src"],
        ),
        ("a workflow with a string trigger", [], [], {"d.yml": {"on": "push", "jobs": {}}}, ["on push"]),
    ],
)
def test_the_checks_narrative(case, helps, hooks, workflows, words):
    text = explain_cicd(helps, hooks, workflows)
    for w in words:
        assert w in text, case
