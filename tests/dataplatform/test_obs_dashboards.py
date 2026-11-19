"""The ops builder, the ops metric views and the dashboards (G8, DPC-0057, ADR-024) on a simulated tyre fortnight.

The run log comes from the FIXTURE runner (obs/fixture.py). What these rows prove is the observability side:
recorder -> run log -> reader -> ops tables -> views -> dashboards and pushed alerts, on realistic volumes and the
world's real incident calendar; and that every screen renders with no run log connected (ADR-024 rule 6).
"""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from dataplatform.adapters.lakeview import compile_dashboard, query_sql
from dataplatform.obs import dashboards as D
from dataplatform.obs import views as V
from dataplatform.obs.ops import TABLES

BOARDS = D.every()

REPO = Path(__file__).parents[2]
SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_world = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre DWD)")


@pytest.fixture(scope="module")
def week():
    from dataplatform.twin.runner import Harness

    return Harness.build(SLICE, days=7)


@pytest.fixture(scope="module")
def fortnight():
    from dataplatform.twin.runner import Harness

    return Harness.build(SLICE, days=14)


@needs_world
@pytest.mark.parametrize("board", D.DASHBOARDS, ids=[d.id for d in D.DASHBOARDS])
def test_a_week_fills_every_dashboard(week, board):
    rendered = D.render(board, week.tables)
    filled = [w for w, rows in rendered.items() if rows]
    assert len(filled) >= len(rendered) - 1, f"{board.id}: empty {set(rendered) - set(filled)}"


@needs_world
@pytest.mark.parametrize("board", BOARDS, ids=[d.id for d in BOARDS])
def test_a_fortnight_fills_every_widget(fortnight, board):
    empty = [w for w, rows in D.render(board, fortnight.tables).items() if not rows]
    assert empty == []


@needs_world
@pytest.mark.parametrize("table", TABLES)
def test_declared_ops_columns_match_the_builder(fortnight, table):
    """obs/ops.py COLUMNS is what the builder makes (the 14-day fixture fills every table)."""
    from dataplatform.obs.ops import COLUMNS
    from dataplatform.obs.ops_export import column_types

    assert column_types(fortnight.tables[table]) == list(COLUMNS[table])


def reads_per_day(t):
    days = sorted({r["day"] for r in t["ops_usage_daily"]})
    return [sum(r["reads"] for r in t["ops_usage_daily"] if r["day"] == d) for d in days]


@needs_world
@pytest.mark.parametrize(
    ("case", "check", "expect"),
    [
        ("one scheduled run per day and one manual re-run", lambda t: sorted(
            (r["trigger"], r["origin"]) for r in t["ops_runs"]).count(("schedule", "run_log")), 14),
        ("every table built", lambda t: sorted(t) == sorted(TABLES), True),
        ("no cost anywhere (ADR-024 rule 8)", lambda t: [c for rows in t.values() for r in rows[:1] for c in r
                                                          if "cost" in c], []),
        ("every job run joins a run-log run", lambda t: {r["origin"] for r in t["ops_runs"]}, {"run_log"}),
        ("the re-run is a retry and a slow-down",
         lambda t: [(r["dataset"], r["attempt"], r["slow"]) for r in t["ops_dataset_runs"] if r["retry"]],
         [("erp.delivery", 2, True)]),
        ("lineage keyed by run ids that exist",
         lambda t: {r["run_id"] for r in t["ops_lineage"]} <= {r["run_id"] for r in t["ops_runs"]}, True),
        ("step rows carry owner, severity and the owner's screen",
         lambda t: sorted({(r["owner"], r["severity"], r["screen"]) for r in t["ops_steps"]}),
         [("des", "blocked", "des_data_expectations"), ("des", "passed", "des_data_expectations"),
          ("engine", "blocked", "sources_ingestion"), ("engine", "warned", "sources_ingestion")]),
        ("run ids are trace ids", lambda t: all(re.match(r"^[0-9a-f]{32}$", r["run_id"]) for r in t["ops_runs"]),
         True),
        ("one alert per rule per day at most (OPS-06)", lambda t: max(r["alerts"] for r in t["ops_alert_counts"]),
         1),
        ("files held back: duplicate, contract, volume",
         lambda t: sorted(r["status"] for r in t["ops_files_held"]),
         ["rejected_contract", "rejected_volume", "skipped_duplicate"]),
        ("the blocked load shows as skipped", lambda t: [(r["dataset"], r["status"]) for r in t["ops_dataset_runs"]
                                                         if r["status"] != "succeeded"], [("erp.material", "skipped")]),
        ("usage grows over the fortnight",
         lambda t: reads_per_day(t)[-1] > reads_per_day(t)[0],
         True),
    ],
)
def test_ops_tables(fortnight, case, check, expect):
    assert check(fortnight.tables) == expect


@needs_world
def test_alerts_went_to_the_routed_people(fortnight):
    sent = {(a.rule, a.channel, a.to) for a in fortnight.fixture.out.sent if not a.rule.endswith(":resolved")}
    assert fortnight.fixture.out.sent == fortnight.fixture.notifier.sent  # pushed by runs, delivered once
    assert ("detector-fired", "page", "erp-data-rota") in sent  # tier 1: order lines (2 h error target)
    assert ("detector-fired", "ticket", "erp-data-rota") in sent  # tier 2: materials (48 h)
    assert ("freshness-breach", "digest", "sales-data@tyre.example") in sent  # tier 3: partner files (120 h)


@needs_world
@pytest.mark.parametrize(
    ("case", "check", "expect"),
    [
        ("no lateness check on a fixed interval: one per run plus due wake-ups only",
         lambda m: m.lateness_checks <= 15 + 14, True),
        ("telemetry copied once per business day", lambda m: m.copy.copied_on is not None, True),
        ("a second copy the same day reads nothing", lambda m: m.copy.copy(m.copy.watermark), False),
        ("a wake-up before the next due time runs nothing",
         lambda m: m.due is not None and m.wake(m.due - __import__("datetime").timedelta(seconds=1)) is None, True),
    ],
)
def test_pushed_not_polled(fortnight, case, check, expect):
    assert check(fortnight.fixture.monitor) == expect


# ------------------------------------------------------------------------------------------------ no run log


def _telemetry_only():
    """Level 1 of ADR-024 rule 6: the platform's telemetry, no run log connected (the null run-log adapter)."""
    from datetime import UTC, datetime, timedelta

    from dataplatform.adapters.memory_meta import MemoryMetaStore
    from dataplatform.adapters.memory_system import MemorySystemTables
    from dataplatform.adapters.null_runlog import NullRunLog
    from dataplatform.obs.alerts import DEFAULT_RULES, load_config
    from dataplatform.obs.monitor import MemoryNotifier, Monitor
    from dataplatform.obs.system import AccessRow, JobRunRow, LineageRow

    t0 = datetime(2026, 11, 9, 1, tzinfo=UTC)
    system = MemorySystemTables()
    system.job_rows = [JobRunRow("901", "erp.orders", "", t0, t0 + timedelta(minutes=4), "succeeded"),
                       JobRunRow("902", "erp.orders", "", t0 + timedelta(days=1), t0 + timedelta(days=1, minutes=9),
                                 "failed")]
    system.lineage_rows = [LineageRow(t0, "dev.bronze.orders", "dev.silver.orders", "", "901")]
    system.access_rows = [AccessRow(t0 + timedelta(hours=3), "ana@x.example", "read", "prod.gold.fact_otif")]
    monitor = Monitor(NullRunLog(), system, MemoryMetaStore(), "tyre", load_config(DEFAULT_RULES, t0),
                      MemoryNotifier(), since=t0 - timedelta(hours=1))
    now = t0 + timedelta(days=2)
    monitor.copy_system(now)
    return monitor.tables(now)


NO_LOG = _telemetry_only()


@pytest.mark.parametrize("board", BOARDS, ids=[d.id for d in BOARDS])
def test_every_screen_renders_with_no_run_log(board):
    rendered = D.render(board, NO_LOG)
    assert set(rendered) == {w.id for w in board.widgets}


@pytest.mark.parametrize(
    ("case", "check", "expect"),
    [
        ("job runs from telemetry", lambda t: [(r["status"], r["origin"]) for r in t["ops_runs"]],
         [("succeeded", "telemetry"), ("failed", "telemetry")]),
        ("job-level columns filled", lambda t: [r["duration_s"] for r in t["ops_dataset_runs"]], [240.0, 540.0]),
        ("run-log columns stay empty", lambda t: {(r["rows_read"], r["rows_written"]) for r in t["ops_dataset_runs"]},
         {(None, None)}),
        ("run-log tables empty", lambda t: [len(t[n]) for n in ("ops_steps", "ops_files_held", "ops_freshness")],
         [0, 0, 0]),
        ("usage and lineage from telemetry", lambda t: (len(t["ops_usage_daily"]), len(t["ops_lineage"])), (1, 1)),
        ("failed job shown on platform health",
         lambda t: [r["dataset"] for r in D.render(D.PLATFORM_HEALTH, t)["failed"]], ["erp.orders"]),
    ],
)
def test_telemetry_only_columns(case, check, expect):
    assert check(NO_LOG) == expect


# ------------------------------------------------------------------------------------------------ definitions


@pytest.mark.parametrize("board", BOARDS, ids=[d.id for d in BOARDS])
def test_definitions_are_valid(board):
    D.validate(board)
    assert board.purpose
    assert board.widgets
    assert "DPC-0057" in board.discharges


def test_three_sets_as_adr_024_names_them():
    """Observability's four, DES's own screen, and the adopted native set; no cost, no data-trust screen in obs."""
    from dataplatform.obs.native import NATIVE

    assert [d.title for d in D.DASHBOARDS] == ["Platform health", "Freshness and SLOs", "Performance",
                                               "Sources and ingestion"]
    assert {d.owner for d in D.DASHBOARDS} == {"observability"}
    assert [(d.id, d.owner) for d in BOARDS if d not in D.DASHBOARDS] == [("des_data_expectations", "des")]
    assert {n.id for n in NATIVE} >= {"jobs_monitoring", "job_failure_notification", "table_insights",
                                      "anomaly_detection"}


@pytest.mark.parametrize(
    ("case", "native_id", "covered"),
    [
        ("job runs and run time", "jobs_monitoring", lambda q: q.table == "ops_runs"),
        ("reads per table and readers", "table_insights", lambda q: q.table == "ops_usage_daily"),
        ("table-to-table flows", "lineage_view", lambda q: q.table == "ops_lineage"),
        ("a latest-age listing", "anomaly_detection",
         lambda q: q.table == "ops_freshness" and "checked_at" in q.columns),
    ],
)
def test_no_screen_rebuilds_what_the_platform_shows(case, native_id, covered):
    """Adopt what the platform has, build only the missing parts (13 Nov): no query of ours covers an adopted screen."""
    from dataplatform.obs.native import NATIVE

    assert native_id in {n.id for n in NATIVE}
    assert [f"{d.id}.{q.id}" for d in BOARDS for q in d.queries if covered(q)] == []
    assert not [q.id for d in BOARDS for q in d.queries if "cost" in q.id or q.table.endswith("cost_daily")]


def test_ops_views_lint_clean():
    assert V.lint() == []


@pytest.mark.parametrize(
    ("case", "query"),
    [
        ("a measure the view does not certify", D.Query("q", "ops_runs", ("day",), (D.Measure("n", "sum", "x"),))),
        ("a dimension the view does not have", D.Query("q", "ops_runs", ("run_id",), (D.Measure("runs", "count"),))),
        ("a filter on a non-dimension", D.Query("q", "ops_runs", ("day",), (D.Measure("runs", "count"),),
                                                where=(("run_id", "=", "x"),))),
    ],
)
def test_aggregates_read_certified_views_only(case, query):
    with pytest.raises(D.DefinitionError):
        D.validate(D.Dashboard("x", "X", "p", (query,), ()))


BAD = [
    ("unknown table", D.Query("q", "ops_nope", columns=("a",))),
    ("not a plain name", D.Query("q", "ops_runs", columns=("a; DROP TABLE x",))),
    ("columns and aggregates together", D.Query("q", "ops_runs", ("day",), (D.Measure("n", "count"),),
                                                columns=("day",))),
    ("unknown aggregate", D.Query("q", "ops_runs", ("day",), (D.Measure("n", "median", "duration_s"),))),
]


@pytest.mark.parametrize(("case", "query"), BAD, ids=[c[0] for c in BAD])
def test_bad_definitions_refused(case, query):
    with pytest.raises(D.DefinitionError):
        D.validate(D.Dashboard("x", "X", "p", (query,), ()))


@pytest.mark.parametrize(
    ("query", "lines"),
    [
        (D.Query("q", "ops_runs", ("day",), (D.Measure("runs", "count"),
                                             D.Measure("avg_duration_s", "avg", "duration_s"))),
         ["SELECT `day`, MEASURE(`runs`) AS `runs`, MEASURE(`avg_duration_s`) AS `avg_duration_s`\n",
          "FROM `cat`.`ops`.`mv_ops_runs`\n", "GROUP BY `day`\n", "ORDER BY `day`"]),
        (D.Query("q", "ops_files_held", columns=("day", "reason"), where=(("status", "in", ("a", "it's")),),
                 order_by=("day",), descending=True, limit=5),
         ["SELECT `day`, `reason`\n", "FROM `cat`.`ops`.`ops_files_held`\n",
          "WHERE `status` IN ('a', 'it\\'s')\n", "ORDER BY `day` DESC\n", "LIMIT 5"]),
        (D.Query("q", "ops_freshness", (), (D.Measure("breaches", "count"),),
                 where=(("breach", "=", True), ("tier", "!=", 3))),
         ["SELECT MEASURE(`breaches`) AS `breaches`\n", "FROM `cat`.`ops`.`mv_ops_freshness`\n",
          "WHERE `breach` = TRUE AND `tier` <> 3"]),
    ],
)
def test_lakeview_sql(query, lines):
    assert query_sql(query, "cat", "ops") == lines


@pytest.mark.parametrize("board", BOARDS, ids=[d.id for d in BOARDS])
def test_lakeview_file_shape(board):
    doc = compile_dashboard(board, "cat", "ops")
    names = {ds["name"] for ds in doc["datasets"]}
    layout = doc["pages"][0]["layout"]
    widgets = [x["widget"] for x in layout if "queries" in x["widget"]]
    assert len(widgets) == len(board.widgets)
    assert all(w["queries"][0]["query"]["datasetName"] in names for w in widgets)
    assert all(x["position"]["x"] >= 0 and x["position"]["x"] + x["position"]["width"] <= 6 for x in layout)
    aggregate = [q for q in board.queries if q.measures]
    assert all(any("MEASURE(" in ln for ln in query_sql(q, "c", "s")) for q in aggregate)  # views, not tables


@pytest.mark.parametrize("view", V.VIEWS, ids=[v.name for v in V.VIEWS])
def test_ops_view_sql(view):
    from dataplatform.adapters.databricks_ops_views import view_sql

    sql = view_sql(view, "cat", "ops")
    assert f"CREATE OR REPLACE VIEW `cat`.`ops`.`{view.name}`\nWITH METRICS\nLANGUAGE YAML" in sql
    assert f"source: cat.ops.{view.table}" in sql
    assert sql.endswith("$$;\n")


def test_generated_files_are_up_to_date():
    """The committed AI/BI files are what the definitions generate (drift check, like the bundle's)."""
    gen = REPO / "realms/databricks/dashboards/generate.py"
    cmd = [sys.executable, str(gen), "--check"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False,  # noqa: S603 - our own generator
                         env={"PYTHONPATH": str(REPO / "src")})
    assert out.returncode == 0, out.stderr
    for board in BOARDS:
        assert json.loads((gen.parent / f"{board.id}.lvdash.json").read_text())["pages"][0]["displayName"]


KERNEL = REPO / "src/dataplatform"
KERNEL_FILES = sorted(str(p.relative_to(KERNEL)) for d in ("obs", "twin", "des") for p in (KERNEL / d).rglob("*.py"))


@pytest.mark.parametrize("path", KERNEL_FILES)
def test_no_product_names_in_the_obs_kernel(path):
    """R1: products are named only in adapters and realms."""
    products = r"\b(postgres|postgresql|psycopg|sqlite|kinesis|kafka|s3|rds|aws|azure|databricks|lakeview|sap|" \
               r"opentelemetry|otel|delta)\b"
    text = (KERNEL / path).read_text()
    assert not re.findall(products, text, flags=re.IGNORECASE), path
