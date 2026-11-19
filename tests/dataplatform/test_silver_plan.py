"""The silver plan (W1c, ADR-007/-008/-016): settings from the metadata store's view, refused when outside the stage
list. Pure: no Spark. Table-driven."""

from pathlib import Path

import pytest
from silver_helpers import TOKEN, view

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.meta import loader
from dataplatform.silver.plan import STAGES, PlanError, check_stage_order, plan_from_view

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_slice = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre manifests)")


@pytest.mark.parametrize(
    ("case", "kw", "check"),
    [
        ("event appends", {}, lambda p: p.history == "append"),
        ("master keeps type 2", {"kind": "master", "load": {"history_order_by": "seq"}},
         lambda p: p.history == "scd2" and p.order_by == "seq"),
        ("transaction keeps type 1", {"kind": "transaction", "dedup": {"mode": "business_key_latest",
                                                                      "business_keys": ["id"], "order_by": "seq"}},
         lambda p: p.history == "scd1" and p.order_by == "seq"),
        ("a transaction may ask for type 2", {"kind": "transaction", "history": {"type": "scd2", "track_columns": ["v"]},
                                              "load": {"history_order_by": "seq"}},
         lambda p: p.history == "scd2" and p.track == ("v",)),
        ("snapshot window is the key minus its last column", {"kind": "snapshot", "keys": ("id", "v")},
         lambda p: p.history == "snapshot" and p.snapshot_by == ("id",)),
        ("projection all by default (ADR-008)", {}, lambda p: p.projection == "all"),
        ("required columns become critical quarantine rules", {"required": ("v",)},
         lambda p: {(r.name, r.action) for r in p.rules} == {("id_present", "quarantine"), ("v_present", "quarantine")}),
        ("a dataset's quality actions override the defaults (ADR-009)",
         {"required": ("v",), "quality": {"actions": {"critical": {"rows": "flag"}}}},
         lambda p: {r.action for r in p.rules} == {"flag"}),
        ("every severity override is kept for the contract's rules",
         {"quality": {"actions": {"major": {"rows": "flag"}, "minor": {"rows": "none"}}}},
         lambda p: p.quality_actions == (("major", "flag"), ("minor", "none"))),
        ("no override, none kept", {}, lambda p: p.quality_actions == ()),
        ("a processor group switched off skips its stage", {"settings": {"processor_groups.hygiene": "disabled"}},
         lambda p: not p.runs("hygiene") and p.runs("parse_type")),
        ("change feeds take validity from the commit time", {"kind": "master", "load": {"history_order_by": "seq"}},
         lambda p: p.effective_time == "_committed_at"),
        ("full copies take it from a timestamp order column",
         {"kind": "master", "read": "full_copy", "columns": [("id", "string"), ("at", "timestamp")],
          "load": {"history_order_by": "at"}}, lambda p: p.effective_time == "at"),
        ("the watermark column is the event time",
         {"columns": [("id", "string"), ("event_ts", "timestamp")], "settings": {"watermark_column": "event_ts"}},
         lambda p: p.event_time == "event_ts"),
        ("column settings are read per column", {"column_settings": [{"name": "v", "on_cast_failure": "empty"}]},
         lambda p: p.column("v").on_cast_failure == "empty"),
        ("measures come from the reconciliation block",
         {"reconciliation": {"measures": [{"name": "row_count"}, {"name": "v_total", "column": "v"}],
                             "entries": [{"measures": [{"name": "seq_total", "agg": "sum", "column": "seq"}]}]}},
         lambda p: [(m.name, m.agg) for m in p.measures] == [("row_count", "count"), ("v_total", "sum"),
                                                              ("seq_total", "sum")]),
    ],
)
def test_plan_reads_the_view(case, kw, check):
    assert check(plan_from_view(view(**kw), TOKEN)), case


@pytest.mark.parametrize(
    ("case", "kw", "message"),
    [
        ("a setting outside the stage list (ADR-007)", {"column_settings": [{"name": "v", "sql": "v * 2"}]},
         "not stage settings"),
        ("settings for a column the contract lacks", {"column_settings": [{"name": "w", "exclude": True}]},
         "contract does not have"),
        ("a key column emptied on a bad cast", {"column_settings": [{"name": "id", "on_cast_failure": "empty"}]},
         "cannot be emptied"),
        ("an unknown cast rule", {"column_settings": [{"name": "v", "on_cast_failure": "guess"}]}, "on_cast_failure"),
        ("an unknown kind", {"kind": "lookup"}, "dataset kind"),
        ("type 2 without an order", {"kind": "master"}, "needs an order"),
        ("tracking a column the contract lacks", {"kind": "master", "history": {"track_columns": ["w"]},
                                                  "load": {"history_order_by": "seq"}}, "not contract columns"),
        ("no business key", {"keys": ()}, "no primary key"),
        ("an unknown quality action", {"quality": {"actions": {"major": {"rows": "delete"}}}}, "quality action"),
        ("an unknown dedup mode", {"dedup": {"mode": "fuzzy", "business_keys": ["id"]}}, "dedup.mode"),
        ("an unknown logical type", {"columns": [("id", "string"), ("v", "decimal")]}, "logical type"),
        ("an unknown letter case", {"column_settings": [{"name": "id", "case": "title"}]}, "upper or lower"),
    ],
)
def test_plan_refuses(case, kw, message):
    with pytest.raises(PlanError, match=message):
        plan_from_view(view(**kw), TOKEN)


@pytest.mark.parametrize(
    ("names", "ok"),
    [(list(STAGES), True), (list(reversed(STAGES)), False), (list(STAGES[:-1]), False), ([*STAGES, "custom"], False)],
)
def test_the_engine_supplies_exactly_the_standard_stages(names, ok):
    if ok:
        check_stage_order(names)
    else:
        with pytest.raises(PlanError):
            check_stage_order(names)


TYRE = {  # dataset -> (history, order, keys, delete rule)
    "erp.sales_order_line": ("scd2", "change_seq", ("order_id", "line_no"), "mark_deleted"),
    "erp.delivery": ("scd1", "change_seq", ("delivery_id",), "mark_deleted"),
    "erp.material": ("scd2", "extracted_at", ("material_code",), "mark_deleted"),
    "mes.press": ("scd2", "extracted_at", ("press_id",), "mark_deleted"),
    "mes.production": ("append", "", ("production_id",), "none"),
    "lims.inspection_result": ("append", "", ("inspection_id",), "none"),
    "partner.dealer_sellout": ("snapshot", "file_name", ("dealer_code", "sale_month", "material_code"), "none"),
    "dealer_api.dealer": ("scd2", "updated_at", ("dealer_code",), "none"),
    "press_stream.press_reading": ("append", "", ("reading_id",), "none"),
}


@needs_slice
@pytest.mark.parametrize(("name", "want"), sorted(TYRE.items()))
def test_every_tyre_dataset_has_a_plan(name, want):
    store = MemoryMetaStore()
    loader.load(store, SLICE / "manifest")
    source, dataset = name.split(".")
    p = plan_from_view(store.dataset_view(source, dataset), TOKEN)
    assert (p.history, p.order_by, p.keys, p.delete_apply) == want


@pytest.mark.parametrize("path", ["silver/__init__.py", "silver/plan.py", "silver/ports.py", "silver/runner.py",
                                  "silver/daybyday.py"])
def test_no_product_names_in_the_silver_kernel(path):
    """R1: the silver kernel names no product (the stages run on an engine behind the stage_runner port)."""
    import re

    text = (Path(__file__).parents[2] / "src/dataplatform" / path).read_text()
    products = (
        r"\b(postgres|postgresql|psycopg|sqlite|delta|deltalake|spark|pyspark|kinesis|kafka|s3|aws|azure|databricks)\b"
    )
    assert not re.findall(products, text, flags=re.IGNORECASE)
