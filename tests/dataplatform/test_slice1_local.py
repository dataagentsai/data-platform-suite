"""Slice 1 on local adapters: the world's seven days through the lander, judged by the oracle.

Table-driven: each row is one day of world.yaml and what the lander must do with that day's deliveries.
"""
import json
from pathlib import Path

import pytest

from dataplatform import contract as odcs
from dataplatform import reconcile
from dataplatform.adapters.files import FileLanding
from dataplatform.adapters.memory import MemoryLog, MemoryMeta, MemoryTables
from dataplatform.keys import batch_key, config_fingerprint
from dataplatform.lander import land
from dataplatform.manifest import fill, validate
from dataplatform.world.oracle import expected_current, judge_silver
from dataplatform.world.shop import ShopWorld

COMPILED = json.loads((Path(__file__).parents[2] / "realms/databricks/compiled/01-orders.json").read_text())
CFG = fill(COMPILED, {"bronze": "b", "silver": "s", "gold": "g"})


def run_days(tmp_path, days):
    world = ShopWorld(CFG["world"])
    landing, meta, tables, log = FileLanding(tmp_path), MemoryMeta(), MemoryTables(), MemoryLog()
    per_day = {}
    for out in world.run(days):
        for window, name, rows in out.deliveries:
            landing.write("shop_db", "orders", window, name, rows)
        per_day[out.day] = land(CFG["dataset"], CFG["contract"], landing, meta, tables, log, f"run-{out.day}", out.day)
    return world, per_day, tables, log, meta


# day -> statuses the lander must report for that day's new files
DAYS = [
    (1, ["loaded"]),
    (2, ["loaded"]),
    (3, ["loaded", "skipped_duplicate"]),       # same-batch-twice: DUP-01
    (4, ["loaded"]),                             # cancelled-order-deleted: deletes arrive as op D
    (5, ["loaded"]),                             # late-status-change: loaded; silver orders it by change_seq
    (6, ["rejected_contract"]),                  # amount-renamed: SCH-02 stops the load
    (7, ["loaded"]),
]


@pytest.mark.parametrize("day,statuses", DAYS)
def test_lander_handles_each_day(tmp_path, day, statuses):
    _, per_day, *_ = run_days(tmp_path, 7)
    assert sorted(o.status for o in per_day[day]) == sorted(statuses)


def test_rename_is_named_in_the_reason(tmp_path):
    _, per_day, *_ = run_days(tmp_path, 6)
    (o,) = per_day[6]
    assert "amount" in o.reason and "order_amount" in o.reason


def test_rerun_of_a_day_lands_nothing(tmp_path):
    world, per_day, tables, log, meta = run_days(tmp_path, 3)
    before = len(tables.tables["b.orders_changes"])
    again = land(CFG["dataset"], CFG["contract"], FileLanding(tmp_path), meta, tables, log, "rerun", 3)
    assert again == [] and len(tables.tables["b.orders_changes"]) == before


def test_bronze_applied_newest_first_equals_oracle(tmp_path):
    """Stand-in for silver: apply bronze by change_seq. The real silver is the Databricks pipeline; this proves the
    lander gave it the right rows, and that the oracle and the world agree with each other."""
    world, per_day, tables, *_ = run_days(tmp_path, 7)
    latest = {}
    for r in tables.tables["b.orders_changes"]:
        if r["order_id"] not in latest or r["change_seq"] > latest[r["order_id"]]["change_seq"]:
            latest[r["order_id"]] = r
    silver = [r for r in latest.values() if r["op"] != "D"]
    rejected = {o.delivery.name for o in per_day[6]}
    results = judge_silver("t", 7, expected_current(world.changes, rejected), silver)
    assert all(r.passed for r in results), [r for r in results if not r.passed]


def test_world_plants_what_the_calendar_says(tmp_path):
    world, *_ = run_days(tmp_path, 7)
    assert len(world.deleted_orders()) == 3
    late = world.late_order()
    assert late is not None
    arrived = [(c.row["status"], c.row["change_seq"]) for c in world.changes if c.row["order_id"] == late]
    assert [s for s, _ in arrived] == ["placed", "shipped", "paid"]       # paid arrives last...
    assert arrived[2][1] < arrived[1][1]                                  # ...with an older change sequence


def test_world_is_deterministic():
    a, b = ShopWorld(CFG["world"]).run(7), ShopWorld(CFG["world"]).run(7)
    assert [d.deliveries for d in a] == [d.deliveries for d in b]


@pytest.mark.parametrize("window,cs_a,cs_b,same", [
    ("2026-11-03", "a" * 64, "a" * 64, True),
    ("2026-11-03", "a" * 64, "b" * 64, False),
    ("2026-11-04", "a" * 64, "a" * 64, False),
])
def test_batch_key(window, cs_a, cs_b, same):
    assert (batch_key("2026-11-03", cs_a) == batch_key(window, cs_b)) is same


@pytest.mark.parametrize("observed,ok,missing", [
    ({"order_id", "customer_id", "store_id", "order_ts", "status", "amount", "currency", "change_seq"}, True, ()),
    ({"order_id", "customer_id", "store_id", "order_ts", "status", "order_amount", "currency", "change_seq"}, False,
     ("amount",)),
    (set(), False, ("amount", "change_seq", "currency", "customer_id", "order_id", "order_ts", "status", "store_id")),
])
def test_check_columns(observed, ok, missing):
    v = odcs.check_columns(CFG["contract"], observed)
    assert (v.ok, v.missing) == (ok, missing)


@pytest.mark.parametrize("measures,passed", [
    ({"src": {"rows": 10}, "bronze": {"rows": 10}}, [True]),
    ({"src": {"rows": 10}, "bronze": {"rows": 9}}, [False]),
    ({"src": {"rows": 10}, "bronze": {}}, [False]),            # nothing to compare is not a pass (GATE-01)
])
def test_reconcile(measures, passed):
    assert [r.passed for r in reconcile.compare("r", 1, measures, {})] == passed


@pytest.mark.parametrize("change,error", [
    ({}, None),
    ({"contract_version": "9.9.9"}, "pins contract"),
    ({"load": {"history_order_by": "arrived_at"}}, "history_order_by"),
    ({"waivers": [{"concern": "X5"}]}, "has no reason"),
])
def test_manifest_validate(change, error):
    import yaml
    m = yaml.safe_load((Path.home() / "data-platform-catalog/slices/01-orders/manifest/orders.yaml").read_text())
    m.update(change)
    errors = validate(m, CFG["contract"])
    assert (error is None and not errors) or any(error in e for e in errors)


def test_fingerprint_ignores_key_order():
    assert config_fingerprint({"a": 1, "b": 2}) == config_fingerprint({"b": 2, "a": 1})


# ---------------------------------------------------------------- slice 1's lander on the W1a metadata store (W1b)
def slice1_store():
    from dataplatform.adapters.memory_meta import MemoryMetaStore
    from dataplatform.meta.loader import columns_of

    store = MemoryMetaStore()
    store.load_domain({"domain": "shop", "settings": {}, "manifest": {}, "fingerprint": "s1"})
    store.load_dataset({"domain": "shop", "source_name": "shop_db", "source_env": "", "dataset_name": "orders",
                        "is_enabled": True, "kind": "dataset", "archetypes": ["PA2"], "ingest_mode": "push",
                        "ingest_read": "change_feed", "history_kind": "", "load_pattern": "history",
                        "contract_id": CFG["contract"]["id"], "contract_version": CFG["contract"]["version"],
                        "fingerprint": config_fingerprint(CFG["dataset"]), "settings": {},
                        "columns": columns_of(CFG["contract"]), "business_keys": {"primary": ["order_id"]},
                        "manifest": CFG["dataset"]})
    return store


def test_slice1_lander_records_in_the_metadata_store(tmp_path):
    """Wrapped, not rewritten: slice 1's lander keeps its statuses day by day (DAYS above) while every batch goes to
    the store's file log through batch_status(source, dataset, key) and log_file."""
    from dataplatform.adapters.store_registry import StoreBatchRegistry

    store, landing, tables, log = slice1_store(), FileLanding(tmp_path), MemoryTables(), MemoryLog()
    registry = StoreBatchRegistry(store, "shop_db", "orders")
    got = {}
    for out in ShopWorld(CFG["world"]).run(7):
        for window, name, rows in out.deliveries:
            landing.write("shop_db", "orders", window, name, rows)
        run = f"run-{out.day}"
        store.start_run(run, "shop", "dev")
        store.start_dataset_run(run, "shop_db", "orders", "s1", {})
        got[out.day] = sorted(o.status for o in land(CFG["dataset"], CFG["contract"], landing, registry, tables, log,
                                                     run, out.day))
        store.finish_dataset_run(run, "shop_db", "orders", "failed" if "rejected_contract" in got[out.day]
                                 else "succeeded")
        store.finish_run(run)
    assert got == {d: sorted(s) for d, s in DAYS}
    report = store.run_report("run-3")
    assert sorted(f["status"] for f in report["files"] if not f["batch_key"].startswith("file:")) == ["loaded"]
    assert any(f["status"] == "skipped_duplicate" for f in report["files"])
