"""The release gate (G10, TEST-03): the comparison table, the ports' contract table, and the gate on the tyre world
blocking a planted bad change and passing a good one.

The Databricks shallow-clone adapter's contract rows are pending until a workspace is bound (DP_GATE_WAREHOUSE): they
are skipped with that reason, so the adapter cannot be bound before they pass (ADR-020). Its SQL is checked here.
"""

import os
from pathlib import Path

import pytest

from dataplatform.adapters.databricks_clone import DatabricksTables, clone_sql, drop_sql, measure_sql
from dataplatform.adapters.memory_tables import MemoryTables
from dataplatform.twin.gate import (
    GateError,
    TableMeasures,
    TableSpec,
    clone_name,
    compare,
    run_gate,
    spec_from_gold,
)

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
SPEC = TableSpec("silver.orders", ("order_id",), ("amount",), {"amount": 0.01})
PROD = TableMeasures("silver.orders", 3, {"amount": 30.0}, "k1")


def cand(rows=3, amount=30.0, keys="k1"):
    return TableMeasures("silver.orders__gate_x", rows, {"amount": amount}, keys)


ORACLE = {"rows": 3, "amount": 30.0}

COMPARE = [
    ("identical and matching the oracle", cand(), ORACLE, set(), []),
    ("within tolerance", cand(amount=30.005), ORACLE, set(), []),
    ("a row too many: oracle and prod both block", cand(rows=4, keys="k2"), ORACLE, set(),
     [("rows", "oracle", True), ("rows", "prod", True), ("keys", "prod", True)]),
    ("declared change that agrees with the oracle passes", cand(amount=31.0), {"rows": 3, "amount": 31.0},
     {"amount"}, []),
    ("declared change that misses the oracle blocks", cand(amount=31.0), ORACLE, {"amount"},
     [("amount", "oracle", True)]),
    ("declared but unchanged is a warning", cand(), ORACLE, {"amount"}, [("amount", "prod", False)]),
    ("an oracle measure the table lacks blocks", cand(), {"margin": 1.0}, set(), [("margin", "oracle", True)]),
    ("no oracle: prod alone decides", cand(amount=29.0), None, set(), [("amount", "prod", True)]),
    ("declared new keys pass", cand(keys="k9"), ORACLE, {"keys"}, []),
]


@pytest.mark.parametrize(("case", "candidate", "oracle", "declared", "expect"), COMPARE, ids=[c[0] for c in COMPARE])
def test_compare(case, candidate, oracle, declared, expect):
    got = compare(SPEC, candidate, PROD, oracle, declared)
    assert [(f.measure, f.against, f.blocking) for f in got] == expect



# DIFF-01: per window, and every difference explained before release. Two days; a dealer's rows move from day 1 to
# day 2: the totals hold, the windows do not.
WSPEC = TableSpec("gold.fact_sales", ("order_id",), ("qty",), {}, "day")
W_PROD = MemoryTables({"gold.fact_sales": [{"order_id": "A", "day": "d1", "qty": 5, "dealer": "D-1"},
                                           {"order_id": "B", "day": "d2", "qty": 7, "dealer": "D-2"}]})
MOVED = [{"order_id": "A", "day": "d2", "qty": 5, "dealer": "D-1"}, {"order_id": "B", "day": "d2", "qty": 7,
                                                                      "dealer": "D-2"}]
DROPPED = [{"order_id": "B", "day": "d2", "qty": 7, "dealer": "D-2"}]

WINDOWS = [
    ("same rows: nothing to explain", None, {}, []),
    ("rows moved between days: the totals hold, both windows show it", MOVED, {},
     [("window d1: qty", True), ("window d1: rows", True), ("window d1: keys", True), ("window d2: qty", True),
      ("window d2: rows", True), ("window d2: keys", True)]),
    ("the same, explained per window: notes, not blockers", MOVED,
     {"window:d1": "dealer D-1's orders now dated by delivery", "window:d2": "same change"},
     [("window d1: qty", False), ("window d1: rows", False), ("window d1: keys", False), ("window d2: qty", False),
      ("window d2: rows", False), ("window d2: keys", False)]),
    ("a dealer dropped: blocked until explained", DROPPED, {},
     [("qty", True), ("rows", True), ("keys", True), ("window d1: qty", True), ("window d1: rows", True),
      ("window d1: keys", True)]),
    ("explained by measure: every finding on it stops blocking", DROPPED,
     {"qty": "D-1 left the network", "rows": "same", "keys": "same"},
     [("qty", False), ("rows", False), ("keys", False), ("window d1: qty", False), ("window d1: rows", False),
      ("window d1: keys", False)]),
]


@pytest.mark.parametrize(("case", "rows", "explained", "expect"), WINDOWS, ids=[c[0] for c in WINDOWS])
def test_compare_per_window(case, rows, explained, expect):
    t = MemoryTables({"gold.fact_sales": rows if rows is not None else W_PROD.tables["gold.fact_sales"]})
    got = compare(WSPEC, t.measure("gold.fact_sales", WSPEC.keys, WSPEC.measures, "day"),
                  W_PROD.measure("gold.fact_sales", WSPEC.keys, WSPEC.measures, "day"), None, set(),
                  explained=explained)
    assert [(f.measure, f.blocking) for f in got] == expect, case
    assert all(f.note.startswith("explained: ") for f in got if not f.blocking)


def test_window_measure_sql():
    sql = measure_sql("gold.fact_sales", ("order_id",), ("qty",), "day")
    assert sql.startswith("SELECT CAST(`day` AS STRING) AS `_w`, COUNT(*)")
    assert sql.endswith("GROUP BY `day`")

@pytest.mark.parametrize(
    ("table", "change", "expect"),
    [("silver.orders", "PR 42", "silver.orders__gate_pr_42"), ("cat.silver.fact_otif", "abc", None),
     ("orders", "x", "orders__gate_x"), ("bad name;", "x", GateError)],
)
def test_clone_name(table, change, expect):
    if expect is GateError:
        with pytest.raises(GateError):
            clone_name(table, change)
    else:
        assert clone_name(table, change) == (expect or "cat.silver.fact_otif__gate_abc")


# ------------------------------------------------------------------------------------------------ ports contract

ROWS = [{"order_id": "A", "amount": 10.0}, {"order_id": "B", "amount": 20.0}]


def memory():
    return MemoryTables({"silver.orders": ROWS})


def databricks():
    if not os.environ.get("DP_GATE_WAREHOUSE"):
        pytest.skip("pending: the shallow-clone adapter's contract rows need a bound workspace (DP_GATE_WAREHOUSE)")
    raise NotImplementedError("bind a statement-execution runner here when the workspace lands")


ADAPTERS = {"memory": memory, "databricks": databricks}


def clone_then(fn):
    def act(t):
        t.clone("silver.orders", "silver.orders__gate_t")
        return fn(t)
    return act


def measured(m):
    return m.rows, m.sums


CONTRACT = [
    ("measure counts and sums", lambda t: measured(t.measure("silver.orders", ("order_id",), ("amount",))),
     (2, {"amount": 30.0})),
    ("a clone measures like its source", clone_then(lambda t: t.measure("silver.orders__gate_t", ("order_id",),
                                                                        ("amount",)).keys_digest
                                                    == t.measure("silver.orders", ("order_id",), ()).keys_digest),
     True),
    ("other keys, other digest", lambda t: t.measure("silver.orders", ("order_id",), ()).keys_digest
     != t.measure("silver.orders", ("amount",), ()).keys_digest, True),
    ("a dropped clone is gone", clone_then(lambda t: (t.drop("silver.orders__gate_t"),
                                                      t.measure("silver.orders__gate_t", (), ()))), GateError),
    ("dropping a missing table is quiet", lambda t: t.drop("silver.nothing"), None),
    ("cloning a missing table is refused", lambda t: t.clone("silver.nothing", "silver.x"), GateError),
]


@pytest.mark.parametrize("adapter", list(ADAPTERS))
@pytest.mark.parametrize(("case", "act", "expect"), CONTRACT, ids=[c[0] for c in CONTRACT])
def test_gate_ports_contract(adapter, case, act, expect):
    tables = ADAPTERS[adapter]()
    if isinstance(expect, type) and issubclass(expect, Exception):
        with pytest.raises(expect):
            act(tables)
    else:
        assert act(tables) == expect


@pytest.mark.parametrize(
    ("sql", "expect"),
    [(clone_sql("cat.silver.orders", "cat.silver.orders__gate_x"),
      "CREATE OR REPLACE TABLE `cat`.`silver`.`orders__gate_x` SHALLOW CLONE `cat`.`silver`.`orders`"),
     (drop_sql("cat.silver.orders__gate_x"), "DROP TABLE IF EXISTS `cat`.`silver`.`orders__gate_x`"),
     (measure_sql("s.t", ("a", "b"), ("v",)),
      "SELECT COUNT(*) AS `rows`, CAST(COALESCE(BIT_XOR(xxhash64(`a`, `b`)), 0) AS STRING) AS `keys_digest`, "
      "CAST(SUM(`v`) AS DOUBLE) AS `sum_v` FROM `s`.`t`")],
)
def test_databricks_clone_sql(sql, expect):
    assert sql == expect


@pytest.mark.parametrize("bad", ["s.t; DROP TABLE x", "s.`t`", "a.b.c.d"])
def test_databricks_clone_refuses_unsafe_names(bad):
    with pytest.raises((ValueError, GateError)):
        measure_sql(bad, (), ())


def test_databricks_adapter_reads_the_measure_row():
    seen = []

    def runner(sql, params):
        seen.append(sql)
        return [{"rows": 2, "keys_digest": "77", "sum_amount": 30}]

    t = DatabricksTables(runner)
    assert t.measure("s.orders", ("order_id",), ("amount",)) == TableMeasures("s.orders", 2, {"amount": 30.0}, "77")


# ------------------------------------------------------------------------------------------------ on the tyre world


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog (the tyre DWD)")
    from dataplatform.adapters.memory_sources import MemoryFileDrop, MemoryRecords, MemorySourceDb, MemoryStream
    from dataplatform.world import dwd as dwd_loader
    from dataplatform.world.domains import model_for
    from dataplatform.world.domains.tyre import SOL
    from dataplatform.world.engine import Sinks, World

    dwd = dwd_loader.load(SLICE)
    model = model_for(dwd, 0.02)
    reports = World(dwd, model, Sinks(MemorySourceDb(), MemoryFileDrop(), MemoryRecords(), MemoryStream()),
                    tmp_path_factory.mktemp("gate-world")).run(7)
    lines = list(model.lines.values())
    silver = [dict(ln.row) for ln in lines if not ln.deleted]
    every = [dict(ln.row) for ln in lines]
    oracle = reports[-1]["silver"][SOL]
    gold = reports[-1]["gold"]["fact_otif"]
    otif = [{"order_id": ln.key[0], "line_no": ln.key[1], "ordered_qty": ln.row["ordered_qty"]} for ln in lines
            if not ln.deleted and ln.row["line_status"] == "open"]
    return {"silver": silver, "every": every, "oracle": {"rows": oracle["row_count"],
                                                         "net_value": oracle["net_value_total"]},
            "otif": otif, "otif_all": [{"order_id": ln.key[0], "line_no": ln.key[1],
                                         "ordered_qty": ln.row["ordered_qty"]} for ln in lines if not ln.deleted],
            "gold_oracle": {"rows": gold["rows"], "ordered_qty": gold["ordered_qty_total"]}}


TYRE = TableSpec("silver.erp__sales_order_line", ("order_id", "line_no"), ("net_value",), {"net_value": 0.005})


def gate(world, change, declared=None):
    tables = MemoryTables({TYRE.name: world["silver"]})
    result = run_gate("change-1", [TYRE], lambda clones: change(tables, clones[TYRE.name]), tables, tables,
                      {TYRE.name: world["oracle"]}, declared)
    assert set(tables.tables) == {TYRE.name}  # clones dropped
    return result


def no_op(tables, clone):
    pass


def adds_a_column(tables, clone):
    for r in tables.tables[clone]:
        r["region_code"] = "x"


def forgets_deletes(world):
    def change(tables, clone):  # a refactor that drops the "not deleted" filter: the 5 deleted lines come back
        tables.tables[clone] = [dict(r) for r in world["every"]]
    return change


def fans_out(tables, clone):  # a join that duplicates one line
    tables.tables[clone].append(dict(tables.tables[clone][0]))


@pytest.mark.parametrize(
    ("case", "change", "verdict", "blocking"),
    [
        ("prod agrees with the oracle (no change)", lambda w: no_op, "pass", []),
        ("a harmless change passes", lambda w: adds_a_column, "pass", []),
        ("planted bad change: deletes forgotten", forgets_deletes, "block",
         ["rows/oracle", "net_value/oracle", "net_value/prod", "rows/prod", "keys/prod"]),  # the lines carry value
        ("planted bad change: a join fans out", lambda w: fans_out, "block",
         ["rows/oracle", "net_value/oracle", "net_value/prod", "rows/prod", "keys/prod"]),
    ],
)
def test_gate_on_the_tyre_world(world, case, change, verdict, blocking):
    result = gate(world, change(world))
    assert result.verdict == verdict
    assert sorted(f"{f.measure}/{f.against}" for f in result.blocking) == sorted(blocking)


def test_gate_drops_clones_when_the_change_crashes(world):
    tables = MemoryTables({TYRE.name: world["silver"]})

    def crash(clones):
        raise RuntimeError("change failed")

    with pytest.raises(RuntimeError):
        run_gate("c", [TYRE], crash, tables, tables)
    assert set(tables.tables) == {TYRE.name}


# ------------------------------------------------------------------------------------------------ gold rows (W1d)


@pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre slice)")
@pytest.mark.parametrize(
    ("fact", "keys", "measures"),
    [("fact_otif", ("order_id", "line_no"), ("ordered_qty",)),
     ("fact_sales", ("dealer_code", "material_code", "sale_month"), ("units_sold_out",)),
     ("fact_scrap", ("press_id", "material_code", "cure_hour"), ("tyres_cured", "tyres_scrapped"))],
)
def test_gold_gate_specs_come_from_the_manifest(fact, keys, measures):
    import yaml

    spec = spec_from_gold(yaml.safe_load((SLICE / f"manifest/gold/{fact}.yaml").read_text()))
    assert (spec.keys, spec.measures) == (keys, measures)
    assert all(spec.tolerance[m] == 0 for m in measures)


def keeps_cancelled_lines(world):
    def change(tables, clone):  # a refactor of the OTIF build that drops the open-lines filter
        tables.tables[clone] = [dict(r) for r in world["otif_all"]]
    return change


@pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre slice)")
@pytest.mark.parametrize(
    ("case", "change", "verdict", "blocking"),
    [("gold agrees with the oracle (no change)", lambda w: no_op, "pass", []),
     ("planted bad change: cancelled lines counted", keeps_cancelled_lines, "block",
      ["rows/oracle", "ordered_qty/oracle", "rows/prod", "ordered_qty/prod", "keys/prod"])],
)
def test_gate_on_gold(world, case, change, verdict, blocking):
    import yaml

    spec = spec_from_gold(yaml.safe_load((SLICE / "manifest/gold/fact_otif.yaml").read_text()))
    tables = MemoryTables({spec.name: world["otif"]})
    result = run_gate("change-g", [spec], lambda clones: change(world)(tables, clones[spec.name]), tables, tables,
                      {spec.name: world["gold_oracle"]})
    assert result.verdict == verdict
    assert sorted(f"{f.measure}/{f.against}" for f in result.blocking) == sorted(blocking)
