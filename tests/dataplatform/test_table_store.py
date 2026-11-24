"""The table port's contract table (ADR-020, ADR-027): every format adapter passes the same cases.

Each case is (name, function); every case runs on every format in FORMATS. Iceberg cases skip when the Iceberg runtime
jar is not on this machine (dataplatform.adapters.table_store.iceberg_jar). Spark only: the stores are Spark adapters.

Rules proven here (DAC): IDEMP-01 (each write is idempotent by its declared mark).
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("pyspark")

FORMATS = ("delta", "iceberg")
_n = itertools.count()


@pytest.fixture
def store(request: pytest.FixtureRequest, spark: Any, engine_name: str, tmp_path: Any) -> Any:
    if engine_name != "spark":
        pytest.skip("the table stores are Spark adapters")
    from dataplatform.adapters.table_store import DeltaPathStore, IcebergStore, iceberg_jar

    if request.param == "iceberg":
        if not iceberg_jar():
            pytest.skip("Iceberg runtime jar not present")
        return IcebergStore(spark, "ice")
    return DeltaPathStore(spark, {layer: tmp_path / layer for layer in ("bronze", "silver", "gold")})


def _t() -> str:
    return f"src__t{next(_n)}"


def _df(spark: Any, rows: list[tuple[Any, ...]], ddl: str = "_row_key string, v double") -> Any:
    return spark.createDataFrame(rows, ddl)


def _rows(store: Any, table: str, layer: str = "silver") -> list[tuple[Any, ...]]:
    df = store.read(layer, table)
    return sorted(tuple(r) for r in df.collect()) if df is not None else []


# ---------------------------------------------------------------------------------------------------- cases
def absent_table(store: Any) -> None:
    t = _t()
    assert store.exists("silver", t) is False
    assert store.read("silver", t) is None


def append_twice_same_txn_writes_once(store: Any) -> None:
    t = _t()
    df = _df(store.spark, [("a", 1.0), ("b", 2.0)])
    assert store.append("silver", t, df, "silver:x", "7") is True
    store.append("silver", t, df, "silver:x", "7")
    assert _rows(store, t) == [("a", 1.0), ("b", 2.0)]


def append_new_txn_adds(store: Any) -> None:
    t = _t()
    store.append("silver", t, _df(store.spark, [("a", 1.0)]), "silver:x", "1")
    store.append("silver", t, _df(store.spark, [("b", 2.0)]), "silver:x", "2")
    assert _rows(store, t) == [("a", 1.0), ("b", 2.0)]


def merge_upserts_deletes_inserts(store: Any) -> None:
    t = _t()
    store.append("silver", t, _df(store.spark, [("a", 1.0), ("b", 2.0)]), "silver:x", "1")
    changes = store.spark.createDataFrame([("a", 10.0, "upsert"), ("b", None, "delete"), ("c", 3.0, "upsert")],
                                          "_row_key string, v double, _action string")
    store.merge("silver", t, changes)
    assert _rows(store, t) == [("a", 10.0), ("c", 3.0)]


def merge_twice_changes_nothing(store: Any) -> None:
    t = _t()
    store.append("silver", t, _df(store.spark, [("a", 1.0)]), "silver:x", "1")
    changes = store.spark.createDataFrame([("a", 5.0, "upsert"), ("d", 4.0, "upsert")],
                                          "_row_key string, v double, _action string")
    store.merge("silver", t, changes)
    store.merge("silver", t, changes)
    assert _rows(store, t) == [("a", 5.0), ("d", 4.0)]


def merge_adds_new_columns(store: Any) -> None:
    t = _t()
    store.append("silver", t, _df(store.spark, [("a", 1.0)]), "silver:x", "1")
    changes = store.spark.createDataFrame([("a", 1.0, "x", "upsert")],
                                          "_row_key string, v double, w string, _action string")
    store.merge("silver", t, changes)
    assert _rows(store, t) == [("a", 1.0, "x")]


def replace_where_touches_one_window(store: Any) -> None:
    t = _t()
    ddl = "win string, v double"
    store.replace_where("gold", t, _df(store.spark, [("2026-01", 1.0), ("2026-02", 2.0), ("2026-03", 3.0)], ddl),
                        "win IN ('2026-01')")  # first write creates
    store.replace_where("gold", t, _df(store.spark, [("2026-02", 22.0)], ddl), "win IN ('2026-02', '2026-03')")
    assert _rows(store, t, "gold") == [("2026-01", 1.0), ("2026-02", 22.0)]
    store.replace_where("gold", t, _df(store.spark, [("2026-02", 22.0)], ddl), "win IN ('2026-02', '2026-03')")
    assert _rows(store, t, "gold") == [("2026-01", 1.0), ("2026-02", 22.0)]


def overwrite_replaces_table_and_schema(store: Any) -> None:
    t = _t()
    store.overwrite("gold", t, _df(store.spark, [("a", 1.0)]))
    store.overwrite("gold", t, store.spark.createDataFrame([("z", "q")], "k string, w string"))
    assert _rows(store, t, "gold") == [("z", "q")]


def upkeep_keeps_counts_and_totals(store: Any) -> None:
    t = _t()
    for i in range(4):
        store.append("silver", t, _df(store.spark, [(f"k{i}", float(i))]), "silver:x", str(i))
    store.upkeep("silver", t, keep_versions=1)
    assert _rows(store, t) == [("k0", 0.0), ("k1", 1.0), ("k2", 2.0), ("k3", 3.0)]


def tables_lists_written_tables(store: Any) -> None:
    t = _t()
    store.overwrite("gold", t, _df(store.spark, [("a", 1.0)]))
    assert t in store.tables("gold")


def bronze_writer_replaces_by_batch_key(store: Any) -> None:
    from dataplatform.adapters.table_store import StoreBronzeWriter

    w = StoreBronzeWriter(store)
    table = f"src.d{next(_n)}"
    cols = [("_batch_key", "string"), ("id", "integer"), ("amount", "number"), ("at", "timestamp")]
    at = datetime(2026, 11, 1, 10, tzinfo=UTC)
    batch = [{"_batch_key": "b1", "id": 1, "amount": 2.5, "at": at},
             {"_batch_key": "b1", "id": 2, "amount": 1.5, "at": at}]
    w.write(table, "b1", batch, cols)
    w.write(table, "b1", batch, cols)  # the same batch again: nothing doubles
    w.write(table, "b2", [{"_batch_key": "b2", "id": 3, "amount": 1.0, "at": at}], cols)
    assert w.fingerprint(table, sums=("amount",)) == {"row_count": 3.0, "amount": 5.0}
    assert w.fingerprint(table, {"_batch_key": "b1"}) == {"row_count": 2.0}
    assert w.keys(table, ("id",), {"_batch_key": "b1"}) == {(1,), (2,)}
    with pytest.raises(ValueError, match="not a safe batch key"):
        w.write(table, "b1'; drop", batch, cols)


def drop_removes_the_table(store: Any) -> None:
    t = _t()
    store.overwrite("silver", t, _df(store.spark, [("a", 1.0)]))
    store.drop("silver", t)
    assert store.exists("silver", t) is False
    store.drop("silver", t)  # dropping what is gone is fine (a rebuild may start from nothing)



def _commits(store: Any, layer: str, table: str) -> int:
    from dataplatform.adapters.table_store import DeltaPathStore

    name = store.name(layer, table)
    if isinstance(store, DeltaPathStore):
        # shiftleft: allow SL-SEC-01 -- name comes from store.name(): quoted identifiers, no values
        return int(store.spark.sql(f"DESCRIBE HISTORY {name}").count())
    # shiftleft: allow SL-SEC-01 -- name comes from store.name(): quoted identifiers, no values
    return int(store.spark.sql(f"SELECT * FROM {name}.snapshots").count())


def scd2_close_and_insert_is_one_commit(store: Any) -> None:
    """B18-4: closing a version and adding its successor is one commit, so no reader sees a closed version with no
    current one."""
    t = _t()
    ddl = "_row_key string, id string, v double, _is_current boolean"
    store.append("silver", t, store.spark.createDataFrame([("a@1", "a", 1.0, True)], ddl), "silver:x", "1")
    before = _commits(store, "silver", t)
    changes = store.spark.createDataFrame([("a@1", "a", 1.0, False, "upsert"), ("a@2", "a", 2.0, True, "upsert")],
                                          f"{ddl}, _action string")
    store.merge("silver", t, changes)
    assert _commits(store, "silver", t) == before + 1
    got = sorted(tuple(r) for r in store.read("silver", t).select("_row_key", "_is_current").collect())
    assert got == [("a@1", False), ("a@2", True)]


def delete_where_removes_only_the_named_batches(store: Any) -> None:
    """Housekeeping's removal edge (B8): rows of the named batch keys go, in one commit; others stay."""
    t = _t()
    ddl = "_batch_key string, v double"
    store.append("bronze", t, store.spark.createDataFrame([("b1", 1.0), ("b1", 2.0), ("b2", 3.0)], ddl), "b:x", "1")
    before = _commits(store, "bronze", t)
    assert store.delete_where("bronze", t, "_batch_key IN ('b1')") == 2
    assert _commits(store, "bronze", t) == before + 1
    assert [tuple(r) for r in store.read("bronze", t).collect()] == [("b2", 3.0)]
    assert store.delete_where("bronze", t, "_batch_key IN ('zz')") == 0


def time_travel_reads_an_old_version(store: Any) -> None:
    """INSP-01: a gold run records the silver version it read; read_at gives the table back as it was then."""
    t = _t()
    store.append("silver", t, _df(store.spark, [("a", 1.0)]), "silver:x", "1")
    first = store.version("silver", t)
    store.append("silver", t, _df(store.spark, [("b", 2.0)]), "silver:x", "2")
    assert store.version("silver", t) != first
    assert sorted(tuple(r) for r in store.read_at("silver", t, first).collect()) == [("a", 1.0)]
    assert store.version("silver", "src__never") is None

CASES = [time_travel_reads_an_old_version, scd2_close_and_insert_is_one_commit,
         delete_where_removes_only_the_named_batches,
         drop_removes_the_table, absent_table, append_twice_same_txn_writes_once, append_new_txn_adds,
         merge_upserts_deletes_inserts,
         merge_twice_changes_nothing, merge_adds_new_columns, replace_where_touches_one_window,
         overwrite_replaces_table_and_schema, upkeep_keeps_counts_and_totals, tables_lists_written_tables,
         bronze_writer_replaces_by_batch_key]


@pytest.mark.parametrize("store", FORMATS, indirect=True)
@pytest.mark.parametrize("case", CASES, ids=[c.__name__ for c in CASES])
def test_table_store_contract(store: Any, case: Any) -> None:
    case(store)


def test_unsafe_names_are_refused() -> None:
    from dataplatform.adapters.table_store import IcebergStore, make_store

    with pytest.raises(ValueError, match="not a safe catalog name"):
        IcebergStore(None, "bad-catalog")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown table_format"):
        make_store(None, "parquet", "/tmp")  # type: ignore[arg-type]
