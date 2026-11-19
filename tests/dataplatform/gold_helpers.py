"""Builders for the gold tests: silver frames as W1c writes them (business columns plus silver's own), and a small
domain in the metadata store (two source datasets, a dimension and a fact) for the harness tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dataplatform.meta.loader import gold_doc
from dataplatform.meta.port import FileRecord

T0 = datetime(2026, 11, 9, 0, 0, tzinfo=UTC)
TZ = "Asia/Kolkata"
FOREVER = None


def ts(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def d(day: int) -> date:
    return date(2026, 11, day)


SILVER_OWN = [("_valid_from", "timestamp"), ("_valid_to", "timestamp"), ("_is_current", "boolean"),
              ("_is_deleted", "boolean"), ("_seq_from", "bigint"), ("_row_key", "string")]


def silver(spark: Any, columns: list[tuple[str, str]], rows: list[dict[str, Any]]) -> Any:
    """A silver frame: each row's business values plus defaults for silver's own columns (current, not deleted,
    valid from T0 to forever)."""
    names = [*columns, *SILVER_OWN]
    out = []
    for i, r in enumerate(rows):
        full = {"_valid_from": T0, "_valid_to": None, "_is_current": True, "_is_deleted": False, "_seq_from": i,
                "_row_key": f"k{i}", **r}
        out.append(tuple(full.get(c) for c, _ in names))
    # plain names in the schema text: SQLFrame keeps backticks as part of a name (docs/DUCKDB-SPIKE.md)
    return spark.createDataFrame(out, ", ".join(f"{c} {t}" for c, t in names))


def contract(name: str, props: list[tuple[str, str]], keys: tuple[str, ...]) -> dict[str, Any]:
    return {"id": f"c-{name}", "version": "0.1.0", "schema": [{"name": name, "properties": [
        {"name": n, "logicalType": t, "required": True, **({"primaryKey": True, "primaryKeyPosition": keys.index(n) + 1}
                                                            if n in keys else {})} for n, t in props]}]}


# a small domain: dealers (type 2) and orders (a change feed), a dealer dimension and a fact of order quantities
DEALER = [("dealer_code", "string"), ("region", "string")]
ORDER = [("order_id", "string"), ("dealer_code", "string"), ("day", "date"), ("qty", "integer"),
         ("order_ts", "timestamp")]
FEED = [("dealer_code", "string"), ("month", "date"), ("units", "integer")]
DIM = {"kind": "gold_dimension", "name": "dim_dealer", "domain": "shop", "pipeline": "one_to_one",
       "contract_version": "0.1.0", "grain": ["dealer_sk"], "inputs": [{"dataset": "src.dealer"}], "history": "scd2"}
FACT = {"kind": "gold_fact", "name": "fact_orders", "domain": "shop", "pipeline": "modelled",
        "contract_version": "0.1.0", "grain": ["order_id"], "inputs": [{"dataset": "src.order"}],
        "dimensions": [{"name": "dim_dealer", "key": "dealer_sk", "business_key": ["dealer_code"],
                        "as_of": "order_ts"}],
        "window": {"column": "day", "unit": "day", "lookback": 5},
        "build": {"kind": "sql", "ref": "orders.sql"}, "corrections": True,
        "checks": {"grain_unique": True, "orphan_rate_max": 0.5,
                   "reconcile": {"measures": [{"name": "qty_total", "column": "qty"}], "tolerance": {"qty_total": 0}}}}
MONTHLY = {"kind": "gold_fact", "name": "fact_monthly", "domain": "shop", "pipeline": "modelled",
           "contract_version": "0.1.0", "grain": ["dealer_code", "month"], "inputs": [{"dataset": "src.feed"}],
           "dimensions": [{"name": "dim_dealer", "key": "dealer_sk", "business_key": ["dealer_code"],
                           "as_of": "month"}],
           "window": {"column": "month", "unit": "month", "lookback": 3},
           "build": {"kind": "sql", "ref": "monthly.sql"}, "checks": {"grain_unique": True}}
DIM_C = contract("dim_dealer", [("dealer_sk", "integer"), ("dealer_code", "string"), ("region", "string"),
                                ("valid_from", "timestamp"), ("valid_to", "timestamp"), ("is_current", "boolean")],
                 ("dealer_sk",))
FACT_C = contract("fact_orders", [("order_id", "string"), ("dealer_sk", "integer"), ("dealer_code", "string"),
                                  ("day", "date"), ("qty", "integer"), ("_is_complete", "boolean"),
                                  ("_run_id", "string")], ("order_id",))
MONTHLY_C = contract("fact_monthly", [("dealer_sk", "integer"), ("dealer_code", "string"), ("month", "date"),
                                      ("units", "integer"), ("_is_complete", "boolean"), ("_run_id", "string")],
                     ("dealer_code", "month"))
ORDERS_SQL = """SELECT order_id, dealer_code, day, qty, order_ts FROM src__order
WHERE _is_current AND NOT _is_deleted"""
MONTHLY_SQL = """SELECT dealer_code, CAST(DATE_TRUNC('MONTH', month) AS DATE) AS month, SUM(units) AS units
FROM src__feed WHERE _is_current AND NOT _is_deleted GROUP BY 1, 2"""


def source_doc(name: str, cols: list[tuple[str, str]], keys: list[str]) -> dict[str, Any]:
    logical = {"string": "string", "integer": "integer", "date": "date", "timestamp": "timestamp"}
    return {"domain": "shop", "source_name": "src", "source_env": "", "dataset_name": name, "is_enabled": True,
            "kind": "dataset", "archetypes": [], "ingest_mode": "push", "ingest_read": "change_feed",
            "history_kind": "", "load_pattern": "", "contract_id": f"c-{name}", "contract_version": "0.1.0",
            "fingerprint": "fp", "settings": {},
            "columns": [{"name": n, "logical_type": logical[t], "required": n in keys,
                         "primary_key_position": keys.index(n) + 1 if n in keys else None, "description": "",
                         "classification": ""} for n, t in cols],
            "business_keys": {"primary": keys, "dedup": []}, "manifest": {}}


def shop_store(store: Any, models: Path, monthly: bool = False) -> Any:
    """Load the small domain into a store and write the SQL builds under `models`."""
    store.load_domain({"domain": "shop", "business_timezone": TZ, "environments": ["dev"], "settings": {},
                       "manifest": {}, "fingerprint": "fp"})
    store.load_dataset(source_doc("dealer", DEALER, ["dealer_code"]))
    store.load_dataset(source_doc("order", ORDER, ["order_id"]))
    store.load_dataset(gold_doc("shop", DIM, DIM_C))
    store.load_dataset(gold_doc("shop", FACT, FACT_C))
    (models / "orders.sql").write_text(ORDERS_SQL)
    if monthly:
        store.load_dataset(source_doc("feed", FEED, ["dealer_code", "month"]))
        store.load_dataset(gold_doc("shop", MONTHLY, MONTHLY_C))
        (models / "monthly.sql").write_text(MONTHLY_SQL)
    return store


def silver_loaded(store: Any, dataset: str, days: list[str], marks: dict[str, str] | None = None) -> None:
    """Record that silver took deliveries for these business days (the input's watermark, ADR-018)."""
    source, name = dataset.split(".", 1)
    run = f"silver-{name}-{days[-1]}-{len(store.dataset_files(source, name))}"
    store.start_run(run, "shop", "dev")
    store.start_dataset_run(run, source, name, "fp", {})
    for w in days:
        store.log_file(FileRecord(run, source, name, f"{w}:{name}", f"{name}-{w}", w, "", None, 1, "loaded"))
    n = len(store.dataset_files(source, name))
    store.finish_dataset_run(run, source, name, "succeeded", bookmarks={"silver.files": str(n), **(marks or {})})
    store.finish_run(run)
