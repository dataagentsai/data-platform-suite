"""The five table formats of the table port that need no Spark session to check (ADR-027): how make_store picks a store,
what a Unity Catalog Delta+UniForm table is created with, the S3 Tables catalog settings and what its store does for
upkeep and drop. The behaviour contract that needs real tables is test_table_store.py; Unity Catalog and S3 Tables
need a workspace or an AWS account and run from the realm jobs (TODO B21)."""

from typing import Any, ClassVar

import pytest

pytest.importorskip("pyspark")  # the store module imports Spark types at load; the CI test job runs without Spark

from dataplatform.adapters.table_store import (
    TABLE_FORMATS,
    UNIFORM_PROPERTIES,
    S3TablesStore,
    UcDeltaUniformStore,
    UcIcebergStore,
    make_store,
    s3tables_spark_conf,
    store_catalog,
)

ARN = "arn:aws:s3tables:us-east-2:123456789012:bucket/dp-dev-tables"


class _Writer:
    def __init__(self, log: list[tuple[Any, ...]]):
        self.log = log

    def using(self, provider: str) -> "_Writer":
        self.log.append(("using", provider))
        return self

    def tableProperty(self, k: str, v: str) -> "_Writer":
        self.log.append(("property", k, v))
        return self

    def create(self) -> None:
        self.log.append(("create",))

    def createOrReplace(self) -> None:
        self.log.append(("createOrReplace",))


class _Frame:
    columns: ClassVar = ["_batch_key", "_key_hash", "v"]

    def __init__(self, log: list[tuple[Any, ...]]):
        self.log = log

    def writeTo(self, name: str) -> _Writer:
        self.log.append(("writeTo", name))
        return _Writer(self.log)


class _Catalog:
    def __init__(self, exists: bool):
        self.exists = exists

    def tableExists(self, name: str) -> bool:
        return self.exists


class _Spark:
    def __init__(self, exists: bool = True):
        self.log: list[tuple[Any, ...]] = []
        self.catalog = _Catalog(exists)

    def sql(self, text: str) -> "_Spark":
        self.log.append(("sql", text))
        return self


def _store(cls: Any, spark: _Spark, **kw: Any) -> Any:
    return cls(spark, "dev", **kw) if issubclass(cls, UcIcebergStore) else cls(spark, "s3t", **kw)


@pytest.mark.parametrize(
    ("case", "fmt", "env", "catalog"),
    [
        ("local folder catalog", "iceberg", "dev", "ice"),
        ("delta folders", "delta", "dev", "ice"),
        ("unity catalog iceberg takes the environment", "iceberg-uc", "prod", "prod"),
        ("unity catalog uniform takes the environment", "delta-uc-uniform", "prod", "prod"),
        ("s3 tables uses its own catalog name", "s3tables", "prod", "s3t"),
    ],
)
def test_store_catalog(case, fmt, env, catalog):
    assert store_catalog(fmt, env) == catalog


@pytest.mark.parametrize(
    ("case", "fmt", "cls", "name"),
    [
        ("managed iceberg", "iceberg-uc", UcIcebergStore, "`dev_silver`.`erp`.`orders`"),
        ("managed delta with uniform", "delta-uc-uniform", UcDeltaUniformStore,
         "`dev_silver`.`erp`.`orders`"),
        ("s3 tables", "s3tables", S3TablesStore, "`s3t`.`silver`.`erp__orders`"),
    ],
)
def test_make_store_picks_the_adapter(case, fmt, cls, name, tmp_path):
    s = make_store(_Spark(), fmt, tmp_path, store_catalog(fmt, "dev"))
    assert type(s) is cls
    assert s.name("silver", "erp__orders") == name


def test_make_store_knows_every_format_it_lists(tmp_path):
    for fmt in TABLE_FORMATS:
        if fmt != "iceberg":  # the local Iceberg store also needs its folder catalog, checked in test_table_store.py
            assert make_store(_Spark(), fmt, tmp_path, store_catalog(fmt, "dev")) is not None
    with pytest.raises(ValueError, match="unknown table_format"):
        make_store(_Spark(), "parquet", tmp_path)


@pytest.mark.parametrize(
    ("case", "cls", "provider", "has_uniform", "clusters"),
    [
        ("managed iceberg", UcIcebergStore, "iceberg", False, True),
        ("managed delta with uniform", UcDeltaUniformStore, "delta", True, False),
    ],
)
def test_create_provider_properties_and_clustering(case, cls, provider, has_uniform, clusters):
    spark = _Spark(exists=False)
    s = _store(cls, spark)
    s._create("silver", "erp__orders", _Frame(spark.log))
    assert ("using", provider) in spark.log
    props = {e[1]: e[2] for e in spark.log if e[0] == "property"}
    assert all(props.get(k) == v for k, v in UNIFORM_PROPERTIES.items()) is has_uniform
    assert any(e[0] == "sql" and "CLUSTER BY" in e[1] for e in spark.log) is clusters


def test_caller_properties_win_for_uniform():
    spark = _Spark(exists=False)
    s = _store(UcDeltaUniformStore, spark, properties={"delta.appendOnly": "true"})
    s._create("bronze", "erp__orders", _Frame(spark.log))
    assert {e[1] for e in spark.log if e[0] == "property"} == {"delta.appendOnly"}


@pytest.mark.parametrize(
    ("case", "arn", "catalog", "ok"),
    [
        ("a table bucket", ARN, "s3t", True),
        ("another catalog name", ARN, "tables_dev", True),
        ("a plain s3 bucket arn", "arn:aws:s3:::dp-dev", "s3t", False),
        ("not an arn", "dp-dev", "s3t", False),
        ("unsafe catalog name", ARN, "s3t;drop", False),
        ("arn with extra path", ARN + "/table/x", "s3t", False),
    ],
)
def test_s3tables_conf(case, arn, catalog, ok):
    if not ok:
        with pytest.raises(ValueError, match=r"not an S3 Tables bucket ARN|not a safe catalog name"):
            s3tables_spark_conf(arn, catalog)
        return
    conf = s3tables_spark_conf(arn, catalog)
    assert conf[f"spark.sql.catalog.{catalog}.warehouse"] == arn
    assert conf[f"spark.sql.catalog.{catalog}.catalog-impl"].endswith("S3TablesCatalog")


@pytest.mark.parametrize(
    ("case", "exists", "upkeep", "drops"),
    [
        ("table present: service maintains it", True, {"managed": 1}, True),
        ("table absent: nothing to do", False, {}, False),
    ],
)
def test_s3tables_upkeep_and_drop(case, exists, upkeep, drops):
    spark = _Spark(exists=exists)
    s = S3TablesStore(spark, "s3t")
    assert s.upkeep("gold", "sales__fact_otif") == upkeep
    s.drop("gold", "sales__fact_otif")
    sql = [e[1] for e in spark.log if e[0] == "sql"]
    assert (sql == ["DROP TABLE `s3t`.`gold`.`sales__fact_otif`"]) is drops
    assert not any("PURGE" in text or "rewrite_data_files" in text for text in sql)
