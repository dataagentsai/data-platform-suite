"""The table port's engine side (data-platform-catalog dpc/ports.yaml table_writer, ADR-027): one small adapter per open
table format, so silver, gold and bronze code never names a format.

    DeltaPathStore     Delta tables in folders, <root of the layer>/<table> (what W1 wrote until 12 Nov)
    IcebergStore       Iceberg tables in a Spark catalog, <catalog>.<layer>.<table> (a local folder catalog on the
                       laptop; on Databricks the Unity Catalog managed Iceberg catalog: only the catalog name changes)

A table is named by (layer, table): layer is bronze, silver or gold; table is `source__dataset` or `schema__name`.
Operations (the port): exists, read, append (idempotent by app + txn), merge (rows with an `_action`: upsert or delete,
on a key), replace_where (one window), overwrite (whole table), add_columns, upkeep. Every adapter passes the same
contract table (tests/dataplatform/test_table_store.py, ADR-020).

Idempotent append: a rerun must write nothing (DUP-02). Delta keeps the (app, txn) pair in its log (txnAppId); Iceberg
keeps it in the snapshot summary (snapshot-property.dp-app / dp-txn) and the adapter looks before writing. The run-level
authority stays the metadata store's file log; this is the write-level guard under it.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, ClassVar, Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
LAYERS = ("bronze", "silver", "gold")
ICEBERG_JAR = "iceberg-spark-runtime-3.5_2.12-1.9.2.jar"


def _check(layer: str, table: str) -> None:
    if layer not in LAYERS or not _NAME.match(table):
        raise ValueError(f"not a safe table name: {layer}.{table}")


def quoted(name: str) -> str:
    """A Spark SQL identifier in backticks, any backtick inside doubled, so a column name from a source cannot end the
    identifier and add SQL of its own."""
    if "\x00" in name or "\n" in name:
        raise ValueError(f"not a usable identifier: {name!r}")
    return "`" + name.replace("`", "``") + "`"


def iceberg_jar() -> str | None:
    """The Iceberg Spark runtime for local runs: DP_ICEBERG_JAR, else ~/.dataplatform/jars/<jar>; None if absent."""
    path = Path(os.environ.get("DP_ICEBERG_JAR") or Path.home() / ".dataplatform/jars" / ICEBERG_JAR)
    return str(path) if path.exists() else None


class TableStore(Protocol):
    spark: SparkSession
    format: str

    def name(self, layer: str, table: str) -> str: ...
    def exists(self, layer: str, table: str) -> bool: ...
    def read(self, layer: str, table: str) -> DataFrame | None: ...
    def tables(self, layer: str) -> list[str]: ...
    def append(self, layer: str, table: str, df: DataFrame, app: str, txn: str) -> bool: ...
    def merge(self, layer: str, table: str, rows: DataFrame, key: str = "_row_key") -> None: ...
    def replace_where(self, layer: str, table: str, df: DataFrame, predicate: str) -> None: ...
    def overwrite(self, layer: str, table: str, df: DataFrame) -> None: ...
    def add_columns(self, layer: str, table: str, df: DataFrame) -> None: ...
    def upkeep(self, layer: str, table: str, keep_versions: int = 1) -> dict[str, Any]: ...
    def delete_where(self, layer: str, table: str, predicate: str) -> int: ...
    def version(self, layer: str, table: str) -> int | None: ...
    def read_at(self, layer: str, table: str, version: int) -> DataFrame | None: ...
    def drop(self, layer: str, table: str) -> None: ...


class _SqlStore:
    """What both formats share: MERGE and ADD COLUMNS in Spark SQL against the table's SQL name."""

    spark: SparkSession

    def name(self, layer: str, table: str) -> str:
        raise NotImplementedError

    def exists(self, layer: str, table: str) -> bool:
        raise NotImplementedError

    def read(self, layer: str, table: str) -> DataFrame | None:
        raise NotImplementedError

    def add_columns(self, layer: str, table: str, df: DataFrame) -> None:
        current = self.read(layer, table)
        if current is None:
            return
        have = {f.name for f in current.schema.fields}
        new = [f for f in df.schema.fields if f.name not in have and f.name != "_action"]
        if new:  # new columns reach the table without a release (ADR-008)
            added = ", ".join(f"{quoted(f.name)} {f.dataType.simpleString()}" for f in new)
            # DDL takes no bind parameters: every identifier is escaped by quoted(), types come from the engine's schema
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values in the statement
            self.spark.sql(f"ALTER TABLE {self.name(layer, table)} ADD COLUMNS ({added})")

    # shiftleft: allow SL-PURE-02 -- the write edge (ADR-013): IO belongs here
    def merge(self, layer: str, table: str, rows: DataFrame, key: str = "_row_key") -> None:
        self.add_columns(layer, table, rows)
        cols = [c for c in rows.columns if c != "_action"]
        view = f"_dp_merge_{abs(hash((layer, table))) % 10**8}"
        rows.createOrReplaceTempView(view)
        k = quoted(key)
        sets = ", ".join(f"t.{quoted(c)} = s.{quoted(c)}" for c in cols)
        names = ", ".join(quoted(c) for c in cols)
        vals = ", ".join(f"s.{quoted(c)}" for c in cols)
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); rows come from a view, not values
        self.spark.sql(
            f"MERGE INTO {self.name(layer, table)} t USING {view} s ON t.{k} = s.{k} "  # noqa: S608 -- escaped ids
            "WHEN MATCHED AND s._action = 'delete' THEN DELETE "
            f"WHEN MATCHED AND s._action = 'upsert' THEN UPDATE SET {sets} "
            f"WHEN NOT MATCHED AND s._action = 'upsert' THEN INSERT ({names}) VALUES ({vals})"
        )
        self.spark.catalog.dropTempView(view)


    # shiftleft: allow SL-PURE-06 -- the read edge (ADR-013): IO belongs here
    def version(self, layer: str, table: str) -> int | None:
        """The table's current version (Delta commit, Iceberg snapshot id), or None (INSP-01)."""
        if not self.exists(layer, table):
            return None
        # shiftleft: allow SL-SEC-01 -- the table name is built by name(), every part checked; no values
        row = self.spark.sql(f"DESCRIBE HISTORY {self.name(layer, table)} LIMIT 1").first()
        return int(row["version"]) if row else None

    # shiftleft: allow SL-PURE-06 -- the read edge (ADR-013): IO belongs here
    def read_at(self, layer: str, table: str, version: int) -> DataFrame | None:
        """The table as it was at `version` (time travel)."""
        if not self.exists(layer, table):
            return None
        # shiftleft: allow SL-SEC-01 -- name() checks every part; the version is an int
        sql = f"SELECT * FROM {self.name(layer, table)} VERSION AS OF {int(version)}"  # noqa: S608 -- checked, int
        return self.spark.sql(sql)

    # shiftleft: allow SL-PURE-02 -- the write edge (ADR-013): IO belongs here
    def delete_where(self, layer: str, table: str, predicate: str) -> int:
        """Delete the rows matching `predicate` (built by the caller from checked values only); returns how many."""
        current = self.read(layer, table)
        if current is None:
            return 0
        n = current.filter(predicate).count()
        if n:
            # shiftleft: allow SL-SEC-01 -- the predicate holds checked batch keys only (check_batch_key)
            self.spark.sql(f"DELETE FROM {self.name(layer, table)} WHERE {predicate}")  # noqa: S608 -- checked keys
        return int(n)


class DeltaPathStore(_SqlStore):
    """Delta tables in folders: <roots[layer]>/<table>."""

    format = "delta"

    def __init__(self, spark: SparkSession, roots: dict[str, str | Path]):
        self.spark, self.roots = spark, {k: Path(v) for k, v in roots.items()}

    def path(self, layer: str, table: str) -> Path:
        _check(layer, table)
        return self.roots[layer] / table

    def name(self, layer: str, table: str) -> str:
        return f"delta.{quoted(str(self.path(layer, table)))}"

    def exists(self, layer: str, table: str) -> bool:
        return (self.path(layer, table) / "_delta_log").exists()

    def read(self, layer: str, table: str) -> DataFrame | None:
        if not self.exists(layer, table):
            return None
        return self.spark.read.format("delta").load(str(self.path(layer, table)))

    def tables(self, layer: str) -> list[str]:
        root = self.roots[layer]
        return sorted(p.name for p in root.glob("*") if (p / "_delta_log").exists()) if root.exists() else []

    def append(self, layer: str, table: str, df: DataFrame, app: str, txn: str) -> bool:
        before = self._version(layer, table)
        (df.write.format("delta").mode("append").option("txnAppId", app).option("txnVersion", txn)
         .option("mergeSchema", "true").save(str(self.path(layer, table))))
        return self._version(layer, table) != before

    def _version(self, layer: str, table: str) -> int:
        if not self.exists(layer, table):
            return -1
        from delta.tables import DeltaTable  # noqa: PLC0415 -- delta-spark only needed by the Delta adapter

        row = DeltaTable.forPath(self.spark, str(self.path(layer, table))).history(1).select("version").first()
        return int(row["version"]) if row else -1

    def replace_where(self, layer: str, table: str, df: DataFrame, predicate: str) -> None:
        writer = df.write.format("delta")
        if self.exists(layer, table):
            writer = writer.mode("overwrite").option("replaceWhere", predicate)
        else:
            writer = writer.mode("append")
        writer.save(str(self.path(layer, table)))

    def overwrite(self, layer: str, table: str, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(self.path(layer, table)))

    def upkeep(self, layer: str, table: str, keep_versions: int = 1) -> dict[str, Any]:
        if not self.exists(layer, table):
            return {}
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
        self.spark.sql(f"OPTIMIZE {self.name(layer, table)}")
        return {"optimized": True}

    def drop(self, layer: str, table: str) -> None:
        import shutil  # noqa: PLC0415 -- only a rebuild removes tables

        shutil.rmtree(self.path(layer, table), ignore_errors=True)


class IcebergStore(_SqlStore):
    """Iceberg tables in a Spark catalog: <catalog>.<layer>.<table>. Locally a folder ("hadoop") catalog configured by
    local_session(iceberg_warehouse=...); on Databricks the workspace's Unity Catalog with managed Iceberg tables."""

    format = "iceberg"

    LOCAL_PROPERTIES: ClassVar[dict[str, str]] = {"format-version": "2"}
    # Unity Catalog managed Iceberg (probed on Free Edition 13 Nov): automatic clustering is refused unless deletion
    # vectors and row tracking are off at creation; then CLUSTER BY AUTO and OPTIMIZE work.
    MANAGED_PROPERTIES: ClassVar[dict[str, str]] = {"delta.enableDeletionVectors": "false",
                                                     "delta.enableRowTracking": "false"}

    def __init__(self, spark: SparkSession, catalog: str = "ice", namespaces: dict[str, str] | None = None,
                 properties: dict[str, str] | None = None):
        if not _NAME.match(catalog):
            raise ValueError(f"not a safe catalog name: {catalog}")
        self.spark, self.catalog = spark, catalog
        self.properties = dict(self.LOCAL_PROPERTIES if properties is None else properties)
        self.namespaces = namespaces or {layer: layer for layer in LAYERS}
        self._made: set[str] = set()

    def name(self, layer: str, table: str) -> str:
        _check(layer, table)
        return f"{quoted(self.catalog)}.{quoted(self.namespaces[layer])}.{quoted(table)}"

    def _with_properties(self, writer: Any) -> Any:
        for k, v in sorted(self.properties.items()):
            writer = writer.tableProperty(k, v)
        return writer

    def _plain(self, layer: str, table: str) -> str:
        return f"{self.catalog}.{self.namespaces[layer]}.{table}"

    def _prepare(self, layer: str, table: str) -> None:
        """Whatever must exist before a table is written (here: the layer's namespace)."""
        self._namespace(layer)

    def _namespace(self, layer: str) -> None:
        if layer not in self._made:
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {quoted(self.catalog)}.{quoted(self.namespaces[layer])}")
            self._made.add(layer)

    def exists(self, layer: str, table: str) -> bool:
        _check(layer, table)
        return bool(self.spark.catalog.tableExists(self._plain(layer, table)))

    def read(self, layer: str, table: str) -> DataFrame | None:
        return self.spark.table(self.name(layer, table)) if self.exists(layer, table) else None

    def tables(self, layer: str) -> list[str]:
        self._namespace(layer)
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
        rows = self.spark.sql(f"SHOW TABLES IN {quoted(self.catalog)}.{quoted(self.namespaces[layer])}").collect()
        return sorted(str(r["tableName"]) for r in rows)

    def _committed(self, layer: str, table: str, app: str, txn: str) -> bool:
        sql = f"SELECT summary FROM {self.name(layer, table)}.snapshots"  # noqa: S608 -- escaped identifiers only
        snaps = self.spark.sql(sql).collect()
        return any((r["summary"] or {}).get("dp-app") == app and (r["summary"] or {}).get("dp-txn") == txn
                   for r in snaps)

    def append(self, layer: str, table: str, df: DataFrame, app: str, txn: str) -> bool:
        self._prepare(layer, table)
        w = df.writeTo(self.name(layer, table)).option("snapshot-property.dp-app", app).option(
            "snapshot-property.dp-txn", txn)
        if not self.exists(layer, table):
            self._with_properties(w.using("iceberg")).create()
            return True
        if self._committed(layer, table, app, txn):
            return False
        self.add_columns(layer, table, df)
        w.append()
        return True

    # shiftleft: allow SL-PURE-02 -- the write edge (ADR-013): IO belongs here
    def replace_where(self, layer: str, table: str, df: DataFrame, predicate: str) -> None:
        """One commit that deletes the rows matching `predicate` and inserts `df`. Iceberg's overwrite-by-filter only
        drops whole files and refuses a file that holds rows on both sides of the filter, so this is a MERGE that
        matches nothing: every new row is inserted, every old row in the window is deleted (NOT MATCHED BY SOURCE)."""
        self._prepare(layer, table)
        if not self.exists(layer, table):
            self._with_properties(df.writeTo(self.name(layer, table)).using("iceberg")).create()
            return
        self.add_columns(layer, table, df)
        view = f"_dp_replace_{abs(hash((layer, table))) % 10**8}"
        df.createOrReplaceTempView(view)
        names = ", ".join(quoted(c) for c in df.columns)
        vals = ", ".join(f"s.{quoted(c)}" for c in df.columns)
        # the predicate is built by the caller from checked window names or a checked batch key, never from data
        # shiftleft: allow SL-SEC-01 -- identifiers escaped by quoted(); the predicate is built from checked values
        self.spark.sql(
            f"MERGE INTO {self.name(layer, table)} t USING {view} s ON 1 = 0 "  # noqa: S608 -- checked predicate
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({vals}) "
            f"WHEN NOT MATCHED BY SOURCE AND ({predicate}) THEN DELETE"
        )
        self.spark.catalog.dropTempView(view)

    def overwrite(self, layer: str, table: str, df: DataFrame) -> None:
        self._prepare(layer, table)
        self._with_properties(df.writeTo(self.name(layer, table)).using("iceberg")).createOrReplace()

    def upkeep(self, layer: str, table: str, keep_versions: int = 1) -> dict[str, Any]:
        if not self.exists(layer, table):
            return {}
        ident = f"{self.namespaces[layer]}.{table}"
        cat = quoted(self.catalog)
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
        done = self.spark.sql(f"CALL {cat}.system.rewrite_data_files(table => '{ident}')").first()
        older = "TIMESTAMP '9999-12-31 00:00:00'"
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
        exp = self.spark.sql(
            f"CALL {cat}.system.expire_snapshots(table => '{ident}', older_than => {older}, "
            f"retain_last => {int(keep_versions)})").first()
        return {"rewritten_files": int(done[0] or 0) if done else 0,
                "expired_files": int(exp[0] or 0) if exp else 0}

    # shiftleft: allow SL-PURE-06 -- the read edge (ADR-013): IO belongs here
    def version(self, layer: str, table: str) -> int | None:
        """Open-source Iceberg has no DESCRIBE HISTORY: the newest id in the snapshots metadata table."""
        if not self.exists(layer, table):
            return None
        # shiftleft: allow SL-SEC-01 -- name() checks every part; no values
        row = self.spark.sql(f"SELECT snapshot_id FROM {self.name(layer, table)}.snapshots "  # noqa: S608 -- checked
                             "ORDER BY committed_at DESC LIMIT 1").first()
        return int(row["snapshot_id"]) if row else None

    def drop(self, layer: str, table: str) -> None:
        if self.exists(layer, table):
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
            self.spark.sql(f"DROP TABLE {self.name(layer, table)} PURGE")


# B17 (14 Nov): Unity Catalog managed Iceberg v3 keeps deletion vectors, row tracking and clustering together (probed on
# Free Edition); v2 refuses clustering unless deletion vectors and row tracking are off. Gold stays v2 until the
# client's outside readers (federated warehouse, the exit drill engine) are confirmed to read v3.
FORMAT_VERSIONS: dict[str, str] = {"bronze": "3", "silver": "3", "gold": "2"}
# Clustering keys every table of a layer carries; empty = let the platform choose (CLUSTER BY AUTO).
CLUSTER_KEYS: dict[str, tuple[str, ...]] = {"bronze": ("_batch_key",), "silver": ("_key_hash",), "gold": ()}
V2_CLUSTERABLE: dict[str, str] = {"delta.enableDeletionVectors": "false", "delta.enableRowTracking": "false"}


def layer_properties(layer: str, versions: dict[str, str] | None = None) -> dict[str, str]:
    """The create-time table properties of a managed Iceberg table in this layer."""
    version = (versions or FORMAT_VERSIONS).get(layer, "2")
    if version not in ("2", "3"):
        raise ValueError(f"Iceberg format version {version!r} for {layer} is not 2 or 3")
    return {"format-version": "3"} if version == "3" else {"format-version": "2", **V2_CLUSTERABLE}


def cluster_clause(layer: str, columns: list[str], keys: dict[str, tuple[str, ...]] | None = None) -> str | None:
    """CLUSTER BY clause for a new table: the layer's keys that the table has, else AUTO; None when the layer is not
    clustered (landing, ops)."""
    table_keys = (keys or CLUSTER_KEYS).get(layer)
    if table_keys is None:
        return None
    have = [k for k in table_keys if k in columns]
    return f"({', '.join(quoted(k) for k in have)})" if have else "AUTO"


class UcIcebergStore(IcebergStore):
    """Unity Catalog managed Iceberg tables on Databricks (probed 13 Nov on Free Edition serverless):
    <env>_<layer>.<source or schema>.<dataset or name> (ADR-012: one catalog per environment and layer). Managed
    Iceberg has no readable snapshot metadata and refuses Delta writes, so a frame that carries batch keys is written
    by replacing those batch keys' rows in one MERGE commit (idempotent); upkeep is OPTIMIZE; clustering needs deletion
    vectors and row tracking off at creation (MANAGED_PROPERTIES)."""

    # shiftleft: allow SL-PURE-06 -- the read edge (ADR-013): IO belongs here
    def version(self, layer: str, table: str) -> int | None:
        """Unity Catalog managed Iceberg answers DESCRIBE HISTORY like Delta (its snapshots table is not readable)."""
        return _SqlStore.version(self, layer, table)

    def __init__(self, spark: SparkSession, env: str = "dev", properties: dict[str, str] | None = None,
                 versions: dict[str, str] | None = None):
        super().__init__(spark, "unused", None, self.MANAGED_PROPERTIES if properties is None else properties)
        if not _NAME.match(env):
            raise ValueError(f"not a safe environment name: {env}")
        self.env = env
        self._schemas: set[str] = set()
        self._override = properties  # a caller's own properties win for every layer (tests, probes)
        self.versions = dict(FORMAT_VERSIONS if versions is None else versions)

    using = "iceberg"  # the table provider a managed table is created with (UcDeltaUniformStore: delta)
    cluster = True  # set the layer's clustering after create (B17)

    def _table_properties(self, layer: str) -> dict[str, str]:
        return self._override if self._override is not None else layer_properties(layer, self.versions)

    def _create(self, layer: str, table: str, df: DataFrame, replace: bool = False) -> None:
        """Create with the layer's properties (format version), then set its clustering (B17)."""
        w = df.writeTo(self.name(layer, table)).using(self.using)
        for k, v in sorted(self._table_properties(layer).items()):
            w = w.tableProperty(k, v)
        w.createOrReplace() if replace else w.create()
        clause = cluster_clause(layer, list(df.columns)) if self.cluster else None
        if clause:
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
            self.spark.sql(f"ALTER TABLE {self.name(layer, table)} CLUSTER BY {clause}")

    def replace_where(self, layer: str, table: str, df: DataFrame, predicate: str) -> None:
        self._prepare(layer, table)
        if not self.exists(layer, table):
            self._create(layer, table, df)
            return
        super().replace_where(layer, table, df, predicate)

    def overwrite(self, layer: str, table: str, df: DataFrame) -> None:
        self._prepare(layer, table)
        self._create(layer, table, df, replace=True)

    @staticmethod
    def _split(table: str, layer: str = "") -> tuple[str, str]:
        """schema__table. The catalog already names the layer, so a schema that repeats it loses the prefix
        (gold's gold_sales__fact_otif -> <env>_gold.sales.fact_otif, as the contracts' physicalName says, ADR-012)."""
        schema, _, name = table.partition("__")
        if not (_NAME.match(schema) and _NAME.match(name.replace("__", "_"))):
            raise ValueError(f"not a schema__table name: {table}")
        if layer and schema.startswith(f"{layer}_") and len(schema) > len(layer) + 1:
            schema = schema[len(layer) + 1:]
        return schema, name

    def name(self, layer: str, table: str) -> str:
        _check(layer, table)
        schema, name = self._split(table, layer)
        return f"{quoted(f'{self.env}_{layer}')}.{quoted(schema)}.{quoted(name)}"

    def _plain(self, layer: str, table: str) -> str:
        schema, name = self._split(table, layer)
        return f"{self.env}_{layer}.{schema}.{name}"

    def _prepare(self, layer: str, table: str) -> None:
        schema, _ = self._split(table, layer)
        key = f"{layer}.{schema}"
        if key not in self._schemas:
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted(f'{self.env}_{layer}')}.{quoted(schema)}")
            self._schemas.add(key)

    def tables(self, layer: str) -> list[str]:
        """Every table of the layer's catalog as schema__table. SHOW reads the catalog itself (information_schema can
        lag a table created moments before, 13 Nov)."""
        catalog = quoted(f"{self.env}_{layer}")
        out = []
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
        for s in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect():
            schema = str(s[0])
            if schema in ("information_schema", "default"):
                continue
            # metric views live beside gold tables; SHOW VIEWS refuses a catalog-qualified schema, the catalog API
            # gives each object's type instead
            views = {t.name for t in self.spark.catalog.listTables(f"{self.env}_{layer}.{schema}")
                     if "VIEW" in str(t.tableType).upper()}
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
            for r in self.spark.sql(f"SHOW TABLES IN {catalog}.{quoted(schema)}").collect():
                if not r["isTemporary"] and r["tableName"] not in views:  # metric views live beside gold tables
                    out.append(f"{schema}__{r['tableName']}")
        return sorted(out)

    def append(self, layer: str, table: str, df: DataFrame, app: str, txn: str) -> bool:
        self._prepare(layer, table)
        if not self.exists(layer, table):
            self._create(layer, table, df)
            return True
        if "_batch_key" in df.columns:  # replace exactly these batches' rows: a rerun writes nothing twice
            keys = sorted({str(r[0]) for r in df.select("_batch_key").distinct().collect()})
            from dataplatform.ingest.ports import check_batch_key  # noqa: PLC0415 -- no ingest import at load

            listed = ", ".join(f"'{check_batch_key(k)}'" for k in keys) or "''"
            self.replace_where(layer, table, df, f"_batch_key IN ({listed})")
            return True
        self.add_columns(layer, table, df)
        df.writeTo(self.name(layer, table)).append()
        return True

    def upkeep(self, layer: str, table: str, keep_versions: int = 1) -> dict[str, Any]:
        if not self.exists(layer, table):
            return {}
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
        self.spark.sql(f"OPTIMIZE {self.name(layer, table)}")
        return {"optimized": 1}

    def drop(self, layer: str, table: str) -> None:
        # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted(); no values
        self.spark.sql(f"DROP TABLE IF EXISTS {self.name(layer, table)}")


# Managed Delta with UniForm (Delta tables that also publish Iceberg metadata, so Iceberg readers see them). Not yet
# probed on a workspace (owed, TODO B21): IcebergCompatV2 needs column mapping by name and no deletion vectors.
UNIFORM_PROPERTIES: dict[str, str] = {
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
    "delta.columnMapping.mode": "name",
    "delta.enableDeletionVectors": "false",
}


class UcDeltaUniformStore(UcIcebergStore):
    """Unity Catalog managed Delta tables with UniForm Iceberg reads: the same names, batch-key replace, DESCRIBE
    HISTORY version and OPTIMIZE upkeep as UcIcebergStore (ADR-012 catalogs <env>_<layer>); only the provider and the
    create-time properties differ. Clustering is off until it is probed together with UniForm."""

    format = "delta-uniform"
    using = "delta"
    cluster = False

    def _table_properties(self, layer: str) -> dict[str, str]:
        return dict(UNIFORM_PROPERTIES) if self._override is None else self._override


S3TABLES_CATALOG_IMPL = "software.amazon.s3tables.iceberg.S3TablesCatalog"
_S3TABLES_ARN = re.compile(r"^arn:aws[a-z-]*:s3tables:[a-z0-9-]+:\d{12}:bucket/[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")


def s3tables_spark_conf(bucket_arn: str, catalog: str = "s3t") -> dict[str, str]:
    """The Spark settings that make `catalog` an AWS S3 Tables Iceberg catalog on the table bucket `bucket_arn`
    (the s3-tables-catalog-for-iceberg runtime jar must be on the session). A session builder applies them."""
    if not _NAME.match(catalog):
        raise ValueError(f"not a safe catalog name: {catalog}")
    if not _S3TABLES_ARN.match(bucket_arn):
        raise ValueError(f"not an S3 Tables bucket ARN: {bucket_arn}")
    return {f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog}.catalog-impl": S3TABLES_CATALOG_IMPL,
            f"spark.sql.catalog.{catalog}.warehouse": bucket_arn,
            f"spark.sql.catalog.{catalog}.cache-enabled": "false"}


class S3TablesStore(IcebergStore):
    """Iceberg tables in an AWS S3 Tables bucket: <catalog>.<layer>.<table>, one namespace per layer. AWS runs the
    maintenance (compaction, snapshot expiry, unreferenced-file removal), so upkeep only reports that; tables are
    format v2; dropping does not purge (the service owns the files)."""

    format = "s3tables"

    def upkeep(self, layer: str, table: str, keep_versions: int = 1) -> dict[str, Any]:
        return {"managed": 1} if self.exists(layer, table) else {}

    def drop(self, layer: str, table: str) -> None:
        if self.exists(layer, table):
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by quoted() or checked by _check(); no values
            self.spark.sql(f"DROP TABLE {self.name(layer, table)}")


TABLE_FORMATS = ("delta", "iceberg", "iceberg-uc", "delta-uc-uniform", "s3tables")
CATALOG_BY_ENV = ("iceberg-uc", "delta-uc-uniform")  # catalogs named <env>_<layer> (ADR-012), made by Unity Catalog


def store_catalog(table_format: str, env: str) -> str:
    """The catalog name make_store takes for a format: the environment for Unity Catalog formats, the session's
    catalog (S3 Tables) or the local folder catalog for the rest."""
    return env if table_format in CATALOG_BY_ENV else "s3t" if table_format == "s3tables" else "ice"


def make_store(spark: SparkSession, table_format: str, out: str | Path, catalog: str = "ice") -> TableStore:
    """The store for a run under `out`: delta folders out/<layer>/, the Iceberg catalog set up by
    local_session(iceberg_warehouse=out/iceberg), Unity Catalog managed Iceberg or Delta+UniForm (catalog = the
    environment), or an S3 Tables catalog the session already has (catalog = its name, default s3t)."""
    if table_format == "delta":
        return DeltaPathStore(spark, {layer: Path(out) / layer for layer in LAYERS})
    if table_format == "iceberg":
        return IcebergStore(spark, catalog)
    if table_format == "iceberg-uc":
        return UcIcebergStore(spark, catalog if catalog != "ice" else "dev")
    if table_format == "delta-uc-uniform":
        return UcDeltaUniformStore(spark, catalog if catalog != "ice" else "dev")
    if table_format == "s3tables":
        return S3TablesStore(spark, catalog if catalog != "ice" else "s3t")
    raise ValueError(f"unknown table_format {table_format!r} (delta, iceberg, iceberg-uc, delta-uc-uniform, s3tables)")


class StoreBronzeWriter:
    """BronzeWriter (dataplatform.ingest.ports) through the table port: bronze table <source>__<dataset>. One write
    replaces the rows of its batch key, so a batch written twice leaves the table unchanged (DUP-01) and a failed
    write leaves nothing (ORCH-03); the first write creates the table."""

    TYPES: ClassVar[dict[str, str]] = {"string": "STRING", "integer": "BIGINT", "number": "DOUBLE",
                                       "boolean": "BOOLEAN", "timestamp": "TIMESTAMP", "date": "DATE"}
    _TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")

    def __init__(self, store: TableStore):
        self.store = store

    def _table(self, table: str) -> str:
        if not self._TABLE.match(table):
            raise ValueError(f"not a source.dataset table name: {table!r}")
        return table.replace(".", "__")

    # shiftleft: allow SL-PURE-02 -- the write edge of bronze (ADR-013): IO belongs here
    def write(self, table: str, batch_key: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]],
              replace: bool = False) -> int:
        from dataplatform.ingest.ports import check_batch_key  # noqa: PLC0415 -- no ingest import at load

        check_batch_key(batch_key)  # it goes into the replace predicate
        ddl = ", ".join(f"{quoted(c)} {self.TYPES[t]}" for c, t in columns)
        df = self.store.spark.createDataFrame([tuple(r.get(c) for c, _ in columns) for r in rows], ddl)
        name = self._table(table)
        # A new batch is appended (cheap; the batch key is the write's idempotency mark). Only a batch written before,
        # a reload, takes the replace path, which rewrites the rows of that batch key in one commit. Replacing on
        # every write scanned the whole bronze table per batch (13 Nov: most of a 3-day run's time).
        # Delta's write mark must rise, so Delta always takes its native replace (cheap and idempotent there)
        delta = self.store.format == "delta"
        if delta or replace or not self.store.append("bronze", name, df, f"bronze:{name}", batch_key):
            self.store.replace_where("bronze", name, df, f"_batch_key = '{batch_key}'")
        return len(rows)

    # shiftleft: allow SL-PURE-02 -- the write edge of bronze (ADR-013): IO belongs here
    def write_many(self, table: str, batches: list[tuple[str, list[dict[str, Any]]]],
                   columns: list[tuple[str, str]]) -> int:
        """New batches in one append: one commit. The mark is the batch keys' digest, so a retry of the same set after
        a crash between the commit and the file log writes nothing twice."""
        import hashlib  # noqa: PLC0415 -- only the batched write needs it

        from dataplatform.ingest.ports import check_batch_key  # noqa: PLC0415 -- no ingest import at load

        if not batches:
            return 0
        keys = sorted(check_batch_key(k) for k, _ in batches)
        ddl = ", ".join(f"{quoted(c)} {self.TYPES[t]}" for c, t in columns)
        rows = [tuple(r.get(c) for c, _ in columns) for _, part in batches for r in part]
        df = self.store.spark.createDataFrame(rows, ddl)
        name = self._table(table)
        if self.store.format == "delta":  # one commit replacing exactly these batch keys
            self.store.replace_where("bronze", name, df, "_batch_key IN ({})".format(", ".join(f"'{k}'" for k in keys)))
        else:
            self.store.append("bronze", name, df, f"bronze:{name}",
                              hashlib.sha256("|".join(keys).encode()).hexdigest())
        return len(rows)

    def _rows(self, table: str, where: dict[str, str] | None) -> DataFrame | None:
        df = self.store.read("bronze", self._table(table))
        if df is None:
            return None
        for c, v in (where or {}).items():
            df = df.where(F.col(c).cast("string") == F.lit(v))
        return df

    # shiftleft: allow SL-PURE-06 -- the read edge of bronze (ADR-013): IO belongs here
    def fingerprint(self, table: str, where: dict[str, str] | None = None,
                    sums: tuple[str, ...] = ()) -> dict[str, float]:
        df = self._rows(table, where)
        if df is None:
            return {"row_count": 0.0, **{c: 0.0 for c in sums}}
        row = df.agg(F.count(F.lit(1)).alias("row_count"), *[F.sum(c).alias(c) for c in sums]).first()
        return {k: float((row[k] if row else 0) or 0) for k in ("row_count", *sums)}

    # shiftleft: allow SL-PURE-06 -- the read edge of bronze (ADR-013): IO belongs here
    def keys(self, table: str, columns: tuple[str, ...], where: dict[str, str]) -> set[tuple[Any, ...]]:
        df = self._rows(table, where)
        if df is None:
            return set()
        return {tuple(r[c] for c in columns) for r in df.select(*columns).distinct().collect()}

    def remove(self, table: str, batch_keys: list[str]) -> int:
        """Housekeeping (B8): delete the rows of these batches in one commit; returns the rows removed."""
        from dataplatform.ingest.ports import check_batch_key  # noqa: PLC0415 -- no ingest import at load

        for k in batch_keys:
            check_batch_key(k)  # each goes into the delete predicate
        if not batch_keys:
            return 0
        keys = ", ".join(f"'{k}'" for k in batch_keys)
        return self.store.delete_where("bronze", self._table(table), f"_batch_key IN ({keys})")
