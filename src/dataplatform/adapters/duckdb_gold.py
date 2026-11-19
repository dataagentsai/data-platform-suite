"""Gold's engine operations on DuckDB through SQLFrame (docs/DUCKDB-SPIKE.md): the Spark adapter's operations, with
the five engine-specific pieces replaced. For laptop and CI runs only; Databricks keeps adapters/spark_gold.py.

Needs `duckdb_frames.activate()` before this module is imported (spark_gold's `pyspark` imports must resolve to
SQLFrame).

    surrogate   Spark's conv(hex, 16, 10) has no SQLFrame function: the 15 hex digits are added up as numbers (the
                same 60-bit key as keys.surrogate_key, tested against the Python recipe)
    row_hash    xxhash64 -> DuckDB's hash (window digests are compared within one engine's runs only)
    sql         Spark binds :name parameters itself; here they are bound into the parsed query by sqlglot as
                literals (quoted by sqlglot, never pasted as text)
    hold        localCheckpoint -> the query run once into a DuckDB table
    release     nothing to unpersist
"""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from dataplatform.adapters.duckdb_frames import checkpoint
from dataplatform.adapters.duckdb_silver import read_delta, to_arrow
from dataplatform.adapters.spark_gold import _WINDOW, DeltaGoldTables, SparkGoldEngine, _safe, key_text
from dataplatform.gold.keys import HEX_DIGITS
from dataplatform.gold.plan import GoldPlan


def surrogate(dataset: str, key: list[Column], version_start: Column | None) -> Column:
    """keys.surrogate_key: the first 15 hex digits of SHA-256(key text) as a whole number, digit by digit (a hex
    digit's value from its character code: 0-9 are 48-57, a-f are 97-102)."""
    digest = F.sha2(key_text(dataset, key, version_start), 256)
    codes = [F.ascii(F.substring(digest, i + 1, 1)) for i in range(HEX_DIGITS)]
    digits = [F.when(c < 58, c - 48).otherwise(c - 87).cast("long") * F.lit(16 ** (HEX_DIGITS - 1 - i))
              for i, c in enumerate(codes)]
    return reduce(lambda a, b: a + b, digits).cast("long")


def bind(text: str, params: dict[str, str]) -> str:
    """The query with each :name parameter replaced by its value as a SQL string literal (sqlglot quotes it)."""
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(text, dialect="spark")

    def literal(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Placeholder) and node.name in params:
            return exp.Literal.string(params[node.name])
        return node

    return str(tree.transform(literal).sql(dialect="spark"))


class DuckGoldEngine(SparkGoldEngine):
    surrogate = staticmethod(surrogate)

    @staticmethod
    def row_hash(columns: list[str]) -> Column:
        return F.hash(*[F.col(c) for c in columns])

    def sql(self, text: str, params: dict[str, str]) -> Any:
        return self.spark.sql(bind(text, params))

    def hold(self, frame: Any) -> Any:
        return checkpoint(frame)

    def release(self, *frames: Any) -> None:
        del frames


class DeltaRsGoldTables(DeltaGoldTables):
    """The read and write edges of gold on Delta folders, by delta-rs; the frames are DuckDB's (SQLFrame)."""

    def _read(self, path: Path) -> Any | None:
        return read_delta(self.spark, path)

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver(self, dataset: str, columns: list[tuple[str, str]]) -> Any:
        source, name = dataset.split(".", 1)
        df = self._read(self.silver_root / f"{_safe(source)}__{_safe(name)}")
        if df is not None:
            return df
        own = [("_valid_from", "timestamp"), ("_valid_to", "timestamp"), ("_is_current", "boolean"),
               ("_is_deleted", "boolean"), ("_seq_from", "bigint"), ("_row_key", "string")]
        # plain names (checked): SQLFrame keeps backticks as part of a name
        return self.spark.createDataFrame([], ", ".join(f"{_safe(c)} {t}" for c, t in [*columns, *own]))

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver_version(self, dataset: str) -> int | None:
        path = self._silver_path(dataset)
        if not (path / "_delta_log").exists():
            return None
        from deltalake import DeltaTable  # noqa: PLC0415 -- delta-rs only where this adapter runs

        return int(DeltaTable(str(path)).version())

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver_at(self, dataset: str, version: int, columns: list[tuple[str, str]]) -> Any:
        df = read_delta(self.spark, self._silver_path(dataset), int(version))
        if df is None:
            raise ValueError(f"{dataset}: no silver table to read at version {version}")
        return df

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def write_dimension(self, plan: GoldPlan, rows: Any) -> int:
        from deltalake import write_deltalake

        data = to_arrow(rows)
        write_deltalake(str(self.path(plan)), data, mode="overwrite", schema_mode="overwrite")
        return int(data.num_rows)

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def replace_windows(self, plan: GoldPlan, rows: Any, windows: list[str]) -> int:
        from deltalake import write_deltalake

        assert plan.window is not None
        bad = [w for w in windows if not _WINDOW.match(w)]
        if bad:
            raise ValueError(f"not window names: {bad[:3]}")
        path, data = self.path(plan), to_arrow(rows)
        if not (path / "_delta_log").exists():
            write_deltalake(str(path), data, mode="overwrite")
            return int(data.num_rows)
        col = _safe(plan.window.column)
        if plan.window.unit == "hour":  # an instant: the window name is its UTC hour
            values = ", ".join(f"arrow_cast('{w[:10]}T{w[11:19]}Z', 'Timestamp(Microsecond, Some(\"UTC\"))')"
                               for w in windows)
        else:
            values = ", ".join(f"arrow_cast('{w}', 'Date32')" for w in windows)
        # window names checked against _WINDOW above, the column name against _safe: no data in the predicate text
        write_deltalake(str(path), data, mode="overwrite", predicate=f"{col} IN ({values})")
        return int(data.num_rows)


class DuckGoldReader:
    """GoldReader on the gold folders through DuckDB: each table a DuckDB table under its own name, the question
    (Spark SQL, as the metric views and the judge write it) bound and transpiled to DuckDB by sqlglot."""

    def __init__(self, spark: Any, gold_root: str | Path):
        self.spark, self.gold_root = spark, Path(gold_root)

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def rows(self, sql: str, params: dict[str, str] | None = None,
             time_zone: str | None = None) -> list[dict[str, Any]]:
        import duckdb
        import sqlglot
        from deltalake import DeltaTable

        conn = duckdb.connect(config={"threads": 2, "memory_limit": "1GB", "TimeZone": time_zone or "UTC"})
        try:
            for folder in sorted(self.gold_root.glob("*__*")):
                if (folder / "_delta_log").exists():
                    conn.register(_safe(folder.name.split("__", 1)[1]), DeltaTable(str(folder)).to_pyarrow_dataset())
            query = sqlglot.transpile(bind(sql, dict(params or {})), read="spark", write="duckdb")[0]
            try:
                cur = conn.execute(query)
            except duckdb.CatalogException as exc:  # the judge reads a missing table as no rows
                raise LookupError(f"TABLE_OR_VIEW_NOT_FOUND: {exc}") from exc
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
        finally:
            conn.close()
