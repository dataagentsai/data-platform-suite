"""Small builders for the silver tests: a dataset view as the metadata store hands it over, and frames from rows."""

from __future__ import annotations

import itertools

from datetime import UTC, date, datetime
from typing import Any

from dataplatform.silver.plan import SilverPlan, plan_from_view

TOKEN = r"^tok_[0-9a-f]{24}$"
T0 = datetime(2026, 11, 9, 0, 0, tzinfo=UTC)


def ts(minutes: int) -> datetime:
    return datetime.fromtimestamp(T0.timestamp() + 60 * minutes, UTC)


def view(
    columns: list[tuple[str, str]] | None = None,
    keys: tuple[str, ...] = ("id",),
    kind: str = "event",
    history: dict[str, Any] | None = None,
    load: dict[str, Any] | None = None,
    dedup: dict[str, Any] | None = None,
    read: str = "change_feed",
    column_settings: list[dict[str, Any]] | None = None,
    quality: dict[str, Any] | None = None,
    settings: dict[str, str] | None = None,
    required: tuple[str, ...] = (),
    restricted: tuple[str, ...] = (),
    reconciliation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cols = columns or [("id", "string"), ("v", "integer"), ("seq", "integer")]
    manifest: dict[str, Any] = {
        "history": {"kind": kind, **(history or {})},
        "load": {"pattern": "append", "batch_key": ["source_batch_id"], "new_columns": "hold_in_bronze",
                 "deletes": {"detect": "none", "apply": "none"}, **(load or {})},
        "ingest": {"read": read},
        "reconciliation": reconciliation or {"measures": [{"name": "row_count"}]},
    }
    if dedup is not None:
        manifest["dedup"] = dedup
    if column_settings is not None:
        manifest["columns"] = column_settings
    if quality is not None:
        manifest["quality"] = quality
    return {
        "source": "src",
        "dataset": "things",
        "ingest_read": read,
        "history_kind": kind,
        "fingerprint": "fp",
        "settings": settings or {},
        "columns": [
            {
                "name": n,
                "logical_type": t,
                "required": n in keys or n in required,
                "primary_key_position": keys.index(n) + 1 if n in keys else None,
                "classification": "restricted" if n in restricted else "",
                "description": "",
            }
            for n, t in cols
        ],
        "business_keys": {"primary": list(keys), "dedup": list((dedup or {}).get("business_keys") or [])},
        "manifest": manifest,
    }


def plan(**kw: Any) -> SilverPlan:
    return plan_from_view(view(**kw), TOKEN)


def frame(spark: Any, ddl: str, rows: list[tuple[Any, ...]], window: str = "2026-11-09", batch: str = "b1") -> Any:
    """Bronze-shaped rows: the given columns plus the lander's load columns."""
    df = spark.createDataFrame(rows, ddl)
    from pyspark.sql import functions as F

    for c, v in (("_batch_key", batch), ("_run_id", "ingest-1"), ("_source_file", "f1"), ("_window", window)):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(v))
    if "_extra" not in df.columns:
        df = df.withColumn("_extra", F.lit(None).cast("string"))
    return df


def hold(df: Any) -> Any:
    """Fix a frame's rows so a later join with its own source reads them as they are: a local checkpoint on Spark,
    a DuckDB temporary table under SQLFrame (which has no checkpoint; each frame there is one SQL query)."""
    if type(df).__module__.startswith("sqlframe"):
        from dataplatform.adapters.duckdb_frames import checkpoint

        return checkpoint(df)
    return df.localCheckpoint()


def stage_hold(df: Any) -> Any:
    """What the engine's stage runner fixes each stage's output with: SQLFrame's checkpoint (DuckStageRunner does the
    same), nothing on Spark (SparkStageRunner checkpoints once at the end)."""
    return hold if type(df).__module__.startswith("sqlframe") else None


def rows(df: Any, *cols: str) -> list[tuple[Any, ...]]:
    """The given columns of every row, sorted (None sorts first)."""
    out = [tuple(r[c] for c in cols) for r in df.select(*cols).collect()]
    return sorted(out, key=lambda t: tuple((x is not None, str(x)) for x in t))


__all__ = ["T0", "TOKEN", "date", "frame", "hold", "plan", "stage_hold", "rows", "ts", "view"]


def assert_history_invariants(silver: Any) -> None:
    """The SCD2 test kit's properties (shift-left-bundle src/shiftleft/testkit.py: assert_unique, the validity
    checks of examples/scd2), engine-neutral so they run on Spark and on DuckDB: every row key once, at most one
    current version per business key, and the versions of a key never overlap."""
    got = [r.asDict() for r in silver.collect()]
    keys = [r["_row_key"] for r in got]
    assert len(keys) == len(set(keys)), "a row key appears twice"
    current: dict[Any, int] = {}
    spans: dict[Any, list[tuple[Any, Any]]] = {}
    for r in got:
        current[r["_key_hash"]] = current.get(r["_key_hash"], 0) + int(bool(r["_is_current"]))
        spans.setdefault(r["_key_hash"], []).append((r["_valid_from"], r["_valid_to"]))
    assert all(n <= 1 for n in current.values()), "a key has more than one current version"
    for key, s in spans.items():
        ordered = sorted(s, key=lambda x: (x[0] is not None, x[0]))
        for (_, end), (start, _) in itertools.pairwise(ordered):
            assert end is not None, f"versions of {key} overlap: an open version is not the last"
            assert start is not None, f"versions of {key}: a version has no start"
            assert end <= start, f"versions of {key} overlap"


def assert_nothing_dropped(batch_keys: set[Any], silver: Any, quarantine: Any | None, column: str = "id") -> None:
    """The kit's assert_nothing_dropped: every input key is in silver or in quarantine."""
    landed = {r[column] for r in silver.select(column).collect()}
    held = set()
    if quarantine is not None:
        import json

        held = {json.loads(r["_record"]).get(column) for r in quarantine.select("_record").collect()}
    lost = batch_keys - landed - held
    assert not lost, f"input keys in neither silver nor quarantine: {sorted(lost)[:5]}"
