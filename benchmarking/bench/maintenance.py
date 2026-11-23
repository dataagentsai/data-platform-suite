"""Table maintenance probes (benchmarking/scenarios/maintenance.yaml M-01 to M-09), run on the office workspace.

The point is to find out what each format really supports, so every statement is tried and its outcome recorded: ok
with its time, or the error the engine gave. A probe that fails is a result, not a crash. Which statements to try is
a table (PROBES): add a row to try another. Spark only; the unit tests use a recording stand-in.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

_ERR = 300
_SAFE = re.compile(r"^[A-Za-z_`][A-Za-z0-9_.`-]*$")  # plain or backtick-quoted dotted names; no space, quote or ;


@dataclass(frozen=True)
class Probe:
    op: str                     # M-xx id in maintenance.yaml
    name: str
    statements: tuple[str, ...]  # {t} = the table's full name, {ident} = namespace.table, {catalog} = the catalog
    formats: tuple[str, ...] | None = None  # None = every format
    needs_rows: bool = False    # run after some rows were deleted (M-06)


PROBES: tuple[Probe, ...] = (
    Probe("M-01", "compact small files (Databricks)", ("OPTIMIZE {t}",), ("iceberg-uc", "delta-uc-uniform")),
    Probe("M-01", "compact small files (Iceberg procedure)",
          ("CALL {catalog}.system.rewrite_data_files(table => '{ident}')",), ("s3tables",)),
    Probe("M-02", "remove old files (Databricks)", ("VACUUM {t}",), ("iceberg-uc", "delta-uc-uniform")),
    Probe("M-02", "expire snapshots (Iceberg procedure)",
          ("CALL {catalog}.system.expire_snapshots(table => '{ident}', retain_last => 1)",), ("s3tables",)),
    Probe("M-02", "remove orphan files (Iceberg procedure)",
          ("CALL {catalog}.system.remove_orphan_files(table => '{ident}')",), ("s3tables",)),
    Probe("M-03", "layout by clustering", ("ALTER TABLE {t} CLUSTER BY (k)",), ("iceberg-uc", "delta-uc-uniform")),
    Probe("M-04", "column statistics", ("ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS",)),
    Probe("M-06", "purge deleted rows (Databricks)", ("DELETE FROM {t} WHERE k % 2 = 0", "OPTIMIZE {t}"),
          ("iceberg-uc", "delta-uc-uniform")),
    Probe("M-06", "purge deleted rows (Iceberg)",
          ("DELETE FROM {t} WHERE k % 2 = 0", "CALL {catalog}.system.rewrite_data_files(table => '{ident}')"),
          ("s3tables",)),
    Probe("M-08", "rewrite manifests (Iceberg procedure)",
          ("CALL {catalog}.system.rewrite_manifests(table => '{ident}')",), ("s3tables",)),
    Probe("M-08", "metadata history (Databricks)", ("DESCRIBE HISTORY {t}",), ("iceberg-uc", "delta-uc-uniform")),
)


def applies(p: Probe, fmt: str) -> bool:
    return p.formats is None or fmt in p.formats


def render(statement: str, table: str, ident: str, catalog: str) -> str:
    """Fill a statement. The names come from the platform's own store (checked identifiers), never from outside."""
    for part in (table, ident, catalog):
        if not _SAFE.match(part):
            raise ValueError(f"not a usable name: {part!r}")
    return statement.format(t=table, ident=ident, catalog=catalog)


def count_files(spark: Any, table: str, fmt: str) -> int | None:
    """Data files in the table now (None if the format will not say)."""
    sql = f"DESCRIBE DETAIL {table}" if fmt != "s3tables" else f"SELECT count(*) AS n FROM {table}.files"
    try:
        row = spark.sql(sql).first()
        return int(row["numFiles"] if fmt != "s3tables" else row["n"])
    except Exception:  # noqa: BLE001 -- a format that cannot answer is a result, recorded as None
        return None


def timed_query(spark: Any, table: str) -> float | None:
    """Seconds for one scan with an aggregate: the stand-in for a report query."""
    t0 = time.monotonic()
    try:
        spark.sql(f"SELECT count(*), sum(v), count(DISTINCT k) FROM {table}").collect()
    except Exception:  # noqa: BLE001 -- recorded as None
        return None
    return round(time.monotonic() - t0, 3)


def run_probes(spark: Any, fmt: str, table: str, ident: str, catalog: str,
               probes: tuple[Probe, ...] = PROBES, clock: Any = time.monotonic) -> list[dict[str, Any]]:
    """Try each probe that applies to `fmt` on `table`; one result row per statement."""
    results: list[dict[str, Any]] = []
    for p in probes:
        if not applies(p, fmt):
            continue
        files_before, q_before = count_files(spark, table, fmt), timed_query(spark, table)
        for template in p.statements:
            stmt = render(template, table, ident, catalog)
            t0 = clock()
            row: dict[str, Any] = {"op": p.op, "probe": p.name, "format": fmt, "statement": stmt}
            try:
                spark.sql(stmt).collect()
                row.update(ok=True, error="")
            except Exception as exc:  # noqa: BLE001 -- the engine's refusal is the finding
                row.update(ok=False, error=str(exc).replace("\n", " ")[:_ERR])
            row["seconds"] = round(clock() - t0, 3)
            results.append(row)
        results[-1].update(files_before=files_before, files_after=count_files(spark, table, fmt),
                           query_s_before=q_before, query_s_after=timed_query(spark, table))
    return results


def verdicts(results: list[dict[str, Any]]) -> dict[str, str]:
    """Per operation: supported (every statement of every probe worked), partial or refused."""
    by_op: dict[str, list[bool]] = {}
    for r in results:
        by_op.setdefault(r["op"], []).append(bool(r["ok"]))
    return {op: "supported" if all(v) else "refused" if not any(v) else "partial" for op, v in sorted(by_op.items())}
