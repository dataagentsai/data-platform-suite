"""The release gate's ports (dataplatform.twin.gate) on Databricks: Delta SHALLOW CLONE and one measuring query.

Runs through a SQL runner `(sql, params) -> list[dict]`, like databricks_system.py. Table and column names are
checked against the plain-name rule before they are quoted; no value is ever put into SQL text.

STATUS: written, not yet run against a workspace. Its rows in the gate's contract table
(tests/dataplatform/test_twin_gate.py) are pending until a workspace is bound (DP_GATE_WAREHOUSE), so this adapter
must not be named in a binding before they pass (ADR-020). The SQL it would send is unit-tested.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from dataplatform.twin.gate import TableMeasures, check_name

SqlRunner = Callable[[str, dict[str, Any]], list[dict[str, Any]]]
_COL = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _q(name: str) -> str:
    """Quote a dotted table name or a column, part by part."""
    parts = name.split(".")
    for p in parts:
        if not _COL.match(p):
            raise ValueError(f"not a plain name: {name!r}")
    return ".".join(f"`{p}`" for p in parts)


def clone_sql(source: str, target: str) -> str:
    return f"CREATE OR REPLACE TABLE {_q(check_name(target))} SHALLOW CLONE {_q(check_name(source))}"


def drop_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {_q(check_name(table))}"


def measure_sql(table: str, keys: tuple[str, ...], columns: tuple[str, ...], window: str = "") -> str:
    """One row for the table, or one row per window value (`_w`) when `window` names the column (DIFF-01)."""
    key_hash = f"xxhash64({', '.join(_q(k) for k in keys)})" if keys else "0"
    sums = "".join(f", CAST(SUM({_q(c)}) AS DOUBLE) AS {_q('sum_' + c)}" for c in columns)
    w = f"CAST({_q(window)} AS STRING) AS `_w`, " if window else ""
    head = f"SELECT {w}COUNT(*) AS `rows`, CAST(COALESCE(BIT_XOR({key_hash}), 0) AS STRING) AS `keys_digest`{sums}"
    tail = f" GROUP BY {_q(window)}" if window else ""
    return f"{head} FROM {_q(check_name(table))}{tail}"  # nosec B608 - names pass _q's plain-name rule


class DatabricksTables:
    def __init__(self, sql: SqlRunner) -> None:
        self.sql = sql

    def clone(self, source: str, target: str) -> None:
        self.sql(clone_sql(source, target), {})

    def drop(self, table: str) -> None:
        self.sql(drop_sql(table), {})

    def measure(self, table: str, keys: tuple[str, ...], columns: tuple[str, ...],
                window: str = "") -> TableMeasures:
        def one(row: dict[str, Any]) -> TableMeasures:
            sums = {c: round(float(row.get(f"sum_{c}") or 0.0), 6) for c in columns}
            return TableMeasures(table, int(row["rows"]), sums, str(row["keys_digest"]))

        whole = one(self.sql(measure_sql(table, keys, columns), {})[0])
        if not window:
            return whole
        per = {str(r["_w"]): one(r) for r in self.sql(measure_sql(table, keys, columns, window), {})}
        return TableMeasures(table, whole.rows, whole.sums, whole.keys_digest, per)
