"""The bronze writer on Delta tables in a local folder, through delta-rs (`deltalake`, Apache 2.0) and pyarrow: no JVM,
no Spark, the same approach as LakeIngestionFramework's LocalDeltaWriter (core/writers/local_delta.py), which only
appends; this one replaces by batch key. Needs `--with deltalake --with pyarrow`.

Layout: <root>/<source>__<dataset>/ is one Delta table. A write is one Delta commit that replaces the rows of its
batch key (an overwrite with a predicate), so writing a batch twice leaves the table as it was (DUP-01) and a failed
write leaves nothing (ORCH-03). Columns are typed from the contract's logical types.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from dataplatform.ingest.ports import Columns, Row, check_batch_key

_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


def _arrow_type(logical: str) -> Any:
    import pyarrow as pa

    return {
        "string": pa.string(),
        "integer": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
    }[logical]


class DeltaBronze:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self._tables: dict[str, Any] = {}  # open table handles: a write through one keeps it current

    def _handle(self, table: str) -> Any:
        from deltalake import DeltaTable

        if table not in self._tables:
            path = self.path(table)
            if not (path / "_delta_log").exists():
                return None
            self._tables[table] = DeltaTable(str(path))
        else:
            self._tables[table].update_incremental()  # another process may have written since it was opened
        return self._tables[table]

    def path(self, table: str) -> Path:
        if not _TABLE.match(table):
            raise ValueError(f"not a source.dataset table name: {table!r}")
        return self.root / table.replace(".", "__")

    def write(self, table: str, batch_key: str, rows: list[Row], columns: Columns, replace: bool = False) -> int:
        import pyarrow as pa
        from deltalake import write_deltalake

        check_batch_key(batch_key)  # it goes into the overwrite predicate
        schema = pa.schema([(c, _arrow_type(t)) for c, t in columns])
        data = pa.Table.from_pylist([{c: r.get(c) for c, _ in columns} for r in rows], schema=schema)
        handle = self._handle(table)
        write_deltalake(
            handle if handle is not None else str(self.path(table)),
            data,
            mode="overwrite",
            predicate=f"_batch_key = '{batch_key}'",
        )
        return int(data.num_rows)

    def write_many(self, table: str, batches: list[tuple[str, list[Row]]], columns: Columns) -> int:
        """New batches in one Delta commit; the lander logs them only after it returns."""
        import pyarrow as pa
        from deltalake import write_deltalake

        if not batches:
            return 0
        for k, _ in batches:
            check_batch_key(k)
        schema = pa.schema([(c, _arrow_type(t)) for c, t in columns])
        data = pa.Table.from_pylist([{c: r.get(c) for c, _ in columns} for _, part in batches for r in part],
                                    schema=schema)
        handle = self._handle(table)
        keys = ", ".join(f"'{k}'" for k, _ in batches)  # each checked by check_batch_key above
        # one commit that replaces exactly these batch keys: idempotent if a crash came between commit and file log
        write_deltalake(handle if handle is not None else str(self.path(table)), data, mode="overwrite",
                        predicate=f"_batch_key IN ({keys})")
        return int(data.num_rows)

    def remove(self, table: str, batch_keys: list[str]) -> int:
        """Housekeeping (B8): delete the rows of these batches in one Delta commit; returns the rows removed."""
        for k in batch_keys:
            check_batch_key(k)  # each goes into the delete predicate
        handle = self._handle(table)
        if handle is None or not batch_keys:
            return 0
        keys = ", ".join(f"'{k}'" for k in batch_keys)
        metrics = handle.delete(f"_batch_key IN ({keys})")
        return int(metrics.get("num_deleted_rows", 0))

    def _dataset(self, table: str) -> Any:
        handle = self._handle(table)
        return None if handle is None else handle.to_pyarrow_dataset()

    @staticmethod
    def _filter(where: dict[str, str] | None) -> Any:
        import pyarrow.dataset as pds

        expr = None
        for c, v in (where or {}).items():
            term = pds.field(c) == v
            expr = term if expr is None else expr & term
        return expr

    def fingerprint(
        self, table: str, where: dict[str, str] | None = None, sums: tuple[str, ...] = ()
    ) -> dict[str, float]:
        import pyarrow.compute as pc

        ds = self._dataset(table)
        if ds is None:
            return {"row_count": 0.0, **{c: 0.0 for c in sums}}
        flt = self._filter(where)
        out: dict[str, float] = {"row_count": float(ds.count_rows(filter=flt))}
        if sums:
            got = ds.to_table(columns=list(sums), filter=flt)
            out.update({c: float(pc.sum(got[c]).as_py() or 0) for c in sums})
        return out

    def keys(self, table: str, columns: tuple[str, ...], where: dict[str, str]) -> set[tuple[Any, ...]]:
        ds = self._dataset(table)
        if ds is None:
            return set()
        got = ds.to_table(columns=list(columns), filter=self._filter(where)).to_pylist()
        return {tuple(r[c] for c in columns) for r in got}
