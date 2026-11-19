"""The bronze writer in memory, for tests: the reference behaviour the table-format adapters are tested against (M5)."""

from __future__ import annotations

from typing import Any

from dataplatform.ingest.ports import Columns, Row, check_batch_key


def _match(row: Row, where: dict[str, str] | None) -> bool:
    return all(str(row.get(c)) == v for c, v in (where or {}).items())


class MemoryBronze:
    def __init__(self) -> None:
        self.tables: dict[str, list[Row]] = {}

    def write(self, table: str, batch_key: str, rows: list[Row], columns: Columns, replace: bool = False) -> int:
        check_batch_key(batch_key)
        names = [c for c, _ in columns]
        kept = [r for r in self.tables.get(table, []) if r.get("_batch_key") != batch_key]
        self.tables[table] = kept + [{c: r.get(c) for c in names} for r in rows]
        return len(rows)

    def write_many(self, table: str, batches: list[tuple[str, list[Row]]], columns: Columns) -> int:
        return sum(self.write(table, key, rows, columns) for key, rows in batches)

    def fingerprint(
        self, table: str, where: dict[str, str] | None = None, sums: tuple[str, ...] = ()
    ) -> dict[str, float]:
        rows = [r for r in self.tables.get(table, []) if _match(r, where)]
        out: dict[str, float] = {"row_count": float(len(rows))}
        for c in sums:
            out[c] = float(sum(r.get(c) or 0 for r in rows))
        return out

    def keys(self, table: str, columns: tuple[str, ...], where: dict[str, str]) -> set[tuple[Any, ...]]:
        return {tuple(r.get(c) for c in columns) for r in self.tables.get(table, []) if _match(r, where)}

    def remove(self, table: str, batch_keys: list[str]) -> int:
        """Housekeeping (B8): drop the rows of these batches; returns the rows removed."""
        for k in batch_keys:
            check_batch_key(k)
        gone = set(batch_keys)
        rows = self.tables.get(table, [])
        kept = [r for r in rows if r.get("_batch_key") not in gone]
        self.tables[table] = kept
        return len(rows) - len(kept)
