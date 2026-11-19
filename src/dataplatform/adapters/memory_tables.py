"""The release gate's ports (dataplatform.twin.gate) in memory: tables are lists of rows; a clone is a copy of the
list (a shallow clone shares data until written; in memory a copy behaves the same for the gate)."""

from __future__ import annotations

import hashlib
from typing import Any

from dataplatform.twin.gate import GateError, TableMeasures, check_name


class MemoryTables:
    def __init__(self, tables: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {k: list(v) for k, v in (tables or {}).items()}

    def clone(self, source: str, target: str) -> None:
        if check_name(source) not in self.tables:
            raise GateError(f"no table {source}")
        self.tables[check_name(target)] = [dict(r) for r in self.tables[source]]

    def drop(self, table: str) -> None:
        self.tables.pop(check_name(table), None)

    def measure(self, table: str, keys: tuple[str, ...], columns: tuple[str, ...],
                window: str = "") -> TableMeasures:
        if check_name(table) not in self.tables:
            raise GateError(f"no table {table}")
        whole = self._measure(table, self.tables[table], keys, columns)
        if not window or not any(window in r for r in self.tables[table]):  # no such column: whole table only
            return whole
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in self.tables[table]:
            groups.setdefault(str(r.get(window)), []).append(r)
        per = {w: self._measure(table, rs, keys, columns) for w, rs in sorted(groups.items())}
        return TableMeasures(table, whole.rows, whole.sums, whole.keys_digest, per)

    @staticmethod
    def _measure(table: str, rows: list[dict[str, Any]], keys: tuple[str, ...],
                 columns: tuple[str, ...]) -> TableMeasures:
        digest = hashlib.sha256()
        for k in sorted("\x1f".join(str(r.get(c)) for c in keys) for r in rows):
            digest.update(k.encode() + b"\x1e")
        sums = {c: round(sum(float(r.get(c) or 0) for r in rows), 6) for c in columns}
        return TableMeasures(table, len(rows), sums, digest.hexdigest()[:32])
