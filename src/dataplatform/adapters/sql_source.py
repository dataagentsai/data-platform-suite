"""SourceDatabase over any DB-API connection. Two thin adapters use it: postgres_source.py and sqlite_source.py.

Layout per system and table:
    <table>           the live table, one row per key; a change only wins if its change sequence is newer
    <table>__changes  the change feed (when the table has one): every in-feed change in commit order, with
                      op, committed_at and feed_pos beside the row's columns
    <table>__copy     the nightly full copies (master tables), keyed by key + extracted_at

Identifiers come from the DWD and the contracts and are checked against a strict pattern before quoting;
every value is a bound parameter (no SQL built from values).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

from dataplatform.source_ports import Row, SourceChange, TableSpec

IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def ident(name: str) -> str:
    if not IDENT.match(name):
        raise ValueError(f"not a safe identifier: {name!r}")
    return f'"{name}"'


@dataclass(frozen=True)
class Dialect:
    placeholder: str  # %s or ?
    types: dict[str, str]  # contract logical type -> column type
    serial: str  # auto-increment column definition for feed_pos
    qualify: Callable[[str, str], str]  # (system, table) -> quoted table name
    create_schema: Callable[[str], str | None]  # system -> DDL, or None when the engine has no schemas


def _plain(v: Any) -> Any:
    """Values read back as the world wrote them: ISO strings for time, floats for money."""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


class SqlSource:
    def __init__(self, conn: Any, dialect: Dialect):
        self.conn = conn
        self.d = dialect

    # ---- DDL ----
    def ensure(self, system: str, tables: list[TableSpec], reset: bool = False) -> None:
        cur = self.conn.cursor()
        ddl = self.d.create_schema(system)
        if ddl:
            cur.execute(ddl)
        for t in tables:
            names = (
                [t.name]
                + ([f"{t.name}__changes"] if t.change_feed else [])
                + ([f"{t.name}__copy"] if t.full_copy else [])
            )
            if reset:
                for n in names:
                    cur.execute(f"DROP TABLE IF EXISTS {self.d.qualify(system, n)}")  # nosec B608 - identifiers checked by ident()
            cols = ", ".join(f"{ident(c)} {self.d.types[ty]}" for c, ty in t.columns)
            key = ", ".join(ident(k) for k in t.key)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.d.qualify(system, t.name)} ({cols}, PRIMARY KEY ({key}))")  # nosec B608
            if t.change_feed:
                cur.execute(  # nosec B608 - identifiers checked by ident()
                    f"CREATE TABLE IF NOT EXISTS {self.d.qualify(system, t.name + '__changes')} "
                    f"(feed_pos {self.d.serial}, op TEXT NOT NULL, "
                    f"committed_at {self.d.types['timestamp']} NOT NULL, {cols})"
                )
            if t.full_copy:
                copy_key = key if "extracted_at" in t.key else f"{key}, {ident('extracted_at')}"
                cur.execute(  # nosec B608 - identifiers checked by ident()
                    f"CREATE TABLE IF NOT EXISTS {self.d.qualify(system, t.name + '__copy')} "
                    f"({cols}, PRIMARY KEY ({copy_key}))"
                )
        self.conn.commit()

    # ---- writes ----
    def apply(self, system: str, table: TableSpec, changes: list[SourceChange]) -> None:
        cur = self.conn.cursor()
        for op, group in _runs(changes):
            if op == "D":
                self._delete(cur, system, table, group)
            else:
                self._upsert(cur, system, table, [c.row for c in group])
            feed = [c for c in group if c.in_feed]
            if table.change_feed and feed:
                self._to_feed(cur, system, table, feed)
        self.conn.commit()

    def _upsert(self, cur: Any, system: str, t: TableSpec, rows: list[Row]) -> None:
        names = [c for c, _ in t.columns]
        p = self.d.placeholder
        sets = ", ".join(f"{ident(c)} = excluded.{ident(c)}" for c in names if c not in t.key)
        newer = " WHERE tgt.change_seq < excluded.change_seq" if "change_seq" in names else ""
        sql = (
            # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
            f"INSERT INTO {self.d.qualify(system, t.name)} AS tgt ({', '.join(ident(c) for c in names)}) "  # nosec B608
            f"VALUES ({', '.join([p] * len(names))}) ON CONFLICT ({', '.join(ident(k) for k in t.key)}) "
            f"DO UPDATE SET {sets}{newer}"
        )
        cur.executemany(sql, [tuple(r.get(c) for c in names) for r in rows])

    def _delete(self, cur: Any, system: str, t: TableSpec, group: list[SourceChange]) -> None:
        where = " AND ".join(f"{ident(k)} = {self.d.placeholder}" for k in t.key)
        cur.executemany(
            # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
            f"DELETE FROM {self.d.qualify(system, t.name)} WHERE {where}",  # nosec B608
            [tuple(c.row[k] for k in t.key) for c in group],
        )

    def _to_feed(self, cur: Any, system: str, t: TableSpec, changes: list[SourceChange]) -> None:
        names = [c for c, _ in t.columns]
        p = self.d.placeholder
        cur.executemany(
            # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
            f"INSERT INTO {self.d.qualify(system, t.name + '__changes')} (op, committed_at, "  # nosec B608
            f"{', '.join(ident(c) for c in names)}) VALUES ({', '.join([p] * (len(names) + 2))})",
            [(c.op, c.committed_at, *(c.row.get(n) for n in names)) for c in changes],
        )

    def publish_copy(self, system: str, table: TableSpec, extracted_at: str, rows: list[Row]) -> None:
        names = [c for c, _ in table.columns]
        cur = self.conn.cursor()
        cur.executemany(
            # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
            f"INSERT INTO {self.d.qualify(system, table.name + '__copy')} ({', '.join(ident(c) for c in names)}) "  # nosec B608
            f"VALUES ({', '.join([self.d.placeholder] * len(names))})",
            [tuple({**r, "extracted_at": extracted_at}.get(c) for c in names) for r in rows],
        )
        self.conn.commit()

    # ---- reads ----
    def _select(self, sql: str, params: Iterable[Any] = ()) -> list[Row]:
        cur = self.conn.cursor()
        cur.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        return [{c: _plain(v) for c, v in zip(cols, r, strict=True)} for r in cur.fetchall()]

    def rows(self, system: str, table: TableSpec) -> list[Row]:
        order = ", ".join(ident(k) for k in table.key)
        # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
        return self._select(f"SELECT * FROM {self.d.qualify(system, table.name)} ORDER BY {order}")  # nosec B608

    def feed(self, system: str, table: TableSpec, after_pos: int = 0) -> list[Row]:
        return self._select(
            # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
            f"SELECT * FROM {self.d.qualify(system, table.name + '__changes')} "  # nosec B608
            f"WHERE feed_pos > {self.d.placeholder} ORDER BY feed_pos",
            (after_pos,),
        )

    def copy(self, system: str, table: TableSpec, extracted_at: str) -> list[Row]:
        order = ", ".join(ident(k) for k in table.key)
        # shiftleft: allow SL-SEC-01 -- table and column names pass ident() (^[a-z_][a-z0-9_]{0,62}$); every value is a bound parameter
        rows = self._select(f"SELECT * FROM {self.d.qualify(system, table.name + '__copy')} ORDER BY {order}")  # nosec B608
        return [r for r in rows if _same_instant(r["extracted_at"], extracted_at)]


    def copies(self, system: str, table: TableSpec) -> list[str]:
        # The only variable is the table name, through d.qualify(), which runs ident() (a strict regex, or raise).
        # shiftleft: allow SL-SEC-01 -- table name validated by ident() inside d.qualify(); no values in the query
        rows = self._select(f"SELECT DISTINCT extracted_at FROM {self.d.qualify(system, table.name + '__copy')}")  # nosec B608
        return [r["extracted_at"] for r in sorted(rows, key=lambda r: datetime.fromisoformat(r["extracted_at"]))]


def _same_instant(a: str, b: str) -> bool:
    return datetime.fromisoformat(a) == datetime.fromisoformat(b)


def _runs(changes: list[SourceChange]) -> list[tuple[str, list[SourceChange]]]:
    """Consecutive changes grouped by kind (delete or not), keeping their order."""
    out: list[tuple[str, list[SourceChange]]] = []
    for c in changes:
        kind = "D" if c.op == "D" else "IU"
        if out and out[-1][0] == kind:
            out[-1][1].append(c)
        else:
            out.append((kind, [c]))
    return out
