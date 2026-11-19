"""SourceDatabase on SQLite (standard library): for tests and for a laptop with no Postgres.
SQLite has no schemas, so a system's tables are prefixed with its name: erp_db__sales_order_line.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from dataplatform.adapters.sql_source import Dialect, SqlSource, ident

DIALECT = Dialect(
    placeholder="?",
    types={
        "string": "TEXT",
        "integer": "INTEGER",
        "number": "REAL",
        "date": "TEXT",
        "timestamp": "TEXT",
        "boolean": "INTEGER",
    },
    serial="INTEGER PRIMARY KEY AUTOINCREMENT",
    qualify=lambda system, table: ident(f"{system}__{table}"),
    create_schema=lambda system: None,
)


class SqliteSource(SqlSource):
    def __init__(self, path: str | Path = ":memory:"):
        super().__init__(sqlite3.connect(str(path)), DIALECT)
