"""Catalog state read from Unity Catalog's information schema (Databricks; dbs/engine/databricks.yaml data_catalog).

The adapter takes a `query(sql, params)` function returning rows as tuples (a databricks-sql-connector cursor, a
Spark session wrapper, a fake in tests), so this module imports no SDK. Values are bound as named parameters; the
statements are constants.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dataplatform.governance.catalog import SYNCED_LAYERS
from dataplatform.governance.port import Entry, ObjectRef

Query = Callable[[str, Mapping[str, str]], Sequence[Sequence[Any]]]

TABLES = (
    "SELECT table_catalog, table_schema, table_name, table_type, comment "
    "FROM system.information_schema.tables "
    "WHERE table_catalog IN (:c0, :c1) AND table_schema <> 'information_schema'"
)
COLUMNS = (
    "SELECT table_catalog, table_schema, table_name, column_name, comment "
    "FROM system.information_schema.columns "
    "WHERE table_catalog IN (:c0, :c1) AND table_schema <> 'information_schema'"
)
TABLE_TAGS = (
    "SELECT catalog_name, schema_name, table_name, tag_name, tag_value "
    "FROM system.information_schema.table_tags WHERE catalog_name IN (:c0, :c1)"
)
COLUMN_TAGS = (
    "SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value "
    "FROM system.information_schema.column_tags WHERE catalog_name IN (:c0, :c1)"
)
PLACEHOLDERS = 2  # :c0, :c1 above: one per synced layer
KINDS = {"VIEW": "view", "METRIC_VIEW": "metric_view"}  # every other table_type is a table


class UcCatalogState:
    def __init__(self, query: Query) -> None:
        self._query = query

    def entries(self, env: str) -> Mapping[ObjectRef, Entry]:
        catalogs = {f"{env}_{layer}": layer for layer in SYNCED_LAYERS}
        if len(catalogs) != PLACEHOLDERS:
            raise ValueError("the information schema queries bind one placeholder per synced layer")
        params = {f"c{i}": c for i, c in enumerate(catalogs)}
        kinds: dict[tuple[str, str, str], str] = {}
        comments: dict[ObjectRef, str | None] = {}
        tags: dict[ObjectRef, dict[str, str]] = {}
        for cat, sch, tbl, ttype, comment in self._query(TABLES, params):
            ref = ObjectRef(catalogs[cat], sch, tbl)
            kinds[(ref.layer, sch, tbl)] = KINDS.get(str(ttype).upper(), "table")
            comments[ref] = comment or None
        for cat, sch, tbl, col, comment in self._query(COLUMNS, params):
            comments[ObjectRef(catalogs[cat], sch, tbl, col)] = comment or None
        for cat, sch, tbl, key, value in self._query(TABLE_TAGS, params):
            tags.setdefault(ObjectRef(catalogs[cat], sch, tbl), {})[key] = value or ""
        for cat, sch, tbl, col, key, value in self._query(COLUMN_TAGS, params):
            tags.setdefault(ObjectRef(catalogs[cat], sch, tbl, col), {})[key] = value or ""
        out = {}
        for ref in set(comments) | set(tags):
            kind = kinds.get((ref.layer, ref.schema, ref.name), "table")
            out[ref] = Entry(kind, comments.get(ref), tuple(sorted(tags.get(ref, {}).items())))
        return out
