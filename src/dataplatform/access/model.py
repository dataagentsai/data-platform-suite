"""Groups and classification tags -> grants, column masks and row filters.

Access goes to groups, never to people (ENV-02). A group reads whole layers (every domain) or only its own domains'
tables in those layers. Columns classified confidential or restricted are masked for every reader except groups
declared `protected: see` that can read the table; restricted columns are tokenised before bronze (DPC-0070), so
even those groups see the token, never a clear value: nothing here detokenises. Row filters are deny by default: a
reader not named on a filter sees no rows of that table.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from dataplatform.governance.tables import LAYERS, TableSpec

PROTECTED_ACCESS = ("see", "masked")
ALL = "all"
GROUP_NAME = re.compile(r"^[a-z][a-z0-9-]*$")
CLEAR, TOKEN, MASKED = "clear", "token", "masked"  # what a reader sees in a column


class AccessError(ValueError):
    """An access model that cannot be planned."""


@dataclass(frozen=True)
class GroupRule:
    name: str
    layers: tuple[str, ...]
    domains: tuple[str, ...]  # (ALL,) for every domain
    protected: str  # see | masked

    @property
    def every_domain(self) -> bool:
        return self.domains == (ALL,)


@dataclass(frozen=True)
class RowFilter:
    layer: str
    schema: str
    table: str
    column: str
    unrestricted: tuple[str, ...]  # groups that see every row
    limited: tuple[tuple[str, tuple[str, ...]], ...]  # (group, values it may see)


@dataclass(frozen=True)
class AccessModel:
    groups: tuple[GroupRule, ...]
    row_filters: tuple[RowFilter, ...] = ()


@dataclass(frozen=True)
class Grant:
    group: str
    privilege: str  # use (enter a catalog or schema) | read (select)
    scope: str  # catalog | schema | table
    layer: str
    schema: str = ""
    name: str = ""


@dataclass(frozen=True)
class ColumnMask:
    layer: str
    schema: str
    table: str
    column: str
    logical_type: str
    classification: str
    tokenised: bool
    allowed: tuple[str, ...]  # groups that see the stored value (a token when tokenised)


@dataclass(frozen=True)
class AccessPlan:
    grants: tuple[Grant, ...]
    masks: tuple[ColumnMask, ...]
    row_filters: tuple[RowFilter, ...]
    model: AccessModel


def model_from_doc(doc: Mapping[str, Any]) -> AccessModel:
    groups = []
    for g in doc.get("groups") or ():
        name = str(g["name"])
        if not GROUP_NAME.match(name):
            raise AccessError(f"group '{name}' is not lower kebab case")
        layers = tuple(str(x) for x in g["layers"])
        bad = [x for x in layers if x not in LAYERS]
        if bad:
            raise AccessError(f"group '{name}': unknown layer(s) {', '.join(bad)}")
        domains = g.get("domains", ALL)
        doms = (ALL,) if domains == ALL else tuple(sorted(str(d) for d in domains))
        protected = str(g.get("protected", "masked"))
        if protected not in PROTECTED_ACCESS:
            raise AccessError(f"group '{name}': protected must be one of {', '.join(PROTECTED_ACCESS)}")
        groups.append(GroupRule(name, layers, doms, protected))
    names = [g.name for g in groups]
    if len(names) != len(set(names)):
        raise AccessError("a group is declared twice")
    filters = []
    for f in doc.get("row_filters") or ():
        layer, schema, table = str(f["table"]).split(".")
        limited = tuple(sorted((str(k), tuple(sorted(str(x) for x in v))) for k, v in (f.get("groups") or {}).items()))
        unknown = sorted({*f.get("unrestricted", ()), *(k for k, _ in limited)} - set(names))
        if unknown:
            raise AccessError(f"row filter on {f['table']} names undeclared group(s) {', '.join(unknown)}")
        filters.append(
            RowFilter(
                layer,
                schema,
                table,
                str(f["column"]),
                tuple(sorted(str(x) for x in f.get("unrestricted", ()))),
                limited,
            )
        )
    return AccessModel(tuple(sorted(groups, key=lambda g: g.name)), tuple(filters))


def _reads(g: GroupRule, t: TableSpec) -> bool:
    return t.layer in g.layers and (g.every_domain or t.domain in g.domains)


def plan_access(model: AccessModel, tables: Iterable[TableSpec]) -> AccessPlan:
    tables = list(tables)
    grants: set[Grant] = set()
    for g in model.groups:
        for layer in g.layers:
            if g.every_domain:
                grants.add(Grant(g.name, "read", "catalog", layer))
                continue
            for t in tables:
                if _reads(g, t) and t.layer == layer:
                    grants.add(Grant(g.name, "use", "catalog", layer))
                    grants.add(Grant(g.name, "use", "schema", layer, t.schema))
                    grants.add(Grant(g.name, "read", "table", layer, t.schema, t.name))
    masks = []
    for t in tables:
        allowed = tuple(g.name for g in model.groups if g.protected == "see" and _reads(g, t))
        masks += [
            ColumnMask(t.layer, t.schema, t.name, c.name, c.logical_type, c.classification, c.tokenised, allowed)
            for c in t.columns
            if c.protected
        ]
    known = {(t.layer, t.schema, t.name) for t in tables}
    for f in model.row_filters:
        if (f.layer, f.schema, f.table) not in known:
            raise AccessError(f"row filter on {f.layer}.{f.schema}.{f.table}: no such published table")
    order = ("catalog", "schema", "table")
    return AccessPlan(
        tuple(sorted(grants, key=lambda x: (x.layer, order.index(x.scope), x.schema, x.name, x.group, x.privilege))),
        tuple(sorted(masks, key=lambda m: (m.layer, m.schema, m.table, m.column))),
        model.row_filters,
        model,
    )


# ---------------------------------------------------------------------------------------------------- check


def can_read(plan: AccessPlan, group: str, table: TableSpec) -> bool:
    for g in plan.grants:
        if g.group != group or g.privilege != "read" or g.layer != table.layer:
            continue
        if g.scope == "catalog" or (g.schema, g.name) == (table.schema, table.name):
            return True
    return False


def visible_columns(plan: AccessPlan, group: str, table: TableSpec) -> dict[str, str] | None:
    """What `group` sees in each column (clear, token or masked); None if it cannot read the table at all."""
    if not can_read(plan, group, table):
        return None
    masks = {m.column: m for m in plan.masks if (m.layer, m.schema, m.table) == (table.layer, table.schema, table.name)}
    out = {}
    for c in table.columns:
        m = masks.get(c.name)
        if m is None:
            out[c.name] = CLEAR
        elif group not in m.allowed:
            out[c.name] = MASKED
        else:
            out[c.name] = TOKEN if m.tokenised else CLEAR
    return out


def row_visible(plan: AccessPlan, group: str, table: TableSpec, value: str) -> bool:
    """Whether `group` sees a row whose filtered column holds `value` (True when no filter covers the table)."""
    if not can_read(plan, group, table):
        return False
    for f in plan.row_filters:
        if (f.layer, f.schema, f.table) != (table.layer, table.schema, table.name):
            continue
        if group in f.unrestricted:
            continue
        values = dict(f.limited).get(group)
        if values is None or value not in values:
            return False
    return True
