"""Lake Formation grants from the same access plan as Unity Catalog (ADR-028 rule 4, B12): AWS engines (Redshift,
Athena) read the Unity Catalog tables through a Glue federated catalog, and Lake Formation decides what each reader
sees. Permissions do not cross a federation boundary, so they are generated here from the one access model, and a
drift check compares what Lake Formation holds with what the model says.

Mapping (product names allowed in a realm):
    a group                      -> the IAM role the binding names for it (role_of)
    read on a layer's catalog    -> SELECT + DESCRIBE on every table of that layer's federated database (TableWildcard)
    read on one table            -> SELECT + DESCRIBE on that table
    a protected column           -> excluded from what readers outside the allowed groups can query (a data-cells
                                    filter with excluded columns): Lake Formation hides a column, it cannot mask one
    a row filter                 -> a data-cells filter with a row filter expression per limited group

Output: Terraform (aws_lakeformation_permissions, aws_lakeformation_data_cells_filter), deterministic.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable

from dataplatform.access.model import AccessPlan

SAFE = re.compile(r"^[A-Za-z0-9_.:/-]+$")
Intent = tuple[str, str, str, str, str]  # (principal, database, table or *, permission, filter or "")


def _q(s: str) -> str:
    return json.dumps(s)


def _db(env: str, layer: str) -> str:
    return f"{env}_{layer}"  # the federated database (one per catalog of record, ADR-012) behind its resource link


def intents(plan: AccessPlan, env: str, role_of: Callable[[str], str]) -> list[Intent]:
    """What Lake Formation must hold: one row per (principal, database, table, permission, filter)."""
    masks_by_table: dict[tuple[str, str, str], list] = {}
    for m in plan.masks:
        masks_by_table.setdefault((m.layer, m.schema, m.table), []).append(m)
    out: set[Intent] = set()
    for g in plan.grants:
        if g.privilege != "read":
            continue
        role = role_of(g.group)
        if g.scope == "catalog":
            out |= {(role, _db(env, g.layer), "*", p, "") for p in ("SELECT", "DESCRIBE")}
            continue
        if g.scope != "table":
            continue
        tname = f"{g.schema}__{g.name}"
        hidden = [m.column for m in masks_by_table.get((g.layer, g.schema, g.name), []) if g.group not in m.allowed]
        flt = f"hide_{g.group}_{tname}" if hidden else ""
        out |= {(role, _db(env, g.layer), tname, p, flt) for p in ("SELECT", "DESCRIBE")}
    return sorted(out)


def terraform(plan: AccessPlan, env: str, role_of: Callable[[str], str], account: str = "${var.account_id}") -> str:
    """The Terraform for the intents, plus one data-cells filter per (group, table) that hides protected columns and
    one per row-filtered table and limited group."""
    blocks = ["# Generated from the access model (ADR-028); do not edit. Lake Formation side of the same grants.\n"]
    for n, (role, db, tbl, perm, flt) in enumerate(intents(plan, env, role_of)):
        for v in (role, db, tbl, perm, flt):
            if v and v != "*" and not SAFE.match(v):
                raise ValueError(f"not a safe name for Terraform: {v!r}")
        target = ('  table {\n    database_name = ' + _q(db) + '\n    wildcard      = true\n  }' if tbl == "*" else
                  '  table {\n    database_name = ' + _q(db) + '\n    name          = ' + _q(tbl) + '\n  }')
        if flt:
            target = ('  data_cells_filter {\n    database_name    = ' + _q(db) + '\n    table_name       = ' + _q(tbl)
                      + '\n    name             = ' + _q(flt) + '\n    table_catalog_id = ' + _q(account) + '\n  }')
        blocks.append(f'resource "aws_lakeformation_permissions" "g{n:04d}" {{\n  principal   = {_q(role)}\n'
                      f'  permissions = [{_q(perm)}]\n{target}\n}}\n')
    seen = set()
    for m in plan.masks:
        for g in plan.grants:
            if g.privilege != "read" or g.scope != "table" or (g.layer, g.schema, g.name) != (m.layer, m.schema,
                                                                                               m.table):
                continue
            if g.group in m.allowed:
                continue
            tname = f"{g.schema}__{g.name}"
            name = f"hide_{g.group}_{tname}"
            if name in seen:
                continue
            seen.add(name)
            cols = sorted(x.column for x in plan.masks if (x.layer, x.schema, x.table) == (m.layer, m.schema, m.table)
                          and g.group not in x.allowed)
            blocks.append(
                f'resource "aws_lakeformation_data_cells_filter" "{name}" {{\n  table_data {{\n'
                f'    database_name    = {_q(_db(env, m.layer))}\n    table_name       = {_q(tname)}\n'
                f'    name             = {_q(name)}\n    table_catalog_id = {_q(account)}\n'
                f'    column_wildcard {{\n      excluded_column_names = {json.dumps(cols)}\n    }}\n'
                f'    row_filter {{\n      all_rows_wildcard {{}}\n    }}\n  }}\n}}\n')
    return "\n".join(blocks)


def drift(intent: Iterable[Intent], observed: Iterable[Intent]) -> dict[str, list[Intent]]:
    """missing: in the model, not in Lake Formation (a reader is blocked). extra: in Lake Formation, not in the model
    (a reader sees more than the model allows: the dangerous side)."""
    want, have = set(intent), set(observed)
    return {"missing": sorted(want - have), "extra": sorted(have - want)}
