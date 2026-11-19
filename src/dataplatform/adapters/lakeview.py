"""The ops dashboards (dataplatform.obs.dashboards) as Databricks AI/BI (Lakeview) dashboard files.

One `<id>.lvdash.json` per dashboard: a dataset per query and one page whose widgets bind to those datasets. An
aggregate query reads the ops metric view over its table (`MEASURE(...)`, ADR-024 rule 7; the views are generated
by adapters/databricks_ops_views.py); a listing reads the compact ops table in `<catalog>.<schema>`. Output is
deterministic (sorted keys, fixed layout), so the files in realms/databricks/dashboards/ are generated, committed and
checked for drift like the bundle.

SQL comes only from validated definitions: every name passes the definition's identifier rule
(obs/dashboards.validate) and every literal is quoted here. No viewer input reaches the text.
"""

from __future__ import annotations

import json
import re
from typing import Any

from dataplatform.obs.dashboards import Dashboard, Query, Widget, validate
from dataplatform.obs.views import view_for

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
WIDTH = 6
SIZES = {"counter": (2, 3), "line": (3, 6), "bar": (3, 6), "table": (6, 6)}
SPEC_VERSION = {"counter": 2, "line": 3, "bar": 3, "table": 1}
SCALES = {"day": "temporal"}


def _ident(name: str) -> str:
    if not _NAME.match(name):
        raise ValueError(f"not a plain name: {name!r}")
    return f"`{name}`"


def _literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _measure_sql(agg: str, column: str) -> str:
    if agg == "count":
        return "COUNT(*)"
    if agg == "count_distinct":
        return f"COUNT(DISTINCT {_ident(column)})"
    return f"{agg.upper()}({_ident(column)})"


def query_sql(q: Query, catalog: str, schema: str) -> list[str]:
    """The query as SQL lines (Lakeview keeps a dataset's query as a list of lines)."""
    view = view_for(q.table) if q.measures else None
    if view is not None:
        select = [_ident(g) for g in q.group_by] + [f"MEASURE({_ident(m.name)}) AS {_ident(m.name)}"
                                                   for m in q.measures]
        source = view.name
    elif q.columns:
        select, source = [_ident(c) for c in q.columns], q.table
    else:
        select = [_ident(g) for g in q.group_by] + [f"{_measure_sql(m.agg, m.column)} AS {_ident(m.name)}"
                                                   for m in q.measures]
        source = q.table
    lines = [f"SELECT {', '.join(select)}",
             f"FROM {_ident(catalog)}.{_ident(schema)}.{_ident(source)}"]
    conds = []
    for col, op, value in q.where:
        if op == "in":
            conds.append(f"{_ident(col)} IN ({', '.join(_literal(v) for v in value)})")
        else:
            conds.append(f"{_ident(col)} {'<>' if op == '!=' else '='} {_literal(value)}")
    if conds:
        lines.append("WHERE " + " AND ".join(conds))
    if q.group_by and q.measures:
        lines.append("GROUP BY " + ", ".join(_ident(g) for g in q.group_by))
    order = q.order_by or q.group_by
    if order:
        lines.append("ORDER BY " + ", ".join(f"{_ident(o)}{' DESC' if q.descending else ''}" for o in order))
    if q.limit:
        lines.append(f"LIMIT {int(q.limit)}")
    return [ln + "\n" for ln in lines[:-1]] + [lines[-1]]


def _field(name: str) -> dict[str, str]:
    return {"name": name, "expression": _ident(name)}


def _encodings(w: Widget, titles: dict[str, str]) -> dict[str, Any]:
    if w.kind == "counter":
        return {"value": {"fieldName": w.y, "displayName": titles.get(w.y, w.y)}}
    if w.kind == "table":
        return {"columns": [{"fieldName": c, "displayName": c.replace("_", " ")} for c in w.columns]}
    enc: dict[str, Any] = {
        "x": {"fieldName": w.x, "scale": {"type": SCALES.get(w.x, "categorical")}, "displayName": w.x},
        "y": {"fieldName": w.y, "scale": {"type": "quantitative"}, "displayName": w.y.replace("_", " ")},
    }
    if w.color:
        enc["color"] = {"fieldName": w.color, "scale": {"type": "categorical"}, "displayName": w.color}
    return enc


def _widget(w: Widget, d: Dashboard) -> dict[str, Any]:
    fields = list(w.columns) if w.kind == "table" else [f for f in (w.x, w.y, w.color) if f]
    return {
        "name": w.id,
        "queries": [{"name": "main_query", "query": {"datasetName": f"{d.id}__{w.query}",
                                                     "fields": [_field(f) for f in fields],
                                                     "disaggregated": True}}],
        "spec": {"version": SPEC_VERSION[w.kind], "widgetType": w.kind, "encodings": _encodings(w, {}),
                 "frame": {"showTitle": True, "title": w.title}},
    }


def _layout(d: Dashboard) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = [{"widget": {"name": "about", "multilineTextboxSpec": {
        "lines": [f"## {d.title}\n", "\n", d.purpose + "\n", "\n",
                  f"Owned by {'data expectation services' if d.owner == 'des' else 'observability'}. Built from "
                  f"the ops metric views over the compact ops tables (run log, daily telemetry copy, freshness, "
                  f"alerts), joined by run id. Discharges {', '.join(d.discharges)}."]}},
        "position": {"x": 0, "y": 0, "width": WIDTH, "height": 2}}]
    x, y, row_h = 0, 2, 0
    for w in d.widgets:
        width, height = SIZES[w.kind]
        if x + width > WIDTH:
            x, y, row_h = 0, y + row_h, 0
        out.append({"widget": _widget(w, d), "position": {"x": x, "y": y, "width": width, "height": height}})
        x, row_h = x + width, max(row_h, height)
    return out


def compile_dashboard(d: Dashboard, catalog: str, schema: str) -> dict[str, Any]:
    validate(d)
    return {
        "datasets": [{"name": f"{d.id}__{q.id}", "displayName": q.id.replace("_", " "),
                      "queryLines": query_sql(q, catalog, schema)} for q in d.queries],
        "pages": [{"name": d.id, "displayName": d.title, "layout": _layout(d)}],
    }


def render_file(d: Dashboard, catalog: str, schema: str) -> str:
    return json.dumps(compile_dashboard(d, catalog, schema), indent=2, sort_keys=True) + "\n"


def bundle_resources(dashboards: tuple[Dashboard, ...], native: tuple[str, ...] = ()) -> str:
    """The bundle resource file: one dashboard resource per file, published to the bundle's SQL warehouse. `native`
    names the platform's own dashboards kept unchanged under native/<id>.lvdash.json (adopted, not rebuilt)."""
    lines = ["# Generated by realms/databricks/dashboards/generate.py from dataplatform.obs.dashboards. Do not edit.",
             "# Include from the bundle (include: [dashboards/resources.yml]) and set var.warehouse_id.",
             "# Run views/*.sql first: the dashboards read the ops metric views.",
             "resources:", "  dashboards:"]
    for d in dashboards:
        prefix = "DES" if d.owner == "des" else "Ops"
        lines += [f"    {prefix.lower()}_{d.id.removeprefix('des_')}:", f"      display_name: \"{prefix} - {d.title}\"",
                  f"      file_path: ./{d.id}.lvdash.json", "      warehouse_id: ${var.warehouse_id}"]
    for n in native:
        title = n.replace("_", " ").capitalize()
        lines += [f"    native_{n}:", f"      display_name: \"Native - {title}\"",
                  f"      file_path: ./native/{n}.lvdash.json", "      warehouse_id: ${var.warehouse_id}"]
    return "\n".join(lines) + "\n"
