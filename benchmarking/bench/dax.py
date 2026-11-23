"""Run the Power BI query pack against a published semantic model and time every query (Power BI REST "Execute Queries").

Needs a bearer token for the Power BI service (an Entra ID token, taken from the PBI_TOKEN environment variable, never
written to a file), the workspace id and the dataset id. Standard library only. The HTTP call is a parameter so the
tests run without a network.

The queries are templates (benchmarking/powerbi/queries.yaml): `{{name}}` marks a table or column of the published
model, filled from a mapping the user supplies, because the model's names are the user's.
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
from collections.abc import Callable
from typing import Any

HTTP_OK = 200
API = "https://api.powerbi.com/v1.0/myorg/groups/{workspace}/datasets/{dataset}/executeQueries"
_HOLE = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")
_UUID = re.compile(r"^[0-9a-fA-F-]{36}$")


def fill(template: str, names: dict[str, str]) -> str:
    """Replace each {{name}}; a name with no value is an error, so a query never runs half-filled."""
    missing = sorted({m for m in _HOLE.findall(template) if m not in names})
    if missing:
        raise KeyError(f"no value for {', '.join(missing)}")
    return _HOLE.sub(lambda m: names[m.group(1)], template)


def request(workspace: str, dataset: str, query: str, token: str) -> urllib.request.Request:
    if not (_UUID.match(workspace) and _UUID.match(dataset)):
        raise ValueError("workspace and dataset must be ids")
    body = {"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}
    return urllib.request.Request(API.format(workspace=workspace, dataset=dataset), data=json.dumps(body).encode(),
                                  headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                                  method="POST")


def _send(req: urllib.request.Request) -> tuple[int, dict[str, Any]]:  # pragma: no cover -- the network edge
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:  # noqa: S310 -- fixed https host
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()[:300]}


def run_pack(pack: list[dict[str, Any]], names: dict[str, str], workspace: str, dataset: str, token: str,
             repeats: int = 3, send: Callable[..., tuple[int, dict[str, Any]]] = _send,
             clock: Callable[[], float] = time.monotonic) -> list[dict[str, Any]]:
    """Each query `repeats` times (the first is the cold one). One result row per run: seconds, status, rows returned."""
    out: list[dict[str, Any]] = []
    for q in pack:
        text = fill(q["dax"], names)
        for i in range(repeats):
            t0 = clock()
            status, body = send(request(workspace, dataset, text, token))
            rows = (((body.get("results") or [{}])[0].get("tables") or [{}])[0].get("rows") or []) if status == HTTP_OK else []
            out.append({"query": q["id"], "case": q.get("case", ""), "run": i + 1, "cold": i == 0, "http": status,
                        "seconds": round(clock() - t0, 3), "rows_returned": len(rows),
                        "error": "" if status == HTTP_OK else str(body.get("error", ""))[:300]})
    return out


def percentile(values: list[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, max(0, round(p / 100 * (len(s) - 1))))] if s else 0.0


def summary(results: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """Per query: cold seconds, warm p50 and p95, failures."""
    by: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        by.setdefault(r["query"], []).append(r)
    out: dict[str, dict[str, float]] = {}
    for q, rows in sorted(by.items()):
        warm = [r["seconds"] for r in rows if not r["cold"] and r["http"] == HTTP_OK]
        out[q] = {"cold_s": next((r["seconds"] for r in rows if r["cold"]), 0.0), "warm_p50_s": percentile(warm, 50),
                  "warm_p95_s": percentile(warm, 95), "failed": float(sum(r["http"] != HTTP_OK for r in rows))}
    return out
