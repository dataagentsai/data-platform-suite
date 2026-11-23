"""Expand the benchmark scenarios into one run plan: every Power BI worst case and every maintenance operation, for each
table format. Pure: it reads the two scenario files and writes a plan; it needs no Spark, no cloud and no Power BI.

    python -m bench.plan --out plan.json            # run from the benchmarking folder
    python -m bench.plan --check                    # only validate the scenario files
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent.parent
FORMATS = ("iceberg-uc", "delta-uc-uniform", "s3tables")
# the table-format flag of the platform scripts -> the name used in maintenance.yaml
FORMAT_KEY = {"iceberg-uc": "uc_managed_iceberg", "delta-uc-uniform": "uc_managed_delta_uniform",
              "s3tables": "aws_s3_tables"}
MODES = ("import", "directquery", "composite", "direct_lake")


def load(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path}: not a mapping")
    return data


def problems(powerbi: dict[str, Any], maintenance: dict[str, Any]) -> list[str]:
    """Everything wrong with the two scenario files; empty when they can be planned."""
    out: list[str] = []
    seen: set[str] = set()
    for c in powerbi.get("cases", []):
        cid = str(c.get("id", ""))
        if not cid or cid in seen:
            out.append(f"power bi case id missing or repeated: {cid!r}")
        seen.add(cid)
        for k in ("name", "hard_because", "data_shape", "modes", "pass_if"):
            if not c.get(k):
                out.append(f"{cid}: {k} is missing")
        out += [f"{cid}: unknown mode {m!r}" for m in c.get("modes", []) if m not in MODES]
    for op in maintenance.get("operations", []):
        oid = str(op.get("id", ""))
        if not oid or oid in seen:
            out.append(f"maintenance id missing or repeated: {oid!r}")
        seen.add(oid)
        out += [f"{oid}: no expectation for {f}" for f in FORMAT_KEY.values() if f not in op.get("expect", {})]
        if not op.get("measure"):
            out.append(f"{oid}: measure is missing")
    return out


def expand(powerbi: dict[str, Any], maintenance: dict[str, Any], formats: tuple[str, ...] = FORMATS,
           days: int = 3, scale: float = 0.1, case_profiles: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """One entry per (case or operation, format, mode). The world settings (days, scale) are the same for every entry
    of a plan; the benchmark raises scale on the office workspace."""
    runs: list[dict[str, Any]] = []
    for fmt in formats:
        for c in powerbi["cases"]:
            for mode in c["modes"]:
                runs.append({"id": f"{c['id']}:{fmt}:{mode}", "kind": "powerbi", "case": c["id"], "format": fmt,
                             "mode": mode, "data_shape": c["data_shape"], "pass_if": c["pass_if"],
                             "world": {"days": days, "scale": scale,
                                       "shape": (case_profiles or {}).get(c["id"], "baseline")}})
        for op in maintenance["operations"]:
            runs.append({"id": f"{op['id']}:{fmt}", "kind": "maintenance", "operation": op["id"], "format": fmt,
                         "measure": op["measure"], "world": {"days": days, "scale": scale}})
    return runs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--scale", type=float, default=0.1)
    ap.add_argument("--format", action="append", choices=FORMATS, help="default: all three")
    a = ap.parse_args(argv)
    pb, mt = load(HERE / "scenarios/powerbi_worst_cases.yaml"), load(HERE / "scenarios/maintenance.yaml")
    bad = problems(pb, mt)
    if bad:
        print("\n".join(bad), file=sys.stderr)
        return 1
    if a.check:
        print("scenario files are valid")
        return 0
    shapes = load(HERE / "scenarios/shapes.yaml")
    unknown = sorted(set(shapes["case_profiles"].values()) - set(shapes["profiles"]))
    if unknown:
        print(f"case_profiles name unknown profiles {unknown}", file=sys.stderr)
        return 1
    runs = expand(pb, mt, tuple(a.format or FORMATS), a.days, a.scale, shapes["case_profiles"])
    text = json.dumps(runs, indent=2)
    if a.out:
        a.out.write_text(text + "\n")
        print(f"{len(runs)} runs -> {a.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
