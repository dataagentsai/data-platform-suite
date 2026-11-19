"""The platform explains itself in plain words. No model and no network: the same inputs always give the same text.

    python scripts/explain.py run build/run                    # what a run did, day by day (its report and ops tables)
    python scripts/explain.py metadata erp.sales_order_line    # what a dataset's manifest makes the platform do
    python scripts/explain.py cicd                             # the checks a change passes through
    python scripts/explain.py code build_silver                # where a function is, who calls it, what tests name it

The long form of all of this, with pictures, is the explainer built by `make explainer` (explainer/).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "explainer"))

from dataplatform.obs.explain import explain as explain_run  # noqa: E402
from dataplatform.obs.explain_meta import explain_cicd, explain_dataset  # noqa: E402

SHOWN_CALLERS = 15
DEFAULT_CATALOG = Path.home() / "data-platform-catalog"


def catalog_of(a: argparse.Namespace) -> Path:
    import os

    return Path(os.environ.get("DP_CATALOG") or a.catalog)


def manifests(catalog: Path) -> dict[str, Path]:
    import yaml

    out = {}
    for p in sorted(catalog.glob("slices/*/manifest/datasets/*.yaml")):
        m = yaml.safe_load(p.read_text()) or {}
        if m.get("source_name") and m.get("dataset_name"):
            out[f"{m['source_name']}.{m['dataset_name']}"] = p
    return out


def cmd_run(a: argparse.Namespace) -> str:
    report = json.loads((a.run_dir / "report.json").read_text())
    tf = a.run_dir / "ops_tables.json"
    modes = {}
    try:
        import yaml

        for name, p in manifests(catalog_of(a)).items():
            ing = (yaml.safe_load(p.read_text()) or {}).get("ingest") or {}
            modes[name] = f"{ing.get('mode', '?')}/{ing.get('read', '?')}"
    except (ImportError, OSError):
        pass
    return explain_run(report, json.loads(tf.read_text()) if tf.exists() else {}, modes)


def cmd_metadata(a: argparse.Namespace) -> str:
    import yaml

    found = manifests(catalog_of(a))
    if a.dataset not in found:
        raise SystemExit(
            f"no manifest for {a.dataset}; known: {', '.join(sorted(found)) or 'none (set DP_CATALOG to the catalog)'}"
        )
    p = found[a.dataset]
    m = yaml.safe_load(p.read_text())
    c = None
    if m.get("contract"):
        cp = (p.parent / m["contract"]).resolve()
        c = yaml.safe_load(cp.read_text()) if cp.exists() else None
    return explain_dataset(m, c)


def cmd_cicd(a: argparse.Namespace) -> str:
    import yaml

    root = a.repo
    helps = [
        re.sub(r"^\s*@echo\s+\"|\"\s*$", "", ln)
        for ln in (root / "Makefile").read_text().splitlines()
        if re.match(r"^\s*@echo\s+\"make ", ln)
    ]
    pc = root / ".pre-commit-config.yaml"
    hooks = []
    if pc.exists():
        for repo in (yaml.safe_load(pc.read_text()) or {}).get("repos", []):
            hooks += [(str(repo.get("repo", "")), str(h["id"])) for h in repo.get("hooks", [])]
    wfs = {p.name: yaml.safe_load(p.read_text()) or {} for p in sorted((root / ".github/workflows").glob("*.yml"))}
    return explain_cicd(helps, hooks, wfs)


def cmd_code(a: argparse.Namespace) -> str:
    import callgraph
    import pages_map

    g = callgraph.build(ROOT)
    want = a.name.split("::")[-1]
    hits = [d for d in g.defs.values() if want in (d.qual, d.name) or d.id.endswith("::" + a.name)]
    if not hits:
        raise SystemExit(f"nothing named {a.name} in src, scripts or realms")
    out = []
    for d in sorted(hits, key=lambda x: (x.file, x.line)):
        page = next((pid for frag, pid in pages_map.PAGE_OF if frag in d.file), None)
        order = {"exact": 0, "value": 1, "by name": 2}
        uses = sorted(g.callers.get(d.id, []), key=lambda u: (order.get(u.how, 3), u.file, u.line))
        out += [
            f"## {d.qual}  ({d.kind})",
            f"{d.file}:{d.line}",
            "",
            d.doc or "No description in the code.",
            "",
            "Called by:",
        ]
        out += [
            f"- {(g.defs[u.by].qual if u.by in g.defs else 'module level')} at {u.file}:{u.line}"
            + ("" if u.how == "exact" else f" ({u.how})")
            for u in uses[:15]
        ] or ["- nothing found in the code"]
        if len(uses) > SHOWN_CALLERS:
            out.append(f"- and {len(uses) - 15} more")
        tests = sorted(g.tested_by.get(d.id, ()))
        out += ["", "Named in tests: " + (", ".join(tests[:6]) or "none")]
        if page:
            out.append(f"Explained at: build/explainer/{page}.html (after `make explainer`)")
        out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--catalog", type=Path, default=DEFAULT_CATALOG, help="the specification catalog (or set DP_CATALOG)"
    )
    ap.add_argument("--repo", type=Path, default=ROOT)
    ap.add_argument("--out", type=Path, default=None, help="write to a file instead of printing")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run").add_argument("run_dir", type=Path)
    sub.add_parser("metadata").add_argument("dataset")
    sub.add_parser("cicd")
    sub.add_parser("code").add_argument("name")
    a = ap.parse_args()
    text = {"run": cmd_run, "metadata": cmd_metadata, "cicd": cmd_cicd, "code": cmd_code}[a.cmd](a)
    if a.out:
        a.out.write_text(text)
        print(f"wrote {a.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
