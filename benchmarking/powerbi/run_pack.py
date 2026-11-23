"""Run the Power BI query pack and write the timings.

    export PBI_TOKEN=...        # a Power BI bearer token (for example: az account get-access-token --resource
                                # https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
    python powerbi/run_pack.py --workspace <id> --dataset <id> --names names.json --label iceberg-uc-import

names.json maps each {{name}} in queries.yaml to a table or column of the published model. The token is read from the
environment and is never written anywhere.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from bench import dax  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--names", type=Path, required=True)
    ap.add_argument("--label", required=True, help="what this run measures, e.g. delta-uc-uniform-directquery")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--only", action="append", help="a query id; default all")
    ap.add_argument("--out", type=Path, default=Path("results"))
    a = ap.parse_args()
    token = os.environ.get("PBI_TOKEN", "")
    if not token:
        print("set PBI_TOKEN", file=sys.stderr)
        return 2
    pack = yaml.safe_load((HERE / "queries.yaml").read_text())["queries"]
    if a.only:
        pack = [q for q in pack if q["id"] in a.only]
    names = json.loads(a.names.read_text())
    results = dax.run_pack(pack, names, a.workspace, a.dataset, token, a.repeats)
    a.out.mkdir(parents=True, exist_ok=True)
    out = a.out / f"powerbi-{a.label}.json"
    out.write_text(json.dumps({"label": a.label, "summary": dax.summary(results), "runs": results}, indent=2))
    print(f"{len(results)} runs -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
