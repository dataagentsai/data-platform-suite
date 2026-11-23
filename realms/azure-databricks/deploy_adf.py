"""Put the generated factory objects (adf/datasets, adf/pipelines) into the environment's factory, and run a mover.

    python3 realms/azure-databricks/deploy_adf.py deploy --factory dpcsh-dev-adf --resource-group dpcsh-dev-rg
    python3 realms/azure-databricks/deploy_adf.py run --factory ... --resource-group ... --pipeline mv_partner_dealer_sellout \
        --param window=2026-11-20 --param stem=sellout_2026-11

It uses the signed-in Azure identity through `az rest`, so no key is read or written. The subscription is the az default
(or ARM_SUBSCRIPTION_ID). Datasets are put before pipelines, because a pipeline refers to them.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
API = "2018-06-01"


def az(*args: str) -> str:
    done = subprocess.run(["az", *args], capture_output=True, text=True)
    if done.returncode:
        raise SystemExit(f"az {' '.join(args[:3])} failed: {done.stderr.strip()[:600]}")
    return done.stdout


def subscription() -> str:
    return os.environ.get("ARM_SUBSCRIPTION_ID") or json.loads(az("account", "show", "-o", "json"))["id"]


def base(a: argparse.Namespace) -> str:
    return (f"https://management.azure.com/subscriptions/{subscription()}/resourceGroups/{a.resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{a.factory}")


def put(url: str, obj: dict) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump({"properties": obj["properties"]}, f)
    try:
        az("rest", "--method", "put", "--url", f"{url}?api-version={API}", "--body", f"@{f.name}", "--output", "none")
    finally:
        os.unlink(f.name)


def deploy(a: argparse.Namespace) -> None:
    for kind in ("datasets", "pipelines"):
        for p in sorted((HERE / "adf" / kind).glob("*.json")):
            obj = json.loads(p.read_text())
            put(f"{base(a)}/{kind}/{obj['name']}", obj)
            print(f"put {kind}/{obj['name']}")


def run(a: argparse.Namespace) -> None:
    params = dict(p.split("=", 1) for p in a.param)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(params, f)
    try:
        out = json.loads(az("rest", "--method", "post", "--url",
                            f"{base(a)}/pipelines/{a.pipeline}/createRun?api-version={API}", "--body", f"@{f.name}"))
    finally:
        os.unlink(f.name)
    print(out["runId"])


def status(a: argparse.Namespace) -> None:
    out = json.loads(az("rest", "--method", "get", "--url", f"{base(a)}/pipelineruns/{a.run_id}?api-version={API}"))
    print(out["status"], out.get("message", "")[:400])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("deploy", deploy), ("run", run), ("status", status)):
        p = sub.add_parser(name)
        p.add_argument("--factory", required=True)
        p.add_argument("--resource-group", required=True)
        if name == "run":
            p.add_argument("--pipeline", required=True)
            p.add_argument("--param", action="append", default=[])
        if name == "status":
            p.add_argument("--run-id", required=True)
        p.set_defaults(fn=fn)
    a = ap.parse_args()
    a.fn(a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
