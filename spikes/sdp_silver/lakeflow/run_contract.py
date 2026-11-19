"""Run the SCD Type 2 contract on the declarative engine (Lakeflow) and compare meaning with the shared cases.

    python run_contract.py     (needs the databricks CLI, profile free; PYTHONPATH=<repo>/src)
"""
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from dataplatform.silver.history_contract import CASES, normalise_closed, slug

P = ["-p", "free"]
HERE = Path(__file__).parent
TAG = f"c{int(time.time())}"


def sh(*cmd: str) -> str:
    out = subprocess.run(list(cmd), cwd=HERE, capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(f"{' '.join(cmd[:4])}: {out.stderr[-800:] or out.stdout[-800:]}")
    return out.stdout


def sql(statement: str) -> list[dict]:
    body = json.dumps({"warehouse_id": "62a5ea0c0e6e5701", "statement": statement, "wait_timeout": "50s"})
    r = json.loads(sh("databricks", "api", "post", "/api/2.0/sql/statements", *P, "--json", body))
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"{statement}: {r['status']}")
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, row)) for row in (r.get("result") or {}).get("data_array") or []]


sh("databricks", "api", "post", "/api/2.0/sql/statements", *P, "--json",
   json.dumps({"warehouse_id": "62a5ea0c0e6e5701", "statement": "CREATE SCHEMA IF NOT EXISTS dev_silver.spike_contract",
               "wait_timeout": "50s"}))
sh("databricks", "bundle", "deploy", *P, "--var", f"contract_tag={TAG}")
rounds = max(len(b) for _, b, _ in CASES)
with tempfile.TemporaryDirectory() as tmp:
    for r in range(1, rounds + 1):
        for case, batches, _ in CASES:
            if r <= len(batches):
                f = Path(tmp) / f"{slug(case)}-{r}.json"
                cols = ["k", "v", "note", "seq", "op", "minute", "pos"]
                f.write_text("\n".join(json.dumps(dict(zip(cols, row))) for row in batches[r - 1]) + "\n")
                sh("databricks", "fs", "mkdir", f"dbfs:/Volumes/dev_ops/spike/changes/contract/{TAG}/{slug(case)}", *P)
                sh("databricks", "fs", "cp", str(f), f"dbfs:/Volumes/dev_ops/spike/changes/contract/{TAG}/{slug(case)}/batch-{r}.json",
                   *P, "--overwrite")
        extra = ["--full-refresh-all"] if r == 1 else []
        sh("databricks", "bundle", "run", "sdp_silver_contract", *P, *extra)
        print(f"round {r} applied")
ok = True
for case, _, want in CASES:
    rows = sql(f"SELECT k, v, __START_AT, __END_AT FROM dev_silver.spike_contract.c_{slug(case)}")
    got = normalise_closed(rows, "k", "v", "__START_AT", "__END_AT")
    good = got == want
    ok &= good
    print(("PASS " if good else "FAIL ") + case + ("" if good else f"\n      want {want}\n      got  {got}"))
sys.exit(0 if ok else 1)
