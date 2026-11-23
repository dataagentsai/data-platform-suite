"""Generate the managed copy service's pipelines for the movers (ADR-033, binding `mover` in dbs/cloud/azure.yaml).

    python3 realms/azure-databricks/generate_adf.py --account dpcshdev            # write adf/*.json
    python3 realms/azure-databricks/generate_adf.py --account dpcshdev --check    # exit 1 if stale

Input: adf/movers.json, one entry per source dataset that a mover (not our own puller) brings in. Output: adf/*.json,
one file per factory object (datasets and pipelines), in the shape the factory's management API takes. `make adf-deploy`
puts them in the factory.

Every pipeline does the same two things, in this order, because that is the landing contract (DPC-0038, ADR-033):
  1. copy the source into landing as JSON lines, under a unique name, in <source>/<dataset>/<window>/
  2. write the delivery's manifest LAST, refusing to overwrite one (If-None-Match: *), with the row count the copy
     reported. The lander does not run until the manifest exists, and a second writer is refused (DUP-02, ORCH-01).
The pipeline never decides what the data means; the lander checks the count and the columns.

Kinds of source:
  csv_files   delimited files in the file drop zone (<system>/<dataset>/<name>), one pipeline run per file
  sql_table   a database table copied whole, through a linked service named ls_<source> (the connection and its secret
              are the owner's to create; none is written here)
Not generated yet: REST pages, change capture (AZ-6).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
OUT = HERE / "adf"
IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
KINDS = ("csv_files", "sql_table")
API_VERSION = "2023-11-03"          # the blob service's, sent with the manifest write
LANDING = "landing"
DROP = "drop"
LINKED_STORAGE = "ls_storage"


class MoverError(ValueError):
    """A movers file the generator will not turn into pipelines."""


def check(mover: dict[str, Any]) -> None:
    for key in ("source", "dataset", "kind"):
        if key not in mover:
            raise MoverError(f"mover {mover}: '{key}' is missing")
    for key in ("source", "dataset"):
        if not IDENT.match(str(mover[key])):
            raise MoverError(f"mover {mover['source']}.{mover['dataset']}: {key} {mover[key]!r} must be lowercase letters, digits and underscores")
    if mover["kind"] not in KINDS:
        raise MoverError(f"{mover['source']}.{mover['dataset']}: kind {mover['kind']!r} is not one of {KINDS}")
    if mover["kind"] == "sql_table" and not IDENT.match(str(mover.get("table", ""))):
        raise MoverError(f"{mover['source']}.{mover['dataset']}: sql_table needs a table named with lowercase letters, digits and underscores")


def pipeline_name(m: dict[str, Any]) -> str:
    return f"mv_{m['source']}_{m['dataset']}"


def landing_name(m: dict[str, Any]) -> str:
    """The delivery's file name, unique per RUN (an expression): dataset_window[_stem]_runid.jsonl. A re-run, or a source
    file that changed, therefore never overwrites a delivery that was already landed (DUP-02); the lander sees the same
    window and bytes and skips the resend (DUP-01), or sees different bytes and loads a new delivery."""
    stem = "pipeline().parameters.stem, '_', " if m["kind"] == "csv_files" else ""
    return f"@concat('{m['dataset']}_', pipeline().parameters.window, '_', {stem}pipeline().RunId, '.jsonl')"


def datasets(m: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The factory datasets one mover needs: the source and the landing sink (JSON lines)."""
    sink = {
        "name": f"ds_land_{m['source']}_{m['dataset']}",
        "properties": {
            "type": "Json",
            "linkedServiceName": {"referenceName": LINKED_STORAGE, "type": "LinkedServiceReference"},
            "parameters": {"window": {"type": "string"}, "file": {"type": "string"}},
            "typeProperties": {"location": {
                "type": "AzureBlobFSLocation", "fileSystem": LANDING,
                "folderPath": {"value": f"@concat('{m['source']}/{m['dataset']}/', dataset().window)", "type": "Expression"},
                "fileName": {"value": "@dataset().file", "type": "Expression"}}},
        },
    }
    out = {sink["name"]: sink}
    if m["kind"] == "csv_files":
        src = {
            "name": f"ds_drop_{m['source']}_{m['dataset']}",
            "properties": {
                "type": "DelimitedText",
                "linkedServiceName": {"referenceName": LINKED_STORAGE, "type": "LinkedServiceReference"},
                "parameters": {"stem": {"type": "string"}},
                "typeProperties": {
                    "location": {"type": "AzureBlobFSLocation", "fileSystem": DROP,
                                 "folderPath": f"{m['source']}/{m['dataset']}",
                                 "fileName": {"value": "@concat(dataset().stem, '.csv')", "type": "Expression"}},
                    "columnDelimiter": ",", "firstRowAsHeader": True, "escapeChar": "\\", "quoteChar": "\""},
            },
        }
    else:
        src = {
            "name": f"ds_db_{m['source']}_{m['dataset']}",
            "properties": {
                "type": "AzureSqlTable",
                "linkedServiceName": {"referenceName": f"ls_{m['source']}", "type": "LinkedServiceReference"},
                "typeProperties": {"schema": m.get("schema", "dbo"), "table": m["table"]},
            },
        }
    out[src["name"]] = src
    return out


def manifest_body(m: dict[str, Any]) -> str:
    """The manifest, as an expression that builds the JSON text from the copy's own row count."""
    name = landing_name(m)[1:]                          # the same file name expression, without the leading @
    return ("@concat('{\"source\":\"" + m["source"] + "\",\"dataset\":\"" + m["dataset"] + "\",\"window\":\"', "
            "pipeline().parameters.window, '\",\"rows\":', string(activity('copy').output.rowsCopied), "
            "',\"file\":\"', " + name + ", '\"}')")


def pipeline(m: dict[str, Any], account: str) -> dict[str, Any]:
    ds = datasets(m)
    sink_ds = f"ds_land_{m['source']}_{m['dataset']}"
    src_ds = next(n for n in ds if n != sink_ds)
    file_expr = {"value": landing_name(m), "type": "Expression"}
    if m["kind"] == "csv_files":
        source = {"type": "DelimitedTextSource",
                  "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": False},
                  "formatSettings": {"type": "DelimitedTextReadSettings"}}
        src_params = {"stem": {"value": "@pipeline().parameters.stem", "type": "Expression"}}
        params = {"window": {"type": "string"}, "stem": {"type": "string"}}
    else:
        source = {"type": "AzureSqlSource", "queryTimeout": "02:00:00"}
        src_params = {}
        params = {"window": {"type": "string"}}
    copy = {
        "name": "copy", "type": "Copy",
        "typeProperties": {
            "source": source,
            "sink": {"type": "JsonSink",
                     "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                     "formatSettings": {"type": "JsonWriteSettings", "filePattern": "setOfObjects"}},
        },
        "inputs": [{"referenceName": src_ds, "type": "DatasetReference", "parameters": src_params}],
        "outputs": [{"referenceName": sink_ds, "type": "DatasetReference",
                     "parameters": {"window": "@pipeline().parameters.window", "file": file_expr}}],
        "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 60},
    }
    key = f"@concat('https://{account}.blob.core.windows.net/{LANDING}/{m['source']}/{m['dataset']}/', pipeline().parameters.window, '/', " + landing_name(m)[1:] + ", '.manifest.json')"
    manifest = {
        "name": "manifest", "type": "WebActivity",
        "dependsOn": [{"activity": "copy", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
            "method": "PUT",
            "url": {"value": key, "type": "Expression"},
            "headers": {"x-ms-blob-type": "BlockBlob", "x-ms-version": API_VERSION, "If-None-Match": "*",
                        "Content-Type": "application/json"},
            "body": {"value": manifest_body(m), "type": "Expression"},
            "authentication": {"type": "MSI", "resource": "https://storage.azure.com/"},
        },
        "policy": {"timeout": "0.00:10:00", "retry": 2, "retryIntervalInSeconds": 30},
    }
    return {"name": pipeline_name(m),
            "properties": {"parameters": params, "activities": [copy, manifest],
                           "annotations": ["generated", f"mover:{m['kind']}", "landing contract DPC-0038"]}}


def build(movers: list[dict[str, Any]], account: str) -> dict[str, dict[str, Any]]:
    """{relative file name: factory object} for every dataset and pipeline of the movers."""
    if not re.match(r"^[a-z0-9]{3,24}$", account):
        raise MoverError(f"storage account {account!r} is not a valid account name")
    files: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for m in movers:        # check every mover first: sorting needs the keys, and a bad one gets a reason, not a KeyError
        check(m)
    for m in sorted(movers, key=lambda x: (x["source"], x["dataset"])):
        pid = pipeline_name(m)
        if pid in seen:
            raise MoverError(f"{pid}: two movers for one dataset")
        seen.add(pid)
        for name, obj in datasets(m).items():
            files[f"datasets/{name}.json"] = obj
        files[f"pipelines/{pid}.json"] = pipeline(m, account)
    return files


def render(files: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {k: json.dumps(v, indent=2, sort_keys=True) + "\n" for k, v in files.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account", required=True, help="the environment's storage account name (landing and drop)")
    ap.add_argument("--movers", type=Path, default=OUT / "movers.json")
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    files = render(build(json.loads(a.movers.read_text()), a.account))
    stale = [k for k, v in files.items() if not (a.out / k).exists() or (a.out / k).read_text() != v]
    if a.check:
        print("\n".join(f"stale: {k}" for k in stale) or "adf/ is up to date")
        return 1 if stale else 0
    for k, v in files.items():
        (a.out / k).parent.mkdir(parents=True, exist_ok=True)
        (a.out / k).write_text(v)
    print(f"{len(files)} factory objects -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
