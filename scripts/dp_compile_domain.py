"""Compile what the job generator needs to know about a domain from its DPM manifest folder.

    uv run --with pyyaml python scripts/dp_compile_domain.py [catalog_dir] [slice]

Writes realms/databricks/compiled/domains/<domain>.json: the domain, its business timezone, its datasets with their
schedules, and its gold tables. The file is committed so the generated bundle can be checked without the catalog
beside the repo; a test checks it is up to date with the catalog when the catalog is there.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCHEDULE_KEYS = ("schedule_trigger", "schedule_cron", "schedule_timezone")


class ManifestError(ValueError):
    pass


def summarise(manifest: Path, slice_name: str) -> dict[str, Any]:
    """Pure over the files it reads: the same manifest folder gives the same summary, keys and lists sorted."""
    platform_file = manifest / "platform.yaml"
    if not platform_file.exists():
        raise ManifestError(f"{manifest} has no platform.yaml")
    platform = yaml.safe_load(platform_file.read_text())
    for key in ("domain", "business_timezone"):
        if not platform.get(key):
            raise ManifestError(f"{platform_file}: `{key}` is missing")
    datasets = []
    for f in sorted((manifest / "datasets").glob("*.yaml")):
        d = yaml.safe_load(f.read_text())
        cfg = d.get("config") or {}
        datasets.append({"source": d["source_name"], "dataset": d["dataset_name"],
                         "is_enabled": bool(d.get("is_enabled", True)),
                         **{k: cfg[k] for k in SCHEDULE_KEYS if k in cfg}})
    if not datasets:
        raise ManifestError(f"{manifest}/datasets holds no dataset")
    gold = sorted(f.stem for f in (manifest / "gold").glob("*.yaml"))
    return {"domain": platform["domain"], "slice": slice_name, "business_timezone": platform["business_timezone"],
            "datasets": datasets, "gold": gold}


def main() -> int:
    catalog = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / "data-platform-catalog"
    slice_name = sys.argv[2] if len(sys.argv) > 2 else "02-tyre"
    summary = summarise(catalog / "slices" / slice_name / "manifest", slice_name)
    out = ROOT / "realms" / "databricks" / "compiled" / "domains" / f"{summary['domain']}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n")
    print(f"compiled domain {summary['domain']} ({len(summary['datasets'])} datasets, {len(summary['gold'])} gold "
          f"tables) -> {out.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
