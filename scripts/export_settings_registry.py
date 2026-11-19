"""Export the settings registry for the metadata store (DPC-0034) from LakeIngestionFramework's key registry.

Reuse, not restate: the keys, types, defaults and allowed values come from
src/lake_ingestion_framework/core/metadata/keys (the registry the old framework validated against), plus the keys the
spec catalog proposes for it (data-platform-catalog dpm/keys.yaml, `proposed`). The kernel reads the JSON this
writes with the standard library only; tests/dataplatform/test_meta_registry.py fails if the two drift apart.

    PYTHONPATH=src uv run --no-project --with pydantic --with pyyaml python scripts/export_settings_registry.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "src/dataplatform/meta/settings_registry.json"
CATALOG_KEYS = Path.home() / "data-platform-catalog/dpm/keys.yaml"
SCOPES = {"platform_config": "platform", "source_config": "source", "dataset_config": "dataset"}


def build(catalog_keys: Path = CATALOG_KEYS) -> list[dict[str, object]]:
    import yaml

    from lake_ingestion_framework.core.metadata.keys import ALL_KEYS

    rows: list[dict[str, object]] = []
    for k in ALL_KEYS:
        rows.append({
            "key": k.name,
            "scope": SCOPES[str(k.table.value)],
            "value_type": str(k.type.value),
            "default": None if k.default is None else str(k.default).lower() if isinstance(k.default, bool)
            else str(k.default),
            "allowed": list(k.enum_values) if k.enum_values else None,
            "description": k.description,
            "status": "registered",
        })
    proposed = yaml.safe_load(catalog_keys.read_text()).get("proposed") or []
    for p in proposed:
        rows.append({"key": p["key"], "scope": "dataset", "value_type": "string", "default": None, "allowed": None,
                     "description": " ".join(str(p["why"]).split()), "status": "proposed"})
    return sorted(rows, key=lambda r: (str(r["scope"]), str(r["key"])))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="fail if the committed JSON is out of date")
    a = ap.parse_args()
    text = json.dumps(build(), indent=1, sort_keys=True) + "\n"
    if a.check:
        if OUT.read_text() != text:
            print(f"{OUT} is out of date; run {Path(__file__).name}")
            return 1
        return 0
    OUT.write_text(text)
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
