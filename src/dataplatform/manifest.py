"""DPM manifest -> compiled config. Runs locally or in CI (needs PyYAML); the platform itself reads only the result,
after the register step has loaded it into the metadata DB (agreed 27 Oct: YAML in git -> validating loader -> DB).

The compiled file carries table names as templates (${bronze}, ${silver}, ...). The register step fills them in per
environment, because one workspace hosts every environment on Free Edition (one schema set per environment).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dataplatform import contract as odcs

REQUIRED = ["source_name", "dataset_name", "is_enabled", "config", "contract", "contract_version", "archetypes",
            "load", "reconciliation", "owners"]


class ManifestError(ValueError):
    pass


def validate(manifest: dict[str, Any], contract: dict[str, Any]) -> list[str]:
    """Problems that stop a compile. An empty list means it is valid."""
    errors = [f"missing field: {f}" for f in REQUIRED if f not in manifest]
    if errors:
        return errors
    if str(manifest["contract_version"]) != str(contract.get("version")):
        errors.append(f"manifest pins contract {manifest['contract_version']} but the contract is {contract.get('version')}")
    names = {c for c, _ in odcs.columns(contract)}
    order_by = manifest["load"].get("history_order_by")
    if order_by and order_by not in names:
        errors.append(f"load.history_order_by '{order_by}' is not a contract column")
    for m in manifest["reconciliation"].get("measures", []):
        if m.get("column") and m["column"] not in names:
            errors.append(f"reconciliation measure '{m['name']}' uses unknown column '{m['column']}'")
    for w in manifest.get("waivers") or []:
        for f in ("concern", "reason", "approver", "expires"):
            if f not in w:
                errors.append(f"waiver for {w.get('concern', '?')} has no {f}")
    return errors


def compile_slice(slice_dir: Path) -> dict[str, Any]:
    import yaml  # local only

    m_path = next((slice_dir / "manifest").glob("*.yaml"))
    manifest = yaml.safe_load(m_path.read_text())
    contract = yaml.safe_load((m_path.parent / manifest["contract"]).read_text())
    world = yaml.safe_load((slice_dir / "world.yaml").read_text())
    errors = validate(manifest, contract)
    if errors:
        raise ManifestError("; ".join(errors))
    ds = manifest["dataset_name"]
    return {
        "dataset": {
            **{k: manifest[k] for k in ("source_name", "dataset_name", "is_enabled", "config", "dedup", "archetypes",
                                        "load", "volume", "reconciliation", "owners", "concerns") if k in manifest},
            "bronze_table": f"${{bronze}}.{ds}_changes",
            "silver_table": f"${{silver}}.{ds}",
            "quarantine_table": f"${{silver}}.{ds}_quarantine",
            "gold_table": f"${{gold}}.{ds}_daily",
            "keys": odcs.primary_key(contract),
            "sequence_by": manifest["load"]["history_order_by"],
            "quality_rules": odcs.row_rules(contract),
            "contract_id": contract["id"],
            "contract_version": contract["version"],
        },
        "contract": json.loads(json.dumps(contract, default=str)),
        "world": json.loads(json.dumps(world, default=str)),
    }


def fill(compiled: dict[str, Any], schemas: dict[str, str]) -> dict[str, Any]:
    """Replace ${bronze} etc. with this environment's schema names."""
    text = json.dumps(compiled)
    for k, v in schemas.items():
        text = text.replace("${" + k + "}", v)
    return json.loads(text)
