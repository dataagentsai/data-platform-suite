"""The validating loader: a domain's manifest folder -> the metadata store (M3: domains are data; REL-01: only the
loader writes configuration, and only from manifest files in version control).

    slices/<n>-<domain>/manifest/platform.yaml        -> one domain row
    slices/<n>-<domain>/manifest/datasets/*.yaml      -> one dataset each: settings, columns (from its ODCS
                                                         contract), business keys, the manifest itself, a fingerprint

Everything is validated before anything is written: a missing field, a contract version mismatch or an unregistered
setting key stops the whole load (DPC-0001, DPC-0034). A value that does not fit its registered type is reported as a
warning. Datasets the store has but the manifest no longer declares are reported, never silently dropped.
Needs PyYAML (local and CI only); the platform reads only the store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dataplatform import contract as odcs
from dataplatform.keys import config_fingerprint
from dataplatform.manifest import ManifestError, validate
from dataplatform.meta import registry
from dataplatform.meta.port import DQ_ACTION_OF, DQ_SEVERITIES, GOLD_KINDS, DqRule, MetadataStore, SettingDef
from dataplatform.silver.engines import engine_problems

DEDUP_KEYS = ("mode", "business_keys", "order_by")


@dataclass
class LoadReport:
    domain: str
    loaded: list[str] = field(default_factory=list)  # source.dataset
    not_in_manifest: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _text(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, list):
        return ",".join(str(x) for x in v)
    return str(v)


def settings_of(manifest: dict[str, Any]) -> dict[str, str]:
    """The manifest's config block plus its dedup block as registry keys (dedup.mode, ...), all as text."""
    out = {k: _text(v) for k, v in (manifest.get("config") or {}).items()}
    for k in DEDUP_KEYS:
        if k in (manifest.get("dedup") or {}):
            out[f"dedup.{k}"] = _text(manifest["dedup"][k])
    return out


def columns_of(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"name": p["name"], "logical_type": p["logicalType"], "required": bool(p.get("required")),
             "primary_key_position": p.get("primaryKeyPosition") if p.get("primaryKey") else None,
             "description": p.get("description", ""), "classification": p.get("classification", "")}
            for p in odcs.properties(contract)]


def dq_rules_of(contract: dict[str, Any]) -> list[DqRule]:
    """The contract's quality rules (ODCS `quality` on a property or on the table) as the store's rules (B5): the
    rule id is <column or table>-<metric or type>-<n>; ODCS severity critical, major or minor sets the action (ADR-009);
    an unknown or missing severity counts as major. The check's own fields (metric, query, arguments, mustBe...) go
    into params."""
    rules: list[DqRule] = []
    places = [(p["name"], p.get("quality") or []) for p in odcs.properties(contract)]
    places.append(("", contract["schema"][0].get("quality") or []))
    for column, items in places:
        for n, q in enumerate(items, 1):
            check = str(q.get("metric") or q.get("type") or "custom")
            severity = str(q.get("severity", "major")).lower()
            severity = severity if severity in DQ_SEVERITIES else "major"
            params = {k: v for k, v in q.items() if k not in ("severity",)}
            rules.append(DqRule(f"{column or 'table'}-{check}-{n}", column, check,
                                json.loads(json.dumps(params, default=str)), severity, DQ_ACTION_OF[severity]))
    return rules


def dataset_doc(domain: str, manifest: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    plain = json.loads(json.dumps(manifest, default=str))
    return {
        "domain": domain,
        "source_name": manifest["source_name"],
        "source_env": manifest.get("source_env", ""),
        "dataset_name": manifest["dataset_name"],
        "is_enabled": bool(manifest["is_enabled"]),
        "kind": manifest.get("kind", "dataset"),
        "archetypes": list(manifest["archetypes"]),
        "ingest_mode": (manifest.get("ingest") or {}).get("mode", ""),
        "ingest_read": (manifest.get("ingest") or {}).get("read", ""),
        "history_kind": (manifest.get("history") or {}).get("kind", ""),
        "load_pattern": manifest["load"].get("pattern", ""),
        "contract_id": contract["id"],
        "contract_version": str(contract["version"]),
        "fingerprint": config_fingerprint({"manifest": plain,
                                           "contract": json.loads(json.dumps(contract, default=str))}),
        "settings": settings_of(manifest),
        "columns": columns_of(contract),
        "dq_rules": dq_rules_of(contract),
        "business_keys": {"primary": odcs.primary_key(contract),
                          "dedup": list((manifest.get("dedup") or {}).get("business_keys") or [])},
        "manifest": plain,
    }


def setting_problems(doc: dict[str, Any], defs: list[SettingDef]) -> tuple[list[str], list[str]]:
    """(errors: unregistered keys, warnings: values that do not fit their type)."""
    known = registry.by_key(defs, "dataset")
    name = f"{doc['source_name']}.{doc['dataset_name']}"
    errors = [f"{name}: setting '{k}' is not registered" for k in doc["settings"] if k not in known]
    warnings = [f"{name}: {p}" for k, v in doc["settings"].items() if k in known
                for p in [registry.type_problem(known[k], v)] if p]
    return errors, warnings


Docs = tuple[dict[str, Any], list[dict[str, Any]], list[str]]


def compile_domain(manifest_dir: Path, defs: list[SettingDef]) -> Docs:
    """Read and validate a domain's manifest folder. Returns (domain doc, dataset docs, warnings)."""
    import yaml  # local and CI only

    platform = yaml.safe_load((manifest_dir / "platform.yaml").read_text())
    domain = platform["domain"]
    plain = json.loads(json.dumps(platform, default=str))
    domain_doc = {"domain": domain, "business_timezone": platform.get("business_timezone", "UTC"),
                  "environments": list(platform.get("environments") or []),
                  "settings": {k: _text(v) for k, v in (platform.get("settings") or {}).items()},
                  "manifest": plain, "fingerprint": config_fingerprint(plain)}
    errors: list[str] = []
    warnings: list[str] = []
    docs: list[dict[str, Any]] = []
    for path in sorted((manifest_dir / "datasets").glob("*.yaml")):
        manifest = yaml.safe_load(path.read_text())
        contract = yaml.safe_load((path.parent / manifest["contract"]).read_text())
        errors += [f"{path.name}: {e}" for e in validate(manifest, contract)]
        doc = dataset_doc(domain, manifest, contract)
        e, w = setting_problems(doc, defs)
        errors, warnings = errors + e, warnings + w
        errors += [f"{path.name}: {p}" for p in engine_problems(
            f"{doc['source_name']}.{doc['dataset_name']}", manifest, doc["settings"], domain_doc["settings"])]
        docs.append(doc)
    domain_keys = registry.by_key(defs, "platform") | registry.by_key(defs, "dataset")
    errors += [f"platform.yaml: setting '{k}' is not registered" for k in domain_doc["settings"]
               if k not in domain_keys]
    if errors:
        raise ManifestError("; ".join(errors))
    return domain_doc, docs, warnings


def load(store: MetadataStore, manifest_dir: Path, defs: list[SettingDef] | None = None,
         extra_settings: dict[str, dict[str, str]] | None = None) -> LoadReport:
    """Register the settings, then write the domain and every dataset. Validation happens before any write.

    `extra_settings` ({"source.dataset": {key: value}}) adds binding-level settings on top of the manifest, e.g. the
    delivery shape of a change-capture service (B4); every key must be registered, and the dataset's fingerprint
    covers them, so the run record shows which shape a run read."""
    defs = defs if defs is not None else registry.load()
    domain_doc, docs, warnings = compile_domain(manifest_dir, defs)
    known = registry.by_key(defs, "dataset")
    bad = [f"{name}: setting '{k}' is not registered" for name, extra in (extra_settings or {}).items()
           for k in extra if k not in known]
    if bad:
        raise ManifestError("; ".join(bad))
    for doc in docs:
        extra = (extra_settings or {}).get(f"{doc['source_name']}.{doc['dataset_name']}")
        if extra:
            doc["settings"] = {**doc["settings"], **extra}
            binding = dict(sorted(extra.items()))
            doc["fingerprint"] = config_fingerprint({"manifest": doc["fingerprint"], "binding": binding})
    store.register_settings(defs)
    store.load_domain(domain_doc)
    report = LoadReport(domain_doc["domain"], warnings=warnings)
    for doc in docs:
        rules = doc.pop("dq_rules", [])
        store.load_dataset(doc)
        store.set_dq_rules(doc["source_name"], doc["dataset_name"], rules)
        report.loaded.append(f"{doc['source_name']}.{doc['dataset_name']}")
    declared = set(report.loaded)
    report.not_in_manifest = sorted(f"{d.source}.{d.dataset}" for d in store.datasets(domain_doc["domain"])
                                    if f"{d.source}.{d.dataset}" not in declared)
    return report



def gold_doc(domain: str, manifest: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    """A gold manifest as a dataset of the store: source `gold_<manifest domain>` (its gold schema, ADR-012), the
    contract's columns, the grain as its primary key. Settings stay empty: gold has no registered keys yet."""
    plain = json.loads(json.dumps(manifest, default=str))
    return {
        "domain": domain,
        "source_name": f"gold_{manifest['domain']}",
        "source_env": "",
        "dataset_name": manifest["name"],
        "is_enabled": True,
        "kind": manifest["kind"],
        "archetypes": [],
        "ingest_mode": "",
        "ingest_read": "",
        "history_kind": str(manifest.get("history", "")),
        "load_pattern": "window_recompute" if manifest["kind"] == "gold_fact" else "replace",
        "contract_id": contract["id"],
        "contract_version": str(contract["version"]),
        "fingerprint": config_fingerprint({"manifest": plain,
                                           "contract": json.loads(json.dumps(contract, default=str))}),
        "settings": {},
        "columns": columns_of(contract),
        "business_keys": {"primary": list(manifest.get("grain") or []), "dedup": []},
        "manifest": plain,
    }


def load_gold(store: MetadataStore, manifest_dir: Path) -> list[str]:
    """Write every gold manifest (manifest/gold/*.yaml) into the store beside the source datasets, after `load`.
    A gold manifest's kind or contract version that does not match is refused before anything is written."""
    import yaml  # local and CI only

    domain = yaml.safe_load((manifest_dir / "platform.yaml").read_text())["domain"]
    docs, errors = [], []
    for path in sorted((manifest_dir / "gold").glob("*.yaml")):
        manifest = yaml.safe_load(path.read_text())
        contract = yaml.safe_load((path.parent / manifest["contract"]).read_text())
        if manifest.get("kind") not in GOLD_KINDS:
            errors.append(f"{path.name}: kind {manifest.get('kind')!r} is not one of {GOLD_KINDS}")
        if str(manifest.get("contract_version")) != str(contract["version"]):
            errors.append(f"{path.name}: contract_version {manifest.get('contract_version')} but the contract is "
                          f"{contract['version']}")
        docs.append(gold_doc(domain, manifest, contract))
    if errors:
        raise ManifestError("; ".join(errors))
    for doc in docs:
        store.load_dataset(doc)
    return [f"{d['source_name']}.{d['dataset_name']}" for d in docs]
