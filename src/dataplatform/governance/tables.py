"""Published tables, data products and the tag standard.

Names follow ADR-012: catalog <env>_<layer>, schema per source in silver and per domain in gold. A contract's
physicalName '<layer>_<schema>.<table>' (e.g. silver_erp.delivery, gold_sales.fact_otif) says both.

A data product is a gold fact with its metric views (product name = the fact without 'fact_'); `consumers` says which
products read each silver dataset and gold dimension. Every job and pipeline carries the tags of COMPUTE_TAGS
(`compute_tags`). Cost per data product is the separate cost tool's, which reads those tags (decision of 4 Nov 2026).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from dataplatform.semantic.metrics import MetricView

LAYERS = ("landing", "bronze", "silver", "gold", "ops")
CLASSIFICATIONS = ("public", "internal", "confidential", "restricted")
DEFAULT_CLASSIFICATION = "internal"  # SEC-01: every column carries one; a contract that says nothing means internal
PROTECTED = ("confidential", "restricted")  # masked for anyone not allowed to see them
TOKENISED = ("restricted",)  # personal data, tokenised before bronze (DPC-0070, PII-01): stored values are tokens
SHARED = "shared"
GOLD_SOURCE = "gold"  # compute tag 'source' of gold jobs

# The tag standard (data_catalog and cost_ledger kernel_owns). Keys are lower snake case.
TABLE_TAGS = ("domain", "layer", "data_product", "used_by", "owner", "contract", "certification", "glossary_terms")
COLUMN_TAGS = ("classification", "protection")
MANAGED_TAGS = tuple(sorted(set(TABLE_TAGS) | set(COLUMN_TAGS)))
COMPUTE_TAGS = ("env", "domain", "data_product", "source", "dataset", "cost_center")
TERM_SEPARATOR = " | "  # tag values avoid commas

_PHYSICAL = re.compile(r"^([a-z]+)_([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)$")


class GovernanceError(ValueError):
    """A contract or manifest that cannot be turned into catalog entries."""


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    logical_type: str
    classification: str
    description: str

    @property
    def protected(self) -> bool:
        return self.classification in PROTECTED

    @property
    def tokenised(self) -> bool:
        return self.classification in TOKENISED


@dataclass(frozen=True)
class TableSpec:
    layer: str
    schema: str
    name: str
    kind: str  # table | metric_view
    domain: str
    owner: str  # the owning team's mailbox (contract team member with role owner)
    team: str  # the owning team's name (contract team.name): the group that owns the data
    description: str
    contract: str  # id@version, "" for a metric view
    data_product: str  # the product, SHARED, or "" if nothing reads it
    used_by: tuple[str, ...]
    columns: tuple[ColumnSpec, ...] = ()
    certification: str = ""
    glossary_terms: tuple[str, ...] = ()

    def qualified(self, env: str) -> str:
        return f"{env}_{self.layer}.{self.schema}.{self.name}"


# ---------------------------------------------------------------------------------------------------- products


def product_of(fact: str) -> str:
    return fact.removeprefix("fact_")


def consumers(gold: Mapping[str, Mapping[str, Any]]) -> dict[str, tuple[str, ...]]:
    """Dataset key ('<source>.<dataset>' for silver, 'gold.<name>' for gold) -> the products that read it."""
    reads: dict[str, set[str]] = {}
    for name, doc in gold.items():
        if doc.get("kind") != "gold_fact":
            continue
        p = product_of(name)
        reads.setdefault(f"{GOLD_SOURCE}.{name}", set()).add(p)
        for i in doc.get("inputs") or ():
            reads.setdefault(str(i["dataset"]), set()).add(p)
        for d in doc.get("dimensions") or ():
            dim = str(d["name"])
            reads.setdefault(f"{GOLD_SOURCE}.{dim}", set()).add(p)
            for i in gold.get(dim, {}).get("inputs") or ():
                reads.setdefault(str(i["dataset"]), set()).add(p)
    return {k: tuple(sorted(v)) for k, v in sorted(reads.items())}


def compute_tags(
    env: str, domain: str, *, source: str, dataset: str, cost_center: str, data_product: str = ""
) -> dict[str, str]:
    """The tags every job and pipeline carries (cost_ledger kernel_owns, DPC-0062). Billing splits by these."""
    tags = {"env": env, "domain": domain, "source": source, "dataset": dataset, "cost_center": cost_center}
    if data_product:
        tags["data_product"] = data_product
    return tags


# ---------------------------------------------------------------------------------------------------- from contracts


def _owner(contract: Mapping[str, Any]) -> tuple[str, str]:
    team = contract.get("team") or {}
    owner = next((str(m["username"]) for m in team.get("members") or () if m.get("role") == "owner"), "")
    return str(team.get("name", "")), owner


def table_from_contract(
    contract: Mapping[str, Any], used: Mapping[str, tuple[str, ...]], dataset_key: str
) -> TableSpec:
    obj = contract["schema"][0]
    m = _PHYSICAL.match(str(obj.get("physicalName", "")))
    if not m or m.group(1) not in LAYERS:
        raise GovernanceError(
            f"{contract.get('id')}: physicalName '{obj.get('physicalName')}' is not <layer>_<schema>.<table> (ADR-012)"
        )
    layer, schema, name = m.groups()
    cols = []
    for p in obj["properties"]:
        cls = str(p.get("classification", DEFAULT_CLASSIFICATION)).lower()
        if cls not in CLASSIFICATIONS:
            raise GovernanceError(
                f"{contract.get('id')}.{p['name']}: classification '{cls}' is not one of " + ", ".join(CLASSIFICATIONS)
            )
        cols.append(ColumnSpec(str(p["name"]), str(p["logicalType"]), cls, str(p.get("description", ""))))
    desc = contract.get("description") or {}
    text = " ".join(str(desc[k]).strip() for k in ("purpose", "limitations") if desc.get(k))
    team, owner = _owner(contract)
    products = used.get(dataset_key, ())
    product = products[0] if len(products) == 1 else (SHARED if products else "")
    return TableSpec(
        layer,
        schema,
        name,
        "table",
        str(contract.get("domain", "")),
        owner,
        team,
        text,
        f"{contract['id']}@{contract['version']}",
        product,
        products,
        tuple(cols),
    )


def dataset_key(contract: Mapping[str, Any]) -> str:
    """The key the manifest and the compute tags use for the dataset a contract describes."""
    m = _PHYSICAL.match(str(contract["schema"][0].get("physicalName", "")))
    if not m:
        raise GovernanceError(f"{contract.get('id')}: physicalName does not follow ADR-012")
    layer, schema, name = m.groups()
    return f"{GOLD_SOURCE}.{name}" if layer == GOLD_SOURCE else f"{schema}.{name}"


def table_from_view(view: MetricView, team: str) -> TableSpec:
    product = product_of(view.source)
    terms = tuple(m.name for m in view.measures)
    text = (
        f"Metric view over {view.source}. "
        + (
            "Certified measures only (ADR-022). "
            if view.certified_only
            else "Preview: holds measures not yet certified; dashboards and the conversation space do not read it. "
        )
        + " ".join(f"{m.name}: {m.description}" for m in view.measures if m.description)
    ).strip()
    return TableSpec(
        "gold",
        view.domain,
        view.name,
        "metric_view",
        view.domain,
        view.owner,
        team,
        text,
        "",
        product,
        (product,),
        (),
        view.certification,
        terms,
    )


def published_tables(
    contracts: Iterable[Mapping[str, Any]], views: Iterable[MetricView], gold: Mapping[str, Mapping[str, Any]]
) -> list[TableSpec]:
    """Every table and view the catalog must describe, sorted by layer, schema and name."""
    used = consumers(gold)
    tables = [table_from_contract(c, used, dataset_key(c)) for c in contracts]
    team_of = {t.owner: t.team for t in tables}
    tables += [table_from_view(v, team_of.get(v.owner, "")) for v in views]
    return sorted(tables, key=lambda t: (LAYERS.index(t.layer), t.schema, t.name))


def owner_findings(pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]]) -> list[str]:
    """(contract, the manifest that pins it) pairs where the two name different owners (CAT-01: every difference is
    reported, never silently resolved). The catalog takes the contract's owner."""
    out = []
    for contract, manifest in pairs:
        owner = _owner(contract)[1]
        declared = str((manifest.get("owners") or {}).get("dataset", ""))
        if declared and owner != declared:
            out.append(f"{contract['id']}: contract owner {owner or '(none)'} but manifest owner {declared}")
    return sorted(out)
