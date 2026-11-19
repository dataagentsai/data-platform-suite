"""Generate the semantic layer, BI, catalog sync, access model and audit files for one domain's slice (cost belongs
to the separate cost tool since 4 Nov 2026; the jobs keep the tag convention, governance.tables.COMPUTE_TAGS).

    uv run --no-project --with pyyaml python realms/databricks/semantic_governance/generate.py           # write
    uv run --no-project --with pyyaml python realms/databricks/semantic_governance/generate.py --check  # 1 if stale

Inputs: the slice in the catalog (DP_SLICE, default ~/data-platform-catalog/slices/02-tyre): manifest metric, gold and
dataset files, ODCS contracts, platform.yaml; and the access model access/<domain>.yaml in this folder.
Output: generated/<domain>/..., byte-identical for the same input, committed so a stale file fails CI (golden files).
The catalog sync is planned against an empty catalog here (every entry written; the statements are idempotent); at
deploy the realm reads the live catalog through dataplatform.adapters.uc_catalog and applies only the difference.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(HERE))

import render  # noqa: E402  (this folder; the realm is not a package)

from dataplatform.access.model import AccessPlan, model_from_doc, plan_access  # noqa: E402
from dataplatform.adapters.memory_catalog import MemoryCatalogState  # noqa: E402
from dataplatform.governance.catalog import Action, desired_entries, plan_sync  # noqa: E402
from dataplatform.governance.tables import (  # noqa: E402
    TableSpec,
    owner_findings,
    published_tables,
    table_from_view,
)
from dataplatform.semantic import experiences  # noqa: E402
from dataplatform.semantic.metrics import (  # noqa: E402
    MetricView,
    known_answer_cases,
    lint,
    metric_view,
    published,
)

DEFAULT_SLICE = Path(os.environ.get("DP_SLICE", str(Path.home() / "data-platform-catalog/slices/02-tyre")))
OUT = HERE / "generated"


class GenerateError(ValueError):
    pass


Doc = dict[str, Any]


@dataclass(frozen=True)
class Slice:
    platform: Doc
    gold: dict[str, Doc]
    metrics: list[Doc]
    datasets: list[Doc]
    contracts: list[Doc]
    pinned: list[tuple[Doc, Doc]]  # (contract, the manifest that pins it)


@dataclass(frozen=True)
class Built:
    domain: str
    environments: tuple[str, ...]
    views: list[MetricView]  # as defined in the metric files
    published: list[MetricView]  # certified views and previews
    tables: list[TableSpec]
    access: AccessPlan
    findings: list[str]


def _yaml(path: Path) -> Doc:
    doc = yaml.safe_load(path.read_text())
    if not isinstance(doc, dict):
        raise GenerateError(f"{path}: not a mapping")
    return doc


def load_slice(slice_dir: Path) -> Slice:
    man = slice_dir / "manifest"
    gold_files = sorted((man / "gold").glob("*.yaml"))
    data_files = sorted((man / "datasets").glob("*.yaml"))
    pinned = []
    for f in gold_files + data_files:
        doc = _yaml(f)
        contract_path = (f.parent / doc["contract"]).resolve()
        pinned.append((_yaml(contract_path), doc))
    gold = {str(d["name"]): d for d in (_yaml(f) for f in gold_files)}
    return Slice(
        _yaml(man / "platform.yaml"),
        gold,
        [_yaml(f) for f in sorted((man / "metrics").glob("*.yaml"))],
        [_yaml(f) for f in data_files],
        [c for c, _ in pinned],
        pinned,
    )


def build(sl: Slice, access_doc: Doc) -> Built:
    views = [metric_view(d, sl.gold) for d in sl.metrics]
    errors = lint(views)
    if errors:
        raise GenerateError("; ".join(errors))
    pub = [p for v in views for p in published(v)]
    tables = published_tables(sl.contracts, pub, sl.gold)
    return Built(
        str(sl.platform["domain"]),
        tuple(str(e) for e in sl.platform["environments"]),
        views,
        pub,
        tables,
        plan_access(model_from_doc(access_doc), tables),
        owner_findings(sl.pinned),
    )


def catalog_actions(b: Built, env: str, current: MemoryCatalogState | None = None) -> list[Action]:
    state = current or MemoryCatalogState()
    return plan_sync(desired_entries(b.tables), state.entries(env))


def _findings_md(b: Built) -> str:
    lines = [
        f"# Generation findings: {b.domain}",
        "",
        "Differences between contracts and manifests the catalog sync reports instead of resolving (CAT-01). "
        "The catalog takes the contract's owner.",
        "",
    ]
    lines += [f"- {f}" for f in b.findings] or ["- none"]
    return "\n".join(lines) + "\n"


def generate(slice_dir: Path = DEFAULT_SLICE, access_dir: Path = HERE / "access") -> dict[str, str]:
    """Relative output path -> file text. Reads the slice and the access model; writes nothing."""
    sl = load_slice(slice_dir)
    b = build(sl, _yaml(access_dir / f"{sl.platform['domain']}.yaml"))
    root = b.domain
    files = {
        f"{root}/findings.md": _findings_md(b),
    }
    team = {t.name: t.team for t in b.tables}
    space = experiences.conversation_space(
        f"{b.domain}_metrics",
        f"{b.domain.capitalize()} metrics",
        b.published,
        str(sl.platform["business_timezone"]),
        b.views,
    )
    for env in b.environments:
        base = f"{root}/{env}"
        for v in b.published:
            comment = table_from_view(v, team.get(v.name, "")).description
            files[f"{base}/semantic/{v.name}.sql"] = render.metric_view_sql(v, env, comment)
        files[f"{base}/semantic/known_answers.sql"] = render.known_answers_sql(
            known_answer_cases(b.published), b.published, env
        )
        for d in experiences.dashboards(b.published):
            files[f"{base}/bi/{d.name}.lvdash.json"] = render.dashboard_json(d, b.published, env)
        files[f"{base}/bi/genie_space.json"] = render.genie_space_json(space, b.published, env)
        files[f"{base}/governance/catalog_sync.sql"] = render.catalog_sync_sql(catalog_actions(b, env), env)
        files[f"{base}/governance/audit.sql"] = render.audit_sql(b.tables, env)
        files[f"{base}/access/access.sql"] = render.access_sql(b.access, env)
    return dict(sorted(files.items()))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true", help="fail if a committed file differs from the generator")
    p.add_argument("--slice", type=Path, default=DEFAULT_SLICE)
    a = p.parse_args()
    files = generate(a.slice)
    existing = {str(f.relative_to(OUT)) for f in OUT.rglob("*") if f.is_file()} if OUT.exists() else set()
    stale = sorted(n for n, text in files.items() if not (OUT / n).exists() or (OUT / n).read_text() != text)
    extra = sorted(existing - set(files))
    if a.check:
        for n in stale + extra:
            sys.stderr.write(f"stale: realms/databricks/semantic_governance/generated/{n}\n")
        return 1 if stale or extra else 0
    for n, text in files.items():
        (OUT / n).parent.mkdir(parents=True, exist_ok=True)
        (OUT / n).write_text(text)
    for n in extra:
        (OUT / n).unlink()
    sys.stdout.write(f"wrote {len(files)} files to realms/databricks/semantic_governance/generated/\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
