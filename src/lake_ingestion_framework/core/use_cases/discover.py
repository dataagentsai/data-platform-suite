"""``lake-ingest discover`` — read-only catalog walk over a registered source.

Given a source already in ``source_master`` (loaded via
``config sync``), this use case:

1. Builds a ``SourceConnector`` for the source's type.
2. Walks INFORMATION_SCHEMA (or equivalent) to enumerate
   schemas / tables / columns.
3. Runs heuristics (watermark candidate, PII classification).
4. Produces a ``DiscoverProposal`` — which dataset rows to add /
   update + which column rows to write.
5. ``--apply`` writes the proposal to the metadata DB; new datasets
   land with ``is_enabled=False`` per CLAUDE.md.

The connector + secret resolver are passed in (dependency injection).
This keeps the use case unit-testable with mocked connectors and
no Postgres / pyodbc / Azure dependency.

Per ``docs/discovery.md``: read-only, dry-run by default, new
datasets disabled.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    MetadataRepository,
    SourceRow,
)
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    SecretResolver,
)
from lake_ingestion_framework.core.source_connectors.factory import make_connector
from lake_ingestion_framework.core.source_connectors.protocol import (
    ColumnInfo,
    SourceConnector,
    TableMeta,
)
from lake_ingestion_framework.core.types.enums import (
    DatasetType,
    LoadType,
    PiiClass,
    Realm,
)
from lake_ingestion_framework.core.use_cases._discover_heuristics import (
    classify_pii,
    find_watermark_candidates,
)

# ──────────────────────────────────────────────────────────────────────
# Proposal shape
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ColumnProposal:
    """One column the discover proposes to register."""

    column_name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    is_primary_key: bool
    pii_class: PiiClass


@dataclass(frozen=True)
class DatasetProposal:
    """One dataset the discover proposes to register.

    ``extra_config`` carries connector-specific dataset_config keys
    that apply only to certain source types (e.g.
    ``rest_endpoint_path`` for REST API sources). They get merged
    into the dataset_config row at apply time and validated by the
    repository's key registry.
    """

    dataset_name: str
    schema_name: str
    object_name: str
    dataset_type: DatasetType
    load_type: LoadType
    watermark_column: str | None
    target_path_template: str
    columns: tuple[ColumnProposal, ...]
    warnings: tuple[str, ...] = field(default_factory=tuple)
    extra_config: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoverProposal:
    """Full result of a discover run."""

    source_name: str
    source_env: str
    datasets: tuple[DatasetProposal, ...]
    schemas_seen: tuple[str, ...]
    skipped: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DiscoverResult:
    """Counts written by ``apply_discover``."""

    datasets_upserted: int
    columns_upserted: int


# ──────────────────────────────────────────────────────────────────────
# Plan
# ──────────────────────────────────────────────────────────────────────


def plan_discover(
    *,
    source_name: str,
    source_env: str,
    repo: MetadataRepository,
    secret_resolver: SecretResolver,
    schemas: tuple[str, ...] | None = None,
    target_path_template: str = "raw/{source}/{schema}/{object}/{yyyy}/{mm}/{dd}",
) -> DiscoverProposal:
    """Walk the source catalog and propose dataset metadata.

    Pure read against the source. No writes anywhere — the proposal
    is the deliverable.

    ``schemas`` filters which schemas to walk. ``None`` means all
    user-visible schemas.
    """
    source = repo.get_source(source_name, source_env)
    if source is None:
        raise ValidationError(
            f"Source ({source_name!r}, env={source_env!r}) not found. "
            "Register it via `lake-ingest config sync` first."
        )

    config = _read_source_config(repo, source)

    connector = make_connector(source, config, secret_resolver)
    try:
        connector.connect()
        return _walk_source(
            connector=connector,
            source=source,
            schemas_filter=schemas,
            target_path_template=target_path_template,
        )
    finally:
        connector.close()


def _walk_source(
    *,
    connector: SourceConnector,
    source: SourceRow,
    schemas_filter: tuple[str, ...] | None,
    target_path_template: str,
) -> DiscoverProposal:
    schemas_seen = tuple(connector.list_schemas())
    schemas = schemas_seen if schemas_filter is None else schemas_filter

    proposals: list[DatasetProposal] = []
    skipped: list[str] = []

    for schema in schemas:
        if schemas_filter is not None and schema not in schemas_seen:
            skipped.append(f"schema {schema!r} not present in source — skipped")
            continue

        for table in connector.list_tables(schema):
            try:
                cols = connector.describe_table(schema, table.name)
            except Exception as exc:
                # Defensive — one bad table shouldn't kill the walk.
                # Re-raise the type would lose the filename context, so we
                # capture and continue.
                skipped.append(
                    f"{schema}.{table.name}: describe failed ({type(exc).__name__}: {exc})"
                )
                continue
            proposals.append(
                _build_dataset_proposal(
                    table=table,
                    columns=cols,
                    target_path_template=target_path_template,
                )
            )

    return DiscoverProposal(
        source_name=source.source_name,
        source_env=source.source_env,
        datasets=tuple(proposals),
        schemas_seen=schemas_seen,
        skipped=tuple(skipped),
    )


def _build_dataset_proposal(
    *,
    table: TableMeta,
    columns: list[ColumnInfo],
    target_path_template: str,
) -> DatasetProposal:
    """Translate one table's catalog metadata into a ``DatasetProposal``."""
    dataset_name = f"{table.schema}_{table.name}"

    column_proposals = tuple(
        ColumnProposal(
            column_name=c.name,
            data_type=c.data_type,
            is_nullable=c.is_nullable,
            ordinal_position=c.ordinal_position,
            is_primary_key=c.is_primary_key,
            pii_class=classify_pii(c.name),
        )
        for c in columns
    )

    watermark_candidates = find_watermark_candidates(columns)
    if watermark_candidates:
        load_type = LoadType.INCREMENTAL
        watermark_column: str | None = watermark_candidates[0].column_name
        warnings: list[str] = []
        if len(watermark_candidates) > 1:
            others = ", ".join(c.column_name for c in watermark_candidates[1:])
            warnings.append(f"multiple watermark candidates; chose strongest. Others: {others}")
    else:
        load_type = LoadType.FULL
        watermark_column = None
        warnings = ["no watermark candidate found — defaulting to FULL load"]

    if not any(c.is_primary_key for c in columns):
        warnings.append("no primary key — review uniqueness for incremental dedup")

    return DatasetProposal(
        dataset_name=dataset_name,
        schema_name=table.schema,
        object_name=table.name,
        dataset_type=DatasetType.TABLE if table.table_type == "TABLE" else DatasetType.VIEW,
        load_type=load_type,
        watermark_column=watermark_column,
        target_path_template=target_path_template,
        columns=column_proposals,
        warnings=tuple(warnings),
        extra_config=dict(table.extra_config),
    )


# ──────────────────────────────────────────────────────────────────────
# Apply
# ──────────────────────────────────────────────────────────────────────


def apply_discover(
    proposal: DiscoverProposal,
    repo: MetadataRepository,
) -> DiscoverResult:
    """Write the proposal to the metadata DB.

    New datasets land with ``is_enabled=False`` (per CLAUDE.md). The
    operator flips them on later, after reviewing.
    """
    source = repo.get_source(proposal.source_name, proposal.source_env)
    if source is None:
        raise ValidationError(
            f"Source ({proposal.source_name!r}, {proposal.source_env!r}) "
            "vanished between plan and apply."
        )

    columns_total = 0

    for ds in proposal.datasets:
        config: dict[str, str] = {
            "realm": Realm.ADF_ADLS.value,
            "dataset_type": ds.dataset_type.value,
            "load_type": ds.load_type.value,
            "target_path_template": ds.target_path_template,
            "schema_name": ds.schema_name,
            "object_name": ds.object_name,
        }
        if ds.watermark_column is not None:
            config["watermark_column"] = ds.watermark_column
        # Connector-specific overrides (e.g. rest_endpoint_path for REST sources).
        # The repository's key registry validates each key at upsert time.
        config.update(ds.extra_config)

        column_rows = [
            DatasetColumnRow(
                dataset_id=0,  # set by repository on insert
                column_name=c.column_name,
                data_type=c.data_type,
                is_nullable=c.is_nullable,
                ordinal_position=c.ordinal_position,
                is_primary_key=c.is_primary_key,
                is_excluded=False,
                pii_class=c.pii_class.value,
                source_path=None,
            )
            for c in ds.columns
        ]

        repo.upsert_dataset(
            source_id=source.source_id,
            dataset_name=ds.dataset_name,
            is_enabled=False,
            config=config,
            columns=column_rows,
        )
        columns_total += len(column_rows)

    return DiscoverResult(
        datasets_upserted=len(proposal.datasets),
        columns_upserted=columns_total,
    )


# ──────────────────────────────────────────────────────────────────────
# YAML output (for --output flag)
# ──────────────────────────────────────────────────────────────────────


def proposal_to_yaml(proposal: DiscoverProposal) -> str:
    """Render a proposal as the customer-repo YAML shape.

    One YAML document per dataset, separated by ``---``. Round-trips
    into ``config/datasets/<source>/<dataset>.yaml`` files for the
    operator to commit + review before ``config sync``.
    """
    docs: list[dict[str, object]] = []
    for ds in proposal.datasets:
        config: dict[str, object] = {
            "realm": Realm.ADF_ADLS.value,
            "dataset_type": ds.dataset_type.value,
            "load_type": ds.load_type.value,
            "target_path_template": ds.target_path_template,
            "schema_name": ds.schema_name,
            "object_name": ds.object_name,
        }
        if ds.watermark_column is not None:
            config["watermark_column"] = ds.watermark_column

        doc: dict[str, object] = {
            "source_name": proposal.source_name,
            "source_env": proposal.source_env,
            "dataset_name": ds.dataset_name,
            "is_enabled": False,
            "config": config,
            "columns": [
                {
                    "column_name": c.column_name,
                    "data_type": c.data_type,
                    "is_nullable": c.is_nullable,
                    "ordinal_position": c.ordinal_position,
                    "is_primary_key": c.is_primary_key,
                    "pii_class": c.pii_class.value,
                }
                for c in ds.columns
            ],
        }
        # Warnings are surfaced in the CLI / proposal object — keeping them
        # out of the YAML lets DatasetConfigFile.model_validate accept the
        # output for round-trip.
        docs.append(doc)

    return yaml.safe_dump_all(
        docs,
        sort_keys=False,
        default_flow_style=False,
    )


def write_proposal_yamls(proposal: DiscoverProposal, target_dir: Path) -> list[Path]:
    """Write one YAML file per dataset under ``target_dir``."""
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for ds in proposal.datasets:
        config: dict[str, object] = {
            "realm": Realm.ADF_ADLS.value,
            "dataset_type": ds.dataset_type.value,
            "load_type": ds.load_type.value,
            "target_path_template": ds.target_path_template,
            "schema_name": ds.schema_name,
            "object_name": ds.object_name,
        }
        if ds.watermark_column is not None:
            config["watermark_column"] = ds.watermark_column

        doc: dict[str, object] = {
            "source_name": proposal.source_name,
            "source_env": proposal.source_env,
            "dataset_name": ds.dataset_name,
            "is_enabled": False,
            "config": config,
            "columns": [
                {
                    "column_name": c.column_name,
                    "data_type": c.data_type,
                    "is_nullable": c.is_nullable,
                    "ordinal_position": c.ordinal_position,
                    "is_primary_key": c.is_primary_key,
                    "pii_class": c.pii_class.value,
                }
                for c in ds.columns
            ],
        }
        path = target_dir / f"{ds.dataset_name}.yaml"
        path.write_text(
            yaml.safe_dump(doc, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
        written.append(path)

    return written


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _read_source_config(
    repo: MetadataRepository,
    source: SourceRow,
) -> dict[str, str]:
    """Reconstruct the source_config KV map from the repository.

    ``MetadataRepository`` doesn't expose this directly yet — the
    use case reads it via a new private method on the SQLite impl.
    To keep ``core/`` clean this helper goes through the protocol
    as a small extension.
    """
    return repo.get_source_config(source.source_id)


__all__ = [
    "ColumnProposal",
    "DatasetProposal",
    "DiscoverProposal",
    "DiscoverResult",
    "apply_discover",
    "plan_discover",
    "proposal_to_yaml",
    "write_proposal_yamls",
]
