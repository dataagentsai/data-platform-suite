"""Unit tests for the discover use case (with mocked connector)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from lake_ingestion_framework.core.config_yaml import load_dataset_yaml
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import (
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    LocalEnvSecretResolver,
)
from lake_ingestion_framework.core.source_connectors.protocol import (
    ColumnInfo,
    SourceConnector,
    TableMeta,
)
from lake_ingestion_framework.core.types.enums import (
    DatasetType,
    LoadType,
    PiiClass,
    SourceType,
)
from lake_ingestion_framework.core.use_cases import discover as discover_uc

# ──────────────────────────────────────────────────────────────────────
# Stub connector — replaces real Postgres for these unit tests
# ──────────────────────────────────────────────────────────────────────


class StubConnector:
    """Hard-coded fake source for testing the use case."""

    def __init__(self) -> None:
        self.connected = False
        self.closed = False

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.closed = True

    def list_schemas(self) -> list[str]:
        return ["sales"]

    def list_tables(self, schema: str) -> list[TableMeta]:
        if schema == "sales":
            return [
                TableMeta(schema="sales", name="customers", table_type="TABLE"),
                TableMeta(schema="sales", name="orders", table_type="TABLE"),
                TableMeta(schema="sales", name="audit_log", table_type="VIEW"),
            ]
        return []

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        if (schema, table) == ("sales", "customers"):
            return [
                ColumnInfo("id", "integer", False, 1, is_primary_key=True),
                ColumnInfo("email", "text", False, 2),
                ColumnInfo("first_name", "text", True, 3),
                ColumnInfo("last_name", "text", True, 4),
                ColumnInfo("modified_at", "timestamp without time zone", False, 5),
            ]
        if (schema, table) == ("sales", "orders"):
            return [
                ColumnInfo("id", "integer", False, 1, is_primary_key=True),
                ColumnInfo("customer_id", "integer", False, 2),
                ColumnInfo("amount", "numeric", False, 3),
                # No watermark column here — should default to FULL
            ]
        if (schema, table) == ("sales", "audit_log"):
            return [
                ColumnInfo("event_id", "integer", False, 1),
                ColumnInfo("created_at", "timestamp", False, 2),
            ]
        return []


@pytest.fixture()
def repo_with_source(monkeypatch: pytest.MonkeyPatch) -> Iterator[SqliteMetadataRepository]:
    """Repo bootstrapped with one Postgres source registered."""
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    r.upsert_source(
        source_name="orders_db",
        source_env="dev",
        source_type=SourceType.POSTGRES,
        is_enabled=True,
        config={
            "connection_pattern": "postgres_basic",
            "auth_method": "basic",
            "connection_secret_name": "kv://local-vault/pg-creds",
            "server": "localhost",
            "database": "orders",
        },
    )
    # Set the env var the resolver looks for
    monkeypatch.setenv("LAKE_INGEST_SECRET_LOCAL_VAULT_PG_CREDS", "user:secret")
    try:
        yield r
    finally:
        r.close()


def _patch_make_connector(monkeypatch: pytest.MonkeyPatch, connector: SourceConnector) -> None:
    """Replace the factory's make_connector so plan_discover uses the stub."""
    monkeypatch.setattr(
        "lake_ingestion_framework.core.use_cases.discover.make_connector",
        lambda *_a, **_kw: connector,
    )


# ──────────────────────────────────────────────────────────────────────
# plan_discover
# ──────────────────────────────────────────────────────────────────────


class TestPlanDiscover:
    def test_walks_source_and_proposes_datasets(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        connector = StubConnector()
        _patch_make_connector(monkeypatch, connector)

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        assert connector.connected
        assert connector.closed
        assert proposal.source_name == "orders_db"
        assert proposal.schemas_seen == ("sales",)
        assert len(proposal.datasets) == 3

    def test_customers_proposal_has_watermark_and_pii(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_make_connector(monkeypatch, StubConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        customers = next(d for d in proposal.datasets if d.dataset_name == "sales_customers")
        assert customers.dataset_type is DatasetType.TABLE
        assert customers.load_type is LoadType.INCREMENTAL
        assert customers.watermark_column == "modified_at"
        # PII flags
        email_col = next(c for c in customers.columns if c.column_name == "email")
        assert email_col.pii_class is PiiClass.DIRECT
        first_name = next(c for c in customers.columns if c.column_name == "first_name")
        assert first_name.pii_class is PiiClass.DIRECT
        # ID is not PII
        id_col = next(c for c in customers.columns if c.column_name == "id")
        assert id_col.pii_class is PiiClass.NONE
        assert id_col.is_primary_key is True

    def test_table_without_watermark_defaults_to_full(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_make_connector(monkeypatch, StubConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        orders = next(d for d in proposal.datasets if d.dataset_name == "sales_orders")
        assert orders.load_type is LoadType.FULL
        assert orders.watermark_column is None
        assert any("no watermark candidate" in w for w in orders.warnings)

    def test_view_proposed_as_view_dataset_type(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_make_connector(monkeypatch, StubConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        audit = next(d for d in proposal.datasets if d.dataset_name == "sales_audit_log")
        assert audit.dataset_type is DatasetType.VIEW

    def test_unknown_source_raises(
        self,
        repo_with_source: SqliteMetadataRepository,
    ) -> None:
        with pytest.raises(ValidationError, match="not found"):
            discover_uc.plan_discover(
                source_name="ghost",
                source_env="dev",
                repo=repo_with_source,
                secret_resolver=LocalEnvSecretResolver(),
            )

    def test_describe_failure_records_skip_not_crash(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class FlakyConnector(StubConnector):
            def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
                if table == "orders":
                    raise RuntimeError("simulated catalog blip")
                return super().describe_table(schema, table)

        _patch_make_connector(monkeypatch, FlakyConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )
        # 2 datasets (customers, audit_log) — orders skipped
        assert len(proposal.datasets) == 2
        assert any("orders" in s for s in proposal.skipped)


# ──────────────────────────────────────────────────────────────────────
# apply_discover
# ──────────────────────────────────────────────────────────────────────


class TestApplyDiscover:
    def test_writes_datasets_disabled(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_make_connector(monkeypatch, StubConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        result = discover_uc.apply_discover(proposal, repo_with_source)
        assert result.datasets_upserted == 3
        assert result.columns_upserted == 5 + 3 + 2

        # Datasets should land disabled
        source = repo_with_source.get_source("orders_db", "dev")
        assert source is not None
        datasets = repo_with_source.list_datasets(source_id=source.source_id)
        for ds in datasets:
            assert ds.is_enabled is False


# ──────────────────────────────────────────────────────────────────────
# YAML output
# ──────────────────────────────────────────────────────────────────────


class TestProposalYaml:
    def test_writes_one_yaml_per_dataset(
        self,
        repo_with_source: SqliteMetadataRepository,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _patch_make_connector(monkeypatch, StubConnector())

        proposal = discover_uc.plan_discover(
            source_name="orders_db",
            source_env="dev",
            repo=repo_with_source,
            secret_resolver=LocalEnvSecretResolver(),
        )

        target = tmp_path / "datasets"
        written = discover_uc.write_proposal_yamls(proposal, target)
        assert len(written) == 3
        assert all(p.exists() for p in written)
        # Round-trip through the config_yaml schema
        for path in written:
            ds = load_dataset_yaml(path)
            assert ds.source_name == "orders_db"
            assert ds.source_env == "dev"
            assert ds.is_enabled is False
