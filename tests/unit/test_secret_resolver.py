"""Unit tests for the local-env secret resolver."""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    LocalEnvSecretResolver,
    SecretResolutionError,
)
from lake_ingestion_framework.core.types.value_objects import KvRef


def test_resolves_from_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKE_INGEST_SECRET_PROD_VAULT_PG_PASS", "secret-value")
    resolver = LocalEnvSecretResolver()
    ref = KvRef.parse("kv://prod-vault/pg-pass")
    assert resolver.resolve(ref) == "secret-value"


def test_missing_env_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKE_INGEST_SECRET_GHOST_VAULT_X", raising=False)
    resolver = LocalEnvSecretResolver()
    with pytest.raises(SecretResolutionError, match="LAKE_INGEST_SECRET_GHOST_VAULT_X"):
        resolver.resolve(KvRef.parse("kv://ghost-vault/x"))


def test_hyphens_normalize_to_underscores() -> None:
    ref = KvRef.parse("kv://my-vault/my-secret-name")
    key = LocalEnvSecretResolver._env_key_for(ref)
    assert key == "LAKE_INGEST_SECRET_MY_VAULT_MY_SECRET_NAME"
