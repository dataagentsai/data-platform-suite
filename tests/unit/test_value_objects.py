"""Unit tests for KvRef, Identifier, SourceEnv."""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.exceptions import (
    InvalidIdentifierError,
    InvalidKvRefError,
    ValidationError,
)
from lake_ingestion_framework.core.types.value_objects import (
    Identifier,
    KvRef,
    SourceEnv,
)

# ──────────────────────────────────────────────────────────────────────
# KvRef
# ──────────────────────────────────────────────────────────────────────


class TestKvRef:
    def test_parse_canonical(self) -> None:
        ref = KvRef.parse("kv://prod-vault/sales-conn")
        assert ref.vault == "prod-vault"
        assert ref.secret_name == "sales-conn"
        assert str(ref) == "kv://prod-vault/sales-conn"

    @pytest.mark.parametrize(
        "raw",
        [
            "https://prod-vault/sales-conn",  # wrong scheme
            "kv://Prod-Vault/sales-conn",  # uppercase in vault
            "kv://prod_vault/sales-conn",  # underscore in vault
            "kv://prod-vault/",  # missing secret name
            "kv:///sales-conn",  # missing vault
            "kv://prod-vault/sales conn",  # space in secret name
            "",
        ],
    )
    def test_invalid_raw_rejected(self, raw: str) -> None:
        with pytest.raises(InvalidKvRefError):
            KvRef.parse(raw)

    def test_immutable(self) -> None:
        ref = KvRef.parse("kv://v/s")
        with pytest.raises(ValueError, match="frozen"):
            ref.vault = "other"  # type: ignore[misc]

    def test_constructor_validates_too(self) -> None:
        # Constructor (without parse) must also enforce shape.
        with pytest.raises(InvalidKvRefError):
            KvRef(vault="V", secret_name="ok")  # uppercase invalid


# ──────────────────────────────────────────────────────────────────────
# Identifier
# ──────────────────────────────────────────────────────────────────────


class TestIdentifier:
    def test_parse_simple(self) -> None:
        ident = Identifier.parse("orders", role="object_name")
        assert ident.name == "orders"
        assert ident.role == "object_name"

    def test_parse_with_underscore(self) -> None:
        ident = Identifier.parse("order_items", role="object_name")
        assert ident.name == "order_items"

    def test_parse_starting_underscore_ok(self) -> None:
        ident = Identifier.parse("_internal", role="column_name")
        assert ident.name == "_internal"

    @pytest.mark.parametrize(
        "raw",
        [
            "1orders",  # starts with digit
            "orders;DROP TABLE",  # SQL injection attempt
            "orders.dbo",  # contains dot
            "order items",  # contains space
            "x" * 129,  # exceeds 128 chars
            "",
            "schema-name",  # contains hyphen
        ],
    )
    def test_invalid_rejected(self, raw: str) -> None:
        with pytest.raises(InvalidIdentifierError):
            Identifier.parse(raw, role="schema_name")

    def test_quoted_tsql(self) -> None:
        assert Identifier.parse("orders", role="object_name").quoted("tsql") == "[orders]"

    def test_quoted_oracle(self) -> None:
        assert Identifier.parse("ORDERS", role="object_name").quoted("oracle") == '"ORDERS"'

    def test_quoted_mysql(self) -> None:
        assert Identifier.parse("orders", role="object_name").quoted("mysql") == "`orders`"

    def test_quoted_default(self) -> None:
        # Anything not in known dialects → double quotes (ANSI SQL standard).
        assert Identifier.parse("orders", role="object_name").quoted("postgres") == '"orders"'

    def test_role_in_error_message(self) -> None:
        with pytest.raises(InvalidIdentifierError, match="schema_name"):
            Identifier.parse("1bad", role="schema_name")


# ──────────────────────────────────────────────────────────────────────
# SourceEnv
# ──────────────────────────────────────────────────────────────────────


class TestSourceEnv:
    @pytest.mark.parametrize(
        "raw",
        ["prod", "dev", "uat", "sit", "qa1", "prod-eu", "non_prod"],
    )
    def test_valid_envs(self, raw: str) -> None:
        env = SourceEnv.parse(raw)
        assert env.value == raw
        assert str(env) == raw

    @pytest.mark.parametrize(
        "raw",
        [
            "PROD",  # uppercase
            "1prod",  # starts with digit
            "_prod",  # starts with underscore
            "prod env",  # space
            "prod.eu",  # dot
            "x" * 21,  # exceeds 20 chars
            "",
        ],
    )
    def test_invalid_envs_rejected(self, raw: str) -> None:
        with pytest.raises(ValidationError):
            SourceEnv.parse(raw)
