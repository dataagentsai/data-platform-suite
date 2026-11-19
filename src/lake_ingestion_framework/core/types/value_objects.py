"""Validated value objects for the framework.

Pydantic models that enforce a regex / shape at construction. Once you
have a ``KvRef`` instance, you can stop checking — the constraint is
enforced. See ``docs/code_design.md`` → "No primitive obsession".
"""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, model_validator

from lake_ingestion_framework.core.exceptions import (
    InvalidIdentifierError,
    InvalidKvRefError,
    ValidationError,
)

# ──────────────────────────────────────────────────────────────────────
# KvRef — kv://<vault>/<secret>
# ──────────────────────────────────────────────────────────────────────

_KV_REF_PATTERN = re.compile(r"^kv://(?P<vault>[a-z0-9-]+)/(?P<secret>[A-Za-z0-9-]+)$")


class KvRef(BaseModel):
    """A Key Vault secret reference. Validated at construction.

    Stored in metadata wherever the framework needs a credential
    reference. Resolved by the realm runner at run time; the secret
    value never appears in metadata, logs, or generated artifacts.
    """

    model_config = ConfigDict(frozen=True)

    vault: str
    secret_name: str

    @classmethod
    def parse(cls, raw: str) -> KvRef:
        m = _KV_REF_PATTERN.match(raw)
        if not m:
            raise InvalidKvRefError(raw)
        return cls(vault=m.group("vault"), secret_name=m.group("secret"))

    def __str__(self) -> str:
        return f"kv://{self.vault}/{self.secret_name}"

    @model_validator(mode="after")
    def _validate(self) -> KvRef:
        if not re.fullmatch(r"[a-z0-9-]+", self.vault):
            raise InvalidKvRefError(str(self))
        if not re.fullmatch(r"[A-Za-z0-9-]+", self.secret_name):
            raise InvalidKvRefError(str(self))
        return self


# ──────────────────────────────────────────────────────────────────────
# Identifier — SQL identifier, ^[A-Za-z_][A-Za-z0-9_]{0,127}$
# ──────────────────────────────────────────────────────────────────────

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class Identifier(BaseModel):
    """A SQL identifier (schema name, table name, column name, etc.).

    Validated against ``^[A-Za-z_][A-Za-z0-9_]{0,127}$``. Reject — don't
    sanitize — anything else. Bracket-quoted at SQL emit time.

    Always carries the ``role`` it plays so error messages are useful.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    role: str = "identifier"

    @classmethod
    def parse(cls, raw: str, *, role: str = "identifier") -> Identifier:
        if not _IDENTIFIER_PATTERN.match(raw):
            raise InvalidIdentifierError(raw, role=role)
        return cls(name=raw, role=role)

    def quoted(self, dialect: str = "tsql") -> str:
        """Return the identifier quoted for a specific SQL dialect.

        T-SQL / Synapse uses square brackets; Oracle / Postgres /
        Snowflake / SAP HANA use double quotes; MySQL uses backticks.
        """
        if dialect in {"tsql", "azure_sql_db", "sql_server", "synapse_sql_pool"}:
            return f"[{self.name}]"
        if dialect in {"mysql", "mariadb"}:
            return f"`{self.name}`"
        return f'"{self.name}"'

    def __str__(self) -> str:
        return self.name

    @model_validator(mode="after")
    def _validate(self) -> Identifier:
        if not _IDENTIFIER_PATTERN.match(self.name):
            raise InvalidIdentifierError(self.name, role=self.role)
        return self


# ──────────────────────────────────────────────────────────────────────
# SourceEnv — ^[a-z][a-z0-9_-]{0,19}$
# ──────────────────────────────────────────────────────────────────────

_SOURCE_ENV_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,19}$")


class SourceEnv(BaseModel):
    """A source environment label (`prod`, `dev`, `prod-eu`, `qa1`, ...).

    See ``docs/target_design.md`` → "Source env as a first-class
    dimension". Free-text within shape — clients have weird env names.
    """

    model_config = ConfigDict(frozen=True)

    value: str

    @classmethod
    def parse(cls, raw: str) -> SourceEnv:
        if not _SOURCE_ENV_PATTERN.match(raw):
            raise ValidationError(
                f"Invalid source_env: {raw!r}. Expected ^[a-z][a-z0-9_-]{{0,19}}$."
            )
        return cls(value=raw)

    def __str__(self) -> str:
        return self.value

    @model_validator(mode="after")
    def _validate(self) -> SourceEnv:
        if not _SOURCE_ENV_PATTERN.match(self.value):
            raise ValidationError(
                f"Invalid source_env: {self.value!r}. Expected ^[a-z][a-z0-9_-]{{0,19}}$."
            )
        return self


__all__ = ["Identifier", "KvRef", "SourceEnv"]
