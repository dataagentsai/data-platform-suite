"""Secret-resolution layer for source connectors.

Source configs reference credentials as ``KvRef`` (``kv://<vault>/<secret>``).
The resolver turns those into actual secret strings at connection time.

Phase 0/1 (current): ``LocalEnvSecretResolver`` reads from environment
variables — no Azure dependency, no real Key Vault. Convention:

    LAKE_INGEST_SECRET_<vault>_<secret_name>=<value>

Hyphens in vault / secret names → underscores (env vars don't allow
hyphens). Case is normalized to upper.

Phase 2+ (Azure realm 1 production): adds an ``AzureKeyVaultResolver``
using ``DefaultAzureCredential``. That lives in the realm package, not
here, so ``core/`` stays SDK-free.

Per ``docs/security.md`` → "Secrets via Key Vault only" — the resolver
is the *only* place where a credential string is materialised. The
caller hands the string to the driver and discards it.
"""

from __future__ import annotations

import os
from typing import Protocol

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.core.types.value_objects import KvRef


class SecretResolutionError(LakeIngestError):
    """The secret reference couldn't be resolved."""


class SecretResolver(Protocol):
    """Resolves a ``KvRef`` to its secret value at runtime."""

    def resolve(self, ref: KvRef) -> str: ...


class LocalEnvSecretResolver:
    """Resolve secrets from environment variables. Local-dev only.

    Lookup key:

        LAKE_INGEST_SECRET_<UPPER_VAULT>_<UPPER_SECRET_NAME>

    where hyphens in vault / secret are replaced with underscores.

    Example: ``KvRef('local-vault', 'pg-password')`` →
    ``LAKE_INGEST_SECRET_LOCAL_VAULT_PG_PASSWORD``.
    """

    PREFIX = "LAKE_INGEST_SECRET_"

    def resolve(self, ref: KvRef) -> str:
        env_key = self._env_key_for(ref)
        value = os.environ.get(env_key)
        if value is None:
            raise SecretResolutionError(f"Secret {ref!s} not found. Expected env var {env_key!r}.")
        return value

    @classmethod
    def _env_key_for(cls, ref: KvRef) -> str:
        vault = ref.vault.replace("-", "_").upper()
        secret = ref.secret_name.replace("-", "_").upper()
        return f"{cls.PREFIX}{vault}_{secret}"


__all__ = [
    "LocalEnvSecretResolver",
    "SecretResolutionError",
    "SecretResolver",
]
