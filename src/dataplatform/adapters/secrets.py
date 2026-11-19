"""secret_store adapters (cloud_ports.SecretStore, DPC-0029). The value is returned to the caller only; nothing here
logs it, and an error names the reference, never the value.

    EnvSecrets          the laptop: DP_SECRET_<NAME> environment variables, else a file per secret in a folder
    AwsSecretsManager   AWS Secrets Manager through a boto3 client passed in (tests use moto); values cached per run
    AzureKeyVault       Azure Key Vault through a SecretClient passed in; values cached per run. Tested against a
                        fake of the client surface only (tests/dataplatform/azure_fakes.py), not against a real vault
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from dataplatform.cloud_ports import SecretNotFound, secret_name

KEY_VAULT_NAME_MAX = 127


class EnvSecrets:
    def __init__(self, folder: str | Path | None = None):
        self.folder = Path(folder) if folder else None

    def get(self, ref: str) -> str:
        name = secret_name(ref)
        env = "DP_SECRET_" + re.sub(r"[^A-Za-z0-9]", "_", name).upper()
        if env in os.environ:
            return os.environ[env]
        if self.folder is not None and (self.folder / name).is_file():
            return (self.folder / name).read_text().strip()
        raise SecretNotFound(f"{ref} is not set (environment {env} or a file in the secrets folder)")


class AwsSecretsManager:
    def __init__(self, client: Any):
        self.client = client
        self._cache: dict[str, str] = {}

    def get(self, ref: str) -> str:
        name = secret_name(ref)
        if name not in self._cache:
            try:
                value = self.client.get_secret_value(SecretId=name)
            except self.client.exceptions.ResourceNotFoundException:
                raise SecretNotFound(f"{ref} is not in Secrets Manager") from None
            self._cache[name] = str(value.get("SecretString") or "")
        return self._cache[name]


def _is_not_found(exc: Exception) -> bool:
    """Key Vault's "no such secret" error. The SDK is imported only here and only when an error has happened, and a
    missing SDK falls back to the class name, so this module never needs the `azure` extra at import."""
    try:
        from azure.core.exceptions import ResourceNotFoundError  # noqa: PLC0415 -- lazy: the `azure` extra is optional
    except ImportError:
        return type(exc).__name__ == "ResourceNotFoundError"
    return isinstance(exc, ResourceNotFoundError) or type(exc).__name__ == "ResourceNotFoundError"


def key_vault_name(name: str) -> str:
    """The Key Vault secret name for a port name. Key Vault allows letters, digits and hyphens only (1 to 127), so the
    "/" separator becomes "--". That is reversible only if no name already holds "--" (the port refuses it) and no
    hyphen sits next to a "/" (a-/b and a/-b would both become a---b), so both are refused."""
    if "--" in name or "-/" in name or "/-" in name:
        raise ValueError("a Key Vault secret name holds no double hyphen and no hyphen next to /")
    mapped = name.replace("/", "--")
    if not 1 <= len(mapped) <= KEY_VAULT_NAME_MAX:
        raise ValueError("a Key Vault secret name is 1 to 127 characters once / is written as --")
    return mapped


class AzureKeyVault:
    def __init__(self, client: Any):
        self.client = client
        self._cache: dict[str, str] = {}

    def get(self, ref: str) -> str:
        name = secret_name(ref)
        if name not in self._cache:
            try:
                secret = self.client.get_secret(key_vault_name(name))
            except Exception as exc:
                if _is_not_found(exc):
                    raise SecretNotFound(f"{ref} is not in Key Vault") from None
                raise
            self._cache[name] = str(secret.value or "")
        return self._cache[name]
