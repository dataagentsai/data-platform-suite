"""In-memory fakes of the Azure SDK surface the adapters use: ContainerClient (azure.storage.blob) and SecretClient
(azure.keyvault.secrets). Test helpers only.

THESE ARE FAKES OF THE SDK SURFACE, WRITTEN FROM THE DOCUMENTED BEHAVIOUR. Nothing here has been checked against
Azurite or a real Azure account, and a passing test proves the adapter agrees with this fake, no more.

Strict where the real service is strict: blob names are case-sensitive, an upload without overwrite onto an existing
blob raises ResourceExistsError, a download of a missing blob raises ResourceNotFoundError, listing is in lexicographic
order, and metadata keys must be identifiers with string values. The two exception classes carry the names of the SDK's
(azure.core.exceptions) so the adapters can recognise them by name without importing the SDK.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


class ResourceExistsError(Exception):
    """Stands for azure.core.exceptions.ResourceExistsError (same name on purpose)."""


class ResourceNotFoundError(Exception):
    """Stands for azure.core.exceptions.ResourceNotFoundError (same name on purpose)."""


@dataclass(frozen=True)
class _Item:
    name: str


@dataclass(frozen=True)
class _Props:
    metadata: dict[str, str]
    size: int
    last_modified: datetime


class _Download:
    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


@dataclass
class FakeContainer:
    """The ContainerClient surface: list_blobs, upload_blob, download_blob, get_blob_client (-> get_blob_properties)."""

    _blobs: dict[str, tuple[bytes, dict[str, str], datetime]] = field(default_factory=dict)
    _clock: datetime = datetime(2026, 11, 1, tzinfo=UTC)

    def list_blobs(self, name_starts_with: str | None = None) -> list[_Item]:
        return [_Item(n) for n in sorted(self._blobs) if n.startswith(name_starts_with or "")]

    def upload_blob(self, name: str, data: bytes, overwrite: bool = False,
                    metadata: dict[str, str] | None = None) -> None:
        if not isinstance(data, bytes):
            raise TypeError("the fake takes bytes only")
        for key, value in (metadata or {}).items():
            if not key.isidentifier():
                raise ValueError(f"metadata key {key!r} is not a valid identifier")
            if not isinstance(value, str):
                raise ValueError(f"metadata value for {key!r} is not a string")
        if name in self._blobs and not overwrite:
            raise ResourceExistsError(f"The specified blob already exists: {name}")
        self._clock += timedelta(seconds=1)  # the service stamps its own time, always moving forward
        self._blobs[name] = (data, dict(metadata or {}), self._clock)

    def download_blob(self, name: str) -> _Download:
        if name not in self._blobs:
            raise ResourceNotFoundError(f"The specified blob does not exist: {name}")
        return _Download(self._blobs[name][0])

    def get_blob_client(self, name: str) -> "_BlobClient":
        return _BlobClient(self, name)

    def _props(self, name: str) -> _Props:
        if name not in self._blobs:
            raise ResourceNotFoundError(f"The specified blob does not exist: {name}")
        data, meta, stamped = self._blobs[name]
        return _Props(dict(meta), len(data), stamped)


@dataclass(frozen=True)
class _BlobClient:
    """BlobClient: the only place the real SDK offers get_blob_properties."""

    _container: FakeContainer
    _name: str

    def get_blob_properties(self) -> _Props:
        return self._container._props(self._name)


@dataclass(frozen=True)
class _Secret:
    value: str


@dataclass
class FakeKeyVault:
    """The SecretClient surface: get_secret(name) -> object with .value. Counts the calls, so a test can show that a
    cached value is read once. Names outside Key Vault's grammar (letters, digits, hyphen; 1 to 127) are refused."""

    secrets: dict[str, str] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)

    def get_secret(self, name: str, **_: Any) -> _Secret:
        self.calls.append(name)
        if not (1 <= len(name) <= 127 and all(c.isascii() and (c.isalnum() or c == "-") for c in name)):
            raise ValueError(f"{name!r} is not a valid Key Vault secret name")
        if name not in self.secrets:
            raise ResourceNotFoundError(f"A secret with (name/id) {name} was not found in this key vault")
        return _Secret(self.secrets[name])
