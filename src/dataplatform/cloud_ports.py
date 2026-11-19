"""Ports only a cloud fills (data-platform-catalog dpc/ports.yaml, binds_in: cloud). The kernel imports only these
shapes; product names stay in the adapters (R1). The source-side ports (file drop, event stream, database, record
interface) are in source_ports.py.
"""
from __future__ import annotations

from typing import Protocol

LAYERS = ("landing", "bronze", "silver", "gold", "quarantine", "ops")


class ObjectStore(Protocol):
    """object_store: the bytes under landing and every table, one location per environment and layer (ADR-012)."""

    def location(self, env: str, layer: str) -> str:
        """The storage location for an environment and a layer, e.g. a bucket URL."""

    def versioned(self, env: str, layer: str) -> bool:
        """True when the location keeps old versions (invariant ORCH-05: storage meets the recovery targets)."""


SECRET_SCHEME = "secret://"  # noqa: S105 -- the reference scheme, not a password


class SecretNotFound(LookupError):
    """A secret reference the store does not hold. The message names the reference, never a value."""


class SecretStore(Protocol):
    """secret_store (DPC-0029): credentials come only from here, by reference (secret://<name>), resolved at run time
    and never written to metadata, logs or generated files."""

    def get(self, ref: str) -> str: ...


def secret_name(ref: str) -> str:
    """The name inside a secret://<name> reference; anything else is refused (a value is never a reference)."""
    if not ref.startswith(SECRET_SCHEME) or len(ref) == len(SECRET_SCHEME):
        raise ValueError("not a secret reference; expected secret://<name>")
    name = ref[len(SECRET_SCHEME):]
    # The grammar is the common subset of the clouds: Key Vault holds only letters, digits and hyphens, so the port
    # allows those and "/" (which that adapter writes as "--"). Secrets Manager's wider set (_+=.@) is deliberately not
    # portable and is refused here. The port also refuses "--" and a hyphen beside "/" (a-/b and a/-b would both become
    # a---b), so the mapping stays reversible and a reference means the same on every cloud (ADR-036).
    ok = all((c.isascii() and c.isalnum()) or c in "/-" for c in name)
    if not ok or "--" in name or "-/" in name or "/-" in name:
        raise ValueError("a secret name holds letters, digits, hyphen and / only; no double hyphen, none beside /")
    return name
