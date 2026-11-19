"""Exception hierarchy.

Rooted at ``LakeIngestError`` so callers can catch all framework errors
without also catching ``KeyboardInterrupt`` / ``SystemExit``. Sub-types
name the failure mode; never raise the base class directly.

See ``docs/code_design.md`` → "Error handling philosophy".
"""

from __future__ import annotations


class LakeIngestError(Exception):
    """Root of the framework's exception hierarchy.

    Catch this at boundaries (CLI, use cases, ADF activity wrappers).
    Sub-types name the failure mode.
    """


# ──────────────────────────────────────────────────────────────────────
# Validation — config / metadata invalid
# ──────────────────────────────────────────────────────────────────────


class ValidationError(LakeIngestError):
    """Metadata or config violates a framework invariant.

    Raised at deploy / register / discover time, not at runtime — a
    clean separation between "this can never run" (validation) and
    "this run failed" (runtime errors).
    """


class InvalidKvRefError(ValidationError):
    """A Key Vault reference doesn't match ``kv://<vault>/<secret>``."""

    def __init__(self, raw: str) -> None:
        super().__init__(
            f"Not a valid Key Vault reference: {raw!r}. Expected format: kv://<vault>/<secret>."
        )
        self.raw = raw


class InvalidIdentifierError(ValidationError):
    """A SQL identifier doesn't match ``^[A-Za-z_][A-Za-z0-9_]{0,127}$``."""

    def __init__(self, identifier: str, role: str) -> None:
        super().__init__(
            f"Invalid {role} identifier: {identifier!r}. Expected ^[A-Za-z_][A-Za-z0-9_]{{0,127}}$."
        )
        self.identifier = identifier
        self.role = role


class UnknownKeyError(ValidationError):
    """A KV-table key isn't in the registered key list."""

    def __init__(self, table: str, key: str) -> None:
        super().__init__(
            f"Unknown key {key!r} for table meta.{table}. "
            f"Register it in core/metadata/keys.py + docs/metadata_keys.md."
        )
        self.table = table
        self.key = key


class SegmentationConfigError(ValidationError):
    """Segmentation config is internally inconsistent."""

    def __init__(self, dataset_name: str, reason: str) -> None:
        super().__init__(f"Segmentation config invalid for {dataset_name!r}: {reason}")
        self.dataset_name = dataset_name
        self.reason = reason


class CrossKeyValidationError(ValidationError):
    """A cross-key invariant is violated.

    Examples: ``load_type='INCREMENTAL'`` without a ``watermark_column``;
    ``flattening_engine='function'`` without ``flatten_function_url``
    set in ``platform_config``.
    """


# ──────────────────────────────────────────────────────────────────────
# Source — source-side problem at runtime
# ──────────────────────────────────────────────────────────────────────


class SourceError(LakeIngestError):
    """Generic source-side runtime failure."""


class SourceUnavailableError(SourceError):
    """Source is unreachable (network, DNS, server down)."""


class AuthenticationError(SourceError):
    """Source rejected the credentials."""


class QueryFailedError(SourceError):
    """Source executed the query and returned an error."""

    def __init__(self, query_hash: str, message: str) -> None:
        super().__init__(f"Source query failed (hash={query_hash}): {message}")
        self.query_hash = query_hash


class SchemaDriftError(SourceError):
    """Source schema has changed since last discovery."""


# ──────────────────────────────────────────────────────────────────────
# Sink — sink-side problem at runtime
# ──────────────────────────────────────────────────────────────────────


class SinkError(LakeIngestError):
    """Generic sink-side runtime failure."""


class SinkUnavailableError(SinkError):
    """Sink (ADLS, Blob, etc.) is unreachable."""


class WriteFailedError(SinkError):
    """Sink rejected or interrupted a write."""


# ──────────────────────────────────────────────────────────────────────
# Metadata DB — runtime failures talking to the metadata store
# ──────────────────────────────────────────────────────────────────────


class MetadataError(LakeIngestError):
    """Generic metadata-DB runtime failure."""


class MetadataConnectionError(MetadataError):
    """Can't reach the metadata DB."""


class WatermarkAdvanceError(MetadataError):
    """Failed to advance a watermark after a successful sink write.

    Surfaced specially because it's the most operationally sensitive
    failure mode — the data is on the lake but the metadata says it
    isn't. Self-healing path: next run re-extracts the same window
    (idempotent at the partition level).
    """


# ──────────────────────────────────────────────────────────────────────
# Runtime state — invariants we believed but turned out false
# ──────────────────────────────────────────────────────────────────────


class RuntimeStateError(LakeIngestError):
    """A runtime invariant was violated. Almost always a bug.

    Examples: framework reads ``run_log.status='STARTED'`` but
    ``run_id`` doesn't exist; ``watermark_after`` is older than
    ``watermark_before``.

    Catch separately at the boundary so we can route bugs to error
    tracking distinct from operational failures.
    """
