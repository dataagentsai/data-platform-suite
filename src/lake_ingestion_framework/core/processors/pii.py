"""pii group — privacy enforcement.

Five strategies, per-env policy resolution. Customer declares
``pii_class`` per column in ``meta.dataset_config_columns``; the
``meta.pii_policy`` table maps ``(pii_class, env) → action`` plus
options (salt for hash/tokenize, mask_visible chars, redact_value).

| Strategy        | What it does |
|-----------------|--------------|
| pass_through    | No transform; lineage-tag only (dev default) |
| hash            | SHA-256 with per-customer salt → fixed-length hex |
| mask            | Keep last N chars; replace prefix with `*` |
| tokenize        | Deterministic synthetic substitute (k-anon-friendly) |
| redact          | Literal placeholder (default `[redacted]`) |

Application order is **before** ``_record_hash`` recompute — the
SCD2 hash is over post-PII values, so two customers with the same
clear-text email map to the same masked/hashed value and dedup
correctly.

Salt resolution (Wave 2 v1): if ``salt_kv_ref`` looks like a Key
Vault reference (``kv://<vault>/<secret>``) we'll resolve via
:mod:`core.security.secrets` in a follow-up. For now we treat any
non-prefix value as a literal salt. Customers can wire real KV
when production deploy lands.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class PiiPolicy:
    """Resolved PII policy for one (pii_class, env) pair.

    Built from a ``meta.pii_policy`` row + salt resolution.
    """

    pii_class: str
    env: str
    action: str  # 'pass_through' | 'hash' | 'mask' | 'tokenize' | 'redact'
    salt: str | None = None
    redact_value: str = "[redacted]"
    mask_visible: int = 4


# ──────────────────────────────────────────────────────────────────────


def _hash_value(value: str, salt: str) -> str:
    """SHA-256 with salt. Stable across runs for the same input —
    SCD2 hash works."""
    h = hashlib.sha256()
    h.update(salt.encode("utf-8"))
    h.update(value.encode("utf-8"))
    return h.hexdigest()


def _mask_value(value: str, visible: int) -> str:
    """Keep last ``visible`` chars; replace prefix with ``*``.

    Empty strings pass through. Strings shorter than ``visible``
    return all-stars (don't reveal the whole value).
    """
    if not value:
        return value
    if len(value) <= visible:
        return "*" * len(value)
    return "*" * (len(value) - visible) + value[-visible:]


def _tokenize_value(value: str, salt: str) -> str:
    """Deterministic synthetic token. Same salt + same value →
    same token across runs (k-anonymity preserved). Different from
    hash in shape — uses base-36 of the SHA-256 prefix to look more
    like an opaque ID and less like a hash."""
    digest = _hash_value(value, salt)
    # First 16 hex chars (64 bits) → base-36 string. Plenty of entropy
    # for k-anonymity; doesn't look like a hash to consumers.
    n = int(digest[:16], 16)
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    out: list[str] = []
    while n > 0:
        n, rem = divmod(n, 36)
        out.append(chars[rem])
    return "".join(reversed(out)) or "0"


# ──────────────────────────────────────────────────────────────────────


class PiiProcessor:
    """Routes a column with non-trivial pii_class through the right
    strategy based on the resolved policy.

    Single processor per pii action — the engine instantiates one
    instance per strategy name and the policy resolution decides
    which fires for which column. Tests cover each strategy.
    """

    group = ProcessorGroup.PII

    def __init__(self, action: str, env: str, policies: dict[str, PiiPolicy]) -> None:
        self.name = f"pii_{action}"
        self._action = action
        self._env = env
        self._policies = policies  # pii_class → PiiPolicy

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        if not ctx.pii_class or ctx.pii_class == "none":
            return False
        policy = self._policies.get(ctx.pii_class)
        if policy is None:
            return False
        return policy.action == self._action

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)

        policy = self._policies[ctx.pii_class]
        src = table.column(col)
        # Force string view of the column — most PII strategies stringify.
        if not pa.types.is_string(src.type) and not pa.types.is_large_string(src.type):
            src = src.cast(pa.string())
        values = src.to_pylist()

        if self._action == "pass_through":
            # Strategy is intentionally a no-op; mark fired=False so
            # describe-CLI shows "configured but doing nothing".
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)

        if self._action == "redact":
            replacement = policy.redact_value or "[redacted]"
            out = [replacement if v is not None else None for v in values]
        elif self._action == "mask":
            out = [_mask_value(v, policy.mask_visible) if v is not None else None for v in values]
        elif self._action == "hash":
            salt = policy.salt or ""
            out = [_hash_value(v, salt) if v is not None else None for v in values]
        elif self._action == "tokenize":
            salt = policy.salt or ""
            out = [_tokenize_value(v, salt) if v is not None else None for v in values]
        else:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)

        new_array = pa.array(out, type=pa.string())
        idx = table.schema.get_field_index(col)
        return table.set_column(idx, col, new_array), ProcessorRunResult(
            self.group, self.name, col, fired=True, rows_affected=len(out)
        )


# Strategies — instantiated once per ProcessorEngine via build_pii_group()
PII_STRATEGIES = ("pass_through", "hash", "mask", "tokenize", "redact")


def build_pii_group(policies: dict[str, PiiPolicy], env: str) -> tuple[PiiProcessor, ...]:
    """Instantiate one ``PiiProcessor`` per strategy, sharing the
    resolved policies. Returned tuple is the default-on processor
    list for the pii group; the engine routes columns through them
    via ``applies_to``.
    """
    return tuple(PiiProcessor(action=a, env=env, policies=policies) for a in PII_STRATEGIES)


def resolve_salt(salt_kv_ref: str | None) -> str | None:
    """Resolve a salt reference to a literal salt string.

    Wave 2 v1: treats values not starting with ``kv://`` as literal
    salts. Real Key Vault resolution lands in a follow-up commit
    via :mod:`core.security.secrets` so the framework can be
    deployed to dev without Azure access.
    """
    if salt_kv_ref is None:
        return None
    if salt_kv_ref.startswith("kv://"):
        # TODO(Wave 2.5): wire to core.security.secrets when KV adapter lands.
        # For now, fall back to literal so tests don't need Azure.
        return salt_kv_ref
    return salt_kv_ref


__all__ = [
    "PII_STRATEGIES",
    "PiiPolicy",
    "PiiProcessor",
    "build_pii_group",
    "resolve_salt",
]
