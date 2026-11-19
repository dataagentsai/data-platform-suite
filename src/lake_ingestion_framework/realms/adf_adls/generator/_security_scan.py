"""Generated-artifact secret-leakage scanner.

Defensive guard for the "Secrets via Key Vault only" invariant
(see ``docs/security.md``). Every LinkedService / Dataset / Pipeline
JSON the framework emits is scanned for literal credential patterns
before being written to disk. Hits surface as deploy-time warnings.

Scope:

- Catches obvious patterns: ``Password=...`` in a connection string,
  ``AccountKey=...`` for storage keys, raw ``Bearer <token>`` headers,
  SAS signatures (``sig=...``). These are the shapes that have caused
  real-world incidents at customers.
- Skips ADF expression placeholders (``@{linkedService().*}``,
  ``@{pipeline().parameters.*}``) — those resolve at runtime and don't
  embed plaintext.
- Skips KV reference dicts (the safe pattern: ``{"type":
  "AzureKeyVaultSecret", ...}``).

Not exhaustive — a determined attacker / careless contributor can
still slip a credential through (e.g. base64'd in a comment, split
across lines). Pre-commit's gitleaks hook is the second line of
defense; this scanner targets the artifact-output specifically.
"""

from __future__ import annotations

import re

# Cap on the snippet shown in a warning message — operator only needs
# enough context to find the offending line; full secret value is by
# definition sensitive and shouldn't be re-emitted into ops logs.
_SNIPPET_MAX_LEN = 80
_SNIPPET_TRUNC_AT = _SNIPPET_MAX_LEN - 3  # leave room for "..."


# (compiled pattern, human-readable description)
# Each pattern requires the value to be at least ``min_len`` chars
# AND NOT start with an ADF expression (``@{...}``).
_LEAKAGE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        # Password=<value> in a connection string. Allow @{...}
        # expressions and empty strings; flag everything else longer
        # than 4 chars.
        re.compile(r'(?i)password\s*=\s*(?!@\{|"")[^"\s;@]{4,}'),
        "embedded password (Password=...)",
    ),
    (
        # AccountKey= for Azure Storage account keys (base64-ish).
        re.compile(r"(?i)accountkey\s*=\s*(?!@\{)[A-Za-z0-9+/=]{16,}"),
        "embedded storage account key (AccountKey=...)",
    ),
    (
        # Bearer <token> in a literal Authorization header value.
        re.compile(r"(?i)bearer\s+(?!@\{)[A-Za-z0-9._\-]{16,}"),
        "embedded bearer token (Authorization: Bearer ...)",
    ),
    (
        # SAS signature — the giveaway for any embedded SAS URI.
        re.compile(r"(?i)[?&]sig=[A-Za-z0-9%]{20,}"),
        "embedded SAS signature (sig=...)",
    ),
    (
        # SharedAccessKey=...
        re.compile(r"(?i)sharedaccesskey\s*=\s*(?!@\{)[A-Za-z0-9+/=]{16,}"),
        "embedded shared access key (SharedAccessKey=...)",
    ),
)


def scan_artifact_for_literal_secrets(*, name: str, rendered: str) -> list[str]:
    """Return zero or more leakage warnings for one rendered artifact.

    Each warning includes the artifact name, the pattern label, and a
    short snippet so the operator can find the offending line. The
    snippet is capped to 80 chars to keep warnings dashboard-friendly
    and to avoid leaking the full secret into logs.
    """
    warnings: list[str] = []
    for pattern, label in _LEAKAGE_PATTERNS:
        for match in pattern.finditer(rendered):
            snippet = match.group(0)
            if len(snippet) > _SNIPPET_MAX_LEN:
                snippet = snippet[:_SNIPPET_TRUNC_AT] + "..."
            warnings.append(
                f"{name}: {label} — found '{snippet}'. Use a Key Vault reference instead."
            )
    return warnings


__all__ = ["scan_artifact_for_literal_secrets"]
