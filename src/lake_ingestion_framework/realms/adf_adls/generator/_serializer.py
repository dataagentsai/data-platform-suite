"""Deterministic JSON serialisation for ADF resources.

ADF accepts our generated JSON via either git-integration mode or
ARM-template deploy. We always serialise with sorted keys + 4-space
indent + Unix newlines so re-running the generator on the same
metadata yields byte-identical bytes.

Per CLAUDE.md "Idempotent deploy" — required for golden-diff CI.
"""

from __future__ import annotations

import json


def serialize_artifact(content: dict[str, object]) -> str:
    """Serialise an ADF JSON content dict deterministically.

    - Sort keys recursively
    - 4-space indentation (matches ADF Studio's canonical export)
    - LF newlines, trailing newline at EOF
    - UTF-8 throughout
    """
    body = json.dumps(content, indent=4, sort_keys=True, ensure_ascii=False)
    if not body.endswith("\n"):
        body += "\n"
    return body


__all__ = ["serialize_artifact"]
