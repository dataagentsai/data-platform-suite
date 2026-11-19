"""Batch keys (DPC-0024) and config fingerprints (CFG-02)."""
from __future__ import annotations

import hashlib
import json
from typing import Any


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def batch_key(window: str, content_checksum: str) -> str:
    """Same window and same bytes give the same key, whatever the file is called. That is what makes a resend a no-op."""
    return f"{window}:{content_checksum[:16]}"


def config_fingerprint(config: dict[str, Any]) -> str:
    """A short, stable hash of the merged config a run used. Key order does not matter."""
    return hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()[:12]
