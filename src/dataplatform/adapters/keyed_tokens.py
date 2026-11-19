"""The tokenizer as a keyed hash (HMAC-SHA256, standard library): the same value always gives the same token, so
joins and duplicate checks still work, and nobody without the key can reverse or recompute it. The key is the
binding's secret (DP_TOKEN_KEY, or a file the local binding creates once); it is never in code or in the store.
A vault that hands out random tokens is a later adapter behind the same port (DPC-0070 names random tokens).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from pathlib import Path


class KeyedTokenizer:
    PATTERN = r"^tok_[0-9a-f]{24}$"  # what a token looks like: silver checks restricted columns hold only these

    def __init__(self, key: bytes):
        if len(key) < 16:
            raise ValueError("a token key needs at least 16 bytes")
        self._key = key

    def token(self, column_class: str, value: str) -> str:
        digest = hmac.new(self._key, f"{column_class}\x1f{value}".encode(), hashlib.sha256).hexdigest()
        return f"tok_{digest[:24]}"

    @classmethod
    def from_env_or_file(cls, path: Path, env: str = "DP_TOKEN_KEY") -> KeyedTokenizer:
        """The key from the environment, else from a local file made once with a random key (local runs only)."""
        if os.environ.get(env):
            return cls(os.environ[env].encode())
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(secrets.token_hex(32))
            path.chmod(0o600)
        return cls(path.read_text().strip().encode())
