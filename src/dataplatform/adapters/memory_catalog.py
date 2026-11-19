"""Catalog state in memory: the reference adapter for the CatalogState port (tests, dry runs, the first deploy)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from dataplatform.governance.catalog import SYNCED_LAYERS, Action, apply
from dataplatform.governance.port import Entry, ObjectRef


class MemoryCatalogState:
    def __init__(self, entries: Mapping[str, Mapping[ObjectRef, Entry]] | None = None) -> None:
        self._by_env: dict[str, dict[ObjectRef, Entry]] = {e: dict(v) for e, v in (entries or {}).items()}

    def entries(self, env: str) -> Mapping[ObjectRef, Entry]:
        return {r: e for r, e in self._by_env.get(env, {}).items() if r.layer in SYNCED_LAYERS}

    def apply(self, env: str, actions: Iterable[Action]) -> None:
        """Stands in for running the realm's statements."""
        self._by_env[env] = apply(self._by_env.get(env, {}), actions)
