"""The catalog-state port (dpc/ports.yaml data_catalog): what the catalog holds now, so the sync plan can diff it.

Read only. Writing is the realm's rendered statements, applied by the deployer; the plan is also the drift report
(CAT-01): an empty plan means the catalog says what the metadata says.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, order=True)
class ObjectRef:
    """A table or view (column None) or one of its columns, without the environment: catalog = <env>_<layer>."""

    layer: str
    schema: str
    name: str
    column: str | None = None


@dataclass(frozen=True)
class Entry:
    kind: str  # table | view | metric_view
    comment: str | None
    tags: tuple[tuple[str, str], ...]  # sorted by key

    def tag_map(self) -> dict[str, str]:
        return dict(self.tags)


class CatalogState(Protocol):
    def entries(self, env: str) -> Mapping[ObjectRef, Entry]:
        """Every table, view and column in the environment's synced catalogs (<env>_silver, <env>_gold), with its
        comment and tags."""
        ...
