"""Lineage graph + queries built from JSONL OL events.

Phase 7-C scope (per ``docs/lineage_design.md`` §13): in-memory graph
materialized from one or more JSONL files emitted by Phase 7-A
(dbt manifest) and Phase 7-B (Silver pipeline). Supports forward,
backward, blast-radius, and path queries.

Future phases will add: time-travel queries (``--as-of <date>``),
SQL-backed graph from ``meta.lineage_*``, drift / PII overlays,
severity-weighted blast radius (impact analysis), and an MCP server
exposing the same queries to AI agents.

Why in-memory, not SQL: at Phase 7-C, lineage volume is small (one
event per pipeline run + one per dbt model). A typical customer's
JSONL is < 10MB. In-memory load is millisecond-fast and avoids the
SQL persistence dependency until we need it.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lake_ingestion_framework.core.exceptions import LakeIngestError


@dataclass(frozen=True)
class DatasetNode:
    """One vertex in the lineage graph — a (namespace, name) pair.

    Datasets are uniquely identified by ``(namespace, name)`` per the
    OpenLineage spec. We store both so consumers can disambiguate when
    two namespaces share a dataset name (e.g., ``delta://lakehouse/orders``
    vs ``postgres://prod/orders``).
    """

    namespace: str
    name: str

    @property
    def qualified(self) -> str:
        """``namespace/name`` form, used as the primary key in the graph."""
        return f"{self.namespace}/{self.name}"

    def __str__(self) -> str:  # human-friendly rendering for the CLI
        return self.qualified


@dataclass(frozen=True)
class JobReference:
    """Which job produced an edge — useful for explaining 'why is X downstream of Y'."""

    namespace: str
    name: str
    run_id: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.namespace}/{self.name}"


@dataclass(frozen=True)
class LineageEdge:
    """One edge: source dataset → destination dataset, produced by job."""

    src: str  # qualified dataset
    dst: str  # qualified dataset
    job: JobReference


@dataclass(frozen=True)
class LineageGraph:
    """Forward/backward adjacency lists + node lookup.

    Both edge maps are populated when events are loaded so all queries
    are O(degree) per traversal step. Use :meth:`from_jsonl` to load.
    """

    nodes: dict[str, DatasetNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)
    forward: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    """downstream: src → {dst, ...}"""
    backward: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    """upstream: dst → {src, ...}"""

    # ── Construction ─────────────────────────────────────────────────

    @classmethod
    def from_jsonl(cls, paths: list[Path]) -> LineageGraph:
        """Load events from one or more JSONL files; missing files raise.

        Multiple paths concatenate — useful for stitching dbt-manifest
        emission + Silver per-run emission into one graph.
        """
        graph = cls()
        for path in paths:
            if not path.is_file():
                raise LineageGraphError(f"Lineage file not found: {path}")
            with path.open(encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()  # noqa: PLW2901 — overwrite to skip blanks
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise LineageGraphError(
                            f"{path}:{line_no} is not valid JSON: {exc}"
                        ) from exc
                    graph._ingest_event(event)
        return graph

    def _ingest_event(self, event: dict[str, Any]) -> None:
        job_data = event.get("job") or {}
        run_data = event.get("run") or {}
        job = JobReference(
            namespace=str(job_data.get("namespace", "")),
            name=str(job_data.get("name", "")),
            run_id=str(run_data.get("runId", "")),
        )

        outputs = [self._upsert_dataset(d) for d in event.get("outputs") or []]
        inputs = [self._upsert_dataset(d) for d in event.get("inputs") or []]

        # Every (input, output) pair forms an edge through this job.
        for src in inputs:
            for dst in outputs:
                edge = LineageEdge(src=src.qualified, dst=dst.qualified, job=job)
                self.edges.append(edge)
                self.forward[src.qualified].add(dst.qualified)
                self.backward[dst.qualified].add(src.qualified)

    def _upsert_dataset(self, raw: dict[str, Any]) -> DatasetNode:
        namespace = str(raw.get("namespace", ""))
        name = str(raw.get("name", ""))
        node = DatasetNode(namespace=namespace, name=name)
        self.nodes.setdefault(node.qualified, node)
        return node

    # ── Public lookup ────────────────────────────────────────────────

    def find(self, term: str) -> list[DatasetNode]:
        """Find dataset nodes by name or qualified form.

        Matches in this priority:

        1. Exact qualified-form match (``namespace/name``)
        2. Exact name match (any namespace)
        3. Substring match on name (case-sensitive; for ergonomics)

        Returns 0 or more nodes — caller decides what to do with
        ambiguous matches.
        """
        if term in self.nodes:
            return [self.nodes[term]]

        exact_name = [n for n in self.nodes.values() if n.name == term]
        if exact_name:
            return exact_name

        return [n for n in self.nodes.values() if term in n.name]

    # ── Queries ──────────────────────────────────────────────────────

    def upstream(self, dataset: str, *, max_depth: int = -1) -> set[str]:
        """Datasets that flow *into* ``dataset`` (recursive).

        ``max_depth=-1`` = unlimited; ``max_depth=1`` = direct parents
        only. Walks the backward adjacency list breadth-first.
        """
        return self._walk(dataset, self.backward, max_depth)

    def downstream(self, dataset: str, *, max_depth: int = -1) -> set[str]:
        """Datasets that ``dataset`` flows into (recursive)."""
        return self._walk(dataset, self.forward, max_depth)

    def blast_radius(self, dataset: str) -> set[str]:
        """Everything potentially affected by a change to ``dataset``.

        Alias for :meth:`downstream` with unlimited depth. Phase 15
        (impact analysis) layers severity weights on top.
        """
        return self.downstream(dataset)

    def path(self, src: str, dst: str) -> list[str] | None:
        """Shortest forward path from ``src`` to ``dst``, or ``None``.

        Returns the chain of *qualified dataset names* including both
        endpoints. BFS — first found is shortest by edge count.
        """
        if src not in self.nodes or dst not in self.nodes:
            return None
        if src == dst:
            return [src]

        visited: set[str] = {src}
        # queue of (node, path-to-node) pairs
        queue: deque[tuple[str, list[str]]] = deque([(src, [src])])
        while queue:
            node, history = queue.popleft()
            for nxt in sorted(self.forward.get(node, ())):
                if nxt == dst:
                    return [*history, nxt]
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append((nxt, [*history, nxt]))
        return None

    def _walk(self, start: str, adjacency: dict[str, set[str]], max_depth: int) -> set[str]:
        if start not in self.nodes:
            return set()

        visited: set[str] = set()
        # queue of (node, depth) pairs
        queue: deque[tuple[str, int]] = deque([(start, 0)])
        while queue:
            node, depth = queue.popleft()
            if max_depth >= 0 and depth >= max_depth:
                continue
            for nxt in adjacency.get(node, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append((nxt, depth + 1))
        return visited


class LineageGraphError(LakeIngestError):
    """Couldn't build the lineage graph — bad input file."""
