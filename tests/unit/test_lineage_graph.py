"""Unit tests for the lineage graph + query primitives (Phase 7-C).

Builds graphs from hand-rolled synthetic JSONL fixtures so each test
runs in milliseconds without needing the real OL library or dbt.
The fixtures match the on-disk shape produced by Phase 7-A/B emitters.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from lake_ingestion_framework.core.lineage import (
    DatasetNode,
    LineageGraph,
    LineageGraphError,
)

# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _event(
    *,
    job: str,
    inputs: list[tuple[str, str]],
    outputs: list[tuple[str, str]],
    run_id: str = "run-1",
    job_namespace: str = "lake-ingest.silver",
) -> dict[str, Any]:
    """Minimal OL event for tests — matches the fields the graph cares about."""
    return {
        "eventType": "COMPLETE",
        "job": {"namespace": job_namespace, "name": job},
        "run": {"runId": run_id},
        "inputs": [{"namespace": ns, "name": name} for ns, name in inputs],
        "outputs": [{"namespace": ns, "name": name} for ns, name in outputs],
    }


def _write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(e) for e in events) + "\n",
        encoding="utf-8",
    )


def _three_layer_jsonl(path: Path) -> None:
    """Build a Bronze → Silver → Gold trail covering all branching shapes:

    bronze/orders ─→ silver/orders ─→ gold/fct_orders ─→ snapshots/snap_orders
                                     ↗
    silver/customers ──────────────╯
      ↑
    bronze/customers
    """
    _write_jsonl(
        path,
        [
            _event(
                job="silver/orders",
                inputs=[("delta://lake", "bronze/orders")],
                outputs=[("delta://lake", "silver/orders")],
            ),
            _event(
                job="silver/customers",
                inputs=[("delta://lake", "bronze/customers")],
                outputs=[("delta://lake", "silver/customers")],
            ),
            _event(
                job="model.acme.fct_orders",
                inputs=[
                    ("delta://lake", "silver/orders"),
                    ("delta://lake", "silver/customers"),
                ],
                outputs=[("delta://lake", "gold/fct_orders")],
                job_namespace="dbt",
            ),
            _event(
                job="snapshot.acme.snap_orders",
                inputs=[("delta://lake", "gold/fct_orders")],
                outputs=[("delta://lake", "snapshots/snap_orders")],
                job_namespace="dbt",
            ),
        ],
    )


# ──────────────────────────────────────────────────────────────────────
# Construction
# ──────────────────────────────────────────────────────────────────────


def test_graph_builds_from_one_jsonl(tmp_path: Path) -> None:
    f = tmp_path / "lineage.jsonl"
    _three_layer_jsonl(f)
    g = LineageGraph.from_jsonl([f])
    # 6 datasets (2 bronze + 2 silver + 1 gold + 1 snapshot)
    assert len(g.nodes) == 6
    # 5 edges: 2x bronze->silver, 2x silver->gold, 1x gold->snapshot
    assert len(g.edges) == 5


def test_graph_concatenates_multiple_files(tmp_path: Path) -> None:
    """Realistic case: one file per emitter (Silver per-run + Gold from manifest)."""
    silver_file = tmp_path / "silver.jsonl"
    gold_file = tmp_path / "gold.jsonl"
    _write_jsonl(
        silver_file,
        [
            _event(
                job="silver/orders",
                inputs=[("delta://lake", "bronze/orders")],
                outputs=[("delta://lake", "silver/orders")],
            ),
        ],
    )
    _write_jsonl(
        gold_file,
        [
            _event(
                job="model.acme.fct_orders",
                inputs=[("delta://lake", "silver/orders")],
                outputs=[("delta://lake", "gold/fct_orders")],
                job_namespace="dbt",
            ),
        ],
    )
    g = LineageGraph.from_jsonl([silver_file, gold_file])
    assert len(g.nodes) == 3  # bronze/orders, silver/orders, gold/fct_orders
    assert len(g.edges) == 2


def test_graph_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(LineageGraphError, match="not found"):
        LineageGraph.from_jsonl([tmp_path / "nope.jsonl"])


def test_graph_invalid_json_line_raises(tmp_path: Path) -> None:
    f = tmp_path / "bad.jsonl"
    f.write_text("not json\n", encoding="utf-8")
    with pytest.raises(LineageGraphError, match="not valid JSON"):
        LineageGraph.from_jsonl([f])


def test_graph_skips_blank_lines(tmp_path: Path) -> None:
    f = tmp_path / "lineage.jsonl"
    f.write_text(
        "\n"
        + json.dumps(
            _event(
                job="silver/orders",
                inputs=[("ns", "in")],
                outputs=[("ns", "out")],
            )
        )
        + "\n\n",
        encoding="utf-8",
    )
    g = LineageGraph.from_jsonl([f])
    assert len(g.edges) == 1


# ──────────────────────────────────────────────────────────────────────
# Queries
# ──────────────────────────────────────────────────────────────────────


def _g(tmp_path: Path) -> LineageGraph:
    f = tmp_path / "lineage.jsonl"
    _three_layer_jsonl(f)
    return LineageGraph.from_jsonl([f])


def test_upstream_unlimited_walks_to_roots(tmp_path: Path) -> None:
    g = _g(tmp_path)
    upstream = g.upstream("delta://lake/snapshots/snap_orders")
    assert upstream == {
        "delta://lake/gold/fct_orders",
        "delta://lake/silver/orders",
        "delta://lake/silver/customers",
        "delta://lake/bronze/orders",
        "delta://lake/bronze/customers",
    }


def test_upstream_max_depth_one_returns_direct_parents(tmp_path: Path) -> None:
    g = _g(tmp_path)
    direct = g.upstream("delta://lake/gold/fct_orders", max_depth=1)
    assert direct == {"delta://lake/silver/orders", "delta://lake/silver/customers"}


def test_downstream_unlimited(tmp_path: Path) -> None:
    g = _g(tmp_path)
    downstream = g.downstream("delta://lake/silver/customers")
    assert downstream == {
        "delta://lake/gold/fct_orders",
        "delta://lake/snapshots/snap_orders",
    }


def test_blast_radius_alias_for_downstream(tmp_path: Path) -> None:
    g = _g(tmp_path)
    assert g.blast_radius("delta://lake/silver/customers") == g.downstream(
        "delta://lake/silver/customers"
    )


def test_unknown_dataset_returns_empty(tmp_path: Path) -> None:
    g = _g(tmp_path)
    assert g.upstream("delta://lake/nope") == set()
    assert g.downstream("delta://lake/nope") == set()


def test_path_returns_shortest_chain(tmp_path: Path) -> None:
    g = _g(tmp_path)
    path = g.path("delta://lake/bronze/orders", "delta://lake/snapshots/snap_orders")
    assert path == [
        "delta://lake/bronze/orders",
        "delta://lake/silver/orders",
        "delta://lake/gold/fct_orders",
        "delta://lake/snapshots/snap_orders",
    ]


def test_path_unreachable_returns_none(tmp_path: Path) -> None:
    g = _g(tmp_path)
    # snapshots flows nowhere — path *to* a root is None
    assert g.path("delta://lake/snapshots/snap_orders", "delta://lake/bronze/orders") is None


def test_path_to_self_returns_singleton(tmp_path: Path) -> None:
    g = _g(tmp_path)
    assert g.path("delta://lake/silver/orders", "delta://lake/silver/orders") == [
        "delta://lake/silver/orders"
    ]


def test_path_unknown_endpoint_returns_none(tmp_path: Path) -> None:
    g = _g(tmp_path)
    assert g.path("delta://lake/nope", "delta://lake/silver/orders") is None
    assert g.path("delta://lake/silver/orders", "delta://lake/nope") is None


# ──────────────────────────────────────────────────────────────────────
# find() — name resolution
# ──────────────────────────────────────────────────────────────────────


def test_find_exact_qualified_match(tmp_path: Path) -> None:
    g = _g(tmp_path)
    matches = g.find("delta://lake/silver/orders")
    assert matches == [DatasetNode("delta://lake", "silver/orders")]


def test_find_exact_name_match_returns_one(tmp_path: Path) -> None:
    g = _g(tmp_path)
    matches = g.find("silver/orders")
    assert len(matches) == 1
    assert matches[0].name == "silver/orders"


def test_find_substring_match_returns_all(tmp_path: Path) -> None:
    g = _g(tmp_path)
    matches = g.find("snap")
    names = {m.name for m in matches}
    assert names == {"snapshots/snap_orders"}


def test_find_no_match_returns_empty(tmp_path: Path) -> None:
    g = _g(tmp_path)
    assert g.find("totally_made_up") == []


def test_find_ambiguous_name_returns_all(tmp_path: Path) -> None:
    """If two namespaces share a dataset name, ``find`` returns both."""
    f = tmp_path / "lineage.jsonl"
    _write_jsonl(
        f,
        [
            _event(job="job1", inputs=[("ns_a", "orders")], outputs=[("ns_b", "out")]),
            _event(job="job2", inputs=[("ns_b", "orders")], outputs=[("ns_c", "other_out")]),
        ],
    )
    g = LineageGraph.from_jsonl([f])
    matches = g.find("orders")
    assert len(matches) == 2
    assert {m.namespace for m in matches} == {"ns_a", "ns_b"}
