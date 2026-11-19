"""End-to-end smoke test: Silver SCD2 lifecycle against real deltalake.

Demonstrates the full SCD2 flow against real Delta files:

- Run 1 (initial): 3 source rows → 3 current versions
- Run 2 (change): id=2 has a new ``updated_at`` → previous current
  closed (``_is_current=false``, ``_valid_to=run-2.updated_at``);
  new version inserted as current
- Run 3 (new key): id=4 first appears → inserted as current
- Run 4 (no-op): re-run with the same source → no new versions, no
  closes (hash matches existing current rows)

Final assertions cover the ``_is_current`` / ``_valid_from`` /
``_valid_to`` invariants per row.

Pre-reqs: ``[local]`` extras (deltalake + pyarrow). No Docker required.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

pytestmark = pytest.mark.smoke


_T1 = datetime(2026, 1, 1, tzinfo=UTC)
_T2 = datetime(2026, 2, 1, tzinfo=UTC)
_T3 = datetime(2026, 3, 1, tzinfo=UTC)
_T4 = datetime(2026, 4, 1, tzinfo=UTC)
_T5 = datetime(2026, 5, 1, tzinfo=UTC)


def _bronze(rows: list[dict[str, object]]) -> pa.Table:
    return pa.table(
        {
            "id": [r["id"] for r in rows],
            "email": [r["email"] for r in rows],
            "tier": [r["tier"] for r in rows],
            "updated_at": pa.array(
                [r["updated_at"] for r in rows], type=pa.timestamp("us", tz="UTC")
            ),
        }
    )


def test_scd2_lifecycle_initial_change_new_noop(tmp_path: Path) -> None:  # noqa: PLR0915 — sequential lifecycle reads top-to-bottom; splitting hides the story
    from deltalake import DeltaTable, write_deltalake
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    runs: list[Any] = []

    def _run(run_id: str) -> None:
        stats = SilverPipeline().run(
            SilverConfig(
                bronze_path=bronze_path,
                silver_path=silver_path,
                run_id=run_id,
                strategy=SilverStrategy.SCD2,
                business_keys=("id",),
                scd2_order_by="updated_at",
            )
        )
        runs.append(stats)

    # ── Run 1: initial load — 3 rows, all become current versions
    write_deltalake(
        str(bronze_path),
        _bronze(
            [
                {"id": 1, "email": "alice@x.com", "tier": "free", "updated_at": _T1},
                {"id": 2, "email": "bob@x.com", "tier": "free", "updated_at": _T1},
                {"id": 3, "email": "carol@x.com", "tier": "free", "updated_at": _T1},
            ]
        ),
        mode="overwrite",
    )
    _run("silver-run-1")

    after_1 = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert after_1.num_rows == 3
    assert all(after_1.column("_is_current").to_pylist())
    # _valid_from set from updated_at on every row
    assert after_1.column("_valid_from").to_pylist() == [_T1, _T1, _T1]
    assert all(v is None for v in after_1.column("_valid_to").to_pylist())

    # ── Run 2: id=2 changes (new tier, new updated_at) → close old, insert new
    write_deltalake(
        str(bronze_path),
        _bronze(
            [
                {"id": 2, "email": "bob@x.com", "tier": "premium", "updated_at": _T2},
            ]
        ),
        mode="overwrite",
    )
    _run("silver-run-2")

    after_2 = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert after_2.num_rows == 4  # 3 originals + 1 new version of id=2

    rows = sorted(after_2.to_pylist(), key=lambda r: (r["id"], r["_valid_from"]))
    # id=1: still current, never closed
    assert rows[0]["id"] == 1
    assert rows[0]["_is_current"] is True
    assert rows[0]["_valid_to"] is None
    # id=2: two rows — old (closed at T2) + new current
    id2 = [r for r in rows if r["id"] == 2]
    assert len(id2) == 2
    closed, current = id2[0], id2[1]
    assert closed["_is_current"] is False
    assert closed["_valid_from"] == _T1
    assert closed["_valid_to"] == _T2
    assert closed["tier"] == "free"
    assert current["_is_current"] is True
    assert current["_valid_from"] == _T2
    assert current["_valid_to"] is None
    assert current["tier"] == "premium"
    assert current["_silver_run_id"] == "silver-run-2"
    # id=3: still current, never closed
    id3 = next(r for r in rows if r["id"] == 3)
    assert id3["_is_current"] is True
    assert id3["_valid_to"] is None

    # ── Run 3: brand-new natural key id=4 → insert as current
    write_deltalake(
        str(bronze_path),
        _bronze(
            [
                {"id": 4, "email": "dave@x.com", "tier": "free", "updated_at": _T3},
            ]
        ),
        mode="overwrite",
    )
    _run("silver-run-3")

    after_3 = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert after_3.num_rows == 5
    id4 = [r for r in after_3.to_pylist() if r["id"] == 4]
    assert len(id4) == 1
    assert id4[0]["_is_current"] is True
    assert id4[0]["_valid_from"] == _T3
    assert id4[0]["_valid_to"] is None
    assert id4[0]["_silver_run_id"] == "silver-run-3"

    # ── Run 4: re-run with the *same* source → no-op (hashes match current versions)
    _run("silver-run-4")

    after_4 = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert after_4.num_rows == 5  # unchanged — no insert, no close
    # All current rows still tagged with their original run_ids (run-1 / run-2 / run-3)
    current_run_ids = {r["_silver_run_id"] for r in after_4.to_pylist() if r["_is_current"] is True}
    assert current_run_ids == {"silver-run-1", "silver-run-2", "silver-run-3"}

    # ── Instrumentation: SilverRunStats reflect what each run actually did
    # Run 1 (initial 3 rows, all new): 3 inserts.
    assert runs[0].rows_inserted == 3
    assert runs[0].rows_updated == 0
    assert runs[0].rows_unchanged == 0
    # Run 2 (id=2 changed): 1 insert (the new version) + 1 update (close-out).
    assert runs[1].rows_inserted == 1
    assert runs[1].rows_updated == 1
    assert runs[1].rows_unchanged == 0
    # Run 3 (id=4 new natural key): 1 pure insert; nothing to close.
    assert runs[2].rows_inserted == 1
    assert runs[2].rows_updated == 0
    assert runs[2].rows_unchanged == 0
    # Run 4 (no-op): everything classified as unchanged.
    assert runs[3].rows_inserted == 0
    assert runs[3].rows_updated == 0
    assert runs[3].rows_unchanged == 1
    # Each run records a non-negative wall-clock duration + the post-commit version.
    for s in runs:
        assert s.duration_seconds >= 0
        assert s.silver_version_after is not None
    # Versions monotonically advance (run 1 commit < run 2 commit < ...).
    versions = [s.silver_version_after for s in runs]
    assert versions[0] is not None
    assert versions[-1] is not None
    assert versions[0] < versions[-1]
