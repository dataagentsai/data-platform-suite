"""End-to-end smoke test: Silver SCD1 strategy against real deltalake.

Demonstrates the SCD1 lifecycle directly against ``deltalake`` (no
postgres / Docker — the merge logic is independent of the source).

Scenario:

1. Write an initial Bronze Delta with 3 rows (id 1, 2, 3).
2. Run SilverPipeline scd1 → creates Silver table with 3 rows.
3. Write a *second* Bronze Delta with id=2 modified, id=4 new.
4. Re-run SilverPipeline scd1 with same target → MERGE applies:
   - id=2 row updated in place
   - id=4 inserted
   - id=1, id=3 untouched
5. Assert final Silver state: 4 rows, id=2 has the new value, id=4
   present, _silver_run_id reflects which run touched each row.

Pre-reqs: ``[local]`` extras (deltalake + pyarrow). No Docker required —
this test is faster than the postgres smoke tests but still smoke-marked
because it writes real Delta files to disk.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

pytestmark = pytest.mark.smoke


def _bronze_table(rows: list[dict[str, object]]) -> pa.Table:
    """Build a Bronze-shaped Arrow table (with Bronze metadata) from row dicts."""
    return pa.table(
        {
            "id": [r["id"] for r in rows],
            "email": [r["email"] for r in rows],
            "tier": [r["tier"] for r in rows],
        }
    )


def test_scd1_lifecycle_initial_load_then_merge(tmp_path: Path) -> None:
    from deltalake import DeltaTable, write_deltalake
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    # ── Run 1: write 3 source rows to Bronze, then SCD1 → Silver
    write_deltalake(
        str(bronze_path),
        _bronze_table(
            [
                {"id": 1, "email": "alice@example.com", "tier": "free"},
                {"id": 2, "email": "bob@example.com", "tier": "free"},
                {"id": 3, "email": "carol@example.com", "tier": "free"},
            ]
        ),
        mode="overwrite",  # fresh table for the smoke run
    )

    SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id="silver-run-1",
            strategy=SilverStrategy.SCD1,
            business_keys=("id",),
        )
    )

    silver_after_run_1 = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert silver_after_run_1.num_rows == 3
    assert {1, 2, 3} == set(silver_after_run_1.column("id").to_pylist())
    # All three rows tagged with run-1
    assert {"silver-run-1"} == set(silver_after_run_1.column("_silver_run_id").to_pylist())

    # ── Run 2: Bronze now has id=2 modified, id=4 new (id=1 / id=3 dropped from
    # the source — SCD1 doesn't delete; they survive in Silver).
    write_deltalake(
        str(bronze_path),
        _bronze_table(
            [
                {"id": 2, "email": "bob.upgraded@example.com", "tier": "premium"},
                {"id": 4, "email": "dave@example.com", "tier": "free"},
            ]
        ),
        mode="overwrite",
    )

    SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id="silver-run-2",
            strategy=SilverStrategy.SCD1,
            business_keys=("id",),
        )
    )

    # ── Final Silver state
    final = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert final.num_rows == 4
    assert {1, 2, 3, 4} == set(final.column("id").to_pylist())

    # id=2 was updated in place — values match run-2 source
    rows_by_id = {row["id"]: row for row in final.to_pylist()}
    assert rows_by_id[2]["email"] == "bob.upgraded@example.com"
    assert rows_by_id[2]["tier"] == "premium"
    assert rows_by_id[2]["_silver_run_id"] == "silver-run-2"

    # id=4 was inserted by run-2
    assert rows_by_id[4]["email"] == "dave@example.com"
    assert rows_by_id[4]["_silver_run_id"] == "silver-run-2"

    # id=1, id=3 are untouched — still tagged with run-1
    assert rows_by_id[1]["_silver_run_id"] == "silver-run-1"
    assert rows_by_id[3]["_silver_run_id"] == "silver-run-1"
