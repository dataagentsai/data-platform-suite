"""Retention (ADR-029, B8): table-driven over dates and settings; the local landing archive on a temp folder."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dataplatform.retention import Retention, bronze_cutoff, landing_actions, window_day

TODAY = date(2026, 12, 31)

SETTINGS_CASES = [
    ("defaults", {}, False, Retention(30, 1095, 1095, True, 7)),
    ("personal data: the archive is 90 days", {}, True, Retention(30, 90, 1095, True, 7)),
    ("personal data: never longer than 90 days, whatever the setting", {"landing_archive_days": "400"}, True,
     Retention(30, 90, 1095, True, 7)),
    ("the archive follows bronze by default", {"bronze_retention_days": "365"}, False,
     Retention(30, 365, 365, True, 7)),
    ("every setting read", {"landing_hot_days": "7", "landing_archive_days": "60", "bronze_retention_days": "730",
                            "bronze_baseline": "false", "version_history_days": "30"}, False,
     Retention(7, 60, 730, False, 30)),
]


@pytest.mark.parametrize(("case", "settings", "personal", "want"), SETTINGS_CASES, ids=[c[0] for c in SETTINGS_CASES])
def test_retention_from_settings(case: str, settings: dict[str, str], personal: bool, want: Retention) -> None:
    assert Retention.from_settings(settings, personal) == want


R = Retention(landing_hot_days=30, landing_archive_days=90)
ACTION_CASES = [
    ("young and hot: keep", ("2026-12-20", "a.csv", "hot"), True, "keep"),
    ("older than hot: archive", ("2026-11-20", "a.csv", "hot"), True, "archive"),
    ("archived and young enough: keep", ("2026-11-01", "a.csv", "archive"), True, "keep"),
    ("past hot + archive: delete", ("2026-09-01", "a.csv", "archive"), True, "delete"),
    ("hot but past both limits: delete at once", ("2026-09-01", "a.csv", "hot"), True, "delete"),
    ("never taken by the lander: always kept", ("2026-01-01", "a.csv", "hot"), False, "keep"),
    ("a month window counts from its first day", ("2026-11", "m.csv", "hot"), True, "archive"),
]


@pytest.mark.parametrize(("case", "delivery", "taken", "want"), ACTION_CASES, ids=[c[0] for c in ACTION_CASES])
def test_landing_actions(case: str, delivery: tuple[str, str, str], taken: bool, want: str) -> None:
    done = {delivery[:2]} if taken else set()
    assert landing_actions([delivery], done, TODAY, R) == [(delivery[0], delivery[1], want)]


def test_window_day_and_cutoff() -> None:
    assert window_day("2026-11-09T10") == date(2026, 11, 9)
    assert bronze_cutoff(TODAY, Retention(bronze_days=365)) == date(2025, 12, 31)


def test_landing_archive_moves_and_deletes(tmp_path: Any) -> None:
    from dataplatform.adapters.files import FileLanding, LandingArchive
    from dataplatform.adapters.memory_meta import MemoryMetaStore
    from dataplatform.meta import registry
    from dataplatform.meta.port import FileRecord
    from dataplatform.retention import apply_landing_retention

    landing = FileLanding(tmp_path)
    for window in ("2026-08-01", "2026-11-15", "2026-12-30"):
        landing.put("lims", "result", window, f"r-{window}.csv", b"id\n1\n", 1)
    landing.put("lims", "result", "2026-11-14", "held.csv", b"id\n1\n", 1)
    store = MemoryMetaStore()
    store.register_settings(registry.load())
    store.load_domain({"domain": "t", "settings": {}, "manifest": {}, "fingerprint": "d"})
    store.load_dataset({"domain": "t", "source_name": "lims", "source_env": "", "dataset_name": "result",
                        "is_enabled": True, "kind": "dataset", "archetypes": [], "ingest_mode": "push",
                        "ingest_read": "files", "history_kind": "event", "load_pattern": "append", "contract_id": "c",
                        "contract_version": "1", "fingerprint": "fp",
                        "settings": {"landing_hot_days": "30", "landing_archive_days": "60"},
                        "columns": [{"name": "id", "logical_type": "string", "required": True,
                                     "primary_key_position": 1, "description": "", "classification": ""}],
                        "business_keys": {"primary": ["id"]}, "manifest": {}})
    store.start_run("i1", "t", "dev")
    store.start_dataset_run("i1", "lims", "result", "fp", {})
    for window in ("2026-08-01", "2026-11-15", "2026-12-30"):
        store.log_file(FileRecord("i1", "lims", "result", f"k{window}", f"r-{window}.csv", window, "c", 1, 1, "loaded"))
    store.log_file(FileRecord("i1", "lims", "result", "kh", "held.csv", "2026-11-14", "c", 1, 0, "rejected_contract"))
    archive = LandingArchive(landing)
    counts = apply_landing_retention(archive, store, "lims", "result", TODAY)
    assert counts == {"keep": 2, "archive": 1, "delete": 1}
    assert sorted(archive.listing("lims", "result")) == [
        ("2026-11-14", "held.csv", "hot"), ("2026-11-15", "r-2026-11-15.csv", "archive"),
        ("2026-12-30", "r-2026-12-30.csv", "hot")]
    assert [d.name for d in landing.deliveries("lims", "result")] == ["held.csv", "r-2026-12-30.csv"]
