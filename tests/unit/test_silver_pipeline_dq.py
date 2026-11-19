"""Phase 4 — DQ wiring inside :class:`SilverPipeline`.

Covers the integration of the DQ engine (Phases 1-3) with the Silver
pipeline:

* No ``dq_context`` → behaviour unchanged (pure pipeline)
* No matching rules → no DQ work, all DQ stats zero
* ``passed`` rule → audit row, Silver write proceeds
* ``failed`` + ``continue_log`` → audit + violations sample, write proceeds
* ``failed`` + ``stop_pipeline`` → DqHaltError; **no Silver write**
* ``failed`` + ``quarantine_record + log_only`` → audit + sample only
* ``errored`` rule → audit, write proceeds
* injection_point filter — only ``post_processor`` rules fire here

The deltalake module is faked at the lazy-import boundary (same pattern
as ``test_silver_pipeline.py``); pyarrow + duckdb are real.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    DeltaSideTableWriter,
    DqHaltError,
    DqRuleSet,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    Severity,
)
from lake_ingestion_framework.core.dq.types import DqRule
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_LOADED_AT,
    BRONZE_RECORD_HASH,
    BRONZE_RUN_ID,
)
from lake_ingestion_framework.core.silver import (
    SilverConfig,
    SilverDqContext,
    SilverPipeline,
    SilverStrategy,
)
from lake_ingestion_framework.core.types.enums import SourceType

# ──────────────────────────────────────────────────────────────────────
# deltalake fake — mirrors test_silver_pipeline.py
# ──────────────────────────────────────────────────────────────────────


class _FakeDeltaTable:
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_pyarrow_table(self) -> pa.Table:
        return self._table


class _FakeDeltalake:
    def __init__(self, bronze: pa.Table) -> None:
        self._bronze = bronze
        self.write_calls: list[dict[str, Any]] = []

    def DeltaTable(self, path: str) -> _FakeDeltaTable:
        return _FakeDeltaTable(self._bronze)

    def write_deltalake(
        self,
        path: str,
        data: pa.Table,
        *,
        mode: str = "append",
        partition_by: list[str] | None = None,
    ) -> None:
        self.write_calls.append({"path": path, "data": data, "mode": mode})


def _patch_deltalake(monkeypatch: pytest.MonkeyPatch, fake: _FakeDeltalake) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.silver.pipeline._lazy_import_deltalake",
        lambda: fake,
    )


# ──────────────────────────────────────────────────────────────────────
# Fixtures + builders
# ──────────────────────────────────────────────────────────────────────


def _bronze_with_one_null_email() -> pa.Table:
    """Bronze table where customer_id=2 has NULL email.

    Used to drive ``email IS NULL`` predicates — exactly one violator.
    """
    return pa.table(
        {
            "customer_id": [1, 2, 3, 4, 5],
            "email": ["a@x.com", None, "c@x.com", "d@x.com", "e@x.com"],
            BRONZE_RUN_ID: ["b1"] * 5,
            BRONZE_LOADED_AT: pa.array(
                [datetime(2026, 5, 5, tzinfo=UTC)] * 5,
                type=pa.timestamp("us", tz="UTC"),
            ),
            BRONZE_RECORD_HASH: ["h1", "h2", "h3", "h4", "h5"],
        }
    )


def _bronze_clean() -> pa.Table:
    """Bronze table with no NULL emails — passes ``email IS NULL`` rules."""
    return pa.table(
        {
            "customer_id": [1, 2, 3],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
            BRONZE_RUN_ID: ["b1"] * 3,
            BRONZE_LOADED_AT: pa.array(
                [datetime(2026, 5, 5, tzinfo=UTC)] * 3,
                type=pa.timestamp("us", tz="UTC"),
            ),
            BRONZE_RECORD_HASH: ["h1", "h2", "h3"],
        }
    )


def _make_rule(
    name: str,
    predicate: str,
    *,
    action: Action = Action.CONTINUE_LOG,
    quarantine_strategy: QuarantineStrategy | None = None,
    injection_point: InjectionPoint = InjectionPoint.POST_PROCESSOR,
    severity: Severity = Severity.WARNING,
    sample_cap: int = 100,
) -> DqRule:
    return DqRule(
        name=name,
        dataset_name="customers",
        layer=Layer.SILVER,
        predicate=predicate,
        injection_point=injection_point,
        severity=severity,
        action=action,
        quarantine_strategy=quarantine_strategy,
        sample_cap=sample_cap,
    )


@pytest.fixture()
def repo_with_dataset() -> Iterator[tuple[SqliteMetadataRepository, int]]:
    """Repo seeded with one source + dataset; yields (repo, dataset_id).

    Tests append rules to this dataset using ``upsert_dq_rule_set``.
    """
    repo = SqliteMetadataRepository(":memory:")
    try:
        repo.bootstrap_schema()
        source_id = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
            },
        )
        dataset_id = repo.upsert_dataset(
            source_id=source_id,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "load_type": "INCREMENTAL",
                "target_path_template": "raw/{source}/{object}",
                "schema_name": "dbo",
                "object_name": "customers",
                "watermark_column": "modified_at",
            },
            columns=[
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="customer_id",
                    data_type="INT",
                    is_nullable=False,
                    ordinal_position=1,
                    is_primary_key=True,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                ),
            ],
        )
        yield repo, dataset_id
    finally:
        repo.close()


def _seed_rules(repo: SqliteMetadataRepository, dataset_id: int, *rules: DqRule) -> None:
    """Bulk-insert rules under ``(dataset_id, layer=silver)``."""
    rs = DqRuleSet(dataset_name="customers", layer=Layer.SILVER, rules=rules)
    repo.upsert_dq_rule_set(rs, dataset_id=dataset_id)


def _ctx(
    repo: SqliteMetadataRepository,
    dataset_id: int,
    *,
    pk_columns: tuple[str, ...] = ("customer_id",),
) -> SilverDqContext:
    return SilverDqContext(
        repo=repo,
        dataset_id=dataset_id,
        dataset_name="customers",
        relation_name="customers",
        pk_columns=pk_columns,
    )


def _config(tmp_path: Path, *, dq_context: SilverDqContext | None = None) -> SilverConfig:
    return SilverConfig(
        bronze_path=tmp_path / "bronze",
        silver_path=tmp_path / "silver",
        run_id="silver-run-42",
        strategy=SilverStrategy.APPEND,
        dq_context=dq_context,
    )


# ──────────────────────────────────────────────────────────────────────
# No DQ context — existing behaviour preserved
# ──────────────────────────────────────────────────────────────────────


def test_no_dq_context_runs_pipeline_and_zero_dq_stats(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeDeltalake(_bronze_clean())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path))

    assert stats.dq_rules_evaluated == 0
    assert stats.dq_rules_passed == 0
    assert stats.dq_rules_failed == 0
    assert stats.dq_violations_logged == 0
    assert len(fake.write_calls) == 1  # Silver write happened


def test_dq_context_with_no_rules_runs_pipeline_normally(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_dataset
    fake = _FakeDeltalake(_bronze_clean())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 0
    assert len(fake.write_calls) == 1


# ──────────────────────────────────────────────────────────────────────
# Status branches
# ──────────────────────────────────────────────────────────────────────


def test_passed_rule_records_audit_and_writes_silver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_dataset
    _seed_rules(repo, dataset_id, _make_rule("email_not_null", "email IS NULL"))
    fake = _FakeDeltalake(_bronze_clean())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 1
    assert stats.dq_rules_passed == 1
    assert stats.dq_rules_failed == 0
    assert len(fake.write_calls) == 1  # write proceeded

    # Audit row is in dq_results, with passed=True.
    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    results = repo.list_dq_results(rule_id=rule_id)
    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].action_taken == "none"
    assert results[0].run_id == "silver-run-42"


def test_failed_continue_log_records_violations_and_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule("email_not_null", "email IS NULL", action=Action.CONTINUE_LOG),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_failed == 1
    assert stats.dq_violations_logged == 1
    assert len(fake.write_calls) == 1  # continue_log → write still happens

    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    [result] = repo.list_dq_results(rule_id=rule_id)
    violations = repo.list_dq_violations(result_id=result.result_id)
    assert len(violations) == 1


def test_failed_stop_pipeline_raises_and_blocks_silver_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "email_not_null_strict",
            "email IS NULL",
            action=Action.STOP_PIPELINE,
            severity=Severity.ERROR,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    with pytest.raises(DqHaltError):
        SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    # The audit row landed (engine writes before raising)
    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    [result] = repo.list_dq_results(rule_id=rule_id)
    assert result.passed is False
    assert result.action_taken == "stop_pipeline"
    # Silver write was NOT attempted — clean halt
    assert fake.write_calls == []


def test_failed_quarantine_log_only_records_sample_and_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "email_not_null_quar",
            "email IS NULL",
            action=Action.QUARANTINE_RECORD,
            quarantine_strategy=QuarantineStrategy.LOG_ONLY,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_failed == 1
    assert stats.dq_violations_logged == 1
    # log_only does NOT route rows to a side table
    assert stats.dq_rows_quarantined == 0
    # Silver write proceeded with all rows (quarantine log_only is flag-only)
    assert len(fake.write_calls) == 1


def test_errored_rule_records_audit_and_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """Predicate references a column that doesn't exist on Silver →
    engine error, fail-open, Silver write still proceeds."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "broken",
            "definitely_not_a_column IS NULL",
            action=Action.STOP_PIPELINE,  # would halt if it ran cleanly
        ),
    )
    fake = _FakeDeltalake(_bronze_clean())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    # Engine errored; pipeline still wrote. Errored counts as errored,
    # not failed (halt is suppressed — fail-open contract from Phase 3).
    assert stats.dq_rules_errored == 1
    assert stats.dq_rules_failed == 0
    assert len(fake.write_calls) == 1

    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    [result] = repo.list_dq_results(rule_id=rule_id)
    assert result.action_taken == "errored"
    assert result.error_message is not None


# ──────────────────────────────────────────────────────────────────────
# Injection-point filter
# ──────────────────────────────────────────────────────────────────────


def test_post_reader_rule_halts_before_enrich(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """Phase 4c: post_reader rules fire against the RAW Bronze pyarrow
    table before _enrich. A stop_pipeline rule at this point halts
    immediately — no Silver write, no enrich, no later injection points."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "halt_before_enrich",
            "email IS NULL",
            action=Action.STOP_PIPELINE,
            injection_point=InjectionPoint.POST_READER,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    with pytest.raises(DqHaltError):
        SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    # Halted before _enrich → no Silver write attempted.
    assert fake.write_calls == []


def test_multiple_rules_aggregate_counts_correctly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """One pass + one fail (continue_log) + one error → 3 evaluated,
    1 passed, 1 failed, 1 errored, 1 violation."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule("clean", "customer_id < 0"),  # passes
        _make_rule("nulls", "email IS NULL"),  # fails (1 violator)
        _make_rule("typo", "no_such_col = 1"),  # errors
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 3
    assert stats.dq_rules_passed == 1
    assert stats.dq_rules_failed == 1
    assert stats.dq_rules_errored == 1
    assert stats.dq_violations_logged == 1
    assert len(fake.write_calls) == 1  # all non-halting → write proceeds


def test_pk_columns_slicing_keeps_audit_compact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """When pk_columns=(customer_id,), only customer_id appears in the
    captured violator JSON — proves Silver-metadata cols (run_id,
    loaded_at, hash) and PII fields don't leak into the audit log."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(repo, dataset_id, _make_rule("email_not_null", "email IS NULL"))
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    SilverPipeline().run(
        _config(
            tmp_path,
            dq_context=_ctx(repo, dataset_id, pk_columns=("customer_id",)),
        )
    )

    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    [result] = repo.list_dq_results(rule_id=rule_id)
    [violation] = repo.list_dq_violations(result_id=result.result_id)
    import json

    payload = json.loads(violation.pk_columns_json)
    assert payload == {"customer_id": 2}  # nothing else


def test_disabled_rules_are_ignored(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """A rule synced with is_enabled=False should not be evaluated.

    We seed the rule, then re-sync the dataset's rule set with an
    empty rule list — soft-deletes (is_enabled=0). Subsequent runs
    must skip it.
    """
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule("would_halt", "email IS NULL", action=Action.STOP_PIPELINE),
    )
    # Soft-delete by syncing an empty rule set for the same (dataset, layer)
    repo.upsert_dq_rule_set(
        DqRuleSet(dataset_name="customers", layer=Layer.SILVER, rules=()),
        dataset_id=dataset_id,
    )

    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    # Rule was soft-deleted → list_dq_rules(only_enabled=True) skips it.
    assert stats.dq_rules_evaluated == 0
    assert len(fake.write_calls) == 1  # would have halted if rule had run


# ──────────────────────────────────────────────────────────────────────
# Phase 4c — post_reader + post_write injection points
# ──────────────────────────────────────────────────────────────────────


def test_post_reader_rule_runs_against_raw_bronze(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """post_reader fires before _enrich, so predicates see the raw
    Bronze schema (BRONZE_RUN_ID etc.) and not Silver metadata cols.

    A continue_log rule fires + Silver write proceeds; the test proves
    the rule was evaluated by checking the failed-count rose.
    """
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "bronze_email_check",
            "email IS NULL",
            action=Action.CONTINUE_LOG,
            injection_point=InjectionPoint.POST_READER,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 1
    assert stats.dq_rules_failed == 1
    assert stats.dq_violations_logged == 1
    assert len(fake.write_calls) == 1  # continue_log → write proceeds


def test_post_write_rule_fires_after_silver_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """post_write rules fire AFTER the Silver write succeeds. v1
    simplification: predicate runs against the same enriched table
    (the data that was written), not a re-read of the Delta.
    """
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "post_write_check",
            "email IS NULL",
            action=Action.CONTINUE_LOG,
            injection_point=InjectionPoint.POST_WRITE,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 1
    assert stats.dq_rules_failed == 1
    assert len(fake.write_calls) == 1  # write happened first


def test_post_write_halt_does_not_undo_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """A stop_pipeline rule at post_write raises AFTER the Silver
    write committed. The audit row is recorded; downstream consumers
    see the halt, but the Silver table is already there."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "halt_after_write",
            "email IS NULL",
            action=Action.STOP_PIPELINE,
            injection_point=InjectionPoint.POST_WRITE,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    with pytest.raises(DqHaltError):
        SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    # Write WAS attempted (halt is post-write); audit row landed too.
    assert len(fake.write_calls) == 1
    rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
    [result] = repo.list_dq_results(rule_id=rule_id)
    assert result.action_taken == "stop_pipeline"


def test_all_three_injection_points_aggregate_in_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """One rule at each of post_reader / post_processor / post_write,
    all continue_log, all fail → 3 evaluated, 3 failed, 3 violations."""
    repo, dataset_id = repo_with_dataset
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "r_reader",
            "email IS NULL",
            action=Action.CONTINUE_LOG,
            injection_point=InjectionPoint.POST_READER,
        ),
        _make_rule(
            "r_processor",
            "email IS NULL",
            action=Action.CONTINUE_LOG,
            injection_point=InjectionPoint.POST_PROCESSOR,
        ),
        _make_rule(
            "r_writer",
            "email IS NULL",
            action=Action.CONTINUE_LOG,
            injection_point=InjectionPoint.POST_WRITE,
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    stats = SilverPipeline().run(_config(tmp_path, dq_context=_ctx(repo, dataset_id)))

    assert stats.dq_rules_evaluated == 3
    assert stats.dq_rules_failed == 3
    assert stats.dq_violations_logged == 3
    assert len(fake.write_calls) == 1


# ──────────────────────────────────────────────────────────────────────
# Phase 4b — DeltaSideTableWriter wired through SilverPipeline
# ──────────────────────────────────────────────────────────────────────


def test_side_table_quarantine_routes_violators_to_delta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repo_with_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """End-to-end: ``quarantine_record + side_table`` rule wired with
    a customer-managed duckdb conn and :class:`DeltaSideTableWriter`.

    Confirms:
    - Silver write proceeds (quarantine is flag-and-trace, not block)
    - Audit row + sample land in dq_results / dq_violations
    - Violator rows land in a Delta dir under ``target_root`` with
      ``dq_run_id`` / ``dq_rule_id`` / ``dq_captured_at`` columns
    - ``dq_rows_quarantined`` is surfaced on SilverRunStats
    """
    import deltalake

    repo, dataset_id = repo_with_dataset
    # Predicate that matches 2 rows in _bronze_with_one_null_email
    # (customer_id 2 has NULL email; the rule below captures both 2 and 4
    # by id so we can test that side-table count is independent of the
    # audit sample_cap=1).
    _seed_rules(
        repo,
        dataset_id,
        _make_rule(
            "early_customers",
            "customer_id IN (2, 4)",
            action=Action.QUARANTINE_RECORD,
            quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
            sample_cap=1,  # cap audit sample; side-table count is independent
        ),
    )
    fake = _FakeDeltalake(_bronze_with_one_null_email())
    _patch_deltalake(monkeypatch, fake)

    quarantine_root = tmp_path / "quarantine"
    quarantine_root.mkdir()

    customer_conn = duckdb.connect(":memory:")
    try:
        writer = DeltaSideTableWriter(customer_conn, target_root=quarantine_root)
        ctx = SilverDqContext(
            repo=repo,
            dataset_id=dataset_id,
            dataset_name="customers",
            relation_name="customers",
            pk_columns=("customer_id",),
            quarantine_writer=writer,
            conn=customer_conn,
        )
        stats = SilverPipeline().run(_config(tmp_path, dq_context=ctx))

        # Silver write proceeded (side_table = flag, not block)
        assert len(fake.write_calls) == 1
        # All 2 NULL-email violators routed to side table — independent of
        # sample_cap (which is 1 here, capping audit only)
        assert stats.dq_rows_quarantined == 2
        assert stats.dq_rules_failed == 1

        # Audit row + capped sample
        rule_id = repo.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)[0].rule_id
        [result] = repo.list_dq_results(rule_id=rule_id)
        assert result.action_taken == "quarantine_side_table"
        assert len(repo.list_dq_violations(result_id=result.result_id)) == 1

        # Side table on disk has both violators with DQ metadata cols
        target_dir = quarantine_root / "customers__quarantine"
        rows = deltalake.DeltaTable(str(target_dir)).to_pyarrow_table().to_pylist()
        assert len(rows) == 2
        assert {r["customer_id"] for r in rows} == {2, 4}
        assert {r["dq_run_id"] for r in rows} == {"silver-run-42"}
        assert {r["dq_rule_id"] for r in rows} == {rule_id}
    finally:
        customer_conn.close()
