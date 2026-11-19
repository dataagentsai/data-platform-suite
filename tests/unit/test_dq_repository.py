"""Unit tests for the DQ rules repository round-trip (DQ engine v1).

Covers ``SqliteMetadataRepository``'s DQ methods:
``upsert_dq_rule_set`` / ``list_dq_rules`` / ``get_dq_rule``.
The full apply path through ``apply_config_sync`` is tested in
``test_dq_config_sync.py``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    Container,
    DqRule,
    DqRuleSet,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    RuleSource,
    RuleType,
    Severity,
    Tech,
)
from lake_ingestion_framework.core.metadata.repository import (
    DqRuleSyncResult,
    SqliteMetadataRepository,
)

# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo() -> Iterator[SqliteMetadataRepository]:
    """Bootstrapped in-memory repo + a seeded dataset_master row.

    The DQ tests don't go through the full source/dataset KV-validated
    upsert path — they need only a ``dataset_id`` to attach rules to.
    """
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    conn = r._connect()
    conn.execute(
        "INSERT INTO source_master (source_name, source_env, source_type) "
        "VALUES ('sales_db', 'dev', 'postgres')"
    )
    conn.execute(
        "INSERT INTO dataset_master (source_id, dataset_name, is_enabled) "
        "VALUES (1, 'orders', 1)"
    )
    conn.commit()
    try:
        yield r
    finally:
        r.close()


def _rule(name: str = "r1", **overrides: object) -> DqRule:
    base: dict[str, object] = {
        "name": name,
        "dataset_name": "orders",
        "layer": Layer.SILVER,
        "predicate": "order_id IS NULL",
        "injection_point": InjectionPoint.POST_PROCESSOR,
        "severity": Severity.ERROR,
        "action": Action.STOP_PIPELINE,
    }
    base.update(overrides)
    return DqRule(**base)  # type: ignore[arg-type]


def _rule_set(*rules: DqRule) -> DqRuleSet:
    return DqRuleSet(dataset_name="orders", layer=Layer.SILVER, rules=rules)


# ──────────────────────────────────────────────────────────────────────
# Insert + read-back
# ──────────────────────────────────────────────────────────────────────


def test_inserts_new_rules(repo: SqliteMetadataRepository) -> None:
    result = repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b")), dataset_id=1)
    assert result == DqRuleSyncResult(inserted=2, updated=0, unchanged=0)


def test_round_trip_preserves_every_field(repo: SqliteMetadataRepository) -> None:
    rule = _rule(
        name="amount_in_range",
        predicate="amount < 0 OR amount > 100000",
        severity=Severity.WARNING,
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
        injection_point=InjectionPoint.POST_WRITE,
        sample_cap=50,
        is_enabled=False,
        authored_by="alice@example.com",
        description="catches outliers",
    )
    repo.upsert_dq_rule_set(_rule_set(rule), dataset_id=1)
    rules = repo.list_dq_rules(dataset_id=1, only_enabled=False)
    assert len(rules) == 1
    out = rules[0]
    assert out.rule_name == "amount_in_range"
    assert out.layer == Layer.SILVER
    assert out.tech == Tech.SQL  # default
    assert out.container == Container.DUCKDB  # default
    assert out.rule_type == RuleType.PREDICATE  # default
    assert out.predicate == "amount < 0 OR amount > 100000"
    assert out.severity == Severity.WARNING
    assert out.action == Action.QUARANTINE_RECORD
    assert out.quarantine_strategy == QuarantineStrategy.SIDE_TABLE
    assert out.injection_point == InjectionPoint.POST_WRITE
    assert out.sample_cap == 50
    assert out.is_enabled is False
    assert out.source == RuleSource.USER_AUTHORED
    assert out.authored_by == "alice@example.com"
    assert out.description == "catches outliers"


def test_get_dq_rule_by_id(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b")), dataset_id=1)
    rule = repo.get_dq_rule(rule_id=1)
    assert rule is not None
    assert rule.rule_name == "a"


def test_get_dq_rule_returns_none_when_missing(repo: SqliteMetadataRepository) -> None:
    assert repo.get_dq_rule(rule_id=999) is None


def test_unique_natural_key_per_dataset_layer(repo: SqliteMetadataRepository) -> None:
    """Inserting two rules with the same name in the same set is caught
    by ``DqRuleSet`` validation; doing it via the DB unique index is the
    last line of defence."""
    repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)
    # Direct SQL bypassing the upsert helper that would have caught the dupe
    conn = repo._connect()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO dq_rules (rule_name, dataset_id, layer, predicate, "
            "injection_point, severity, action) VALUES "
            "('a', 1, 'silver', 'x IS NULL', 'post_processor', 'error', 'stop_pipeline')"
        )


# ──────────────────────────────────────────────────────────────────────
# Idempotence + diff
# ──────────────────────────────────────────────────────────────────────


def test_resync_same_rules_is_unchanged(repo: SqliteMetadataRepository) -> None:
    rule_set = _rule_set(_rule("a"), _rule("b"))
    repo.upsert_dq_rule_set(rule_set, dataset_id=1)
    second = repo.upsert_dq_rule_set(rule_set, dataset_id=1)
    assert second == DqRuleSyncResult(inserted=0, updated=0, unchanged=2)


def test_changed_predicate_triggers_update(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a", predicate="order_id IS NULL")), dataset_id=1)
    second = repo.upsert_dq_rule_set(
        _rule_set(_rule("a", predicate="order_id IS NULL OR amount < 0")),
        dataset_id=1,
    )
    assert second == DqRuleSyncResult(inserted=0, updated=1, unchanged=0)


def test_changed_severity_triggers_update(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a", severity=Severity.WARNING)), dataset_id=1)
    second = repo.upsert_dq_rule_set(_rule_set(_rule("a", severity=Severity.ERROR)), dataset_id=1)
    assert second.updated == 1
    rules = repo.list_dq_rules(dataset_id=1)
    assert rules[0].severity == Severity.ERROR


def test_changed_action_triggers_update(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a", action=Action.STOP_PIPELINE)), dataset_id=1)
    second = repo.upsert_dq_rule_set(
        _rule_set(
            _rule(
                "a",
                action=Action.QUARANTINE_RECORD,
                quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
            )
        ),
        dataset_id=1,
    )
    assert second.updated == 1
    rules = repo.list_dq_rules(dataset_id=1)
    assert rules[0].action == Action.QUARANTINE_RECORD
    assert rules[0].quarantine_strategy == QuarantineStrategy.SIDE_TABLE


# ──────────────────────────────────────────────────────────────────────
# Soft + hard delete
# ──────────────────────────────────────────────────────────────────────


def test_dropped_rule_soft_deletes_by_default(repo: SqliteMetadataRepository) -> None:
    """When a rule is in the DB but not in the new YAML, default behavior
    is to set ``is_enabled=0`` — preserves audit history."""
    repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b")), dataset_id=1)
    second = repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)
    assert second.soft_deleted == 1
    assert second.hard_deleted == 0

    enabled = repo.list_dq_rules(dataset_id=1, only_enabled=True)
    assert {r.rule_name for r in enabled} == {"a"}

    all_rules = repo.list_dq_rules(dataset_id=1, only_enabled=False)
    assert {r.rule_name for r in all_rules} == {"a", "b"}
    b = next(r for r in all_rules if r.rule_name == "b")
    assert b.is_enabled is False


def test_dropped_rule_hard_deletes_with_flag(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b")), dataset_id=1)
    second = repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1, delete_missing=True)
    assert second.hard_deleted == 1
    assert second.soft_deleted == 0

    all_rules = repo.list_dq_rules(dataset_id=1, only_enabled=False)
    assert {r.rule_name for r in all_rules} == {"a"}


def test_already_disabled_rule_not_recounted_as_soft_delete(
    repo: SqliteMetadataRepository,
) -> None:
    """Removing a rule that's *already* disabled shouldn't bump
    soft_deleted again — only enabled→disabled transitions count."""
    repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b")), dataset_id=1)
    repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)  # b → soft-deleted
    third = repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)
    assert third == DqRuleSyncResult(inserted=0, updated=0, unchanged=1)


def test_re_enabling_a_soft_deleted_rule(repo: SqliteMetadataRepository) -> None:
    """Customer adds back a previously-removed rule. Should re-enable
    via the predicate-changed path (is_enabled is one of the diff fields)."""
    repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)
    repo.upsert_dq_rule_set(_rule_set(), dataset_id=1)  # a → soft-deleted (drops the only rule)


# ──────────────────────────────────────────────────────────────────────
# Filtering on list
# ──────────────────────────────────────────────────────────────────────


def test_list_filters_by_dataset_id(repo: SqliteMetadataRepository) -> None:
    # add a second dataset
    conn = repo._connect()
    conn.execute(
        "INSERT INTO dataset_master (source_id, dataset_name, is_enabled) "
        "VALUES (1, 'customers', 1)"
    )
    conn.commit()
    repo.upsert_dq_rule_set(_rule_set(_rule("a")), dataset_id=1)
    repo.upsert_dq_rule_set(
        DqRuleSet(
            dataset_name="customers",
            layer=Layer.SILVER,
            rules=(_rule("c", dataset_name="customers"),),
        ),
        dataset_id=2,
    )

    only_orders = repo.list_dq_rules(dataset_id=1)
    only_customers = repo.list_dq_rules(dataset_id=2)
    assert len(only_orders) == 1
    assert len(only_customers) == 1
    assert only_orders[0].rule_name == "a"
    assert only_customers[0].rule_name == "c"


def test_list_filters_by_layer(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("silver_rule", layer=Layer.SILVER)), dataset_id=1)
    repo.upsert_dq_rule_set(
        DqRuleSet(
            dataset_name="orders",
            layer=Layer.GOLD,
            rules=(_rule("gold_rule", layer=Layer.GOLD),),
        ),
        dataset_id=1,
    )
    silver = repo.list_dq_rules(dataset_id=1, layer=Layer.SILVER)
    gold = repo.list_dq_rules(dataset_id=1, layer=Layer.GOLD)
    assert {r.rule_name for r in silver} == {"silver_rule"}
    assert {r.rule_name for r in gold} == {"gold_rule"}


def test_only_enabled_default_excludes_disabled(repo: SqliteMetadataRepository) -> None:
    repo.upsert_dq_rule_set(_rule_set(_rule("a"), _rule("b", is_enabled=False)), dataset_id=1)
    enabled = repo.list_dq_rules(dataset_id=1)
    all_rules = repo.list_dq_rules(dataset_id=1, only_enabled=False)
    assert {r.rule_name for r in enabled} == {"a"}
    assert {r.rule_name for r in all_rules} == {"a", "b"}
