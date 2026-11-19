"""Unit tests for DQ types + YAML loader (DQ engine v1, Phase 1).

Covers ``core/dq/types.py`` (DqRule construction invariants) and
``core/dq/yaml_loader.py`` (parse + validate + cheap-eval lint).
The repository round-trip is tested separately in
``test_dq_repository.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    Container,
    DqRule,
    DqRuleSet,
    DqValidationError,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    RuleSource,
    RuleType,
    Severity,
    Tech,
    load_rules_from_yaml,
)
from lake_ingestion_framework.core.dq.yaml_loader import (
    lint_predicate_for_cheap_eval,
)

# ──────────────────────────────────────────────────────────────────────
# DqRule construction invariants
# ──────────────────────────────────────────────────────────────────────


def _rule(**overrides: object) -> DqRule:
    base: dict[str, object] = {
        "name": "test_rule",
        "dataset_name": "orders",
        "layer": Layer.SILVER,
        "predicate": "order_id IS NULL",
        "injection_point": InjectionPoint.POST_PROCESSOR,
        "severity": Severity.ERROR,
        "action": Action.STOP_PIPELINE,
    }
    base.update(overrides)
    return DqRule(**base)  # type: ignore[arg-type]


def test_rule_constructs_with_minimal_fields() -> None:
    rule = _rule()
    assert rule.name == "test_rule"
    assert rule.severity == Severity.ERROR
    assert rule.tech == Tech.SQL  # default
    assert rule.container == Container.DUCKDB  # default
    assert rule.rule_type == RuleType.PREDICATE  # default
    assert rule.source == RuleSource.USER_AUTHORED  # default


def test_rule_rejects_empty_name() -> None:
    with pytest.raises(DqValidationError, match="name must be non-empty"):
        _rule(name="")


def test_rule_rejects_empty_dataset_name() -> None:
    with pytest.raises(DqValidationError, match="dataset_name must be non-empty"):
        _rule(dataset_name="")


def test_rule_rejects_empty_predicate() -> None:
    with pytest.raises(DqValidationError, match="predicate must be non-empty"):
        _rule(predicate="   ")


def test_rule_rejects_quarantine_record_without_strategy() -> None:
    with pytest.raises(DqValidationError, match="action=quarantine_record requires"):
        _rule(action=Action.QUARANTINE_RECORD)


def test_rule_rejects_strategy_with_non_quarantine_action() -> None:
    """Strategy is meaningful only when action=quarantine_record."""
    with pytest.raises(DqValidationError, match="quarantine_strategy is only valid"):
        _rule(
            action=Action.STOP_PIPELINE,
            quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
        )


def test_rule_quarantine_record_with_strategy_works() -> None:
    rule = _rule(
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    assert rule.action == Action.QUARANTINE_RECORD
    assert rule.quarantine_strategy == QuarantineStrategy.SIDE_TABLE


def test_rule_rejects_negative_sample_cap_below_minus_one() -> None:
    with pytest.raises(DqValidationError, match="sample_cap must be >= -1"):
        _rule(sample_cap=-2)


def test_rule_accepts_minus_one_sample_cap_uncapped() -> None:
    rule = _rule(sample_cap=-1)
    assert rule.sample_cap == -1


def test_rule_accepts_zero_sample_cap_no_capture() -> None:
    rule = _rule(sample_cap=0)
    assert rule.sample_cap == 0


# ──────────────────────────────────────────────────────────────────────
# DqRuleSet invariants
# ──────────────────────────────────────────────────────────────────────


def test_rule_set_rejects_rule_with_mismatched_dataset() -> None:
    rule = _rule(dataset_name="orders")
    with pytest.raises(DqValidationError, match="doesn't match set's dataset_name"):
        DqRuleSet(dataset_name="customers", layer=Layer.SILVER, rules=(rule,))


def test_rule_set_rejects_rule_with_mismatched_layer() -> None:
    rule = _rule(layer=Layer.SILVER)
    with pytest.raises(DqValidationError, match="doesn't match set's layer"):
        DqRuleSet(dataset_name="orders", layer=Layer.GOLD, rules=(rule,))


def test_rule_set_rejects_duplicate_names() -> None:
    rule_a = _rule(name="dup")
    rule_b = _rule(name="dup")
    with pytest.raises(DqValidationError, match="Duplicate rule name"):
        DqRuleSet(dataset_name="orders", layer=Layer.SILVER, rules=(rule_a, rule_b))


# ──────────────────────────────────────────────────────────────────────
# YAML loader — happy path
# ──────────────────────────────────────────────────────────────────────


_VALID_YAML = """\
dataset: orders
layer: silver
rules:
  - name: order_id_not_null
    predicate: order_id IS NULL
    severity: error
    action: stop_pipeline
    injection_point: post_processor

  - name: amount_in_range
    predicate: amount < 0 OR amount > 100000
    severity: warning
    action: quarantine_record
    quarantine_strategy: side_table
    injection_point: post_processor
"""


def test_loads_valid_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "silver_orders.yaml"
    yaml_path.write_text(_VALID_YAML)
    rule_set, warnings = load_rules_from_yaml(yaml_path)

    assert rule_set.dataset_name == "orders"
    assert rule_set.layer == Layer.SILVER
    assert len(rule_set.rules) == 2
    assert rule_set.source_path == str(yaml_path)
    assert warnings == ()

    rule_a, rule_b = rule_set.rules
    assert rule_a.name == "order_id_not_null"
    assert rule_a.severity == Severity.ERROR
    assert rule_a.action == Action.STOP_PIPELINE
    assert rule_b.action == Action.QUARANTINE_RECORD
    assert rule_b.quarantine_strategy == QuarantineStrategy.SIDE_TABLE


def test_load_rules_propagates_dataset_and_layer_to_each_rule(tmp_path: Path) -> None:
    """Top-level dataset+layer become the rule's fields — no need to repeat."""
    yaml_path = tmp_path / "silver_orders.yaml"
    yaml_path.write_text(_VALID_YAML)
    rule_set, _ = load_rules_from_yaml(yaml_path)
    for rule in rule_set.rules:
        assert rule.dataset_name == "orders"
        assert rule.layer == Layer.SILVER


# ──────────────────────────────────────────────────────────────────────
# YAML loader — validation paths
# ──────────────────────────────────────────────────────────────────────


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(DqValidationError, match="not found"):
        load_rules_from_yaml(tmp_path / "nope.yaml")


def test_empty_file_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "empty.yaml"
    yaml_path.write_text("")
    with pytest.raises(DqValidationError, match="empty"):
        load_rules_from_yaml(yaml_path)


def test_non_mapping_root_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text("- 1\n- 2\n")
    with pytest.raises(DqValidationError, match="must be a YAML mapping"):
        load_rules_from_yaml(yaml_path)


def test_unknown_top_level_key_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(_VALID_YAML + "\nbogus_key: 1\n")
    with pytest.raises(DqValidationError, match="bogus_key"):
        load_rules_from_yaml(yaml_path)


def test_unknown_rule_key_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "    injection_point: post_processor\n",
            "    injection_point: post_processor\n    bogus_field: 1\n",
            1,
        )
    )
    with pytest.raises(DqValidationError, match="bogus_field"):
        load_rules_from_yaml(yaml_path)


def test_missing_rules_list_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text("dataset: orders\nlayer: silver\nrules: []\n")
    with pytest.raises(DqValidationError, match="must be non-empty"):
        load_rules_from_yaml(yaml_path)


def test_invalid_layer_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(_VALID_YAML.replace("layer: silver", "layer: muddy"))
    with pytest.raises(DqValidationError, match="not a valid Layer"):
        load_rules_from_yaml(yaml_path)


def test_invalid_action_raises(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(_VALID_YAML.replace("action: stop_pipeline", "action: explode"))
    with pytest.raises(DqValidationError, match="not a valid Action"):
        load_rules_from_yaml(yaml_path)


# ──────────────────────────────────────────────────────────────────────
# Predicate safety — the security-critical surface
# ──────────────────────────────────────────────────────────────────────


def test_predicate_with_semicolon_rejected(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: order_id IS NULL; SELECT 1",
        )
    )
    with pytest.raises(DqValidationError, match="must not contain ';'"):
        load_rules_from_yaml(yaml_path)


def test_predicate_with_line_comment_rejected(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: order_id IS NULL -- comment hides injection",
        )
    )
    with pytest.raises(DqValidationError, match="must not contain SQL comments"):
        load_rules_from_yaml(yaml_path)


def test_predicate_with_block_comment_rejected(tmp_path: Path) -> None:
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: order_id IS NULL /* block comment */",
        )
    )
    with pytest.raises(DqValidationError, match="must not contain SQL comments"):
        load_rules_from_yaml(yaml_path)


@pytest.mark.parametrize(
    "keyword",
    ["DROP", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "TRUNCATE", "GRANT"],
)
def test_predicate_with_ddl_or_dml_keyword_rejected(tmp_path: Path, keyword: str) -> None:
    """Keyword must trigger word-boundary match — using it as a free-
    standing token (not as a column name like ``drop_count``, where the
    underscore prevents the boundary)."""
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            f'predicate: "tx_count > 0 OR {keyword} flag_set"',
        )
    )
    with pytest.raises(DqValidationError, match=f"forbidden keyword '{keyword}'"):
        load_rules_from_yaml(yaml_path)


def test_predicate_with_keyword_as_identifier_prefix_is_allowed(tmp_path: Path) -> None:
    """Underscore-prefixed identifiers (``drop_count``, ``alter_ts``) are
    legitimate column names; the word-boundary regex correctly skips
    them. Verifies we don't false-positive."""
    yaml_path = tmp_path / "ok.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: drop_count > 0 OR alter_ts < update_ts",
        )
    )
    rule_set, _ = load_rules_from_yaml(yaml_path)
    assert "drop_count" in rule_set.rules[0].predicate


# ──────────────────────────────────────────────────────────────────────
# Cheap-eval linter — non-fatal warnings
# ──────────────────────────────────────────────────────────────────────


def test_count_star_predicate_warns(tmp_path: Path) -> None:
    yaml_path = tmp_path / "warn.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: COUNT(*) > 0",
        )
    )
    rule_set, warnings = load_rules_from_yaml(yaml_path)
    assert len(rule_set.rules) == 2  # rules still parse — warning is non-fatal
    assert any("COUNT(*)" in w.suggestion for w in warnings)
    assert any(w.rule_name == "order_id_not_null" for w in warnings)


def test_group_by_predicate_warns(tmp_path: Path) -> None:
    yaml_path = tmp_path / "warn.yaml"
    yaml_path.write_text(
        _VALID_YAML.replace(
            "predicate: order_id IS NULL",
            "predicate: 'cust_id IN (SELECT cust_id FROM o GROUP BY cust_id HAVING sum(x) < 0)'",
        )
    )
    _, warnings = load_rules_from_yaml(yaml_path)
    assert any("GROUP BY" in w.pattern or "GROUP BY" in w.suggestion for w in warnings)


def test_window_predicate_warns() -> None:
    findings = lint_predicate_for_cheap_eval(
        "rank_check",
        "ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) > 1",
    )
    assert any("Window" in w.suggestion or "OVER" in w.pattern for w in findings)


def test_cheap_predicate_no_warnings() -> None:
    findings = lint_predicate_for_cheap_eval(
        "ok",
        "amount < 0 OR (status = 'cancelled' AND total > 0)",
    )
    assert findings == ()
