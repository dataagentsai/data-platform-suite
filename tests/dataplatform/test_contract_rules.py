"""Contract row rules reach the quality stage (silver/contract_rules.py; DAC DQ-01, ADR-009)."""

import pytest

from dataplatform.meta.port import DqRule
from dataplatform.silver.contract_rules import contract_rules, translate, with_overrides
from dataplatform.silver.plan import Rule

NEG = DqRule("ordered_qty-sql-1", "ordered_qty", "sql",
             {"type": "sql", "query": "SELECT COUNT(*) FROM ${object} WHERE ordered_qty < 0", "mustBe": 0},
             "critical", "quarantine")
PLANT = DqRule("plant_code-invalidValues-1", "plant_code", "invalidValues",
               {"type": "library", "metric": "invalidValues", "arguments": {"validValues": ["P-CHN", "P-HUN"]},
                "mustBe": 0}, "major", "quarantine")
REJECT = DqRule("rejection_reason-sql-1", "rejection_reason", "sql",
                {"query": "SELECT COUNT(*) FROM ${object} WHERE (line_status = 'rejected') <> (rejection_reason IS NOT "
                          "NULL)", "mustBe": 0}, "major", "quarantine")


@pytest.mark.parametrize(
    ("case", "rule", "want"),
    [
        ("sql count-where becomes a row condition", NEG,
         Rule("ordered_qty-sql-1", "coalesce(NOT (ordered_qty < 0), true)", "critical", "quarantine")),
        ("critical 'stop' quarantines the rows (ADR-009)",
         DqRule("q-sql-1", "q", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE q < 0", "mustBe": 0},
                "critical", "stop"),
         Rule("q-sql-1", "coalesce(NOT (q < 0), true)", "critical", "quarantine")),
        ("minor 'warn' flags the rows",
         DqRule("q-sql-2", "q", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE q > 9", "mustBe": 0},
                "minor", "warn"),
         Rule("q-sql-2", "coalesce(NOT (q > 9), true)", "minor", "flag")),
        ("valid values become an IN list, nulls left to the required rule", PLANT,
         Rule("plant_code-invalidValues-1", "plant_code IS NULL OR plant_code IN ('P-CHN', 'P-HUN')", "major",
              "quarantine")),
        ("a condition across columns", REJECT,
         Rule("rejection_reason-sql-1",
              "coalesce(NOT ((line_status = 'rejected') <> (rejection_reason IS NOT NULL)), true)", "major",
              "quarantine")),
        ("a quote in a valid value is escaped",
         DqRule("c-invalidValues-1", "c", "invalidValues", {"arguments": {"validValues": ["o'k"]}, "mustBe": 0}),
         Rule("c-invalidValues-1", "c IS NULL OR c IN ('o''k')", "major", "quarantine")),
    ],
)
def test_translated(case, rule, want):
    assert translate(rule) == want


@pytest.mark.parametrize(
    ("case", "rule", "says"),
    [
        ("table duplicates: kept by the duplicates stage", DqRule("table-duplicateValues-1", "", "duplicateValues"),
         "duplicates stage"),
        ("table row count: kept by the lander", DqRule("table-rowCount-2", "", "rowCount"), "volume check"),
        ("a subquery is not a row condition",
         DqRule("x-sql-1", "x", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE x IN (SELECT y FROM t)",
                                         "mustBe": 0}), "not a plain row condition"),
        ("a statement separator is refused",
         DqRule("x-sql-2", "x", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE x < 0; DROP TABLE t",
                                         "mustBe": 0}), "not a plain row condition"),
        ("a count that must not be zero is not a row rule",
         DqRule("x-sql-3", "x", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE x < 0", "mustBe": 3}),
         "not a row rule"),
        ("an unknown check is reported", DqRule("x-range-1", "x", "range", {}), "not a row rule"),
    ],
)
def test_not_translated_says_why(case, rule, says):
    got = translate(rule)
    assert isinstance(got, str)
    assert says in got


def test_contract_rules_splits_rules_and_reasons():
    rules, reasons = contract_rules([NEG, PLANT, DqRule("table-rowCount-1", "", "rowCount")])
    assert [r.name for r in rules] == ["ordered_qty-sql-1", "plant_code-invalidValues-1"]
    assert len(reasons) == 1


RULES = (Rule("c", "x IS NOT NULL", "critical", "quarantine"), Rule("m", "y > 0", "major", "quarantine"),
         Rule("n", "z > 0", "minor", "flag"))


@pytest.mark.parametrize(
    ("case", "overrides", "want"),
    [
        ("no override: the store's actions stand", {}, ["quarantine", "quarantine", "flag"]),
        ("major flagged by the dataset", {"major": "flag"}, ["quarantine", "flag", "flag"]),
        ("minor made to quarantine", {"minor": "quarantine"}, ["quarantine", "quarantine", "quarantine"]),
        ("critical flagged by the dataset", {"critical": "flag"}, ["flag", "quarantine", "flag"]),
        ("a severity no rule has changes nothing", {"blocker": "none"}, ["quarantine", "quarantine", "flag"]),
        ("several at once", {"major": "none", "minor": "none"}, ["quarantine", "none", "none"]),
    ],
)
def test_the_datasets_quality_actions_lie_over_the_contracts_rules(case, overrides, want):
    assert [r.action for r in with_overrides(RULES, overrides)] == want, case
