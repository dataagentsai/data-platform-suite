"""DAC coverage (governance/coverage.py, B20 C20). Table-driven."""

from datetime import date

import pytest

from dataplatform.governance.coverage import Rule, coverage, left, rules_from, summary, valid_waiver

TODAY = date(2026, 11, 16)
RULES = [Rule("DEL-01", "deletes", "X6", "MUST"), Rule("DEL-04", "mass delete", "X6", "MUST"),
         Rule("PERF-05", "file size", "X19", "SHOULD"), Rule("SEC-03", "forget", "X12", "MUST")]
OK_WAIVER = {"id": "SEC-03", "reason": "no personal data deletion path on the demo", "approved_by": "owner",
             "until": "2027-01-31"}


@pytest.mark.parametrize(("case", "tests", "code", "waivers", "want"), [
    ("cited in a test is tested", {"t.py": "# DEL-01 proven here"}, {}, [], {"DEL-01": "tested"}),
    ("cited only in code is built", {}, {"c.py": "DEL-04 guard"}, [], {"DEL-04": "built"}),
    ("a longer id does not count for a shorter one", {"t.py": "DEL-011 and XDEL-01"}, {}, [], {"DEL-01": "none"}),
    ("a valid waiver counts", {}, {}, [OK_WAIVER], {"SEC-03": "waived"}),
    ("an expired waiver does not", {}, {}, [{**OK_WAIVER, "until": "2026-01-01"}], {"SEC-03": "none"}),
    ("a waiver without an approver does not", {}, {}, [{**OK_WAIVER, "approved_by": " "}], {"SEC-03": "none"}),
    ("a test beats a waiver", {"t.py": "SEC-03"}, {}, [OK_WAIVER], {"SEC-03": "tested"}),
])
def test_coverage(case, tests, code, waivers, want):
    got = {c.rule.id: c.state for c in coverage(RULES, tests, code, waivers, TODAY)}
    assert {k: got[k] for k in want} == want, case


def test_summary_and_what_is_left_must_first():
    covered = coverage(RULES, {"t.py": "DEL-01"}, {"c.py": "PERF-05 DEL-04"}, [], TODAY)
    assert summary(covered) == {"tested": 1, "waived": 0, "built": 2, "none": 1}
    assert [c.rule.id for c in left(covered)] == ["DEL-04", "SEC-03", "PERF-05"]


def test_rules_from_catalog_files():
    docs = [[{"id": "A-01", "title": "a", "concern": "X1", "level": "MUST"}], None]
    assert rules_from(docs) == [Rule("A-01", "a", "X1", "MUST")]


def test_valid_waiver_needs_a_real_reason():
    assert not valid_waiver({**OK_WAIVER, "reason": "because"}, TODAY)
