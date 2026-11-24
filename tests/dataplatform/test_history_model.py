"""The reference model (silver/history_model.py) must agree with every case the contract and the hand-written history tests
already state. If it did not, the property tests would be judging silver against a wrong rule.

Needs no Spark: the model is plain Python. The cases come from silver/history_contract.py and from the unmarked cases of
test_history_type2_more (the ones marked as known bugs describe what silver should do, so they are checked too).
"""

import pytest
from test_silver_stages import SCD2_MORE

from dataplatform.silver.history_contract import CASES
from dataplatform.silver.history_model import type1_rows, type2_versions


def _changes(batches):
    return [(k, v, seq, op) for batch in batches for k, v, _note, seq, op, _minute, _pos in batch]


def _by_str(items):
    return sorted(items, key=lambda t: tuple((x is not None, str(x)) for x in t))


@pytest.mark.parametrize(("case", "batches", "want"), CASES, ids=[c[0][:50] for c in CASES])
def test_the_model_matches_the_contract(case, batches, want):
    assert _by_str(type2_versions(_changes(batches))) == _by_str(want), case


MORE = [(p.id, *p.values) for p in SCD2_MORE]  # the values only: the known-bug marks belong to silver, not to the model


@pytest.mark.parametrize(("pid", "case", "batches", "want"), MORE, ids=[m[0] for m in MORE])
def test_the_model_matches_the_extra_cases_including_the_known_bugs(pid, case, batches, want):
    """The known-bug cases state what silver SHOULD hold; the model must say the same, or it is the model that is wrong."""
    assert _by_str(type2_versions(_changes(batches))) == _by_str(want), case


@pytest.mark.parametrize(
    ("case", "changes", "want"),
    [
        ("the latest change wins", [("a", 1, 1, "I"), ("a", 2, 2, "U")], [("a", 2, 2, True, False)]),
        ("an older change arriving late does not overwrite", [("a", 2, 2, "U"), ("a", 1, 1, "I")], [("a", 2, 2, True, False)]),
        ("a delete is soft", [("a", 1, 1, "I"), ("a", 1, 2, "D")], [("a", 1, 2, True, True)]),
        ("the same change twice is one", [("a", 1, 1, "I"), ("a", 1, 1, "I")], [("a", 1, 1, True, False)]),
        ("two keys", [("b", 1, 1, "I"), ("a", 2, 2, "I")], [("a", 2, 2, True, False), ("b", 1, 1, True, False)]),
    ],
)
def test_type1_model(case, changes, want):
    assert type1_rows(changes) == want, case
