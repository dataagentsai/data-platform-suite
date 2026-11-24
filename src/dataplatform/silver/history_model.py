"""A reference model of SCD Type 1 and Type 2 history: what silver must hold, worked out from the full set of changes alone.

It is deliberately small and slow-and-obvious. It is not used to build silver. It is what the property tests
(tests/dataplatform/test_silver_properties.py) and, later, the data twin's oracle compare silver with. Where the model and
the stages disagree, one of them is wrong, and the disagreement is the finding.

A change is (key, tracked value, sequence, op), op being I, U or D. A delete row carries the value its key had just before
it, as in the contract's cases (silver/history_contract.py). The model reads that contract as a rule:

  Type 2: order a key's changes by sequence. A new version opens when the tracked value differs from the one before. A
  version starts at its first change. The last version is current. A version is deleted when its last change is a delete.
  Type 1: one row per key, from its change with the highest sequence; deleted when that change is a delete.

Open questions the contract does not answer (so the model's choice is not a ruling): what a second delete of an
already deleted key means, and what a delete whose value differs from the stored version means.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

Change = tuple[str, int, int, str]  # key, tracked value, sequence, op
Version = tuple[str, int, int, bool, bool]  # key, value, sequence it starts at, is current, is deleted


def _distinct(changes: Iterable[Change]) -> list[Change]:
    """The same change delivered twice is one change."""
    return sorted(set(changes), key=lambda c: (c[0], c[2], c[3]))


def type2_versions(changes: Iterable[Change]) -> list[Version]:
    by_key: dict[str, list[Change]] = defaultdict(list)
    for c in _distinct(changes):
        by_key[c[0]].append(c)
    out: list[Version] = []
    for key in sorted(by_key):
        runs: list[list[Change]] = []
        for c in sorted(by_key[key], key=lambda c: c[2]):
            if runs and runs[-1][0][1] == c[1]:
                runs[-1].append(c)
            else:
                runs.append([c])
        out += [(key, r[0][1], r[0][2], i == len(runs) - 1, r[-1][3] == "D") for i, r in enumerate(runs)]
    return out


def type1_rows(changes: Iterable[Change]) -> list[Version]:
    latest: dict[str, Change] = {}
    for c in _distinct(changes):
        if c[0] not in latest or c[2] > latest[c[0]][2]:
            latest[c[0]] = c
    return [(k, c[1], c[2], True, c[3] == "D") for k, c in sorted(latest.items())]
