"""A reference model of SCD Type 1 and Type 2 history: what silver must hold, worked out from the full set of changes alone.

It is deliberately small and slow-and-obvious. It is not used to build silver. It is what the property tests
(tests/dataplatform/test_silver_properties.py) and, later, the data twin's oracle compare silver with. Where the model and
the stages disagree, one of them is wrong, and the disagreement is the finding.

A change is (key, tracked value, sequence, op), op being I, U or D. A delete row carries the value its key had just before
it, as in the contract's cases (silver/history_contract.py). The model reads that contract as a rule:

  Type 2: order a key's changes by sequence. A change that is not a delete opens a version when its value differs from
  the current one. A delete never opens a version and never changes a value: it marks the version it falls in (a delete
  for a key with no version yet makes the deleted version from its own row). The last version is current. A version is
  deleted when its last change is a delete.
  Type 1: one row per key, from its change with the highest sequence; deleted when that change is a delete.

Decided by the owner on 24 Nov 2026: the same changes give the same silver however the feed is split into batches; a second
delete of an already deleted key does not make a second version; a delete never changes values (it marks the version it
falls in).
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
    """One pass per key, in sequence order. A change that is not a delete opens a version when its value differs from the
    current version's, and otherwise joins it. A delete never opens a version and never changes one's value: it marks the
    version it falls in. The one exception is a key's first change: a delete for a key with no version makes the deleted
    version from its own row. A version is deleted when its last change is a delete."""
    by_key: dict[str, list[Change]] = defaultdict(list)
    for c in _distinct(changes):
        by_key[c[0]].append(c)
    out: list[Version] = []
    for key in sorted(by_key):
        versions: list[list] = []  # [value, start sequence, last op]
        for _, value, seq, op in sorted(by_key[key], key=lambda c: c[2]):
            if op == "D":
                if not versions:
                    versions.append([value, seq, "D"])
                else:
                    versions[-1][2] = "D"
            elif versions and versions[-1][0] == value:
                versions[-1][2] = op
            else:
                versions.append([value, seq, op])
        out += [(key, v[0], v[1], i == len(versions) - 1, v[2] == "D") for i, v in enumerate(versions)]
    return out


def type1_rows(changes: Iterable[Change]) -> list[Version]:
    latest: dict[str, Change] = {}
    for c in _distinct(changes):
        if c[0] not in latest or c[2] > latest[c[0]][2]:
            latest[c[0]] = c
    return [(k, c[1], c[2], True, c[3] == "D") for k, c in sorted(latest.items())]
