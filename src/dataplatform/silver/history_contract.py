"""The SCD Type 2 contract every bronze-to-silver engine must pass (ADR-035, DAC ENG-02). Pure data, product-free.

Each case: the change batches in arrival order, then what silver must mean after all of them, one row per version as
(key, value, sequence the version starts at, is current, is deleted). Engines store this differently (ours keeps a
deleted version current and flagged; a managed change-capture engine closes it), so each engine's output is first
normalised to this shape (`normalise`), then compared.

A change row: (key, value, note, sequence, op, committed minute, feed position). `value` is tracked (a change opens a
version); `note` is not (a change updates the version in place).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

Change = tuple[str, int, str, int, str, int, int]
Version = tuple[str, int, int, bool, bool]

CASES: list[tuple[str, list[list[Change]], list[Version]]] = [
    ("first load: one current version per key",
     [[("a", 1, "x", 1, "I", 0, 1), ("b", 5, "x", 2, "I", 0, 2)]],
     [("a", 1, 1, True, False), ("b", 5, 2, True, False)]),
    ("a tracked change opens a version",
     [[("a", 1, "x", 1, "I", 0, 1)], [("a", 2, "x", 3, "U", 5, 2)]],
     [("a", 1, 1, False, False), ("a", 2, 3, True, False)]),
    ("an untracked change updates the version in place",
     [[("a", 1, "x", 1, "I", 0, 1)], [("a", 1, "y", 3, "U", 5, 2)]],
     [("a", 1, 1, True, False)]),
    ("several changes in one batch become several versions, in order",
     [[("a", 1, "x", 1, "I", 0, 1), ("a", 3, "x", 5, "U", 9, 3), ("a", 2, "x", 3, "U", 5, 2)]],
     [("a", 1, 1, False, False), ("a", 2, 3, False, False), ("a", 3, 5, True, False)]),
    ("an older change arriving after a newer one goes into history, the newer stays current (SCD-02)",
     [[("a", 1, "x", 1, "I", 0, 1)], [("a", 3, "x", 5, "U", 5, 2)], [("a", 2, "x", 4, "U", 65, 3)]],
     [("a", 1, 1, False, False), ("a", 2, 4, False, False), ("a", 3, 5, True, False)]),
    ("a late change equal to the version after it moves that version's start",
     [[("a", 1, "x", 1, "I", 0, 1)], [("a", 3, "x", 5, "U", 5, 2)], [("a", 3, "x", 4, "U", 4, 3)]],
     [("a", 1, 1, False, False), ("a", 3, 4, True, False)]),
    ("a delete marks the key deleted, nothing removed (DEL-01)",
     [[("a", 1, "x", 1, "I", 0, 1)], [("a", 1, "x", 2, "D", 5, 2)]],
     [("a", 1, 1, True, True)]),
    ("the same batch twice changes nothing",
     [[("a", 1, "x", 1, "I", 0, 1), ("a", 2, "x", 2, "U", 1, 2)],
      [("a", 1, "x", 1, "I", 0, 1), ("a", 2, "x", 2, "U", 1, 2)]],
     [("a", 1, 1, False, False), ("a", 2, 2, True, False)]),
]


def slug(case: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in case.lower())[:40].strip("_")


def normalise_closed(rows: Iterable[Mapping[str, Any]], key: str, value: str, start: str, end: str) -> list[Version]:
    """An engine that closes a deleted key's last version (end set, nothing after it): the last version of a key is
    'current'; it is 'deleted' when it is closed."""
    by_key: dict[Any, list[Mapping[str, Any]]] = {}
    for r in rows:
        by_key.setdefault(r[key], []).append(r)
    out: list[Version] = []
    for k in sorted(by_key):
        vs = sorted(by_key[k], key=lambda r: int(r[start]))
        for i, r in enumerate(vs):
            last = i == len(vs) - 1
            out.append((str(k), int(r[value]), int(r[start]), last, last and r[end] is not None))
    return out
