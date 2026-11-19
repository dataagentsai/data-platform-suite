"""Catalog entries filled from the contracts and the manifest (DPC-0075; SERV-01, SEC-01, CAT-01).

desired_entries() says what the catalog must hold; plan_sync() compares that with what it holds (read through the
CatalogState port) and returns the smallest list of actions that closes the gap. Only tags in the tag standard are
ever changed or removed: tags someone else set are left alone. Comments are written where the contract has text and
never cleared where it has none.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from dataplatform.governance.port import Entry, ObjectRef
from dataplatform.governance.tables import MANAGED_TAGS, TERM_SEPARATOR, TableSpec

OPS = ("comment", "set_tags", "unset_tags")
SYNCED_LAYERS = ("silver", "gold")  # the layers with contracts; others are never touched


@dataclass(frozen=True)
class Action:
    op: str  # comment | set_tags | unset_tags
    ref: ObjectRef
    kind: str  # table | view | metric_view: the statement differs
    comment: str = ""
    tags: tuple[tuple[str, str], ...] = ()
    keys: tuple[str, ...] = ()


def _table_tags(t: TableSpec) -> dict[str, str]:
    tags = {"domain": t.domain, "layer": t.layer, "owner": t.owner}
    optional = {
        "data_product": t.data_product,
        # shiftleft: allow SL-COST-CODE-14 -- str.join on a tuple of names, not a DataFrame join
        "used_by": TERM_SEPARATOR.join(t.used_by),
        "contract": t.contract,
        "certification": t.certification,
        # shiftleft: allow SL-COST-CODE-14 -- str.join on a tuple of names, not a DataFrame join
        "glossary_terms": TERM_SEPARATOR.join(t.glossary_terms),
    }
    tags.update({k: v for k, v in optional.items() if v})
    return tags


def desired_entries(tables: Iterable[TableSpec]) -> dict[ObjectRef, Entry]:
    out: dict[ObjectRef, Entry] = {}
    for t in tables:
        out[ObjectRef(t.layer, t.schema, t.name)] = Entry(
            t.kind, t.description or None, tuple(sorted(_table_tags(t).items()))
        )
        for c in t.columns:
            tags = {"classification": c.classification}
            if c.tokenised:
                tags["protection"] = "tokenised"
            elif c.protected:
                tags["protection"] = "masked"
            out[ObjectRef(t.layer, t.schema, t.name, c.name)] = Entry(
                t.kind, c.description or None, tuple(sorted(tags.items()))
            )
    return out


def _diff(ref: ObjectRef, want: Entry, have: Entry | None) -> list[Action]:
    actions = []
    if want.comment is not None and (have is None or have.comment != want.comment):
        actions.append(Action("comment", ref, want.kind, comment=want.comment))
    current = have.tag_map() if have else {}
    to_set = tuple((k, v) for k, v in want.tags if current.get(k) != v)
    if to_set:
        actions.append(Action("set_tags", ref, want.kind, tags=to_set))
    wanted = want.tag_map()
    stale = tuple(sorted(k for k in current if k in MANAGED_TAGS and k not in wanted))
    if stale:
        actions.append(Action("unset_tags", ref, want.kind, keys=stale))
    return actions


def plan_sync(desired: Mapping[ObjectRef, Entry], current: Mapping[ObjectRef, Entry]) -> list[Action]:
    """Actions that make the catalog match the metadata, in a stable order (tables before their columns). Objects the
    metadata does not declare lose only our tags (a dropped contract must not leave its owner tag behind)."""
    actions: list[Action] = []
    for ref in sorted(set(desired) | set(current), key=lambda r: (r.layer, r.schema, r.name, r.column or "")):
        if ref.layer not in SYNCED_LAYERS:
            continue
        want, have = desired.get(ref), current.get(ref)
        if want is not None:
            actions += _diff(ref, want, have)
        elif have is not None:
            stale = tuple(sorted(k for k, _ in have.tags if k in MANAGED_TAGS))
            if stale:
                actions.append(Action("unset_tags", ref, have.kind, keys=stale))
    return actions


def apply(current: Mapping[ObjectRef, Entry], actions: Iterable[Action]) -> dict[ObjectRef, Entry]:
    """What the catalog holds after the actions run: the realm's statements must have exactly this effect.
    Used by tests and by the memory adapter; also lets a plan be checked to converge (apply then plan is empty)."""
    out = dict(current)
    for a in actions:
        have = out.get(a.ref, Entry(a.kind, None, ()))
        tags = have.tag_map()
        if a.op == "comment":
            have = Entry(have.kind, a.comment, have.tags)
        elif a.op == "set_tags":
            tags.update(dict(a.tags))
            have = Entry(have.kind, have.comment, tuple(sorted(tags.items())))
        elif a.op == "unset_tags":
            have = Entry(have.kind, have.comment, tuple(sorted((k, v) for k, v in tags.items() if k not in a.keys)))
        else:
            raise ValueError(f"unknown catalog action '{a.op}'")
        out[a.ref] = have
    return out
