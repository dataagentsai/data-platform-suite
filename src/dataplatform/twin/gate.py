"""The release gate (G10, TEST-03): run a change on clones of prod, compare against the oracle and prod, pass or block.

    clones  each table the change touches is cloned from prod (a shallow clone: metadata only, no copy of data),
            the change runs against the clones, and the clones are dropped whatever happens.
    oracle  the clone's measures (rows, sums of the manifest's measure columns) must equal the world's oracle within
            the manifest's tolerance. A miss blocks.
    prod    every measure and the key set must equal prod's, unless the change declares that it changes them. An
            undeclared difference blocks; a declared change that changed nothing is a warning.
    windows (DIFF-01) when the table has a window column, rows, measure totals and keys are compared per window too,
            so a change that moves one dealer's rows between days shows even when the totals hold.
    explain (DIFF-01) every difference must be explained before release: an explanation for a measure, "keys" or
            "window:<w>" turns the matching findings into notes that carry it, and they stop blocking.

`compare` is pure; `run_gate` drives the two ports below. Adapters: adapters/memory_tables.py (tests) and
adapters/databricks_clone.py (shallow clones on the engine; its contract rows are pending until a workspace is bound).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

VERDICTS = ("pass", "block")
_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


class GateError(ValueError):
    pass


@dataclass(frozen=True)
class TableMeasures:
    table: str
    rows: int
    sums: dict[str, float]
    keys_digest: str  # same keys <-> same digest (within one adapter)
    windows: dict[str, TableMeasures] = field(default_factory=dict)  # per window value, when the spec names one


@dataclass(frozen=True)
class TableSpec:
    """A table the change touches: its prod name, business keys and the measure columns to sum."""

    name: str
    keys: tuple[str, ...]
    measures: tuple[str, ...] = ()
    tolerance: dict[str, float] = field(default_factory=dict)  # measure ("rows" or a column) -> absolute tolerance
    window: str = ""  # the window column: measures are compared per window too (DIFF-01)


@dataclass(frozen=True)
class Finding:
    table: str
    measure: str
    against: str  # oracle | prod
    expected: float | str
    got: float | str
    blocking: bool
    note: str


@dataclass(frozen=True)
class GateResult:
    verdict: str
    findings: tuple[Finding, ...]
    clones: tuple[str, ...]

    @property
    # shiftleft: allow SL-TEST-02 -- a property: test_twin_gate reads result.blocking, which the scan counts as no call
    def blocking(self) -> list[Finding]:
        return [f for f in self.findings if f.blocking]


class TableCloner(Protocol):
    def clone(self, source: str, target: str) -> None:
        """A shallow clone of `source` named `target` (replacing any older clone of that name)."""
        ...

    def drop(self, table: str) -> None: ...


class TableMeasurer(Protocol):
    def measure(self, table: str, keys: tuple[str, ...], columns: tuple[str, ...],
                window: str = "") -> TableMeasures:
        """Rows, measure totals and a key digest; with `window`, the same per window value in `windows`."""
        ...


def spec_from_gold(manifest: dict[str, Any], schema: str = "") -> TableSpec:
    """A gold fact's gate spec from its manifest (W1d): the grain as keys, the reconcile measures' columns as the
    measures to sum, and their tolerance (W3's note: the gate's TableSpec comes from the manifest)."""
    rec: dict[str, Any] = (manifest.get("checks") or {}).get("reconcile") or {}
    measures = list(rec.get("measures") or [])
    by_name = {str(m["name"]): str(m["column"]) for m in measures}
    tol = {by_name.get(k, k): float(v) for k, v in (rec.get("tolerance") or {}).items()}
    name = f"{schema or 'gold_' + str(manifest['domain'])}.{manifest['name']}"
    window = str((manifest.get("window") or {}).get("column") or "")
    return TableSpec(check_name(name), tuple(str(g) for g in manifest.get("grain") or []),
                     tuple(by_name.values()), tol, window)


def check_name(name: str) -> str:
    if not _TABLE.match(name):
        raise GateError(f"not a plain table name: {name!r}")
    return name


def clone_name(table: str, change_id: str) -> str:
    """The clone sits beside the prod table, named for the change, so two gates never share a clone."""
    tag = re.sub(r"[^a-z0-9]", "_", change_id.lower())[:40]
    *schema, name = check_name(table).split(".")
    return ".".join([*schema, f"{name}__gate_{tag}"])


def _close(a: float, b: float, tol: float) -> bool:
    return abs(a - b) <= tol + 1e-9


def compare(spec: TableSpec, candidate: TableMeasures, prod: TableMeasures, oracle: dict[str, float] | None,
            declared: set[str], *, explained: dict[str, str] | None = None) -> list[Finding]:
    """Findings for one table. `declared` names the measures (and "keys") the change says it changes; `explained`
    maps a measure, "keys" or "window:<w>" to the reason a difference is expected (DIFF-01)."""
    out = _compare_table(spec, candidate, prod, oracle, declared)
    out += _compare_windows(spec, candidate, prod, declared)
    return [_explain(f, explained or {}) for f in out]


def _explain(f: Finding, explained: dict[str, str]) -> Finding:
    if not f.blocking or f.against != "prod":
        return f
    window, _, measure = f.measure.partition(": ") if f.measure.startswith("window ") else ("", "", f.measure)
    why = explained.get(f"window:{window[len('window '):]}") if window else None
    why = why or explained.get(measure)
    if not why:
        return f
    return Finding(f.table, f.measure, f.against, f.expected, f.got, False, f"explained: {why}")


def _compare_windows(spec: TableSpec, candidate: TableMeasures, prod: TableMeasures,
                     declared: set[str]) -> list[Finding]:
    if not spec.window:
        return []
    out = []
    empty = TableMeasures("", 0, {}, "")
    for w in sorted(set(candidate.windows) | set(prod.windows)):
        c, p = candidate.windows.get(w, empty), prod.windows.get(w, empty)
        got, was = {"rows": float(c.rows), **c.sums}, {"rows": float(p.rows), **p.sums}
        for m in sorted(set(got) | set(was)):
            if m not in declared and not _close(was.get(m, 0.0), got.get(m, 0.0), spec.tolerance.get(m, 0.0)):
                out.append(Finding(spec.name, f"window {w}: {m}", "prod", was.get(m, 0.0), got.get(m, 0.0), True,
                                   "this window changed, and the change does not declare it"))
        if c.keys_digest != p.keys_digest and "keys" not in declared:
            out.append(Finding(spec.name, f"window {w}: keys", "prod", p.keys_digest, c.keys_digest, True,
                               "the keys in this window changed, and the change does not declare it"))
    return out


def _compare_table(spec: TableSpec, candidate: TableMeasures, prod: TableMeasures, oracle: dict[str, float] | None,
                   declared: set[str]) -> list[Finding]:
    out = []
    got = {"rows": float(candidate.rows), **candidate.sums}
    was = {"rows": float(prod.rows), **prod.sums}
    for measure, expected in sorted((oracle or {}).items()):
        tol = spec.tolerance.get(measure, 0.0)
        if measure not in got:
            out.append(Finding(spec.name, measure, "oracle", expected, "missing", True, "the oracle names a measure "
                               "the table does not have"))
        elif not _close(got[measure], expected, tol):
            out.append(Finding(spec.name, measure, "oracle", expected, got[measure], True,
                               f"disagrees with the world's oracle (tolerance {tol})"))
    for measure in sorted(set(got) | set(was)):
        a, b = was.get(measure), got.get(measure)
        same = a is not None and b is not None and _close(a, b, spec.tolerance.get(measure, 0.0))
        if not same and measure not in declared:
            out.append(Finding(spec.name, measure, "prod", a if a is not None else "missing",
                               b if b is not None else "missing", True, "changed, and the change does not declare it"))
        elif same and measure in declared:
            out.append(Finding(spec.name, measure, "prod", a if a is not None else "", b if b is not None else "",
                               False, "declared as changed, but nothing changed"))
    if candidate.keys_digest != prod.keys_digest and "keys" not in declared:
        out.append(Finding(spec.name, "keys", "prod", prod.keys_digest, candidate.keys_digest, True,
                           "the set of business keys changed, and the change does not declare it"))
    return out


Change = Callable[[dict[str, str]], None]  # receives {prod table: clone table}; runs the change on the clones


def run_gate(change_id: str, tables: list[TableSpec], change: Change, cloner: TableCloner, measurer: TableMeasurer,
             oracle: dict[str, dict[str, float]] | None = None,
             declared: dict[str, set[str]] | None = None, *,
             explained: dict[str, dict[str, str]] | None = None) -> GateResult:
    clones = {t.name: clone_name(t.name, change_id) for t in tables}
    findings: list[Finding] = []
    try:
        for t in tables:
            cloner.clone(t.name, clones[t.name])
        change(dict(clones))
        for t in tables:
            cand = measurer.measure(clones[t.name], t.keys, t.measures, t.window)
            prod = measurer.measure(t.name, t.keys, t.measures, t.window)
            findings += compare(t, cand, prod, (oracle or {}).get(t.name), (declared or {}).get(t.name, set()),
                                explained=(explained or {}).get(t.name))
    finally:
        for name in clones.values():
            cloner.drop(name)
    verdict = "block" if any(f.blocking for f in findings) else "pass"
    return GateResult(verdict, tuple(findings), tuple(clones.values()))
