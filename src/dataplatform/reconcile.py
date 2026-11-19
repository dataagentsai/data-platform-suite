"""Reconciliation between hops (DPC-0015, DPC-0016): the same measure, counted at each hop, must agree."""
from __future__ import annotations

from dataplatform.model import CheckResult


def compare(run_id: str, day: int, measures: dict[str, dict[str, float]], tolerance: dict[str, float],
            obligation: str = "REC-01") -> list[CheckResult]:
    """measures = {hop: {measure: value}}, hops in spine order. Each hop is compared with the first hop.

    A hop with no value for a measure fails: "nothing to compare" is never a pass (GATE-01).
    """
    hops = list(measures)
    base = hops[0]
    out = []
    for hop in hops[1:]:
        for m, expected in measures[base].items():
            observed = measures[hop].get(m)
            tol = tolerance.get(m, 0)
            ok = observed is not None and abs(observed - expected) <= tol
            out.append(CheckResult(run_id, day, f"reconcile.{m}.{base}->{hop}", obligation, ok, expected, observed,
                                   "" if ok else ("no value at this hop" if observed is None else
                                                  f"differs by {observed - expected} (tolerance {tol})")))
    return out


# ------------------------------------------------------------------------------------- live source vs silver (B9)
SEP = "\x1f"


def bucket_of(key_values: tuple[object, ...], buckets: int) -> int:
    """The bucket of a business key: the first 8 hex digits of sha256 over the key's text parts, mod `buckets`. The
    engine side computes the same (sha2 over concat_ws of the parts cast to text), so both sides agree on buckets."""
    import hashlib  # noqa: PLC0415 -- keeps the module's import light for compare()

    # shiftleft: allow SL-COST-CODE-14 -- str.join of key parts, not a DataFrame join
    text = SEP.join("" if v is None else str(v) for v in key_values)
    return int(hashlib.sha256(text.encode()).hexdigest()[:8], 16) % buckets


def bucket_totals(rows: list[dict[str, object]], keys: tuple[str, ...], measures: dict[str, str | None],
                  buckets: int = 16) -> dict[int, dict[str, float]]:
    """{bucket: {measure: total}} for rows held in memory (a source's current rows). `measures` maps a measure name
    to the column it sums, or None for a row count."""
    out: dict[int, dict[str, float]] = {}
    for r in rows:
        b = out.setdefault(bucket_of(tuple(r.get(k) for k in keys), buckets), {m: 0.0 for m in measures})
        for m, col in measures.items():
            b[m] += 1.0 if col is None else float(r.get(col) or 0)  # type: ignore[arg-type]
    return out


def compare_buckets(left: dict[int, dict[str, float]], right: dict[int, dict[str, float]],
                    tolerance: dict[str, float] | None = None) -> list[tuple[int, str, float, float]]:
    """(bucket, measure, left, right) for every bucket and measure that differ beyond the tolerance (REC-05: the
    finding names where it drifted, not only that it did). A bucket on one side only counts as zero on the other."""
    tol = tolerance or {}
    measures = sorted({m for side in (left, right) for b in side.values() for m in b})
    out = []
    for bucket in sorted(set(left) | set(right)):
        for m in measures:
            a, b = left.get(bucket, {}).get(m, 0.0), right.get(bucket, {}).get(m, 0.0)
            if abs(a - b) > tol.get(m, 0.0):
                out.append((bucket, m, a, b))
    return out
