"""The oracle: what silver must hold, computed from the world's own record, never from the platform's logs."""
from __future__ import annotations

from typing import Any, Iterable

from dataplatform.model import CheckResult
from dataplatform.world.shop import Change


def expected_current(changes: Iterable[Change], rejected_files: set[str] = frozenset()) -> dict[str, dict[str, Any]]:
    """Current orders after applying every change the platform should have accepted, newest change sequence wins.

    Changes in a delivery the contract rightly stopped (a renamed column) are not expected in silver until the
    source resends them; `rejected_files` names those deliveries.
    """
    latest: dict[str, dict[str, Any]] = {}
    for c in changes:
        if c.delivery in rejected_files:
            continue
        r = c.row
        if r["order_id"] not in latest or r["change_seq"] > latest[r["order_id"]]["change_seq"]:
            latest[r["order_id"]] = r
    return {k: v for k, v in latest.items() if v["op"] != "D"}


def judge_silver(run_id: str, day: int, expected: dict[str, dict[str, Any]],
                 silver_current: list[dict[str, Any]]) -> list[CheckResult]:
    """oracle silver-agrees-with-world: count, keys, amount total and status per order."""
    got = {r["order_id"]: r for r in silver_current}
    exp_total = round(sum(r["amount"] for r in expected.values()), 2)
    got_total = round(sum(float(r["amount"]) for r in got.values()), 2)
    missing, extra = sorted(set(expected) - set(got)), sorted(set(got) - set(expected))
    wrong_status = sorted(k for k in set(expected) & set(got) if expected[k]["status"] != got[k]["status"])
    name = "oracle.silver-agrees-with-world"
    return [
        CheckResult(run_id, day, f"{name}.count", "DUP-01", len(got) == len(expected), len(expected), len(got)),
        CheckResult(run_id, day, f"{name}.keys", "DEL-01", not missing and not extra, 0, len(missing) + len(extra),
                    f"missing {missing[:5]} extra {extra[:5]}" if missing or extra else ""),
        CheckResult(run_id, day, f"{name}.amount_total", "REC-01", abs(got_total - exp_total) < 0.005,
                    exp_total, got_total),
        CheckResult(run_id, day, f"{name}.status", "SCD-02", not wrong_status, 0, len(wrong_status),
                    f"wrong status for {wrong_status[:5]}" if wrong_status else ""),
    ]
