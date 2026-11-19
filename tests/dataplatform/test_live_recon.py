"""Live source against silver (silver/live_recon.py, REC-05): buckets first, then the exact keys in differing
buckets."""

import pytest

from dataplatform.reconcile import bucket_of, bucket_totals
from dataplatform.silver.live_recon import reconcile_live
from dataplatform.silver.plan import SilverPlan

PLAN = SilverPlan(source="erp", dataset="delivery", columns=(), keys=("delivery_id",), history="scd1",
                  kind="transaction", ingest_read="change_feed")


class FakeSilver:
    """Silver's current rows held in memory, answering like the engine adapter does."""

    def __init__(self, ids):
        self.rows = [{"delivery_id": i} for i in ids]

    def bucket_totals(self, plan, measures, buckets):
        return bucket_totals(self.rows, plan.keys, measures, buckets)

    def keys_in_buckets(self, plan, buckets, wanted):
        return {(str(r["delivery_id"]),) for r in self.rows if bucket_of((r["delivery_id"],), buckets) in wanted}


SRC = [f"DL-{n:04d}" for n in range(1, 41)]


@pytest.mark.parametrize(
    ("case", "source", "silver", "ok", "missing", "extra"),
    [
        ("silver holds every live row", SRC, SRC, True, [], []),
        ("the feed silently skipped three rows", SRC, [i for i in SRC if i not in ("DL-0003", "DL-0017", "DL-0031")],
         False, ["DL-0003", "DL-0017", "DL-0031"], []),
        ("silver has a row the source deleted without telling", SRC[:-1], SRC, False, [], ["DL-0040"]),
        ("both empty", [], [], True, [], []),
    ],
)
def test_reconcile_live(case, source, silver, ok, missing, extra):  # noqa: PLR0917 -- one parameter per table column
    r = reconcile_live("erp.delivery", [{"delivery_id": i, "qty": 1} for i in source], PLAN, FakeSilver(silver))
    assert r.ok is ok
    assert [k[0] for k in r.missing] == missing
    assert [k[0] for k in r.extra] == extra


def test_the_message_names_the_hop_and_the_keys():
    r = reconcile_live("erp.delivery", [{"delivery_id": i} for i in SRC], PLAN, FakeSilver(SRC[1:]))
    assert "hop: sources to landing" in r.message()
    assert "DL-0001" in r.message()


def test_only_differing_buckets_are_drilled():
    seen = []

    class Watching(FakeSilver):
        def keys_in_buckets(self, plan, buckets, wanted):
            seen.append(set(wanted))
            return super().keys_in_buckets(plan, buckets, wanted)

    reconcile_live("erp.delivery", [{"delivery_id": i} for i in SRC], PLAN, Watching(SRC[1:]))
    assert seen == [{bucket_of(("DL-0001",), 16)}]
