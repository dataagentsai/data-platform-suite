"""The clothing shop's orders database, as a change feed, for slice 1 (data-platform-catalog slices/01-orders/world.yaml).

Deterministic: the same seed gives the same rows, byte for byte, on any machine. Ground truth is kept beside every
change (which day it happened, which delivery carried it), so the oracle never has to trust the platform.

Generator: Python's random module behind this class, as world.yaml names it (`generator: seeded_random`): no package,
so the kernel stays on the standard library and the world runs on any Python.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

FLOW = ["placed", "paid", "shipped", "delivered"]
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class Change:
    row: dict[str, Any]
    day: int                 # the day the change happened at the source
    delivery: str            # the file that carried it


@dataclass
class DayOutput:
    day: int
    deliveries: list[tuple[str, str, list[dict[str, Any]]]] = field(default_factory=list)  # (window, name, rows)
    incidents: list[dict[str, Any]] = field(default_factory=list)


class ShopWorld:
    def __init__(self, world: dict[str, Any]):
        self.cfg = world
        self.rng = random.Random(world["seed"])
        self.start = date.fromisoformat(str(world["clock"]["start"])[:10])
        self.pop = world["population"]
        self.calendar = {e["day"]: e for e in world["calendar"]}
        self.orders: dict[str, dict[str, Any]] = {}
        self.created_on: dict[str, int] = {}
        self.frozen: set[str] = set()
        self.held: list[dict[str, Any]] = []     # a change kept back to be delivered late
        self.changes: list[Change] = []
        self.files: dict[str, list[dict[str, Any]]] = {}
        self.seq = 0
        self.days: list[DayOutput] = []

    def window(self, day: int) -> str:
        return (self.start + timedelta(days=day - 1)).isoformat()

    def run(self, days: int) -> list[DayOutput]:
        while len(self.days) < days:
            self.days.append(self._day(len(self.days) + 1))
        return self.days[:days]

    # ---- one day at the source ----
    def _day(self, day: int) -> DayOutput:
        out = DayOutput(day)
        rows = self._progress(day) + self._new_orders(day)
        incident = self.calendar.get(day)
        kind = incident["kind"] if incident else None

        if day == 2 and self._planned("source_delete"):
            rows += self._force_cancel(day, self._planned("source_delete").get("pick", 3))
        if day == 3 and self._planned("late_change"):
            rows += self._split_change(day)
        if kind == "source_delete":
            rows += self._deletes(day, incident.get("pick", 3))
        if kind == "late_change":
            rows += self.held
            self.held = []

        name = f"orders_{self.window(day)}.jsonl"
        if kind == "renamed_column":
            rows = [{("order_amount" if k == "amount" else k): v for k, v in r.items()} for r in rows]
        self._deliver(out, day, self.window(day), name, rows)

        if kind == "duplicate_delivery":
            first = self.days[day - 2].deliveries[0]
            out.deliveries.append((first[0], f"orders_{first[0]}_resent.jsonl", first[2]))
        if incident:
            out.incidents.append(incident)
        return out

    def _deliver(self, out: DayOutput, day: int, window: str, name: str, rows: list[dict[str, Any]]) -> None:
        out.deliveries.append((window, name, rows))
        self.files[name] = rows
        self.changes += [Change(r, day, name) for r in rows]

    def _next(self, order: dict[str, Any], op: str, **upd: Any) -> dict[str, Any]:
        self.seq += 1
        order.update(upd)
        return {**order, "change_seq": self.seq, "op": op}

    def _new_orders(self, day: int) -> list[dict[str, Any]]:
        n = self.rng.randint(self.pop["orders_per_day"]["min"], self.pop["orders_per_day"]["max"])
        rows = []
        for i in range(n):
            oid = f"O{day:02d}{i:04d}"
            ts = datetime.combine(self.start + timedelta(days=day - 1), datetime.min.time(), IST) + \
                timedelta(seconds=self.rng.randint(0, 86399))
            self.orders[oid] = {"order_id": oid, "customer_id": f"C{self.rng.randint(1, 900):04d}",
                                "store_id": f"S{self.rng.randint(1, 12):02d}", "order_ts": ts.isoformat(),
                                "status": "placed", "amount": round(self.rng.uniform(5, 300), 2), "currency": "INR"}
            self.created_on[oid] = day
            rows.append(self._next(self.orders[oid], "I"))
        return rows

    def _progress(self, day: int) -> list[dict[str, Any]]:
        rows = []
        for oid, o in self.orders.items():
            if oid in self.frozen or o["status"] in ("delivered", "cancelled", "deleted"):
                continue
            r = self.rng.random()
            if o["status"] in ("placed", "paid") and r < self.pop["cancel_share"]:
                rows.append(self._next(o, "U", status="cancelled"))
            elif r < 0.6:
                rows.append(self._next(o, "U", status=FLOW[FLOW.index(o["status"]) + 1]))
        return rows

    def _planned(self, kind: str) -> dict[str, Any] | None:
        return next((e for e in self.calendar.values() if e["kind"] == kind), None)

    def _force_cancel(self, day: int, pick: int) -> list[dict[str, Any]]:
        """Make sure enough day-1 orders are cancelled for the planned delete to have something to delete."""
        have = [o for o, d in self.created_on.items() if d == 1 and self.orders[o]["status"] == "cancelled"]
        open_ = [o for o, d in self.created_on.items() if d == 1 and self.orders[o]["status"] in ("placed", "paid")]
        return [self._next(self.orders[o], "U", status="cancelled") for o in open_[:max(0, pick - len(have))]]

    def _deletes(self, day: int, pick: int) -> list[dict[str, Any]]:
        victims = [o for o, d in self.created_on.items() if d == 1 and self.orders[o]["status"] == "cancelled"][:pick]
        rows = [self._next(self.orders[o], "D") for o in victims]
        for o in victims:
            self.orders[o]["status"] = "deleted"
            self.frozen.add(o)
        return rows

    def _split_change(self, day: int) -> list[dict[str, Any]]:
        """For one day-2 order still 'placed': two changes happen; the newer is sent now, the older is held back."""
        oid = next(o for o, d in self.created_on.items() if d == 2 and self.orders[o]["status"] == "placed")
        older = self._next(self.orders[oid], "U", status="paid")
        newer = self._next(self.orders[oid], "U", status="shipped")
        self.frozen.add(oid)
        self.held.append(older)
        self.late = oid
        return [newer]

    # ---- ground truth ----
    def late_order(self) -> str | None:
        return getattr(self, "late", None)

    def deleted_orders(self) -> list[str]:
        return [c.row["order_id"] for c in self.changes if c.row["op"] == "D"]
