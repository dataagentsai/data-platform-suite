"""The tyre company (data-platform-catalog slices/02-tyre/world.yaml): six source systems, day by day.

Deterministic: the same seed and scale give the same data on any machine. Each concern draws from its own random
stream, so changing how readings are made never changes an order. The model keeps the world's own truth beside
everything it emits; tyre_oracle.py turns that truth into the expected totals (R9: never from the platform's logs).

Volumes follow the DWD's population; `scale` shrinks orders and reading frequency for tests (1.0 = as specified).
"""

from __future__ import annotations

import csv
import io
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator
from zoneinfo import ZoneInfo

from dataplatform.meta.port import BackfillPlan, Correction
from dataplatform.source_ports import Row, SourceChange
from dataplatform.world.clock import SimClock, iso
from dataplatform.world.dwd import Dwd
from dataplatform.world.engine import Chunk, Emission

UTC = timezone.utc
SOL, DLV, MAT = "erp.sales_order_line", "erp.delivery", "erp.material"
PROD, PRESS = "mes.production", "mes.press"
INSP, SELL, DEALER, READ = (
    "lims.inspection_result",
    "partner.dealer_sellout",
    "dealer_api.dealer",
    "press_stream.press_reading",
)

REGIONS = ["north", "south", "east", "west", "central_europe"]
CLASSES = [
    ("passenger", 0.55, [13, 14, 15, 16, 17, 18, 19], (40, 120)),
    ("two_wheeler", 0.20, [13, 14, 15, 16, 17], (15, 40)),
    ("truck", 0.15, [17, 19, 20, 22], (150, 400)),
    ("off_highway", 0.10, [20, 22], (300, 900)),
]
DEFECTS = [f"DF{i:02d}" for i in range(1, 13)]


# Optional `stress:` block of the world file: shapes that make the data harder (benchmarking/, worst cases). Every default
# is what the world did before the block existed, so a world without it produces the same data and the same oracle.
STRESS_DEFAULTS: dict[str, float] = {
    "dealers_multiplier": 1,  # more distinct dealers: higher key cardinality
    "materials_multiplier": 1,  # more distinct materials
    "dealer_skew_alpha": 1.2,  # Pareto shape of the dealers' share of orders: lower = a few dealers dominate more
    "dealer_weight_cap": 20.0,  # the largest dealer weight
    "material_revisions_per_day": 2,  # materials revised each day after the first: deepens SCD2 history
}


class Line:
    """One sales order line: its current source row plus the truth the oracle needs."""

    __slots__ = (
        "key",
        "row",
        "day",
        "region",
        "first_promised",
        "cancel_day",
        "deleted",
        "deliver_day",
        "deliver_qty",
        "versions",
        "reschedule",
    )

    def __init__(self, row: Row, day: int, region: str, promised: date):
        self.key = (row["order_id"], row["line_no"])
        self.row, self.day, self.region, self.first_promised = row, day, region, promised
        self.cancel_day: int | None = None
        self.deleted = False
        self.deliver_day: int | None = None
        self.deliver_qty = 0
        self.versions = 1
        self.reschedule: tuple[int, int] | None = None  # (day, shift in days)


class TyreCompany:
    handles = frozenset(
        {
            "late_events",
            "source_delete",
            "late_delivery",
            "duplicate_delivery",
            "renamed_column",
            "late_change",
            "short_full_copy",
            "correction",
            "late_dimension",
            "silent_loss",
            "replay",
        }
    )

    def __init__(self, dwd: Dwd, scale: float = 1.0):
        w = dwd.world
        self.dwd, self.scale, self.pop = dwd, scale, w["population"]
        given = w.get("stress") or {}
        unknown = sorted(set(given) - set(STRESS_DEFAULTS))
        if unknown:
            raise ValueError(f"unknown stress setting {unknown}; known: {sorted(STRESS_DEFAULTS)}")
        self.stress = {**STRESS_DEFAULTS, **given}
        self.sys = {name: ds.system for name, ds in dwd.datasets.items()}
        self.clock = SimClock(str(w["clock"]["start"]), w["clock"]["business_timezone"])
        self.plants = {code: {**p, "tz": ZoneInfo(p["timezone"])} for code, p in w["clock"]["plants"].items()}
        # Each incident's exact values come from its `params:` block in the DWD (and `pick`, how many); no local table.
        self.incident = {e["kind"]: {**e, **(e.get("params") or {})} for e in w["calendar"]}
        seed = w["seed"]
        self.rng = {
            k: random.Random(f"{seed}:{k}") for k in ("master", "orders", "plant", "stream", "partner", "dealers")
        }
        self.erp_seq = self.mes_seq = 0
        self.order_no = self.delivery_no = 0
        self.lines: dict[tuple[str, int], Line] = {}
        self.deliveries: dict[str, dict[str, Any]] = {}
        self.due: dict[int, list[tuple[datetime, str, Line]]] = {}
        self.production: list[dict[str, Any]] = []
        self.inspections: dict[str, dict[str, Any]] = {}
        self.pending_insp: list[dict[str, Any]] = []
        self.sellout: dict[tuple[str, str, str], int] = {}
        self.sellout_files: dict[str, dict[str, Any]] = {}
        self.sellout_due: dict[str, int] = {}  # month -> dealers whose file is due inside the clock
        self.readings: dict[int, dict[str, Any]] = {}
        self.read_hours: dict[str, int] = {}
        self.ledger: dict[int, dict[str, dict[str, int]]] = {}
        self.findings: dict[int, list[dict[str, Any]]] = {}
        self.corrections: list[dict[str, Any]] = []  # the world's truth: which lines, which dates, when active
        self.pending_corrections: list[tuple[BackfillPlan, list[Correction]]] = []  # recorded in the store today
        self.dealer_seen: set[str] = set()
        self.material_versions = self.press_versions = self.dealer_versions = 0
        self._last_copy: dict[str, dict[str, tuple[Any, ...]]] = {MAT: {}, PRESS: {}}
        self._make_population()

    # ------------------------------------------------------------------ population
    def _make_population(self) -> None:
        r = self.rng["master"]
        n_dealers = int(self.pop["dealers"] * self.stress["dealers_multiplier"])
        self.dealers: dict[str, Row] = {}
        self.dealer_weight: dict[str, float] = {}
        for i in range(1, n_dealers + 1):
            code = f"D-{i:04d}"
            self.dealers[code] = self._dealer_row(code, REGIONS[(i - 1) % len(REGIONS)], r)
            self.dealer_weight[code] = min(r.paretovariate(self.stress["dealer_skew_alpha"]),
                                           self.stress["dealer_weight_cap"])
        self.materials: dict[str, Row] = {}
        self.price: dict[str, float] = {}
        for i in range(1, int(self.pop["materials"] * self.stress["materials_multiplier"]) + 1):
            cls, _, rims, (lo, hi) = r.choices(CLASSES, weights=[c[1] for c in CLASSES])[0]
            code = f"MAT-{i:05d}"
            rim = r.choice(rims)
            self.materials[code] = {
                "material_code": code,
                "description": f"{cls.replace('_', ' ')} {rim} inch pattern {r.randint(1, 60)}",
                "compound_code": f"C-{r.randint(1, 24):02d}",
                "rim_size_in": rim,
                "tyre_class": cls,
            }
            self.price[code] = round(r.uniform(lo, hi), 2)
        codes = list(self.materials)
        self.material_weight = [1 / (i + 1) ** 0.8 for i in range(len(codes))]
        self.presses: dict[str, Row] = {}
        self.press_material: dict[str, str] = {}
        curing = [c for c in codes if self.materials[c]["tyre_class"] in ("passenger", "truck")][:200]
        for plant, p in self.plants.items():
            for j in range(1, int(p["presses"]) + 1):
                pid = f"{plant[2:]}-PR{j:02d}"
                self.presses[pid] = {
                    "press_id": pid,
                    "plant_code": plant,
                    "press_type": r.choice(["hydraulic", "mechanical"]),
                    "mould_code": f"MLD-{r.randint(100, 999)}",
                    "ideal_cycle_seconds": r.randint(600, 900),
                }
                self.press_material[pid] = r.choice(curing)
        self.curing_materials = curing

    def _dealer_row(self, code: str, region: str, r: random.Random) -> Row:
        n = int(code[2:])
        return {
            "dealer_code": code,
            "dealer_name": f"Dealer {n:04d} Tyres",
            "region": region,
            "contact_email": f"owner{n:04d}@dealer.example",
            "contact_phone": f"+91-90{r.randint(10000000, 99999999)}",
        }

    def _plant_of(self, dealer: str) -> str:
        return "P-HUN" if self.dealers.get(dealer, {}).get("region") == "central_europe" else "P-CHN"

    # ------------------------------------------------------------------ one day
    def day(self, d: int) -> Emission:
        e = Emission()
        self.ledger[d] = {"source": {}, "bronze": {}}
        self.findings[d] = []
        self._masters(d, e)
        self._erp(d, e)
        self._plant(d, e)
        self._partner(d, e)
        self._dealer_api(d, e)
        e.events[(self.sys[READ], READ)] = self._readings(d)
        e.corrections = self.pending_corrections
        self.pending_corrections = []
        return e

    def _count(self, d: int, ds: str, source: int, bronze: int) -> None:
        led = self.ledger[d]
        led["source"][ds] = led["source"].get(ds, 0) + source
        led["bronze"][ds] = led["bronze"].get(ds, 0) + bronze

    def _find(self, d: int, kind: str, **detail: Any) -> None:
        inc = self.incident[kind]
        self.findings[d].append(
            {
                "incident": inc["id"],
                "story": inc["story"],
                "dataset": inc["dataset"],
                "detector": inc["expect"]["detector"],
                "handled_by": inc["expect"]["handled_by"],
                "detail": detail,
            }
        )

    def _on(self, kind: str, d: int) -> dict[str, Any] | None:
        inc = self.incident.get(kind)
        return inc if inc and inc["day"] == d else None

    # ------------------------------------------------------------------ masters: materials, presses, dealers
    def _masters(self, d: int, e: Emission) -> None:
        r, change_at = self.rng["master"], self.clock.at(d, 0, 20)
        mat_changes = []
        revisions = min(int(self.stress["material_revisions_per_day"]), len(self.materials))
        for code in r.sample(sorted(self.materials), revisions) if d > 1 else []:
            self.materials[code]["description"] += " (revised)"
            mat_changes.append(
                SourceChange("U", {**self.materials[code], "extracted_at": iso(change_at)}, 0, iso(change_at))
            )
        if d == 1:
            mat_changes = [
                SourceChange("I", {**m, "extracted_at": iso(change_at)}, 0, iso(change_at))
                for m in self.materials.values()
            ]
        e.changes.append((self.sys[MAT], MAT, mat_changes))
        press_changes = []
        if d == 1:
            press_changes = [
                SourceChange("I", {**p, "extracted_at": iso(change_at)}, 0, iso(change_at))
                for p in self.presses.values()
            ]
        elif d % 3 == 0:
            pid = r.choice(sorted(self.presses))
            self.presses[pid]["mould_code"] = f"MLD-{r.randint(100, 999)}"
            press_changes = [
                SourceChange("U", {**self.presses[pid], "extracted_at": iso(change_at)}, 0, iso(change_at))
            ]
        e.changes.append((self.sys[PRESS], PRESS, press_changes))
        copy_at = iso(self.clock.at(d, 1))
        for ds, rows, track in (
            (MAT, self.materials, ("description", "compound_code", "rim_size_in", "tyre_class")),
            (PRESS, self.presses, ("plant_code", "mould_code", "ideal_cycle_seconds")),
        ):
            copy = [dict(v) for _, v in sorted(rows.items())]
            short = self._on("short_full_copy", d) if ds == MAT else None
            if short:
                drop = set(r.sample([c["material_code"] for c in copy], round(len(copy) * short["missing_share"])))
                copy = [c for c in copy if c["material_code"] not in drop]
                self._find(
                    d,
                    "short_full_copy",
                    rows_expected=len(rows),
                    rows_delivered=len(copy),
                    missing_share=round(1 - len(copy) / len(rows), 3),
                )
                self._count(d, ds, len(copy), 0)  # blocked: nothing appended to bronze
            else:
                self._accept_copy(ds, copy, track)
                self._count(d, ds, len(copy), len(copy))
            e.copies.append((self.sys[ds], ds, copy_at, copy))

    def _accept_copy(self, ds: str, copy: list[Row], track: tuple[str, ...]) -> None:
        """A copy the platform accepts: a new silver version for each row whose tracked columns changed."""
        key = "material_code" if ds == MAT else "press_id"
        last = self._last_copy[ds]
        for row in copy:
            t = tuple(row[c] for c in track)
            if last.get(row[key]) != t:
                last[row[key]] = t
                if ds == MAT:
                    self.material_versions += 1
                else:
                    self.press_versions += 1

    # ------------------------------------------------------------------ ERP: orders, lines, deliveries
    def _erp(self, d: int, e: Emission) -> None:
        r = self.rng["orders"]
        self._prepare_incidents(d)
        actions: list[tuple[datetime, int, str, Any]] = []
        n = max(1, round(r.randint(self.pop["orders_per_day"]["min"], self.pop["orders_per_day"]["max"]) * self.scale))
        new_dealer = self.incident.get("late_dimension")
        forced = 0
        if new_dealer and d >= new_dealer["day"]:
            forced = max(2, round(n * 0.01))
        for i in range(n):
            dealer = new_dealer["dealer"] if i < forced else None
            actions.append((self.clock.day_start(d) + timedelta(seconds=r.randint(0, 86399)), 0, "order", dealer))
        for when, kind, line in self.due.pop(d, []):
            actions.append((when, 1, kind, line))
        late = self._on("late_change", d)
        if late:
            actions.append((self.clock.at(d, 9), 2, "late_change", late))
        if self._on("source_delete", d):
            actions.append((self.clock.at(d, 11), 2, "delete", self._on("source_delete", d)))
        actions.sort(key=lambda a: (a[0], a[1]))
        changes: dict[str, list[SourceChange]] = {SOL: [], DLV: []}
        for when, _, kind, arg in actions:
            for ds, ch in getattr(self, f"_act_{kind}")(d, when, arg):
                changes[ds].append(ch)
        self._silent_loss(d, changes[DLV])
        corr = self._on("correction", d)
        if corr:
            self._correct(d, corr)
        for ds in (SOL, DLV):
            chs = sorted(changes[ds], key=lambda c: c.committed_at)  # commit order: a late change lands last
            e.changes.append((self.sys[ds], ds, chs))
            self._count(d, ds, len(chs), sum(c.in_feed for c in chs))

    def _seq(self) -> int:
        self.erp_seq += 1
        return self.erp_seq

    def _act_order(self, d: int, when: datetime, dealer: str | None) -> list[tuple[str, SourceChange]]:
        r = self.rng["orders"]
        if dealer is None:
            dealer = r.choices(list(self.dealer_weight), weights=list(self.dealer_weight.values()))[0]
        region = self.dealers[dealer]["region"] if dealer in self.dealers else self.incident["late_dimension"]["region"]
        self.order_no += 1
        oid = f"SO{self.order_no:08d}"
        out = []
        for ln in range(1, r.randint(self.pop["lines_per_order"]["min"], self.pop["lines_per_order"]["max"]) + 1):
            mat = r.choices(list(self.materials), weights=self.material_weight)[0]
            qty = (
                r.randint(2, 40) if self.materials[mat]["tyre_class"] in ("truck", "off_highway") else r.randint(4, 120)
            )
            eur = region == "central_europe"
            promise_in = r.randint(3, 10)
            promised = self.clock.date(d) + timedelta(days=promise_in)
            row = {
                "order_id": oid,
                "line_no": ln * 10,
                "dealer_code": dealer,
                "material_code": mat,
                "plant_code": "P-HUN" if eur else "P-CHN",
                "order_ts": iso(when),
                "promised_date": promised.isoformat(),
                "ordered_qty": qty,
                "net_value": round(qty * self.price[mat] * (1 if eur else 90), 2),
                "currency": "EUR" if eur else "INR",
                "line_status": "open",
                "rejection_reason": None,  # set only when a line is rejected (contract 0.2.0)
                "change_seq": self._seq(),
            }
            line = Line(row, d, region, promised)
            self.lines[line.key] = line
            self._plan(line, d, promise_in, r)
            out.append((SOL, SourceChange("I", dict(row), row["change_seq"], iso(when))))
        return out

    def _plan(self, line: Line, d: int, promise_in: int, r: random.Random) -> None:
        """Decide now what happens to the line later: cancelled, rescheduled, delivered on time or late, in full or not."""
        if r.random() < self.pop["cancel_share"]:
            line.cancel_day = d + r.randint(1, 2)
            self._schedule(line.cancel_day, r, "cancel", line, 8, 18)
            return
        if r.random() < 0.05 and promise_in > 2:
            line.reschedule = (d + r.randint(1, 2), r.randint(1, 3))
            self._schedule(line.reschedule[0], r, "reschedule", line, 8, 18)
        on_time = r.random() < self.pop["on_time_share"]
        line.deliver_day = d + (r.randint(1, promise_in) if on_time else promise_in + r.randint(1, 4))
        qty = line.row["ordered_qty"]
        line.deliver_qty = qty if r.random() < self.pop["in_full_share"] else max(1, int(qty * r.uniform(0.5, 0.95)))
        self._schedule(line.deliver_day, r, "deliver", line, 6, 20)

    def _schedule(self, day: int, r: random.Random, kind: str, line: Line, h0: int, h1: int) -> None:
        when = self.clock.at(day, 0) + timedelta(seconds=r.randint(h0 * 3600, h1 * 3600 - 1))
        self.due.setdefault(day, []).append((when, kind, line))

    def _update(self, line: Line, when: datetime, **upd: Any) -> tuple[str, SourceChange]:
        track = ("promised_date", "ordered_qty", "line_status")  # the manifest's history track_columns
        tracked = tuple(line.row[c] for c in track)
        line.row.update(upd, change_seq=self._seq())
        if tuple(line.row[c] for c in track) != tracked:
            line.versions += 1
        return SOL, SourceChange("U", dict(line.row), line.row["change_seq"], iso(when))

    def _act_cancel(self, d: int, when: datetime, line: Line) -> list[tuple[str, SourceChange]]:
        if line.deleted:
            return []
        return [self._update(line, when, line_status="cancelled")]  # keeps its quantity and value (DWD fidelity)

    def _act_reschedule(self, d: int, when: datetime, line: Line) -> list[tuple[str, SourceChange]]:
        if line.cancel_day or line.reschedule is None:
            return []
        new = date.fromisoformat(line.row["promised_date"]) + timedelta(days=line.reschedule[1])
        return [self._update(line, when, promised_date=new.isoformat())]

    def _act_deliver(self, d: int, when: datetime, line: Line) -> list[tuple[str, SourceChange]]:
        if line.cancel_day is not None or line.deleted or line.deliver_day != d:
            return []
        self.delivery_no += 1
        row = {
            "delivery_id": f"DL{self.delivery_no:08d}",
            "order_id": line.key[0],
            "line_no": line.key[1],
            "delivered_qty": line.deliver_qty,
            "delivered_ts": iso(when),
            "plant_code": line.row["plant_code"],
            "change_seq": self._seq(),
        }
        self.deliveries[row["delivery_id"]] = {**row, "day": d, "lost": False, "line": line.key}
        return [(DLV, SourceChange("I", row, row["change_seq"], iso(when)))]

    def _act_delete(self, d: int, when: datetime, inc: dict[str, Any]) -> list[tuple[str, SourceChange]]:
        victims = [
            ln
            for ln in self.lines.values()
            if ln.day == inc["from_day"] and ln.cancel_day is not None and ln.cancel_day < d
        ][: inc["pick"]]
        if len(victims) < inc["pick"]:  # planting fewer than promised would let a check pass on nothing
            raise ValueError(f"{inc['id']}: {len(victims)} of {inc['pick']} cancelled lines from day {inc['from_day']} "
                             f"to delete on day {d}; lines are cancelled on day {inc['from_day'] + 1}, so the "
                             "incident's day must be at least two after from_day")
        out = []
        for ln in victims:
            ln.deleted = True
            out.append((SOL, SourceChange("D", {**ln.row, "change_seq": self._seq()}, self.erp_seq, iso(when))))
        self._find(d, "source_delete", keys=[list(ln.key) for ln in victims])
        return out

    def _act_late_change(self, d: int, when: datetime, inc: dict[str, Any]) -> list[tuple[str, SourceChange]]:
        """Two promised-date changes: the older one commits six hours after the newer one."""
        open_ = [
            ln
            for ln in self.lines.values()
            if ln.day == inc["from_day"] and ln.cancel_day is None and not ln.deleted and ln.reschedule is None
        ]
        undelivered = [ln for ln in open_ if (ln.deliver_day or 0) > d]
        if not open_:
            raise ValueError(f"no open line from day {inc['from_day']} to plant {inc['id']} on; raise the scale")
        line = (undelivered or open_)[0]
        promised = date.fromisoformat(line.row["promised_date"])
        _, older = self._update(line, when, promised_date=(promised + timedelta(days=1)).isoformat())
        _, newer = self._update(line, when, promised_date=(promised + timedelta(days=2)).isoformat())
        held = SourceChange("U", older.row, older.change_seq, iso(when + timedelta(hours=6)))
        self._find(
            d,
            "late_change",
            key=list(line.key),
            older_seq=older.change_seq,
            newer_seq=newer.change_seq,
            current_promised_date=line.row["promised_date"],
        )
        self.late_change_key = line.key
        return [(SOL, newer), (SOL, held)]

    def _prepare_incidents(self, d: int) -> None:
        """Make sure an incident has something to act on at small scales (tests): enough cancelled lines to delete."""
        inc = self.incident.get("source_delete")
        if inc and d == inc["from_day"] + 1:
            have = [ln for ln in self.lines.values() if ln.day == inc["from_day"] and ln.cancel_day is not None]
            spare = [
                ln
                for ln in self.lines.values()
                if ln.day == inc["from_day"]
                and ln.cancel_day is None
                and (ln.deliver_day or 0) > d
                and ln.reschedule is None
            ]
            for ln in spare[: max(0, inc["pick"] - len(have))]:
                ln.cancel_day, ln.deliver_day = d, None
                self.due.setdefault(d, []).append((self.clock.at(d, 7), "cancel", ln))

    def _silent_loss(self, d: int, deliveries: list[SourceChange]) -> None:
        inc = self._on("silent_loss", d)
        if not inc:
            return
        lost = sorted(deliveries, key=lambda c: c.committed_at)[: inc["pick"]]
        ids = {c.row["delivery_id"] for c in lost}
        for i, c in enumerate(deliveries):
            if c.row["delivery_id"] in ids:
                deliveries[i] = SourceChange(c.op, c.row, c.change_seq, c.committed_at, in_feed=False)
                self.deliveries[c.row["delivery_id"]]["lost"] = True
        self._find(
            d, "silent_loss", keys=sorted(ids), window=[lost[0].committed_at, lost[-1].committed_at] if lost else []
        )

    def _correct(self, d: int, inc: dict[str, Any]) -> None:
        """The sales team records corrections as rows in the metadata store's corrections table (ADR-017), with a
        backfill plan that activates them later and names the windows to rebuild. The ERP is not changed."""
        late_key = getattr(self, "late_change_key", None)
        picks = [
            ln
            for ln in self.lines.values()
            if ln.day == inc["from_day"] and ln.cancel_day is None and not ln.deleted and ln.key != late_key
        ][: inc["pick"]]
        rows = []
        for ln in picks:
            corrected = (ln.first_promised + timedelta(days=inc["shift_days"])).isoformat()
            self.corrections.append(
                {
                    "order_id": ln.key[0],
                    "line_no": ln.key[1],
                    "column": "promised_date",
                    "was": ln.first_promised.isoformat(),
                    "corrected": corrected,
                    "recorded_day": d,
                    "activate_day": inc["activate_day"],
                }
            )
            rows.append(
                Correction(
                    key={"order_id": ln.key[0], "line_no": ln.key[1]},
                    field="promised_date",
                    value=corrected,
                    was=ln.first_promised.isoformat(),
                )
            )
        windows = sorted({c["was"] for c in self.corrections} | {c["corrected"] for c in self.corrections})
        source, dataset = SOL.split(".")
        plan = BackfillPlan(
            plan_id=f"bp-{inc['id']}",
            source=source,
            dataset=dataset,
            windows=tuple(windows),
            activate_on=self.clock.date(inc["activate_day"]).isoformat(),
            reason=" ".join(str(inc["does"]).split()),
            recorded_by="sales-data@tyre.example",
        )
        self.pending_corrections.append((plan, rows))
        self._find(d, "correction", lines=len(picks), activate_day=inc["activate_day"], windows=windows,
                   plan_id=plan.plan_id)

    def promise(self, line: Line, d: int) -> date:
        """The date first promised, as the world knows it on day d (a correction counts once its plan is active)."""
        for c in self.corrections:
            if (c["order_id"], c["line_no"]) == line.key and d >= c["activate_day"]:
                return date.fromisoformat(c["corrected"])
        return line.first_promised

    # ------------------------------------------------------------------ plant: production, inspections, lab files
    def _plant(self, d: int, e: Emission) -> None:
        r = self.rng["plant"]
        lo, hi = self.pop["tyres_cured_per_press_hour"]["min"], self.pop["tyres_cured_per_press_hour"]["max"]
        prod: list[SourceChange] = []
        start, end = self.clock.day_start(d), self.clock.day_end(d)
        for pid, press in sorted(self.presses.items()):
            plant = self.plants[press["plant_code"]]
            for hour in _local_hours(start, end, plant["tz"]):
                local = hour.astimezone(plant["tz"])
                shift = _shift(local.hour, int(plant["shifts_per_day"]))
                if shift is None:
                    continue
                if local.hour == 6 and r.random() < 0.2:
                    self.press_material[pid] = r.choice(self.curing_materials)
                self.mes_seq += 1
                cured = hour + timedelta(minutes=r.randint(50, 59))
                row = {
                    "production_id": f"PR-{pid}-{hour.astimezone(UTC):%Y%m%d%H%M}",
                    "press_id": pid,
                    "material_code": self.press_material[pid],
                    "shift_id": f"{press['plant_code']}-{local:%Y%m%d}-S{shift}",
                    "cured_ts": iso(cured),
                    "tyres_cured": r.randint(lo, hi),
                    "change_seq": self.mes_seq,
                }
                prod.append(SourceChange("I", row, self.mes_seq, iso(cured + timedelta(seconds=30))))
                self.production.append({**row, "day": d, "plant": press["plant_code"], "cure_hour": iso(hour)})
                self._inspect(row, press["plant_code"], cured, r)
        prod.sort(key=lambda c: c.committed_at)
        e.changes.append((self.sys[PROD], PROD, prod))
        self._count(d, PROD, len(prod), len(prod))
        self._lab_files(d, e)

    def _inspect(self, row: Row, plant: str, cured: datetime, r: random.Random) -> None:
        share = max(0.0, r.gauss(self.pop["scrap_share"]["mean"], self.pop["scrap_share"]["sd"]))
        scrapped = min(row["tyres_cured"], round(row["tyres_cured"] * share + r.random() - 0.5))
        scrapped = max(0, scrapped)
        result = "scrap" if scrapped else ("rework" if r.random() < 0.03 else "pass")
        insp = {
            "inspection_id": f"IN-{row['production_id'][3:]}",
            "production_id": row["production_id"],
            "press_id": row["press_id"],
            "inspected_ts": iso(cured + timedelta(minutes=r.randint(20, 80))),
            "result": result,
            "tyres_inspected": row["tyres_cured"],
            "tyres_scrapped": scrapped,
            "defect_code": r.choice(DEFECTS) if scrapped else "",
            "inspector_id": f"INSP-{r.randint(1, 40):03d}",
        }
        self.inspections[insp["inspection_id"]] = {**insp, "plant": plant, "filed_day": None, "rejected": False}
        self.pending_insp.append(insp)

    def _lab_files(self, d: int, e: Emission) -> None:
        """One CSV per plant per local hour of inspection, arriving a few minutes after the hour closes."""
        end = self.clock.day_end(d)
        rename = self.incident.get("renamed_column")
        files: dict[tuple[str, datetime], list[Row]] = {}
        keep = []
        for insp in self.pending_insp:
            plant = self.inspections[insp["inspection_id"]]["plant"]
            tz = self.plants[plant]["tz"]
            hour = datetime.fromisoformat(insp["inspected_ts"]).astimezone(tz).replace(minute=0, second=0)
            if hour + timedelta(minutes=65) <= end:
                files.setdefault((plant, hour), []).append(insp)
            else:
                keep.append(insp)
        self.pending_insp = keep
        for (plant, hour), rows in sorted(files.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            renamed = bool(
                rename
                and plant == rename["plant"]
                and hour >= self._rename_from(rename)
                and d <= rename.get("until_day", rename["day"])
            )
            name = f"inspection_{plant}_{hour:%Y%m%dT%H}00.csv"  # matches the manifest's file_pattern
            arrived = iso(hour + timedelta(minutes=65))
            cols = [c for c, _ in self.dwd.datasets[INSP].table.columns]
            header = (
                [rename["renamed_to"] if c == rename["column"] else c for c in cols] if renamed and rename else None
            )
            delimiter = str(self.dwd.datasets[INSP].manifest["config"].get("csv_delimiter", ","))
            e.files.append((self.sys[INSP], INSP, name, _csv(rows, cols, header, delimiter), arrived))
            for row in rows:
                self.inspections[row["inspection_id"]].update(filed_day=d, rejected=renamed)
            self._count(d, INSP, len(rows), 0 if renamed else len(rows))
            if renamed and not any(f["incident"] == rename["id"] for f in self.findings[d]):
                self._find(
                    d,
                    "renamed_column",
                    plant=plant,
                    first_file=name,
                    missing_column=rename["column"],
                    new_column=rename["renamed_to"],
                )

    def _rename_from(self, inc: dict[str, Any]) -> datetime:
        local = self.clock.date(inc["day"])
        return datetime(
            local.year, local.month, local.day, inc["from_local_hour"], tzinfo=self.plants[inc["plant"]]["tz"]
        )

    # ------------------------------------------------------------------ partner sell-out files
    def _partner(self, d: int, e: Emission) -> None:
        """Last month's file from every dealer arrives in the world's first five days (DWD: due on day 1 for D-0042)."""
        late, resend = self.incident.get("late_delivery"), self.incident.get("duplicate_delivery")
        month = (late or {}).get("month", (self.clock.date(1).replace(day=1) - timedelta(days=1)).strftime("%Y-%m"))
        r = self.rng["partner"]
        if d == 1:
            window = int(self.dwd.systems[self.sys[SELL]].get("file_window_days", 5))
            self.sellout_plan = {code: (1 + (int(code[2:]) % window)) for code in self.dealers}
            self.sellout_due = {month: len(self.sellout_plan)}
            if late:
                self.sellout_plan[late["dealer"]] = late["day"]
        for code in sorted(c for c, day in getattr(self, "sellout_plan", {}).items() if day == d):
            rows = self._sellout_rows(code, month, r)
            name = f"sellout_{code}_{month}.csv"
            data = _csv(rows, ["dealer_code", "sale_month", "material_code", "units_sold"],
                        delimiter=str(self.dwd.datasets[SELL].manifest["config"].get("csv_delimiter", ",")))
            arrived = self.clock.at(d, 9) + timedelta(seconds=r.randint(0, 8 * 3600))
            e.files.append((self.sys[SELL], SELL, name, data, iso(arrived)))
            self.sellout_files[name] = {"dealer": code, "month": month, "day": d, "rows": len(rows), "data": data}
            for row in rows:
                self.sellout[(code, row["sale_month"], row["material_code"])] = row["units_sold"]
            self._count(d, SELL, len(rows), len(rows))
            if late and code == late["dealer"]:
                self._find(d, "late_delivery", dealer=code, month=month, due_day=late["due_day"], arrived_day=d)
        if resend and d == resend["day"]:
            original = self.sellout_files[f"sellout_{resend['dealer']}_{resend['month']}.csv"]
            name = f"sellout_{resend['dealer']}_{resend['month']}_resent.csv"
            e.files.append((self.sys[SELL], SELL, name, original["data"], iso(self.clock.at(d, 10))))
            self._count(d, SELL, original["rows"], 0)  # same bytes: skipped as a duplicate
            self._find(d, "duplicate_delivery", file=name, same_as=f"sellout_{resend['dealer']}_{resend['month']}.csv")

    def _sellout_rows(self, dealer: str, month: str, r: random.Random) -> list[Row]:
        n = max(3, round(r.randint(10, 40) * min(1.0, self.dealer_weight[dealer] / 3)))
        mats = sorted(set(r.choices(list(self.materials), weights=self.material_weight, k=n)))
        return [
            {"dealer_code": dealer, "sale_month": f"{month}-01", "material_code": m, "units_sold": r.randint(2, 160)}
            for m in mats
        ]

    # ------------------------------------------------------------------ dealer interface
    def _dealer_api(self, d: int, e: Emission) -> None:
        r = self.rng["dealers"]
        late = self.incident.get("late_dimension")
        changed: list[tuple[str, datetime]] = []
        if d == 1:
            changed = [(code, self.clock.day_start(1) - timedelta(days=30)) for code in sorted(self.dealers)]
            self.dealer_versions += len(self.dealers)
        else:
            for code in r.sample(sorted(self.dealers), r.randint(1, 4)):
                if r.random() < 0.2:
                    self.dealers[code]["dealer_name"] += " & Sons"
                    self.dealer_versions += 1
                else:
                    self.dealers[code]["contact_phone"] = f"+91-90{r.randint(10000000, 99999999)}"
                changed.append((code, self.clock.day_start(d) + timedelta(seconds=r.randint(0, 86399))))
        if late and d == late["appears_day"]:
            self.dealers[late["dealer"]] = {
                **self._dealer_row(late["dealer"], late["region"], r),
                "dealer_name": "New Dealer 9001 Tyres",
            }
            self.dealer_versions += 1
            changed.append((late["dealer"], self.clock.at(d, 9)))
        for code, at in changed:
            self.dealer_seen.add(code)
            e.records.append((self.sys[DEALER], DEALER, code, {**self.dealers[code], "updated_at": iso(at)}, iso(at)))
        self._count(d, DEALER, len(changed), len(changed))
        if late and late["day"] <= d < late["appears_day"]:
            orphans = [list(ln.key) for ln in self.lines.values() if ln.row["dealer_code"] == late["dealer"]]
            self._find(
                d, "late_dimension", dealer=late["dealer"], orphan_lines=len(orphans), appears_day=late["appears_day"]
            )

    # ------------------------------------------------------------------ press sensor stream
    def _readings(self, d: int) -> Iterator[Chunk]:
        """Hour by hour so a day of readings never sits in memory at once. Late readings carry into the next hour."""
        r = self.rng["stream"]
        every = max(10, round(10 / self.scale)) if self.scale < 1 else 10
        late = self._on("late_events", d)
        replay = self._on("replay", d)
        start = int(self.clock.day_start(d).timestamp())
        late_from = int(self.clock.at(d, late["from_hour"]).timestamp()) if late else 0
        late_to = late_from + 3600 * late["hours"] if late else 0
        late_presses = set(r.sample(sorted(self.presses), late["presses"])) if late else set()
        restart = int(self.clock.at(d, replay["restart_hour"]).timestamp()) if replay else 0
        stats = self.readings.setdefault(
            d, {"arrived": 0, "unique": 0, "late": 0, "max_delay_s": 0, "replayed": 0, "by_hour": {}}
        )
        carry: list[tuple[float, str, Row]] = []
        replay_buf: list[tuple[str, Row]] = []
        presses = sorted(self.presses.items())
        for h in range(24):
            h0, h1 = start + h * 3600, start + (h + 1) * 3600
            batch = carry
            for i, (pid, press) in enumerate(presses):
                cycle, tz = press["ideal_cycle_seconds"] + 60, self.plants[press["plant_code"]]["tz"]
                on = _shift(
                    datetime.fromtimestamp(h0, UTC).astimezone(tz).hour,
                    int(self.plants[press["plant_code"]]["shifts_per_day"]),
                )
                for t in range(h0 + (i % every), h1, every):
                    phase = (t + i * 37) % cycle
                    state = (
                        "idle"
                        if on is None
                        else ("loading" if phase < 30 else "curing" if phase < cycle - 30 else "unloading")
                    )
                    hot = state == "curing"
                    row = {
                        "reading_id": f"{pid}-{t}",
                        "press_id": pid,
                        "event_ts": _ts(t),
                        "temperature_c": round(r.gauss(170, 2.5) if hot else r.uniform(60, 90), 1),
                        "pressure_bar": round(max(0.0, r.gauss(24, 1.2) if hot else r.gauss(1, 0.4)), 2),
                        "cycle_state": state,
                    }
                    delay = r.uniform(0.2, 2.0)
                    if late and pid in late_presses and late_from <= t < late_to and r.random() < late["share"]:
                        delay = r.uniform(10, late["max_delay_s"])
                        stats["late"] += 1
                        stats["max_delay_s"] = max(stats["max_delay_s"], round(delay))
                    batch.append((t + delay, pid, row))
                    if replay and restart - 60 * replay["minutes"] <= t < restart:
                        replay_buf.append((pid, row))
            batch.sort(key=lambda x: x[0])
            ready = [(pid, _ts(at), row) for at, pid, row in batch if at < h1]
            carry = [x for x in batch if x[0] >= h1]
            if replay and h0 <= restart < h1:
                ready = (
                    [x for x in ready if x[1] < _ts(restart)]
                    + [(pid, _ts(restart + 5 + k // 200), row) for k, (pid, row) in enumerate(replay_buf)]
                    + [x for x in ready if x[1] >= _ts(restart)]
                )
                stats["replayed"] = len(replay_buf)
            stats["arrived"] += len(ready)
            yield ready
        if carry:
            stats["arrived"] += len(carry)
            yield [(pid, _ts(at), row) for at, pid, row in carry]
        self._reading_truth(d, every)

    def _reading_truth(self, d: int, every: int) -> None:
        """Readings per event-time hour: every press makes one per `every` seconds, whatever arrives late or twice."""
        stats = self.readings[d]
        start = int(self.clock.day_start(d).timestamp())
        presses = sorted(self.presses)
        by_hour = {}
        for h in range(24):
            h0 = start + h * 3600
            by_hour[_ts(h0)] = sum(len(range(h0 + (i % every), h0 + 3600, every)) for i in range(len(presses)))
        stats["by_hour"] = by_hour
        stats["unique"] = sum(by_hour.values())
        self._count(d, READ, stats["arrived"], stats["arrived"])
        if self._on("late_events", d):
            self._find(d, "late_events", late_readings=stats["late"], max_delay_s=stats["max_delay_s"])
        if self._on("replay", d):
            self._find(d, "replay", replayed_readings=stats["replayed"], minutes=self.incident["replay"]["minutes"])

    # ------------------------------------------------------------------ oracle
    def oracle(self, d: int) -> dict[str, Any]:
        from dataplatform.world.domains.tyre_oracle import day_oracle

        return day_oracle(self, d)

    def incidents(self) -> list[dict[str, Any]]:
        return [f for d in sorted(self.findings) for f in [{**x, "day": d} for x in self.findings[d]]]


def _local_hours(start: datetime, end: datetime, tz: ZoneInfo) -> list[datetime]:
    """Whole local hours of a plant whose start falls inside [start, end)."""
    first = start.astimezone(tz).replace(minute=0, second=0, microsecond=0)
    if first < start:
        first += timedelta(hours=1)
    out, h = [], first
    while h < end:
        out.append(h)
        h += timedelta(hours=1)
    return out


def _shift(local_hour: int, shifts: int) -> int | None:
    """Shift 1 from 06:00, 2 from 14:00, 3 from 22:00; a two-shift plant is idle overnight."""
    if 6 <= local_hour < 14:
        return 1
    if 14 <= local_hour < 22:
        return 2
    return 3 if shifts >= 3 else None


def _ts(epoch: float) -> str:
    return datetime.fromtimestamp(int(epoch), UTC).isoformat()


def _csv(rows: list[Row], columns: list[str], header: list[str] | None = None, delimiter: str = ",") -> bytes:
    """CSV with a header line, in the delimiter the dataset's manifest declares. `header` renames columns on the way
    out (the lab's renamed column)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", delimiter=delimiter)
    w.writerow(header or columns)
    for row in rows:
        w.writerow([row.get(c, "") for c in columns])
    return buf.getvalue().encode()
