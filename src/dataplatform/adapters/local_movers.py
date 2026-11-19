"""The push movers for the local stack: they stand in for what a cloud supplies (a change-capture service, a file
transfer service) and meet the landing contract every push mover meets (DPC-0038): each delivery written once,
under a unique name, the manifest last. The lander does not know which mover filled landing.

    ChangeFeedMover   reads a table's change feed from its own checkpoint (the last feed position it landed) and
                      lands one JSON-lines delivery per run window, the feed rows exactly as the feed gave them
    DropMover         copies new files from a drop zone into landing byte for byte, windowed by arrival date
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import ClassVar
from zoneinfo import ZoneInfo

from dataplatform.adapters.files import to_bytes
from dataplatform.ingest.ports import DeliveryExists, Landing
from dataplatform.model import Delivery
from dataplatform.source_ports import FileDrop, SourceDatabase, TableSpec


class ChangeFeedMover:
    def __init__(self, db: SourceDatabase, landing: Landing):
        self.db, self.landing = db, landing

    def checkpoint(self, source: str, dataset: str) -> int:
        landed = [self.landing.manifest(d) for d in self.landing.deliveries(source, dataset)]
        return max((int(m.get("feed_pos_to", 0)) for m in landed), default=0)

    def move(self, system: str, source: str, table: TableSpec, window: str) -> Delivery | None:
        rows = self.db.feed(system, table, self.checkpoint(source, table.name))
        if not rows:
            return None  # nothing changed: a change-capture service writes no empty file
        first, last = rows[0]["feed_pos"], rows[-1]["feed_pos"]
        name = f"{table.name}_{window}_{first:010d}-{last:010d}.jsonl"
        try:
            return self.landing.put(
                source,
                table.name,
                window,
                name,
                to_bytes(rows),
                len(rows),
                {"feed_pos_from": first, "feed_pos_to": last},
            )
        except DeliveryExists:
            return None


class DropMover:
    def __init__(self, drop: FileDrop, landing: Landing, business_tz: str):
        self.drop, self.landing, self.tz = drop, landing, ZoneInfo(business_tz)

    def move(self, system: str, drop_dataset: str, source: str, dataset: str) -> list[Delivery]:
        landed = {d.name for d in self.landing.deliveries(source, dataset)}
        out = []
        for f in self.drop.list(system, drop_dataset):
            if f.name in landed:
                continue
            data = self.drop.get(system, drop_dataset, f.name)
            window = datetime.fromisoformat(f.arrived_at).astimezone(self.tz).date().isoformat()
            rows = max(0, data.count(b"\n") - 1)  # a header line, then one line per row
            try:
                out.append(self.landing.put(source, dataset, window, f.name, data, rows, {"arrived_at": f.arrived_at}))
            except DeliveryExists:
                continue
        return out


class DmsMover(ChangeFeedMover):
    """A change-capture mover shaped like a managed database migration service writing to object storage (B4,
    ADR-031): the same changes as ChangeFeedMover, delivered the way such a service writes them. The first delivery
    is a full-load file (LOAD00000001.parquet, no operation column); later ones are change files
    (<yyyymmdd-hhmmss>-<first>.parquet) with the operation in `Op` (I, U, D) and the commit time in `ar_commit_ts`;
    there is no integer feed position (the service orders by its own change sequence; history orders by the
    table's change_seq). The mover keeps its own checkpoint in the manifest, as the service keeps its task state.
    Datasets read with: file_format_data=parquet, cdc_op_column=Op, cdc_position_column='', cdc_commit_column=
    ar_commit_ts (DMS_SETTINGS)."""

    RENAME: ClassVar[dict[str, str]] = {"op": "Op", "committed_at": "ar_commit_ts"}

    def move(self, system: str, source: str, table: TableSpec, window: str) -> Delivery | None:
        import pyarrow as pa  # noqa: PLC0415 -- only the local stack's change-capture stand-in needs it
        import pyarrow.parquet as pq  # noqa: PLC0415

        start = self.checkpoint(source, table.name)
        rows = self.db.feed(system, table, start)
        if not rows:
            return None
        first, last = rows[0]["feed_pos"], rows[-1]["feed_pos"]
        full_load = start == 0
        shaped = []
        for r in rows:
            out = {self.RENAME.get(k, k): v for k, v in r.items() if k != "feed_pos"}
            if full_load:
                out.pop("Op", None)
            shaped.append(out)
        stamp = str(rows[-1].get("committed_at") or window).replace("-", "").replace(":", "").replace("T", "-")[:15]
        name = (f"{table.name}_{window}_LOAD00000001.parquet" if full_load
                else f"{table.name}_{window}_{stamp}-{first:010d}.parquet")
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(shaped), buf)
        try:
            return self.landing.put(source, table.name, window, name, buf.getvalue(), len(rows),
                                    {"feed_pos_from": first, "feed_pos_to": last, "shape": "dms"})
        except DeliveryExists:
            return None


DMS_SETTINGS = {"file_format_data": "parquet", "cdc_op_column": "Op", "cdc_position_column": "",
                "cdc_commit_column": "ar_commit_ts"}
