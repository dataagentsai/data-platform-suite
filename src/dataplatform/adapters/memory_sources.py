"""The four source ports in memory: the reference behaviour every real adapter is tested against (M5)."""

from __future__ import annotations

from typing import Any

from dataplatform.keys import checksum
from dataplatform.source_ports import DropFile, Row, SourceChange, StreamRecord, TableSpec


class MemorySourceDb:
    def __init__(self) -> None:
        self.live: dict[tuple[str, str], dict[tuple[Any, ...], Row]] = {}
        self.feeds: dict[tuple[str, str], list[Row]] = {}
        self.copy_rows: dict[tuple[str, str], list[Row]] = {}

    def ensure(self, system: str, tables: list[TableSpec], reset: bool = False) -> None:
        for t in tables:
            k = (system, t.name)
            if reset or k not in self.live:
                self.live[k], self.feeds[k], self.copy_rows[k] = {}, [], []

    def apply(self, system: str, table: TableSpec, changes: list[SourceChange]) -> None:
        live, names = self.live[(system, table.name)], [c for c, _ in table.columns]
        for c in changes:
            k = tuple(c.row[x] for x in table.key)
            if c.op == "D":
                live.pop(k, None)
            elif k not in live or "change_seq" not in names or live[k]["change_seq"] < c.row["change_seq"]:
                live[k] = {n: c.row.get(n) for n in names}
            if table.change_feed and c.in_feed:
                feed = self.feeds[(system, table.name)]
                feed.append(
                    {
                        "feed_pos": len(feed) + 1,
                        "op": c.op,
                        "committed_at": c.committed_at,
                        **{n: c.row.get(n) for n in names},
                    }
                )

    def publish_copy(self, system: str, table: TableSpec, extracted_at: str, rows: list[Row]) -> None:
        names = [c for c, _ in table.columns]
        self.copy_rows[(system, table.name)] += [
            {n: {**r, "extracted_at": extracted_at}.get(n) for n in names} for r in rows
        ]

    def rows(self, system: str, table: TableSpec) -> list[Row]:
        return [v for _, v in sorted(self.live[(system, table.name)].items())]

    def feed(self, system: str, table: TableSpec, after_pos: int = 0) -> list[Row]:
        return [r for r in self.feeds[(system, table.name)] if r["feed_pos"] > after_pos]

    def copy(self, system: str, table: TableSpec, extracted_at: str) -> list[Row]:
        rows = [r for r in self.copy_rows[(system, table.name)] if r["extracted_at"] == extracted_at]
        return sorted(rows, key=lambda r: tuple(r[k] for k in table.key))

    def copies(self, system: str, table: TableSpec) -> list[str]:
        return sorted({r["extracted_at"] for r in self.copy_rows[(system, table.name)]})


class MemoryFileDrop:
    def __init__(self) -> None:
        self.files: dict[tuple[str, str], dict[str, tuple[bytes, str]]] = {}

    def put(self, system: str, dataset: str, name: str, data: bytes, arrived_at: str) -> DropFile:
        self.files.setdefault((system, dataset), {})[name] = (data, arrived_at)
        return DropFile(name, arrived_at, len(data), checksum(data))

    def list(self, system: str, dataset: str) -> list[DropFile]:
        items = sorted(self.files.get((system, dataset), {}).items(), key=lambda kv: (kv[1][1], kv[0]))
        return [DropFile(n, at, len(d), checksum(d)) for n, (d, at) in items]

    def get(self, system: str, dataset: str, name: str) -> bytes:
        return self.files[(system, dataset)][name][0]


class MemoryRecords:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str, str], tuple[str, Row]] = {}

    def upsert(self, system: str, resource: str, key: str, record: Row, updated_at: str) -> None:
        self.records[(system, resource, key)] = (updated_at, record)

    def page(self, system: str, resource: str, cursor: str, limit: int) -> tuple[list[Row], str]:
        ts, _, key = cursor.partition("|")
        mine = sorted((u, k, r) for (s, res, k), (u, r) in self.records.items() if (s, res) == (system, resource))
        after = [(u, k, r) for u, k, r in mine if (u, k) > (ts, key)][: max(1, min(limit, 200))]
        return ([r for *_, r in after], f"{after[-1][0]}|{after[-1][1]}") if after else ([], cursor)


class MemoryStream:
    def __init__(self, shards: int = 4) -> None:
        from dataplatform.adapters.local_stream import LocalStream

        self._router = LocalStream(".", shards)._shard_of  # same partitioning as the file adapter
        self.log: dict[tuple[str, str], list[StreamRecord]] = {}

    def append(self, stream: str, records: list[tuple[str, str, Row]]) -> None:
        for key, at, data in records:
            shard = self._router(key)
            log = self.log.setdefault((stream, shard), [])
            log.append(StreamRecord(shard, len(log) + 1, at, data))

    def shards(self, stream: str) -> list[str]:
        return sorted(s for st, s in self.log if st == stream)

    def read(self, stream: str, shard: str, after_seq: int, limit: int) -> list[StreamRecord]:
        return [r for r in self.log.get((stream, shard), []) if r.seq > after_seq][:limit]
