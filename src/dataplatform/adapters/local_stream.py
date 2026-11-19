"""EventStream on local files: a log of shards, each an append-only JSON-lines file. A managed stream is a later
adapter with the same port; the shard count and the partition-key hashing mirror how such streams spread records.

Layout: <root>/<stream>/<shard>.jsonl.gz, one line per record: {"seq", "arrived_at", "data"}. Each append adds a
gzip member (a valid gzip file may hold many), so the log stays append-only and about eight times smaller:
a week of press readings is ~4.4 million records.
Sequence numbers are per shard and only grow, so a reader resumes from the last one it saw.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

from dataplatform.source_ports import StreamRecord


class LocalStream:
    def __init__(self, root: str | Path, shards: int = 4):
        self.root = Path(root)
        self.n = shards
        self._last: dict[tuple[str, str], int] = {}

    def _shard_of(self, key: str) -> str:
        return f"shard-{int(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest(), 16) % self.n:03d}"

    def _path(self, stream: str, shard: str) -> Path:
        return self.root / stream / f"{shard}.jsonl.gz"

    def _last_seq(self, stream: str, shard: str) -> int:
        if (stream, shard) not in self._last:
            path, seq = self._path(stream, shard), 0
            if path.exists():
                with gzip.open(path, "rb") as f:
                    for line in f:
                        seq = json.loads(line)["seq"]
            self._last[(stream, shard)] = seq
        return self._last[(stream, shard)]

    def append(self, stream: str, records: list[tuple[str, str, dict[str, Any]]]) -> None:
        lines: dict[str, list[str]] = {}
        for key, arrived_at, data in records:
            shard = self._shard_of(key)
            seq = self._last_seq(stream, shard) + 1
            self._last[(stream, shard)] = seq
            lines.setdefault(shard, []).append(
                json.dumps({"seq": seq, "arrived_at": arrived_at, "data": data}, separators=(",", ":"))
            )
        for shard, ls in lines.items():
            path = self._path(stream, shard)
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "at", compresslevel=1) as f:
                f.write("\n".join(ls) + "\n")

    def shards(self, stream: str) -> list[str]:
        return sorted(p.name.split(".")[0] for p in (self.root / stream).glob("shard-*.jsonl.gz"))

    def read(self, stream: str, shard: str, after_seq: int, limit: int) -> list[StreamRecord]:
        out: list[StreamRecord] = []
        path = self._path(stream, shard)
        if not path.exists():
            return out
        with gzip.open(path, "rt") as f:
            for line in f:
                r = json.loads(line)
                if r["seq"] > after_seq:
                    out.append(StreamRecord(shard, r["seq"], r["arrived_at"], r["data"]))
                    if len(out) >= limit:
                        break
        return out
