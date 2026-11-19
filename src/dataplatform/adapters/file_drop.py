"""FileDrop on a local folder: the partner and lab drop zones. Object storage is a later adapter with the same port.

Layout: <root>/<system>/<dataset>/<name>. Arrival time is the file's modified time, as object storage keeps it.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from dataplatform.keys import checksum
from dataplatform.source_ports import DropFile


class LocalFileDrop:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _dir(self, system: str, dataset: str) -> Path:
        return self.root / system / dataset

    def put(self, system: str, dataset: str, name: str, data: bytes, arrived_at: str) -> DropFile:
        if "/" in name or name.startswith("."):
            raise ValueError(f"not a plain file name: {name!r}")
        path = self._dir(system, dataset) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        ts = datetime.fromisoformat(arrived_at).timestamp()
        os.utime(path, (ts, ts))
        return DropFile(name, _iso(ts), len(data), checksum(data))

    def list(self, system: str, dataset: str) -> list[DropFile]:
        folder = self._dir(system, dataset)
        if not folder.exists():
            return []
        files = [(p.stat().st_mtime, p) for p in folder.iterdir() if p.is_file()]
        return [DropFile(p.name, _iso(ts), p.stat().st_size, checksum(p.read_bytes())) for ts, p in sorted(files)]

    def get(self, system: str, dataset: str, name: str) -> bytes:
        return (self._dir(system, dataset) / name).read_bytes()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()
