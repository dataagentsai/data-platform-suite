"""landing_store on a plain folder. On Databricks the folder is a Unity Catalog volume path, which serverless
Python reads and writes as ordinary files, so the same adapter serves both.

Layout:  <root>/<source>/<dataset>/<window>/<name>            the data file, exactly as delivered
         <root>/<source>/<dataset>/<window>/<name>.manifest.json   what the mover wrote beside it (DPC-0004)

A delivery is written once: a second write of the same window and name is refused (DeliveryExists, DUP-02), so a
replay always reads the bytes the first run read. The manifest is written last; a file without one is not a delivery.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dataplatform.ingest.ports import DeliveryExists
from dataplatform.keys import checksum
from dataplatform.model import Delivery


def to_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Stable serialisation: the same rows always give the same bytes (so a resend has the same checksum)."""
    return "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows).encode()


def from_bytes(data: bytes) -> list[dict[str, Any]]:
    """The rows of a landed JSON-lines file, whoever wrote it. A managed copy service starts its JSON files with a
    UTF-8 byte-order mark (seen on the second cloud's factory, 20 Nov 2026), and a Windows tool ends lines with CRLF;
    neither changes the rows. The checksum is still taken over the raw bytes, so a resend is still the same delivery."""
    return [json.loads(line) for line in data.decode("utf-8-sig").splitlines() if line.strip()]


class FileLanding:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _dir(self, source: str, dataset: str) -> Path:
        return self.root / source / dataset

    def write(self, source: str, dataset: str, window: str, name: str, rows: list[dict[str, Any]]) -> Delivery:
        """Slice 1's movers hand over rows; they land as JSON lines."""
        return self.put(source, dataset, window, name, to_bytes(rows), len(rows))

    def put(self, source: str, dataset: str, window: str, name: str, data: bytes, rows: int,
            extra: dict[str, Any] | None = None) -> Delivery:
        if "/" in name or name.startswith(".") or "/" in window:
            raise ValueError(f"not a plain delivery name: {window}/{name}")
        folder = self._dir(source, dataset) / window
        manifest_path = folder / f"{name}.manifest.json"
        if manifest_path.exists():
            raise DeliveryExists(f"{source}.{dataset} {window}/{name} is already landed; landing is written once")
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_bytes(data)
        manifest = {**(extra or {}), "source": source, "dataset": dataset, "window": window, "rows": rows,
                    "file": name}
        manifest_path.write_text(json.dumps(manifest, sort_keys=True))
        return Delivery(source, dataset, window, name, checksum(data), rows)

    def deliveries(self, source: str, dataset: str) -> list[Delivery]:
        out = []
        for m in sorted(self._dir(source, dataset).glob("*/*.manifest.json")):
            meta = json.loads(m.read_text())
            data = (m.parent / meta["file"]).read_bytes()
            out.append(Delivery(source, dataset, meta["window"], meta["file"], checksum(data), meta["rows"]))
        return out

    def read(self, delivery: Delivery) -> list[dict[str, Any]]:
        return from_bytes(self.read_bytes(delivery))

    def read_bytes(self, delivery: Delivery) -> bytes:
        return (self._dir(delivery.source, delivery.dataset) / delivery.window / delivery.name).read_bytes()

    def manifest(self, delivery: Delivery) -> dict[str, Any]:
        path = self._dir(delivery.source, delivery.dataset) / delivery.window / f"{delivery.name}.manifest.json"
        loaded: dict[str, Any] = json.loads(path.read_text())
        return loaded


class LandingArchive:
    """The landing archive on a local folder (ADR-029): <root>/_archive/<source>/<dataset>/<window>/. Moving a
    delivery keeps its file and manifest together; on a cloud this is a cheaper storage class set by lifecycle rules."""

    def __init__(self, landing: FileLanding):
        self.landing = landing
        self.root = landing.root / "_archive"

    def _dir(self, source: str, dataset: str) -> Path:
        return self.root / source / dataset

    def archive(self, source: str, dataset: str, window: str, name: str) -> None:
        src, dst = self.landing._dir(source, dataset) / window, self._dir(source, dataset) / window
        dst.mkdir(parents=True, exist_ok=True)
        for f in (name, f"{name}.manifest.json"):  # the data first, the manifest last, as when it landed
            (src / f).replace(dst / f)

    def delete(self, source: str, dataset: str, window: str, name: str, place: str) -> None:
        folder = (self._dir(source, dataset) if place == "archive" else self.landing._dir(source, dataset)) / window
        for f in (f"{name}.manifest.json", name):  # the manifest first: a half-deleted delivery is never listed
            (folder / f).unlink(missing_ok=True)

    def listing(self, source: str, dataset: str) -> list[tuple[str, str, str]]:
        """(window, name, place) for every delivery, hot and archived."""
        out = []
        for place, root in (("hot", self.landing._dir(source, dataset)), ("archive", self._dir(source, dataset))):
            for m in sorted(root.glob("*/*.manifest.json")):
                out.append((m.parent.name, m.name.removesuffix(".manifest.json"), place))
        return out
