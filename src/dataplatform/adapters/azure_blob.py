"""Object storage on Azure Blob Storage (cloud layer): landing_store, the partner file drop and object_store.
Needs azure-storage-blob (the `azure` extra) only when a real client is built; nothing here imports an Azure SDK. The
container is passed in as an object with the surface of azure.storage.blob.ContainerClient (list_blobs,
upload_blob, download_blob, get_blob_client), built by the platform from the job's managed identity; no keys are
ever read here.

Tested against a fake of that surface (tests/dataplatform/azure_fakes.py) and, 19 Nov 2026, against a real ADLS Gen2
account (landing write/list/read/manifest/duplicate refusal and the file drop). The first real run found that the fake
had a method (container.get_blob_properties) the real ContainerClient lacks; the fake now matches the real surface.

Landing layout matches adapters/files.py and adapters/s3.py, so the lander cannot tell them apart:
    <container>/<prefix><source>/<dataset>/<window>/<name>                  the data file, JSON lines
    <container>/<prefix><source>/<dataset>/<window>/<name>.manifest.json    the manifest the mover wrote
Naming: one storage account per environment, <prefix><env>; one container per layer, named exactly by the layer.
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dataplatform.adapters.files import from_bytes, to_bytes
from dataplatform.cloud_ports import LAYERS
from dataplatform.ingest.ports import DeliveryExists
from dataplatform.keys import checksum
from dataplatform.model import Delivery
from dataplatform.source_ports import DropFile

MANIFEST = ".manifest.json"
_ACCOUNT = re.compile(r"[a-z0-9]{3,24}")


def account_name(prefix: str, env: str) -> str:
    """The storage account for an environment: <prefix><env>. Azure allows lowercase letters and digits only, 3 to 24
    characters, and the name is global across Azure; a name that does not fit is refused, never changed."""
    name = f"{prefix}{env}"
    if not _ACCOUNT.fullmatch(name):
        raise ValueError(f"storage account name {name!r} must be 3 to 24 characters, lowercase letters and digits only")
    return name


def _is_exists(exc: Exception) -> bool:
    return type(exc).__name__ == "ResourceExistsError"  # the SDK class, found by name so the SDK is never imported


def _names(container: Any, prefix: str) -> list[str]:
    return sorted(b.name for b in container.list_blobs(name_starts_with=prefix))


class AzureLanding:
    """landing_store on a blob container (the `landing` container of the environment's account), the same layout and
    semantics as S3Landing."""

    def __init__(self, container: Any, prefix: str = ""):
        self.container, self.prefix = container, prefix

    def _dir(self, source: str, dataset: str) -> str:
        return f"{self.prefix}{source}/{dataset}/"

    def write(self, source: str, dataset: str, window: str, name: str, rows: list[dict[str, Any]]) -> Delivery:
        return self.put(source, dataset, window, name, to_bytes(rows), len(rows))

    # Seven positional parameters on purpose: the landing_store port's put(), shared with FileLanding and called
    # positionally by the movers (local_movers.py) and the port contract tests.
    # shiftleft: allow SL-CLEAN-02 -- the port's put(), the same signature as S3Landing and FileLanding
    def put(self, source: str, dataset: str, window: str, name: str, data: bytes, rows: int,  # noqa: PLR0917
            extra: dict[str, Any] | None = None) -> Delivery:
        """Written once: a delivery whose manifest exists is refused (DUP-02)."""
        key = f"{self._dir(source, dataset)}{window}/{name}"
        refused = DeliveryExists(f"{source}.{dataset} {window}/{name} is already landed; landing is written once")
        if _names(self.container, key + MANIFEST):
            raise refused
        # A data file without a manifest is not a delivery, so a crashed earlier attempt may be overwritten.
        self.container.upload_blob(key, data, overwrite=True)
        manifest = {**(extra or {}), "source": source, "dataset": dataset, "window": window, "rows": rows,
                    "file": name}
        # The manifest goes last (ORCH-01), and never over an existing one: the service refuses a second writer.
        try:
            self.container.upload_blob(key + MANIFEST, json.dumps(manifest, sort_keys=True).encode(), overwrite=False)
        except Exception as exc:
            if _is_exists(exc):
                raise refused from None
            raise
        return Delivery(source, dataset, window, name, checksum(data), rows)

    def deliveries(self, source: str, dataset: str) -> list[Delivery]:
        out = []
        for key in _names(self.container, self._dir(source, dataset)):
            if not key.endswith(MANIFEST):
                continue
            meta = json.loads(self._get(key))
            data = self._get(key.rsplit("/", 1)[0] + "/" + meta["file"])
            out.append(Delivery(source, dataset, meta["window"], meta["file"], checksum(data), meta["rows"]))
        return out

    def read(self, delivery: Delivery) -> list[dict[str, Any]]:
        return from_bytes(self.read_bytes(delivery))

    def read_bytes(self, delivery: Delivery) -> bytes:
        return self._get(f"{self._dir(delivery.source, delivery.dataset)}{delivery.window}/{delivery.name}")

    def manifest(self, delivery: Delivery) -> dict[str, Any]:
        key = f"{self._dir(delivery.source, delivery.dataset)}{delivery.window}/{delivery.name}{MANIFEST}"
        loaded: dict[str, Any] = json.loads(self._get(key))
        return loaded

    def _get(self, key: str) -> bytes:
        data: bytes = self.container.download_blob(key).readall()
        return data


class AzureFileDrop:
    """FileDrop (source_ports) on a blob container: <prefix><system>/<dataset>/<name>. The service stamps its own
    upload time, so the arrival time the world declares travels as blob metadata `arrived_at` (with the checksum as
    `checksum`, so listing never downloads the files). Metadata keys must be valid identifiers, hence underscores."""

    def __init__(self, container: Any, prefix: str = ""):
        self.container, self.prefix = container, prefix

    def _dir(self, system: str, dataset: str) -> str:
        return f"{self.prefix}{system}/{dataset}/"

    def put(self, system: str, dataset: str, name: str, data: bytes, arrived_at: str) -> DropFile:
        if "/" in name or name.startswith("."):
            raise ValueError(f"not a plain file name: {name!r}")
        arrived = _iso(arrived_at)
        f = DropFile(name, arrived, len(data), checksum(data))
        self.container.upload_blob(self._dir(system, dataset) + name, data, overwrite=True,
                                   metadata={"arrived_at": arrived, "checksum": f.checksum})
        return f

    def list(self, system: str, dataset: str) -> list[DropFile]:
        files = []
        for key in _names(self.container, self._dir(system, dataset)):
            props = self.container.get_blob_client(key).get_blob_properties()  # the real ContainerClient has no get_blob_properties of its own
            meta = props.metadata or {}
            arrived = meta.get("arrived_at") or props.last_modified.astimezone(UTC).isoformat()
            files.append(DropFile(key.rsplit("/", 1)[1], arrived, props.size, meta.get("checksum", "")))
        return sorted(files, key=lambda f: (f.arrived_at, f.name))

    def get(self, system: str, dataset: str, name: str) -> bytes:
        data: bytes = self.container.download_blob(self._dir(system, dataset) + name).readall()
        return data


class AzureObjectStore:
    """object_store: one storage account per environment (account_name) and one container per layer, named exactly by
    the layer. The location is the ADLS Gen2 (abfss) URL.

    Versioning is an account-level setting of the management plane (the storage account's blob service properties); the
    data-plane container client cannot read it. So `versioning_of(account_name) -> bool` is injected: the platform
    passes a callable backed by the management API (or by the deployed infrastructure's outputs)."""

    def __init__(self, prefix: str, versioning_of: Callable[[str], bool]):
        self.prefix, self.versioning_of = prefix, versioning_of

    def location(self, env: str, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return f"abfss://{layer}@{account_name(self.prefix, env)}.dfs.core.windows.net/"

    def versioned(self, env: str, layer: str) -> bool:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return bool(self.versioning_of(account_name(self.prefix, env)))


def _iso(ts: str) -> str:
    return datetime.fromisoformat(ts).astimezone(UTC).isoformat()
