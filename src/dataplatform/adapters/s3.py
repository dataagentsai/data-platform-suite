"""Object storage on Amazon S3 (cloud layer, dbs/cloud/aws.yaml): landing_store, the partner file drop and
object_store. Needs boto3 (the `s3` extra). The client is passed in, so tests use a local stand-in (moto) and the
platform passes one built from the job's role; no keys are ever read here.

Landing layout matches adapters/files.py, so the lander cannot tell the two apart:
    s3://<bucket>/<prefix><source>/<dataset>/<window>/<name>                  the data file, JSON lines
    s3://<bucket>/<prefix><source>/<dataset>/<window>/<name>.manifest.json    the manifest the mover wrote
Bucket names follow the Terraform modules: <name_prefix>-<env>-<layer> (realms/aws-databricks/infra).
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from dataplatform.adapters.files import from_bytes, to_bytes
from dataplatform.cloud_ports import LAYERS
from dataplatform.ingest.ports import DeliveryExists
from dataplatform.keys import checksum
from dataplatform.model import Delivery
from dataplatform.source_ports import DropFile

MANIFEST = ".manifest.json"


def bucket_name(name_prefix: str, env: str, layer: str) -> str:
    """The one naming rule shared with Terraform (modules/storage): <prefix>-<env>-<layer>."""
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
    return f"{name_prefix}-{env}-{layer}"


def _keys(client: Any, bucket: str, prefix: str) -> list[str]:
    out: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += [o["Key"] for o in page.get("Contents", [])]
    return sorted(out)


class S3Landing:
    """landing_store on an S3 bucket (the <env>-landing bucket), the same layout as FileLanding."""

    def __init__(self, client: Any, bucket: str, prefix: str = ""):
        self.s3, self.bucket, self.prefix = client, bucket, prefix

    def _dir(self, source: str, dataset: str) -> str:
        return f"{self.prefix}{source}/{dataset}/"

    def write(self, source: str, dataset: str, window: str, name: str, rows: list[dict[str, Any]]) -> Delivery:
        return self.put(source, dataset, window, name, to_bytes(rows), len(rows))

    # Seven positional parameters on purpose: the landing_store port's put(), shared with FileLanding and called
    # positionally by the movers (local_movers.py) and the port contract tests.
    def put(self, source: str, dataset: str, window: str, name: str, data: bytes, rows: int,  # noqa: PLR0917
            extra: dict[str, Any] | None = None) -> Delivery:
        """Written once: a delivery whose manifest exists is refused (DUP-02)."""
        key = f"{self._dir(source, dataset)}{window}/{name}"
        if _keys(self.s3, self.bucket, key + MANIFEST):
            raise DeliveryExists(f"{source}.{dataset} {window}/{name} is already landed; landing is written once")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        manifest = {**(extra or {}), "source": source, "dataset": dataset, "window": window, "rows": rows,
                    "file": name}
        # The manifest goes last: a delivery without one is never picked up (mover invariant, ORCH-01).
        self.s3.put_object(Bucket=self.bucket, Key=key + MANIFEST, Body=json.dumps(manifest, sort_keys=True).encode())
        return Delivery(source, dataset, window, name, checksum(data), rows)

    def deliveries(self, source: str, dataset: str) -> list[Delivery]:
        out = []
        for key in _keys(self.s3, self.bucket, self._dir(source, dataset)):
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
        body: bytes = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return body


class S3FileDrop:
    """FileDrop (source_ports) on S3: <prefix><system>/<dataset>/<name>, listed with ListObjectsV2. S3 stamps its own
    upload time, so the arrival time the world declares travels as object metadata x-amz-meta-arrived-at (with the
    checksum as x-amz-meta-checksum, so listing never downloads the files)."""

    def __init__(self, client: Any, bucket: str, prefix: str = ""):
        self.s3, self.bucket, self.prefix = client, bucket, prefix

    def _dir(self, system: str, dataset: str) -> str:
        return f"{self.prefix}{system}/{dataset}/"

    def put(self, system: str, dataset: str, name: str, data: bytes, arrived_at: str) -> DropFile:
        if "/" in name or name.startswith("."):
            raise ValueError(f"not a plain file name: {name!r}")
        arrived = _iso(arrived_at)
        f = DropFile(name, arrived, len(data), checksum(data))
        self.s3.put_object(Bucket=self.bucket, Key=self._dir(system, dataset) + name, Body=data,
                           Metadata={"arrived-at": arrived, "checksum": f.checksum})
        return f

    def list(self, system: str, dataset: str) -> list[DropFile]:
        files = []
        for key in _keys(self.s3, self.bucket, self._dir(system, dataset)):
            head = self.s3.head_object(Bucket=self.bucket, Key=key)
            meta = head.get("Metadata", {})
            arrived = meta.get("arrived-at") or head["LastModified"].astimezone(UTC).isoformat()
            files.append(DropFile(key.rsplit("/", 1)[1], arrived, head["ContentLength"], meta.get("checksum", "")))
        return sorted(files, key=lambda f: (f.arrived_at, f.name))

    def get(self, system: str, dataset: str, name: str) -> bytes:
        body: bytes = self.s3.get_object(Bucket=self.bucket, Key=self._dir(system, dataset) + name)["Body"].read()
        return body


class S3ObjectStore:
    """object_store: one bucket per environment and layer, named by bucket_name()."""

    def __init__(self, client: Any, name_prefix: str):
        self.s3, self.name_prefix = client, name_prefix

    def location(self, env: str, layer: str) -> str:
        return f"s3://{bucket_name(self.name_prefix, env, layer)}/"

    def versioned(self, env: str, layer: str) -> bool:
        status = self.s3.get_bucket_versioning(Bucket=bucket_name(self.name_prefix, env, layer)).get("Status")
        return bool(status == "Enabled")


def _iso(ts: str) -> str:
    return datetime.fromisoformat(ts).astimezone(UTC).isoformat()
