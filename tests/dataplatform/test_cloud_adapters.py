"""Contract tests for the cloud movers (M2, M5): the S3 and Kinesis adapters pass the same table of cases as the local
adapters of the same port, against a local stand-in for AWS (moto). Nothing here reaches a real account.

The azure rows are different in kind: the Azure Blob adapter runs against tests/dataplatform/azure_fakes.py, a fake
of the SDK surface only. Not against Azurite and not against a real Azure account. Same cases, weaker evidence.

Run: uv run --no-project --with pytest --with pyyaml --with boto3 --with 'moto[s3,kinesis]' \
       env PYTHONPATH=src pytest tests/dataplatform/test_cloud_adapters.py
"""
import json
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from azure_fakes import FakeContainer  # noqa: E402

from dataplatform.adapters.azure_blob import AzureLanding  # noqa: E402
from dataplatform.adapters.files import FileLanding  # noqa: E402
from dataplatform.adapters.kinesis import KinesisStream  # noqa: E402
from dataplatform.adapters.memory import MemoryLog, MemoryMeta, MemoryTables  # noqa: E402
from dataplatform.adapters.s3 import S3Landing, S3ObjectStore, bucket_name  # noqa: E402
from dataplatform.cloud_ports import LAYERS  # noqa: E402
from dataplatform.lander import land  # noqa: E402
from dataplatform.manifest import fill  # noqa: E402
from dataplatform.world.shop import ShopWorld  # noqa: E402

REGION = "eu-west-1"
T = "2026-11-09T0{}:00:00+00:00"
COMPILED = json.loads((Path(__file__).parents[2] / "realms/databricks/compiled/01-orders.json").read_text())
CFG = fill(COMPILED, {"bronze": "b", "silver": "s", "gold": "g"})


@pytest.fixture
def aws(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        yield {"s3": boto3.client("s3", region_name=REGION), "kinesis": boto3.client("kinesis", region_name=REGION)}


def _bucket(s3, name):
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION})
    return name


# ---------------------------------------------------------------- landing_store: local folder and S3, same cases
LANDINGS = {
    "files": lambda tmp, aws: FileLanding(tmp),
    "s3": lambda tmp, aws: S3Landing(aws["s3"], _bucket(aws["s3"], "dp-dev-landing")),
    "s3-prefixed": lambda tmp, aws: S3Landing(aws["s3"], _bucket(aws["s3"], "dp-dev-landing"), prefix="world/"),
    "azure": lambda tmp, aws: AzureLanding(FakeContainer()),
    "azure-prefixed": lambda tmp, aws: AzureLanding(FakeContainer(), prefix="world/"),
}
ROWS_A = [{"order_id": "O1", "amount": 10.0}, {"order_id": "O2", "amount": 5.5}]
ROWS_B = [{"order_id": "O3", "amount": 1.0}]

# name -> (writes as (window, name, rows), expected deliveries as (window, name, declared_rows), same checksum pairs)
LANDING_CASES = {
    "nothing delivered": ([], [], []),
    "one delivery": ([("2026-11-02", "d1.jsonl", ROWS_A)], [("2026-11-02", "d1.jsonl", 2)], []),
    "listed by window then name": (
        [("2026-11-03", "d2.jsonl", ROWS_B), ("2026-11-02", "d1.jsonl", ROWS_A)],
        [("2026-11-02", "d1.jsonl", 2), ("2026-11-03", "d2.jsonl", 1)], [],
    ),
    "a resend under a new name has the same checksum": (
        [("2026-11-02", "d1.jsonl", ROWS_A), ("2026-11-02", "d1_resent.jsonl", ROWS_A)],
        [("2026-11-02", "d1.jsonl", 2), ("2026-11-02", "d1_resent.jsonl", 2)], [(0, 1)],
    ),
    "empty delivery is still a delivery": ([("2026-11-02", "empty.jsonl", [])], [("2026-11-02", "empty.jsonl", 0)], []),
}


@pytest.mark.parametrize("adapter", LANDINGS)
@pytest.mark.parametrize("case", LANDING_CASES)
def test_landing_store(tmp_path, aws, adapter, case):
    store = LANDINGS[adapter](tmp_path, aws)
    writes, expected, same = LANDING_CASES[case]
    written = {(w, n): store.write("shop_db", "orders", w, n, rows) for w, n, rows in writes}
    got = store.deliveries("shop_db", "orders")
    assert [(d.window, d.name, d.declared_rows) for d in got] == expected
    assert all(d == written[(d.window, d.name)] for d in got)                 # write and list agree
    for d in got:
        assert store.read(d) == dict(((w, n), r) for w, n, r in writes)[(d.window, d.name)]
    for i, j in same:
        assert got[i].checksum == got[j].checksum
    assert store.deliveries("shop_db", "other") == []                         # datasets do not leak into each other


# name -> (first write, second write to the same window and name); the second is refused and changes nothing (DUP-02)
TWICE_CASES = {
    "same rows again": (ROWS_A, ROWS_A),
    "different rows under the same name": (ROWS_A, ROWS_B),
    "empty after rows": (ROWS_A, []),
}


@pytest.mark.parametrize("adapter", LANDINGS)
@pytest.mark.parametrize("case", TWICE_CASES)
def test_landing_is_written_once(tmp_path, aws, adapter, case):
    """DUP-02: the second write to the same window and name is refused and changes nothing, on every adapter."""
    from dataplatform.ingest.ports import DeliveryExists

    store = LANDINGS[adapter](tmp_path, aws)
    first, second = TWICE_CASES[case]
    kept = store.write("shop_db", "orders", "2026-11-02", "d1.jsonl", first)
    with pytest.raises(DeliveryExists, match="already landed"):
        store.write("shop_db", "orders", "2026-11-02", "d1.jsonl", second)
    assert store.deliveries("shop_db", "orders") == [kept]
    assert store.read(kept) == first


# adapter -> (store, function that puts a data file with no manifest beside it, the way a half-finished mover would)
def _half_s3(aws):
    store = S3Landing(aws["s3"], _bucket(aws["s3"], "dp-dev-landing"))
    return store, lambda key: aws["s3"].put_object(Bucket="dp-dev-landing", Key=key, Body=b"{}\n")


def _half_azure(aws):
    container = FakeContainer()
    return AzureLanding(container), lambda key: container.upload_blob(key, b"{}\n")


HALF_DELIVERIES = {"s3": _half_s3, "azure": _half_azure}


@pytest.mark.parametrize("adapter", HALF_DELIVERIES)
def test_landing_without_manifest_is_not_a_delivery(aws, adapter):
    """ORCH-01: a data file whose manifest was never written is not a delivery."""
    store, put_half = HALF_DELIVERIES[adapter](aws)
    put_half("shop_db/orders/2026-11-02/half.jsonl")
    assert store.deliveries("shop_db", "orders") == []


# The whole of slice 1's lander on S3: same statuses, day by day, as on a local folder (test_slice1_local.DAYS).
@pytest.mark.parametrize("adapter", ["files", "s3", "azure"])
def test_slice1_lander_on_landing_adapter(tmp_path, aws, adapter):
    landing, meta, tables, log = LANDINGS[adapter](tmp_path, aws), MemoryMeta(), MemoryTables(), MemoryLog()
    statuses = {}
    for out in ShopWorld(CFG["world"]).run(7):
        for window, name, rows in out.deliveries:
            landing.write("shop_db", "orders", window, name, rows)
        statuses[out.day] = sorted(o.status for o in land(CFG["dataset"], CFG["contract"], landing, meta, tables, log,
                                                         f"run-{out.day}", out.day))
    assert statuses == {1: ["loaded"], 2: ["loaded"], 3: ["loaded", "skipped_duplicate"], 4: ["loaded"],
                        5: ["loaded"], 6: ["rejected_contract"], 7: ["loaded"]}


# ---------------------------------------------------------------- Kinesis beyond the shared table
# The shared FileDrop and EventStream cases (test_source_ports.py) run the s3 and kinesis rows; these cover what only a
# managed stream has: the 500-record PutRecords limit, read limits, and the arrival time carried inside the record.
@pytest.fixture
def stream(aws):
    aws["kinesis"].create_stream(StreamName="dp-dev-press_reading", ShardCount=4)
    return KinesisStream(aws["kinesis"], name_prefix="dp-dev-")


@pytest.mark.parametrize("n", [499, 500, 501, 1201])
def test_kinesis_append_splits_into_put_calls(stream, n):
    stream.append("press_reading", [(f"PR{i % 48:02d}", T.format(i % 3), {"i": i}) for i in range(n)])
    got = [r for s in stream.shards("press_reading") for r in stream.read("press_reading", s, 0, 10_000)]
    assert sorted(r.data["i"] for r in got) == list(range(n))
    assert {r.arrived_at for r in got} <= {T.format(h) for h in range(3)}


@pytest.mark.parametrize(("n", "limit"), [(10, 3), (10, 10), (10, 50)])
def test_kinesis_read_limit(stream, n, limit):
    stream.append("press_reading", [("one-key", T.format(1), {"i": i}) for i in range(n)])
    (shard,) = [s for s in stream.shards("press_reading") if stream.read("press_reading", s, 0, 1)]
    assert [r.data["i"] for r in stream.read("press_reading", shard, 0, limit)] == list(range(min(n, limit)))


# ---------------------------------------------------------------- object store: buckets named as Terraform names them
@pytest.mark.parametrize(("env", "layer", "versioning", "url"), [
    ("dev", "landing", "Enabled", "s3://acme-dp-dev-landing/"),
    ("prod", "gold", "Enabled", "s3://acme-dp-prod-gold/"),
    ("dev", "quarantine", None, "s3://acme-dp-dev-quarantine/"),
])
def test_s3_object_store(aws, env, layer, versioning, url):
    name = _bucket(aws["s3"], bucket_name("acme-dp", env, layer))
    if versioning:
        aws["s3"].put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": versioning})
    store = S3ObjectStore(aws["s3"], "acme-dp")
    assert store.location(env, layer) == url
    assert store.versioned(env, layer) is bool(versioning)


@pytest.mark.parametrize("layer", [*LAYERS, "platinum"])
def test_bucket_name_knows_the_layers(layer):
    if layer in LAYERS:
        assert bucket_name("p", "dev", layer) == f"p-dev-{layer}"
    else:
        with pytest.raises(ValueError, match="unknown layer"):
            bucket_name("p", "dev", layer)


# A managed copy service writes JSON lines with a byte-order mark; a Windows tool ends lines with CRLF. Neither changes the
# rows, and the checksum stays over the raw bytes (found on the second cloud's factory, 20 Nov 2026).
@pytest.mark.parametrize(
    ("case", "raw", "rows"),
    [
        ("plain", b'{"a": 1}\n{"a": 2}\n', [{"a": 1}, {"a": 2}]),
        ("byte-order mark", b'\xef\xbb\xbf{"a": 1}\n{"a": 2}\n', [{"a": 1}, {"a": 2}]),
        ("CRLF line ends", b'{"a": 1}\r\n{"a": 2}\r\n', [{"a": 1}, {"a": 2}]),
        ("blank lines and no final newline", b'{"a": 1}\n\n   \n{"a": 2}', [{"a": 1}, {"a": 2}]),
        ("empty file", b"", []),
    ],
)
@pytest.mark.parametrize("adapter", ["files", "azure"])
def test_landing_reads_json_lines_whoever_wrote_them(tmp_path, aws, adapter, case, raw, rows):
    from dataplatform.adapters.files import from_bytes

    assert from_bytes(raw) == rows, case
    landing = LANDINGS[adapter](tmp_path, aws)
    delivery = landing.put("src", "ds", "2026-11-20", "f.jsonl", raw, len(rows))
    assert landing.read(delivery) == rows
