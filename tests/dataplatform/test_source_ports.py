"""Contract tests for the four mock-source ports (M5): every adapter of a port passes the same table of cases.

Postgres runs when a local server answers (WORLD_PG_TEST_DSN, default dbname=tyre_world_test); otherwise it is skipped.
The S3 file drop and the Kinesis stream (cloud adapters, session W2) run against moto, a local stand-in for AWS, when
boto3 and moto are installed; otherwise they are skipped. The Azure Blob file drop runs against a fake of the SDK
surface only (tests/dataplatform/azure_fakes.py), not against Azurite and not against a real Azure account.
"""

import os
import re
from pathlib import Path

import pytest
from azure_fakes import FakeContainer

from dataplatform.adapters.azure_blob import AzureFileDrop
from dataplatform.adapters.file_drop import LocalFileDrop
from dataplatform.adapters.http_api import HttpRecordClient, LocalRecordStore, serve
from dataplatform.adapters.local_stream import LocalStream
from dataplatform.adapters.memory_sources import MemoryFileDrop, MemoryRecords, MemorySourceDb, MemoryStream
from dataplatform.adapters.sqlite_source import SqliteSource
from dataplatform.source_ports import SourceChange, TableSpec

LINES = TableSpec(
    "order_line",
    (
        ("order_id", "string"),
        ("line_no", "integer"),
        ("promised_date", "date"),
        ("net_value", "number"),
        ("change_seq", "integer"),
    ),
    ("order_id", "line_no"),
    change_feed=True,
)
MATERIALS = TableSpec(
    "material",
    (("material_code", "string"), ("description", "string"), ("extracted_at", "timestamp")),
    ("material_code",),
    full_copy=True,
)
T = "2026-11-09T0{}:00:00+00:00"

try:  # the cloud adapters: only with boto3 + moto (CI and `make -C realms/aws-databricks test` install them)
    import boto3
    import moto

    from dataplatform.adapters.kinesis import KinesisStream
    from dataplatform.adapters.s3 import S3FileDrop
except ImportError:  # pragma: no cover - local runs without the AWS extras
    moto = None
AWS_REGION = "eu-west-1"


@pytest.fixture(autouse=True)
def _aws_stand_in(monkeypatch):
    """Every test runs inside moto when it is installed, so the s3 and kinesis rows never reach a real account."""
    if moto is None:
        yield
        return
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_DEFAULT_REGION": AWS_REGION}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        yield


def _s3_drop(tmp):
    if moto is None:
        pytest.skip("boto3/moto not installed")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.create_bucket(Bucket="dp-dev-landing", CreateBucketConfiguration={"LocationConstraint": AWS_REGION})
    return S3FileDrop(s3, "dp-dev-landing", prefix="drops/")


def _kinesis(tmp):
    if moto is None:
        pytest.skip("boto3/moto not installed")
    k = boto3.client("kinesis", region_name=AWS_REGION)
    k.create_stream(StreamName="dp-dev-press_reading", ShardCount=4)
    return KinesisStream(k, name_prefix="dp-dev-")


KERNEL = Path(__file__).parents[2] / "src/dataplatform"


@pytest.mark.parametrize(
    "path",
    [
        "source_ports.py",
        "world/clock.py",
        "world/dwd.py",
        "world/engine.py",
        "world/domains/__init__.py",
        "world/domains/tyre.py",
        "world/domains/tyre_oracle.py",
    ],
)
def test_no_product_names_outside_adapters(path):
    """R1: products are named only in adapters and realms."""
    products = r"\b(postgres|postgresql|psycopg|sqlite|kinesis|kafka|s3|rds|aws|azure|databricks|sap)\b"
    assert not re.findall(products, (KERNEL / path).read_text(), flags=re.IGNORECASE)


def _postgres():
    try:
        from dataplatform.adapters.postgres_source import PostgresSource

        return PostgresSource.connect(os.environ.get("WORLD_PG_TEST_DSN", "dbname=tyre_world_test"))
    except Exception as exc:  # noqa: BLE001 - any failure to reach a server means skip, with the reason
        pytest.skip(f"no local Postgres: {exc}")


DBS = {"memory": MemorySourceDb, "sqlite": SqliteSource, "postgres": _postgres}


def row(line=10, promised="2026-11-12", value=100.0, seq=1):
    return {"order_id": "SO1", "line_no": line, "promised_date": promised, "net_value": value, "change_seq": seq}


def ch(op, r, at=1, in_feed=True):
    return SourceChange(op, r, r["change_seq"], T.format(at), in_feed)


# name -> (changes applied in this order, expected live rows, expected feed as (op, change_seq))
DB_CASES = {
    "insert": ([ch("I", row())], [row()], [("I", 1)]),
    "newer change wins": (
        [ch("I", row()), ch("U", row(promised="2026-11-13", seq=2), 2)],
        [row(promised="2026-11-13", seq=2)],
        [("I", 1), ("U", 2)],
    ),
    "older change arriving late does not overwrite": (
        [ch("I", row()), ch("U", row(promised="2026-11-14", seq=3), 2), ch("U", row(promised="2026-11-13", seq=2), 3)],
        [row(promised="2026-11-14", seq=3)],
        [("I", 1), ("U", 3), ("U", 2)],
    ),
    "hard delete leaves the feed a D": ([ch("I", row()), ch("D", row(seq=2), 2)], [], [("I", 1), ("D", 2)]),
    "silent loss: in the table, not in the feed": (
        [ch("I", row()), ch("I", row(line=20, seq=2), 2, in_feed=False)],
        [row(), row(line=20, seq=2)],
        [("I", 1)],
    ),
}


@pytest.mark.parametrize("db_name", DBS)
@pytest.mark.parametrize("case", DB_CASES)
def test_source_database(db_name, case):
    db, system = DBS[db_name](), "contract_test"
    changes, live, feed = DB_CASES[case]
    db.ensure(system, [LINES], reset=True)
    db.apply(system, LINES, changes)
    assert db.rows(system, LINES) == live
    got = db.feed(system, LINES)
    assert [(r["op"], r["change_seq"]) for r in got] == feed
    assert [r["feed_pos"] for r in got] == sorted(r["feed_pos"] for r in got)
    if got:
        assert [r["change_seq"] for r in db.feed(system, LINES, got[0]["feed_pos"])] == [s for _, s in feed[1:]]


@pytest.mark.parametrize("db_name", DBS)
def test_source_database_full_copies(db_name):
    db, system = DBS[db_name](), "contract_test"
    db.ensure(system, [MATERIALS], reset=True)
    assert db.copies(system, MATERIALS) == []
    a = [{"material_code": f"M{i}", "description": "x"} for i in range(3)]
    db.publish_copy(system, MATERIALS, T.format(2), a[:2])
    db.publish_copy(system, MATERIALS, T.format(1), a)
    assert db.copies(system, MATERIALS) == [T.format(1), T.format(2)]  # W1b: oldest first, whatever the publish order
    assert [r["material_code"] for r in db.copy(system, MATERIALS, T.format(1))] == ["M0", "M1", "M2"]
    assert [r["material_code"] for r in db.copy(system, MATERIALS, T.format(2))] == ["M0", "M1"]
    assert all(r["extracted_at"] == T.format(2) for r in db.copy(system, MATERIALS, T.format(2)))


# ---------------------------------------------------------------- file drop
DROPS = {"memory": lambda tmp: MemoryFileDrop(), "local": lambda tmp: LocalFileDrop(tmp), "s3": _s3_drop,
         "azure": lambda tmp: AzureFileDrop(FakeContainer(), prefix="drops/")}


@pytest.mark.parametrize("drop_name", DROPS)
@pytest.mark.parametrize(
    "puts,listed",
    [
        ([("a.csv", b"x\n1\n", 2), ("b.csv", b"x\n2\n", 1)], ["b.csv", "a.csv"]),  # listed in arrival order
        ([("a.csv", b"x\n1\n", 1), ("a_resent.csv", b"x\n1\n", 3)], ["a.csv", "a_resent.csv"]),
    ],
)
def test_file_drop(tmp_path, drop_name, puts, listed):
    drop = DROPS[drop_name](tmp_path)
    for name, data, hour in puts:
        assert drop.put("partner_drop", "p.sellout", name, data, T.format(hour)).size == len(data)
    files = drop.list("partner_drop", "p.sellout")
    assert [f.name for f in files] == listed
    assert [f.arrived_at for f in files] == sorted(T.format(h) for *_, h in puts)
    assert drop.get("partner_drop", "p.sellout", listed[0]) == dict((n, d) for n, d, _ in puts)[listed[0]]
    if len({d for _, d, _ in puts}) == 1:  # a resend has the same checksum, whatever its name
        assert len({f.checksum for f in files}) == 1


@pytest.mark.parametrize("drop_name", ["local", "s3", "azure"])
@pytest.mark.parametrize("name", ["../escape.csv", "a/b.csv", ".hidden"])
def test_file_drop_refuses_paths(tmp_path, drop_name, name):
    with pytest.raises(ValueError):
        DROPS[drop_name](tmp_path).put("s", "d", name, b"", T.format(1))


# ---------------------------------------------------------------- record interface
@pytest.fixture(params=["memory", "store", "http"])
def records(request):
    if request.param == "memory":
        yield MemoryRecords()
    else:
        store = LocalRecordStore()
        if request.param == "store":
            yield store
        else:
            server = serve(store)
            yield HttpRecordClient(f"http://127.0.0.1:{server.server_address[1]}", store)
            server.shutdown()


@pytest.mark.parametrize("n,limit,pages", [(0, 200, 0), (199, 200, 1), (450, 200, 3), (450, 1000, 3), (5, 2, 3)])
def test_record_interface_pages(records, n, limit, pages):
    for i in range(n):  # many share a timestamp: the key breaks the tie
        records.upsert("dealer_api", "dealer", f"D-{i:04d}", {"dealer_code": f"D-{i:04d}"}, T.format(i % 3))
    seen, cursor, count = [], "", 0
    while True:
        items, cursor = records.page("dealer_api", "dealer", cursor, limit)
        if not items:
            break
        assert len(items) <= 200
        seen += [r["dealer_code"] for r in items]
        count += 1
    assert count == pages and len(seen) == n and len(set(seen)) == n


def test_record_interface_update_moves_past_cursor(records):
    records.upsert("dealer_api", "dealer", "D-1", {"v": 1}, T.format(1))
    records.upsert("dealer_api", "dealer", "D-2", {"v": 1}, T.format(1))
    _, cursor = records.page("dealer_api", "dealer", "", 200)
    records.upsert("dealer_api", "dealer", "D-1", {"v": 2}, T.format(2))
    assert records.page("dealer_api", "dealer", cursor, 200)[0] == [{"v": 2}]


# ---------------------------------------------------------------- event stream
STREAMS = {"memory": lambda tmp: MemoryStream(), "local": lambda tmp: LocalStream(tmp), "kinesis": _kinesis}


@pytest.mark.parametrize("stream_name", STREAMS)
@pytest.mark.parametrize("keys", [["P1"] * 5, ["P1", "P2", "P3", "P1", "P2", "P9", "P4"]])
def test_event_stream(tmp_path, stream_name, keys):
    s = STREAMS[stream_name](tmp_path)
    s.append("press_reading", [(k, T.format(1), {"k": k, "i": i}) for i, k in enumerate(keys)])
    got = {shard: s.read("press_reading", shard, 0, 1000) for shard in s.shards("press_reading")}
    assert sum(len(v) for v in got.values()) == len(keys)
    for recs in got.values():
        seqs = [r.seq for r in recs]  # positions only grow; local streams count 1, 2, 3, Kinesis uses large numbers
        assert seqs == sorted(set(seqs))
        by_key = {}
        for r in recs:
            by_key.setdefault(r.data["k"], []).append(r.data["i"])
        assert all(v == sorted(v) for v in by_key.values())  # one key's records keep their order
    shard, recs = next((sh, r) for sh, r in got.items() if r)  # a managed stream also lists shards still empty
    assert s.read("press_reading", shard, recs[0].seq, 1000) == recs[1:]


def test_memory_and_local_stream_shard_alike(tmp_path):
    keys = [f"CHN-PR{i:02d}" for i in range(1, 49)]
    a, b = MemoryStream(), LocalStream(tmp_path)
    for s in (a, b):
        s.append("r", [(k, T.format(1), {"k": k}) for k in keys])
    assert {sh: [r.data for r in a.read("r", sh, 0, 99)] for sh in a.shards("r")} == {
        sh: [r.data for r in b.read("r", sh, 0, 99)] for sh in b.shards("r")
    }
