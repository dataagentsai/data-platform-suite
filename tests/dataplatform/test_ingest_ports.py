"""Contract tests for the ingestion ports (ADR-020, M5): every adapter of a port passes the same table of cases.

bronze writer   memory, Delta via delta-rs (needs deltalake + pyarrow), Spark + Delta (DP_TEST_SPARK=1 and pyspark
                with delta-spark; skipped otherwise: a local Spark is heavy for a laptop run)
landing         a local folder, S3 (moto, when boto3 and moto are installed)
tokenizer       the keyed-hash adapter
"""

import os
from datetime import UTC
from pathlib import Path

import pytest

from dataplatform.adapters.files import FileLanding
from dataplatform.adapters.keyed_tokens import KeyedTokenizer
from dataplatform.adapters.memory_bronze import MemoryBronze
from dataplatform.ingest.ports import DeliveryExists

COLS = [("id", "string"), ("qty", "integer"), ("_batch_key", "string"), ("_run_id", "string")]


def rows(key, ids, run="r1"):
    return [{"id": i, "qty": n, "_batch_key": key, "_run_id": run} for n, i in enumerate(ids, 1)]


def _delta(tmp):
    pytest.importorskip("deltalake")
    from dataplatform.adapters.delta_bronze import DeltaBronze

    return DeltaBronze(tmp / "bronze")


def _spark(tmp):
    if os.environ.get("DP_TEST_SPARK") != "1":
        pytest.skip("set DP_TEST_SPARK=1 (with pyspark and delta-spark) to run the Spark bronze writer")
    from pyspark.sql import SparkSession  # pragma: no cover - opt-in

    from dataplatform.adapters.spark_delta import SparkBronzeWriter

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.warehouse.dir", str(tmp / "wh"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS src")
    return SparkBronzeWriter(spark)


WRITERS = {"memory": lambda tmp: MemoryBronze(), "delta": _delta, "spark": _spark}


def seq(*calls):
    def run(w):
        out = None
        for c in calls:
            out = c(w)
        return out

    return run


def write(key, ids, run="r1"):
    return lambda w: w.write("src.ds", key, rows(key, ids, run), COLS)


def count(where=None):
    return lambda w: w.fingerprint("src.ds", where)["row_count"]


BRONZE_CASES = [
    ("a write returns its rows", write("k1", "AB"), 2),
    ("a table never written is empty", count(), 0),
    ("rows are kept", seq(write("k1", "AB"), count()), 2),
    ("the same batch twice changes nothing (DUP-01)", seq(write("k1", "AB"), write("k1", "AB"), count()), 2),
    ("a rewrite replaces only its batch", seq(write("k1", "AB"), write("k2", "C"), write("k1", "A"), count()), 2),
    ("batches side by side", seq(write("k1", "AB"), write("k2", "CDE"), count({"_batch_key": "k2"})), 3),
    ("filter on the run", seq(write("k1", "AB", "r1"), write("k2", "C", "r2"), count({"_run_id": "r2"})), 1),
    ("sums", seq(write("k1", "ABC"), lambda w: w.fingerprint("src.ds", None, ("qty",))["qty"]), 6),
    (
        "keys of one batch",
        seq(write("k1", "AB"), write("k2", "C"), lambda w: w.keys("src.ds", ("id",), {"_batch_key": "k1"})),
        {("A",), ("B",)},
    ),
    ("keys of a table never written", lambda w: w.keys("src.ds", ("id",), {"_batch_key": "k1"}), set()),
    ("an unsafe batch key is refused", write("k1' OR '1'='1", "A"), ValueError),
]


@pytest.mark.parametrize("adapter", list(WRITERS))
@pytest.mark.parametrize(("case", "act", "expect"), BRONZE_CASES, ids=[c[0] for c in BRONZE_CASES])
def test_bronze_writer_contract(tmp_path, adapter, case, act, expect):
    w = WRITERS[adapter](tmp_path)
    if isinstance(expect, type) and issubclass(expect, Exception):
        with pytest.raises(expect):
            act(w)
    else:
        assert act(w) == expect


def test_delta_types_follow_the_contract(tmp_path):
    from datetime import date, datetime

    w = _delta(tmp_path)
    cols = [("t", "timestamp"), ("d", "date"), ("v", "number"), ("b", "boolean"), ("_batch_key", "string")]
    t = datetime(2026, 11, 9, 4, 30, tzinfo=UTC)
    w.write("src.ds", "k1", [{"t": t, "d": date(2026, 11, 12), "v": 2.5, "b": True, "_batch_key": "k1"}], cols)
    assert w.keys("src.ds", ("t", "d", "v", "b"), {"_batch_key": "k1"}) == {(t, date(2026, 11, 12), 2.5, True)}


# ---------------------------------------------------------------- landing: written once (DUP-02)
def _s3(tmp):
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    from dataplatform.adapters.s3 import S3Landing

    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    mock = moto.mock_aws()
    mock.start()
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket="dp-dev-landing", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    return S3Landing(s3, "dp-dev-landing"), mock


LANDINGS = {"files": lambda tmp: (FileLanding(tmp), None), "s3": _s3}


def put(name="f.csv", data=b"a\n1\n", window="2026-11-09", extra=None):
    return lambda s: s.put("lims", "inspection_result", window, name, data, 1, extra)


LANDING_CASES = [
    (
        "a delivery is listed with its bytes",
        seq(put(), lambda s: [(d.window, d.name, d.declared_rows) for d in s.deliveries("lims", "inspection_result")]),
        [("2026-11-09", "f.csv", 1)],
    ),
    (
        "bytes come back exactly",
        seq(put(data=b"x;y\r\n1;2\n"), lambda s: s.read_bytes(s.deliveries("lims", "inspection_result")[0])),
        b"x;y\r\n1;2\n",
    ),
    (
        "the manifest keeps the mover's fields",
        seq(
            put(extra={"feed_pos_to": 9}),
            lambda s: s.manifest(s.deliveries("lims", "inspection_result")[0])["feed_pos_to"],
        ),
        9,
    ),
    ("a second write of the same delivery is refused (DUP-02)", seq(put(), put(data=b"other\n")), DeliveryExists),
    (
        "the refused write changes nothing",
        seq(
            put(),
            lambda s: _try(put(data=b"other\n"), s),
            lambda s: s.read_bytes(s.deliveries("lims", "inspection_result")[0]),
        ),
        b"a\n1\n",
    ),
    (
        "the same name in another window is another delivery",
        seq(put(), put(window="2026-11-10"), lambda s: len(s.deliveries("lims", "inspection_result"))),
        2,
    ),
    (
        "slice 1's row writes are refused the same way",
        seq(
            lambda s: s.write("shop_db", "orders", "w", "d.jsonl", [{"a": 1}]),
            lambda s: s.write("shop_db", "orders", "w", "d.jsonl", [{"a": 2}]),
        ),
        DeliveryExists,
    ),
]


def _try(act, s):
    try:
        act(s)
    except DeliveryExists:
        return None
    raise AssertionError("expected a refusal")


@pytest.mark.parametrize("adapter", list(LANDINGS))
@pytest.mark.parametrize(("case", "act", "expect"), LANDING_CASES, ids=[c[0] for c in LANDING_CASES])
def test_landing_contract(tmp_path, adapter, case, act, expect):
    store, mock = LANDINGS[adapter](tmp_path)
    try:
        if isinstance(expect, type) and issubclass(expect, Exception):
            with pytest.raises(expect):
                act(store)
        else:
            assert act(store) == expect
    finally:
        if mock is not None:
            mock.stop()


# ---------------------------------------------------------------- tokenizer
K = b"0123456789abcdef0123456789abcdef"
TOKEN_CASES = [
    ("same value, same token", lambda t: t.token("restricted", "a@x") == t.token("restricted", "a@x"), True),
    ("another value, another token", lambda t: t.token("restricted", "a@x") == t.token("restricted", "b@x"), False),
    ("the class is part of the token", lambda t: t.token("restricted", "a") == t.token("confidential", "a"), False),
    ("the value is not in the token", lambda t: "a@x" in t.token("restricted", "a@x"), False),
    (
        "another key, another token",
        lambda t: t.token("restricted", "a") == KeyedTokenizer(K[::-1]).token("restricted", "a"),
        False,
    ),
]


@pytest.mark.parametrize(("case", "check", "expect"), TOKEN_CASES, ids=[c[0] for c in TOKEN_CASES])
def test_tokenizer_contract(case, check, expect):
    assert check(KeyedTokenizer(K)) is expect


def test_tokenizer_key_from_a_local_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DP_TOKEN_KEY", raising=False)
    a = KeyedTokenizer.from_env_or_file(tmp_path / "k").token("restricted", "v")
    assert a == KeyedTokenizer.from_env_or_file(tmp_path / "k").token("restricted", "v")
    assert oct((tmp_path / "k").stat().st_mode)[-3:] == "600"
    with pytest.raises(ValueError, match="16 bytes"):
        KeyedTokenizer(b"short")


@pytest.mark.parametrize(
    "path",
    [
        "ingest/__init__.py",
        "ingest/ports.py",
        "ingest/steps.py",
        "ingest/pipelines.py",
        "ingest/runner.py",
        "ingest/daybyday.py",
    ],
)
def test_no_product_names_in_ingestion(path):
    """R1: the ingestion kernel names no product."""
    import re

    text = (Path(__file__).parents[2] / "src/dataplatform" / path).read_text()
    products = (
        r"\b(postgres|postgresql|psycopg|sqlite|delta|deltalake|spark|pyspark|kinesis|kafka|s3|aws|azure|databricks)\b"
    )
    assert not re.findall(products, text, flags=re.IGNORECASE)
