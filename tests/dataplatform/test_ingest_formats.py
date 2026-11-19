"""Delivered formats and change-capture column names (ADR-031, B3): table-driven, no Spark."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any

import pytest

from dataplatform.ingest.pipelines import _op_value, feed_columns
from dataplatform.ingest.steps import parse

AT = datetime(2026, 11, 1, 10, 30, tzinfo=UTC)


def _parquet(rows: list[dict[str, Any]]) -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


PARSE_CASES = [
    ("json_lines default", b'{"id": 1, "v": "a"}\n{"id": 2, "v": "b"}\n', {},
     [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
    ("csv with header", b"id,v\n1,a\n", {"file_format_data": "csv"}, [{"id": "1", "v": "a"}]),
    ("pipe", b"id|v\n1|a\n", {"file_format_data": "pipe"}, [{"id": "1", "v": "a"}]),
]


@pytest.mark.parametrize(("name", "data", "settings", "want"), PARSE_CASES, ids=[c[0] for c in PARSE_CASES])
def test_parse_text_formats(name: str, data: bytes, settings: dict[str, str], want: list[dict[str, Any]]) -> None:
    assert parse(data, settings) == want


def test_parse_parquet_keeps_types_and_nulls() -> None:
    rows = [{"Op": "I", "id": 1, "amount": 2.5, "at": AT, "note": None},
            {"Op": "D", "id": 2, "amount": None, "at": AT, "note": "x"}]
    got = parse(_parquet(rows), {"file_format_data": "parquet"})
    assert [r["id"] for r in got] == [1, 2]
    assert got[0]["amount"] == 2.5
    assert got[1]["amount"] is None
    assert got[0]["at"] == AT
    assert got[0]["note"] is None


FEED_CASES = [
    ("defaults (W1's own feed)", {}, {"op": "_op", "feed_pos": "_feed_pos", "committed_at": "_committed_at"}),
    ("change-capture service names", {"cdc_op_column": "Op", "cdc_position_column": "", "cdc_commit_column": "ts"},
     {"Op": "_op", "ts": "_committed_at"}),
    ("only the op renamed", {"cdc_op_column": "Op"},
     {"Op": "_op", "feed_pos": "_feed_pos", "committed_at": "_committed_at"}),
]


@pytest.mark.parametrize(("name", "settings", "want"), FEED_CASES, ids=[c[0] for c in FEED_CASES])
def test_feed_columns(name: str, settings: dict[str, str], want: dict[str, str]) -> None:
    assert feed_columns(settings) == want


OP_CASES = [
    ("insert letter", "Op", "I", "I"),
    ("lower case", "Op", "d", "D"),
    ("word", "Op", "update", "U"),
    ("empty is none", "Op", "", None),
    ("other columns untouched", "name", "delete me", "delete me"),
]


@pytest.mark.parametrize(("name", "column", "value", "want"), OP_CASES, ids=[c[0] for c in OP_CASES])
def test_op_values(name: str, column: str, value: Any, want: Any) -> None:
    assert _op_value(column, value, {"Op": "_op"}) == want


class _Feed:
    """A source database stand-in: feed() returns the changes after a position."""

    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows

    def feed(self, system: str, table: Any, after_pos: int = 0) -> list[dict[str, Any]]:
        return [r for r in self.rows if r["feed_pos"] > after_pos]


def test_dms_mover_writes_a_full_load_then_change_files(tmp_path: Any) -> None:
    pytest.importorskip("pyarrow")
    from dataplatform.adapters.files import FileLanding
    from dataplatform.adapters.local_movers import DMS_SETTINGS, DmsMover
    from dataplatform.source_ports import TableSpec

    rows = [{"order_id": 1, "change_seq": 1, "op": "I", "feed_pos": 1, "committed_at": "2026-11-09T10:00:00+00:00"}]
    db = _Feed(rows)
    landing = FileLanding(tmp_path)
    mover = DmsMover(db, landing)  # type: ignore[arg-type]
    table = TableSpec("orders", (("order_id", "integer"),), ("order_id",), change_feed=True)
    first = mover.move("erp_db", "erp", table, "2026-11-09")
    db.rows = [*rows, {"order_id": 1, "change_seq": 2, "op": "D", "feed_pos": 2,
                       "committed_at": "2026-11-10T10:00:00+00:00"}]
    second = mover.move("erp_db", "erp", table, "2026-11-10")
    assert first is not None
    assert second is not None
    assert first.name.endswith("LOAD00000001.parquet")
    assert mover.move("erp_db", "erp", table, "2026-11-10") is None  # nothing new: no empty file
    load = parse(landing.read_bytes(first), DMS_SETTINGS)
    change = parse(landing.read_bytes(second), DMS_SETTINGS)
    assert "Op" not in load[0]  # a full load carries no operation
    assert "feed_pos" not in load[0]
    assert [(r["Op"], r["change_seq"]) for r in change] == [("D", 2)]
    assert feed_columns(DMS_SETTINGS) == {"Op": "_op", "ar_commit_ts": "_committed_at"}
