"""The lander steps (dataplatform.ingest.steps), table-driven: each row is one input and what the step must say."""

from datetime import UTC, date, datetime

import pytest

from dataplatform.adapters.keyed_tokens import KeyedTokenizer
from dataplatform.adapters.memory_bronze import MemoryBronze
from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.ingest.steps import (
    Batch,
    Ctx,
    Spec,
    cast_rows,
    check_columns,
    content_key,
    enter,
    mass_delete_problem,
    parse,
    protect,
    volume_problem,
)
from dataplatform.meta.port import Column, SettingDef

COLS = (
    Column("id", "string", True, 1),
    Column("qty", "integer", True),
    Column("note", "string"),
    Column("email", "string", False, None, "", "restricted"),
)


def spec(read="files", new_columns="hold_in_bronze", band=(1, 10), mass=None, columns=COLS, settings=None):
    load = {"new_columns": new_columns, **({"mass_delete_max_share": mass} if mass is not None else {})}
    return Spec(
        "src",
        "ds",
        "push" if read in ("files", "change_feed") else "pull",
        read,
        columns,
        ("id",),
        settings or {"file_format_data": "csv", "csv_delimiter": ";"},
        load,
        band,
        "fp",
    )


# ---------------------------------------------------------------- parse
@pytest.mark.parametrize(
    ("case", "data", "settings", "expect"),
    [
        (
            "csv with the declared delimiter",
            b"id;qty\nA;1\n",
            {"file_format_data": "csv", "csv_delimiter": ";"},
            [{"id": "A", "qty": "1"}],
        ),
        ("tsv", b"id\tqty\nA\t1\n", {"file_format_data": "tsv"}, [{"id": "A", "qty": "1"}]),
        ("json lines by default", b'{"id": "A"}\n\n{"id": "B"}\n', {}, [{"id": "A"}, {"id": "B"}]),
        ("header only", b"id;qty\n", {"file_format_data": "csv", "csv_delimiter": ";"}, []),
    ],
)
def test_parse(case, data, settings, expect):
    assert parse(data, settings) == expect


def test_parse_refuses_csv_without_header():
    with pytest.raises(ValueError, match="header"):
        parse(b"A;1\n", {"file_format_data": "csv", "csv_has_header": "false"})


# ---------------------------------------------------------------- columns (SCH-01, SCH-02)
@pytest.mark.parametrize(
    ("case", "rows", "new_columns", "reason_has", "extra"),
    [
        ("all contract columns", [{"id": "A", "qty": 1, "note": "", "email": "x"}], "hold_in_bronze", "", []),
        (
            "an optional column renamed stops the batch, naming it",
            [{"id": "A", "qty": 1, "remark": "", "email": "x"}],
            "hold_in_bronze",
            "missing contract column(s) note",
            ["remark"],
        ),
        (
            "the renamed column is named as new",
            [{"id": "A", "qty": 1, "remark": "", "email": "x"}],
            "hold_in_bronze",
            "new column(s) seen: remark",
            ["remark"],
        ),
        (
            "a new column is held in bronze",
            [{"id": "A", "qty": 1, "note": "", "email": "x", "colour": "red"}],
            "hold_in_bronze",
            "",
            ["colour"],
        ),
        (
            "a new column stops the batch when the manifest says stop",
            [{"id": "A", "qty": 1, "note": "", "email": "x", "colour": "red"}],
            "stop",
            "new_columns: stop",
            ["colour"],
        ),
        (
            "position columns are not contract columns",
            [{"id": "A", "qty": 1, "note": "", "email": "x", "_op": "I"}],
            "stop",
            "",
            [],
        ),
    ],
)
def test_check_columns(case, rows, new_columns, reason_has, extra):
    reason, seen = check_columns(spec(new_columns=new_columns), rows)
    assert (reason_has in reason) if reason_has else reason == ""
    assert seen == extra


# ---------------------------------------------------------------- types
TS = datetime(2026, 11, 9, 4, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("case", "columns", "row", "expect", "reason_has"),
    [
        (
            "csv text to contract types",
            [("q", "integer"), ("v", "number"), ("t", "timestamp"), ("d", "date")],
            {"q": "3", "v": "2.5", "t": "2026-11-09T04:30:00+00:00", "d": "2026-11-12"},
            {"q": 3, "v": 2.5, "t": TS, "d": date(2026, 11, 12)},
            "",
        ),
        (
            "empty text is null except for strings",
            [("q", "integer"), ("s", "string")],
            {"q": "", "s": ""},
            {"q": None, "s": ""},
            "",
        ),
        ("a naive time is UTC", [("t", "timestamp")], {"t": "2026-11-09T04:30:00"}, {"t": TS}, ""),
        ("booleans", [("b", "boolean")], {"b": "TRUE"}, {"b": True}, ""),
        ("a retyped column stops the batch, naming it", [("q", "integer")], {"q": "three"}, None, "column q"),
        ("a fraction is not a whole number", [("q", "integer")], {"q": 2.5}, None, "column q"),
    ],
)
def test_cast_rows(case, columns, row, expect, reason_has):
    rows = [dict(row)]
    reason = cast_rows(columns, rows)
    if reason_has:
        assert reason_has in reason
    else:
        assert reason == "" and rows == [expect]


# ---------------------------------------------------------------- volume (DPC-0013) and mass delete (DEL-04)
@pytest.mark.parametrize(
    ("case", "declared", "n", "band", "bad"),
    [
        ("inside the band", 5, 5, (1, 10), False),
        ("declared count differs", 6, 5, (1, 10), True),
        ("empty is held even with min 0", None, 0, (0, 10), True),
        ("under the minimum", None, 2, (3, 10), True),
        ("over the maximum", None, 11, (1, 10), True),
        ("no maximum", None, 10**6, (1, None), False),
    ],
)
def test_volume(case, declared, n, band, bad):
    assert bool(volume_problem(spec(band=band), Batch("w", "f", b"", None, declared), n)) is bad


@pytest.mark.parametrize(
    ("case", "read", "previous", "now", "limit", "held"),
    [
        ("first copy", "full_copy", None, ["A"], 0.05, False),
        ("nothing missing", "full_copy", {("A",), ("B",)}, ["A", "B", "C"], 0.05, False),
        ("40% missing", "full_copy", {(k,) for k in "ABCDEFGHIJ"}, list("ABCDEF"), 0.05, True),
        ("at the limit", "full_copy", {(k,) for k in "ABCDEFGHIJ"}, list("ABCDEFGHI"), 0.10, False),
        ("not a full copy", "files", {("A",), ("B",)}, [], 0.05, False),
        ("no limit declared", "full_copy", {("A",), ("B",)}, [], None, False),
    ],
)
def test_mass_delete(case, read, previous, now, limit, held):
    reason = mass_delete_problem(spec(read=read, mass=limit), [{"id": k} for k in now], previous)
    assert bool(reason) is held


# ---------------------------------------------------------------- protect (DPC-0070)
KEY = b"k" * 32


@pytest.mark.parametrize(
    ("case", "value", "tokenised"),
    [
        ("a restricted value is replaced", "owner@dealer.example", True),
        ("an empty value stays empty", "", False),
        ("a missing value stays missing", None, False),
    ],
)
def test_protect(case, value, tokenised):
    rows = [{"id": "A", "email": value}]
    n = protect(spec(), rows, KeyedTokenizer(KEY))
    assert n == int(tokenised)
    assert (
        (rows[0]["email"] != value and rows[0]["email"].startswith("tok_")) if tokenised else rows[0]["email"] == value
    )
    assert rows[0]["id"] == "A"


def test_content_key_ignores_names():
    assert content_key(b"same bytes") == content_key(b"same bytes") != content_key(b"other bytes")


# ---------------------------------------------------------------- enter: every outcome is logged with the run
def ctx():
    store = MemoryMetaStore()
    store.register_settings([SettingDef("csv_delimiter", "dataset", "string")])
    store.load_domain({"domain": "d", "settings": {}, "manifest": {}, "fingerprint": "x"})
    store.load_dataset(
        {
            "domain": "d",
            "source_name": "src",
            "dataset_name": "ds",
            "is_enabled": True,
            "kind": "dataset",
            "archetypes": [],
            "ingest_mode": "push",
            "ingest_read": "files",
            "contract_id": "c",
            "contract_version": "1",
            "fingerprint": "fp",
            "settings": {},
            "columns": [c.__dict__ for c in COLS],
            "business_keys": {"primary": ["id"]},
            "manifest": {},
        }
    )
    store.start_run("r1", "d", "dev")
    store.start_dataset_run("r1", "src", "ds", "fp", {})
    return Ctx(spec(), store, MemoryBronze(), KeyedTokenizer(KEY), "r1")


GOOD = b"id;qty;note;email\nA;1;;a@x.example\nB;2;hi;\n"


@pytest.mark.parametrize(
    ("case", "batches", "statuses", "bronze_rows"),
    [
        ("a good file loads", [GOOD], ["loaded"], 2),
        ("the same bytes again are skipped (DUP-01)", [GOOD, GOOD], ["loaded", "skipped_duplicate"], 2),
        ("a renamed column is stopped (SCH-02)", [b"id;qty;remark;email\nA;1;;x\n"], ["rejected_contract"], 0),
        ("a retyped value is stopped (SCH-02)", [b"id;qty;note;email\nA;one;;x\n"], ["rejected_contract"], 0),
        ("an empty file is held (DPC-0013)", [b"id;qty;note;email\n"], ["rejected_volume"], 0),
    ],
)
def test_enter(case, batches, statuses, bronze_rows):
    c = ctx()
    got = [enter(Batch("2026-11-09", f"f{i}.csv", data), c).status for i, data in enumerate(batches)]
    assert got == statuses
    assert c.writer.fingerprint("src.ds")["row_count"] == bronze_rows
    assert [f.status for f in c.store.dataset_files("src", "ds")] == statuses  # every outcome is in the file log


def test_enter_stamps_and_protects():
    c = ctx()
    enter(Batch("2026-11-09", "f.csv", GOOD), c)
    rows = c.writer.tables["src.ds"]
    assert {r["_run_id"] for r in rows} == {"r1"} and {r["_source_file"] for r in rows} == {"f.csv"}
    assert {r["_batch_key"] for r in rows} == {content_key(GOOD)} and {r["_window"] for r in rows} == {"2026-11-09"}
    assert rows[0]["email"].startswith("tok_") and rows[1]["email"] == "" and rows[0]["qty"] == 1
