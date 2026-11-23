"""The managed copy service's pipelines for the movers (generate_adf.py): the landing contract (DPC-0038, ADR-033) holds
in what is generated, bad movers are refused, and the output is deterministic. Offline; no factory is called."""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("generate_adf", HERE / "generate_adf.py")
adf = importlib.util.module_from_spec(spec)
sys.modules["generate_adf"] = adf
spec.loader.exec_module(adf)

CSV = {"source": "partner", "dataset": "dealer_sellout", "kind": "csv_files"}
SQL = {"source": "erp", "dataset": "sales_order_line", "kind": "sql_table", "table": "sales_order_line"}
ACCOUNT = "acct123"


def activities(m):
    return {a["name"]: a for a in adf.build([m], ACCOUNT)[f"pipelines/mv_{m['source']}_{m['dataset']}.json"]["properties"]["activities"]}


@pytest.mark.parametrize("m", [CSV, SQL], ids=["csv files", "sql table"])
def test_every_pipeline_copies_then_writes_the_manifest_last(m):
    acts = activities(m)
    assert list(acts) == ["copy", "manifest"]
    assert acts["manifest"]["dependsOn"] == [{"activity": "copy", "dependencyConditions": ["Succeeded"]}]


@pytest.mark.parametrize("m", [CSV, SQL], ids=["csv files", "sql table"])
def test_the_manifest_refuses_a_second_writer_and_signs_in_without_a_key(m):
    web = activities(m)["manifest"]["typeProperties"]
    assert web["method"] == "PUT" and web["headers"]["If-None-Match"] == "*"      # DUP-02: written once
    assert web["authentication"] == {"type": "MSI", "resource": "https://storage.azure.com/"}  # no key anywhere
    url = web["url"]["value"]
    assert f"https://{ACCOUNT}.blob.core.windows.net/landing/{m['source']}/{m['dataset']}/" in url and url.endswith("'.manifest.json')")


@pytest.mark.parametrize("m", [CSV, SQL], ids=["csv files", "sql table"])
def test_the_manifest_carries_what_the_lander_reads(m):
    body = activities(m)["manifest"]["typeProperties"]["body"]["value"]
    for needle in (f'"source":"{m["source"]}"', f'"dataset":"{m["dataset"]}"', '"window":"', "activity('copy').output.rowsCopied", '"file":"'):
        assert needle in body


@pytest.mark.parametrize("m", [CSV, SQL], ids=["csv files", "sql table"])
def test_the_copy_writes_json_lines_one_object_per_line(m):
    sink = activities(m)["copy"]["typeProperties"]["sink"]
    assert sink["type"] == "JsonSink" and sink["formatSettings"]["filePattern"] == "setOfObjects"


@pytest.mark.parametrize(("m", "stem_in_name"), [(CSV, True), (SQL, False)], ids=["a file's name is in the delivery name", "a table has no file name"])
def test_the_delivery_name_is_unique_per_run_so_nothing_landed_is_overwritten(m, stem_in_name):
    name = adf.landing_name(m)
    assert ("parameters.stem" in name) is stem_in_name
    assert "pipeline().RunId" in name and name.endswith(".jsonl')") and "parameters.window" in name


@pytest.mark.parametrize(
    ("m", "message"),
    [
        ({"source": "Partner", "dataset": "d", "kind": "csv_files"}, "lowercase"),
        ({"source": "p", "dataset": "d x", "kind": "csv_files"}, "lowercase"),
        ({"source": "p", "dataset": "d", "kind": "rest"}, "not one of"),
        ({"source": "p", "dataset": "d", "kind": "sql_table"}, "needs a table"),
        ({"source": "p", "dataset": "d", "kind": "sql_table", "table": "t; drop"}, "needs a table"),
        ({"dataset": "d", "kind": "csv_files"}, "'source' is missing"),
    ],
)
def test_a_bad_mover_is_refused_with_a_reason(m, message):
    with pytest.raises(adf.MoverError, match=message):
        adf.build([m], ACCOUNT)


@pytest.mark.parametrize("account", ["Acct", "ab", "a-b", "x" * 25])
def test_a_bad_storage_account_name_is_refused(account):
    with pytest.raises(adf.MoverError, match="not a valid account"):
        adf.build([CSV], account)


def test_two_movers_for_one_dataset_are_refused():
    with pytest.raises(adf.MoverError, match="two movers"):
        adf.build([CSV, dict(CSV)], ACCOUNT)


def test_the_output_is_the_same_every_time_whatever_the_input_order():
    a = adf.render(adf.build([CSV, SQL], ACCOUNT))
    b = adf.render(adf.build([SQL, CSV], ACCOUNT))
    assert a == b and all(json.loads(v) for v in a.values())


def test_the_files_name_the_factory_objects_they_hold():
    for path, text in adf.render(adf.build([CSV, SQL], ACCOUNT)).items():
        assert json.loads(text)["name"] == Path(path).stem


def test_no_key_or_secret_is_written_in_any_generated_file():
    text = "".join(adf.render(adf.build([CSV, SQL], ACCOUNT)).values())
    for bad in ("AccountKey", "SharedAccessKey", "sig=", "password", "connectionString"):
        assert bad.lower() not in text.lower()
