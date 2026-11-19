"""The Genie deploy's pure parts (realms/databricks/semantic_governance/deploy_genie.py). Table-driven."""

import importlib.util
import json
from pathlib import Path

import pytest

HERE = Path(__file__).parents[2] / "realms/databricks/semantic_governance"
spec = importlib.util.spec_from_file_location("deploy_genie", HERE / "deploy_genie.py")
genie = importlib.util.module_from_spec(spec)
spec.loader.exec_module(genie)


def test_payload_carries_the_generated_space_and_the_warehouse():
    doc = json.loads((HERE / "generated/tyre/dev/bi/genie_space.json").read_text())
    body = genie.payload(doc, "wh-1")
    space = json.loads(body["serialized_space"])
    assert body["warehouse_id"] == "wh-1"
    ids = [t["identifier"] for t in space["data_sources"]["tables"]]
    assert ids == sorted(ids)  # the API refuses an unsorted list
    assert all(i.startswith("dev_gold.") for i in ids)


@pytest.mark.parametrize(("case", "spaces", "want"), [
    ("none yet: create", [{"space_id": "a", "title": "Other"}], None),
    ("one with the title: update it", [{"space_id": "a", "title": "Tyre metrics"}], "a"),
])
def test_existing(case, spaces, want):
    assert genie.existing(spaces, "Tyre metrics") == want


def test_two_with_the_title_stop():
    with pytest.raises(SystemExit, match="2 Genie spaces"):
        genie.existing([{"space_id": "a", "title": "T"}, {"space_id": "b", "title": "T"}], "T")
