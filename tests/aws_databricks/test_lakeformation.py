"""Lake Formation grants from the access model (ADR-028, B12): the same model as Unity Catalog, table-driven."""

import importlib.util
from pathlib import Path

import pytest

from dataplatform.access.model import model_from_doc, plan_access
from dataplatform.governance.tables import ColumnSpec, TableSpec

REPO = Path(__file__).parents[2]
spec = importlib.util.spec_from_file_location("lakeformation", REPO / "realms/aws-databricks/lakeformation.py")
lf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lf)


def tbl(layer, schema, name, domain, cols):
    return TableSpec(layer, schema, name, "table", domain, "o@x", "team", "", "c@1", "", (),
                     tuple(ColumnSpec(c, "string", cls, "") for c, cls in cols))


TABLES = [tbl("gold", "sales", "fact_sales", "sales", [("dealer", ""), ("contact", "restricted")]),
          tbl("gold", "mfg", "fact_scrap", "manufacturing", [("press", "")])]
MODEL = model_from_doc({"groups": [
    {"name": "analysts", "layers": ["gold"], "domains": "all"},
    {"name": "sales-team", "layers": ["gold"], "domains": ["sales"], "protected": "see"},
    {"name": "plant-ops", "layers": ["gold"], "domains": ["manufacturing"]},
]})
PLAN = plan_access(MODEL, TABLES)


def role(group):
    return f"arn:aws:iam::111122223333:role/dp-{group}"


INTENT_CASES = [
    ("a whole-layer reader gets every table of the layer", (role("analysts"), "dev_gold", "*", "SELECT", ""), True),
    ("a domain reader gets its table, protected columns visible to it",
     (role("sales-team"), "dev_gold", "sales__fact_sales", "SELECT", ""), True),
    ("a domain reader gets its own domain only",
     (role("plant-ops"), "dev_gold", "sales__fact_sales", "SELECT", ""), False),
    ("plant ops reads its table with no filter (nothing protected there)",
     (role("plant-ops"), "dev_gold", "mfg__fact_scrap", "SELECT", ""), True),
]


@pytest.mark.parametrize(("case", "row", "present"), INTENT_CASES, ids=[c[0] for c in INTENT_CASES])
def test_intents(case, row, present):
    assert (row in lf.intents(PLAN, "dev", role)) is present, case


def test_protected_column_is_hidden_from_readers_not_allowed():
    model = model_from_doc({"groups": [{"name": "sales-viewers", "layers": ["gold"], "domains": ["sales"]}]})
    plan = plan_access(model, TABLES[:1])
    rows = lf.intents(plan, "dev", role)
    assert (role("sales-viewers"), "dev_gold", "sales__fact_sales", "SELECT",
            "hide_sales-viewers_sales__fact_sales") in rows
    tf = lf.terraform(plan, "dev", role)
    assert 'excluded_column_names = ["contact"]' in tf
    assert tf == lf.terraform(plan, "dev", role)  # deterministic


DRIFT_CASES = [
    ("in step: nothing found", [], [], 0, 0),
    ("a grant removed by hand: missing", [0], [], 1, 0),
    ("a grant added by hand: extra", [], [("arn:aws:iam::1:role/x", "dev_gold", "*", "SELECT", "")], 0, 1),
]


@pytest.mark.parametrize(("case", "remove", "add", "missing", "extra"), DRIFT_CASES, ids=[c[0] for c in DRIFT_CASES])
def test_drift(case, remove, add, missing, extra):
    want = lf.intents(PLAN, "dev", role)
    have = [r for i, r in enumerate(want) if i not in remove] + add
    got = lf.drift(want, have)
    assert (len(got["missing"]), len(got["extra"])) == (missing, extra), case


def test_unsafe_names_refused():
    with pytest.raises(ValueError, match="not a safe name"):
        lf.terraform(PLAN, "dev", lambda g: 'x" ; rm')
