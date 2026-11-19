"""Access model as code (DPC-0066; SEC-02, ENV-02): groups and classification tags -> grants, masks, row filters.

Table-driven: each row is a group, a table and what that group must see in each column (clear, token, masked) or
None when it cannot read the table at all. SEC-02's test: the lowest-privilege persona reads the most sensitive table
directly and gets nothing it should not.
"""

import pytest

from dataplatform.access.model import (
    CLEAR,
    MASKED,
    TOKEN,
    AccessError,
    can_read,
    model_from_doc,
    plan_access,
    row_visible,
    visible_columns,
)
from dataplatform.governance.tables import ColumnSpec, TableSpec


def tbl(layer, schema, name, domain, cols):
    return TableSpec(
        layer,
        schema,
        name,
        "table",
        domain,
        "o@x",
        "t",
        "",
        "",
        "",
        (),
        tuple(ColumnSpec(c, "string", cls, "") for c, cls in cols),
    )


DEALER = tbl(
    "silver", "crm", "dealer", "sales", [("code", "internal"), ("email", "restricted"), ("margin", "confidential")]
)
INSPECT = tbl("silver", "lims", "inspection", "quality", [("id", "internal"), ("inspector", "restricted")])
FACT = tbl("gold", "sales", "fact_otif", "sales", [("order_id", "internal"), ("otif", "internal")])
DIM = tbl("gold", "sales", "dim_dealer", "sales", [("code", "internal"), ("region", "internal")])
TABLES = [DEALER, INSPECT, FACT, DIM]

MODEL = {
    "groups": [
        {"name": "jobs", "layers": ["bronze", "silver", "gold"], "domains": "all", "protected": "see"},
        {"name": "engineers", "layers": ["silver", "gold"], "domains": "all", "protected": "masked"},
        {"name": "sales-data", "layers": ["silver", "gold"], "domains": ["sales"], "protected": "see"},
        {"name": "quality-data", "layers": ["silver"], "domains": ["quality"], "protected": "see"},
        {"name": "analysts", "layers": ["gold"], "domains": "all", "protected": "masked"},
        {"name": "south-managers", "layers": ["gold"], "domains": ["sales"], "protected": "masked"},
    ],
    "row_filters": [
        {
            "table": "gold.sales.dim_dealer",
            "column": "region",
            "unrestricted": ["jobs", "sales-data"],
            "groups": {"south-managers": ["south"]},
        }
    ],
}
PLAN = plan_access(model_from_doc(MODEL), TABLES)

# case -> (group, table, what it sees per column, or None)
SEES = {
    "lowest privilege cannot read silver at all": ("analysts", DEALER, None),
    "analysts read gold in the clear": ("analysts", FACT, {"order_id": CLEAR, "otif": CLEAR}),
    "domain team sees its restricted column as a token": (
        "sales-data",
        DEALER,
        {"code": CLEAR, "email": TOKEN, "margin": CLEAR},
    ),
    "engineers read everything but protected columns are masked": (
        "engineers",
        DEALER,
        {"code": CLEAR, "email": MASKED, "margin": MASKED},
    ),
    "jobs see tokens, never clear personal data": ("jobs", DEALER, {"code": CLEAR, "email": TOKEN, "margin": CLEAR}),
    "another domain's team cannot read the table": ("quality-data", DEALER, None),
    "quality team sees its inspector token": ("quality-data", INSPECT, {"id": CLEAR, "inspector": TOKEN}),
    "sales team cannot read quality's table": ("sales-data", INSPECT, None),
    "quality team has no gold": ("quality-data", FACT, None),
    "a group nobody declared sees nothing": ("interns", FACT, None),
}


@pytest.mark.parametrize("case", SEES)
def test_visible_columns(case):
    group, table, want = SEES[case]
    assert visible_columns(PLAN, group, table) == want


# case -> (group, region value, sees the row)
ROWS = {
    "unrestricted group sees every region": ("sales-data", "north", True),
    "limited group sees its region": ("south-managers", "south", True),
    "limited group does not see another region": ("south-managers", "north", False),
    "reader not on the filter sees no rows (deny by default)": ("analysts", "south", False),
    "group without read access sees no rows": ("quality-data", "south", False),
}


@pytest.mark.parametrize("case", ROWS)
def test_row_filters(case):
    group, value, want = ROWS[case]
    assert row_visible(PLAN, group, DIM, value) is want


def test_unfiltered_table_shows_every_row_to_readers():
    assert row_visible(PLAN, "analysts", FACT, "anything") is True


def test_grants_go_to_groups_by_layer_or_by_table():
    g = {(x.group, x.privilege, x.scope, x.layer, x.schema, x.name) for x in PLAN.grants}
    assert ("analysts", "read", "catalog", "gold", "", "") in g  # ADR-012: grants by layer are one line
    assert ("sales-data", "read", "table", "silver", "crm", "dealer") in g
    assert ("sales-data", "use", "schema", "silver", "crm", "") in g
    assert not any(x[0] == "sales-data" and x[5] == "inspection" for x in g)
    assert [m.column for m in PLAN.masks] == ["email", "margin", "inspector"]


def test_masks_allow_only_see_groups_that_read_the_table():
    by_col = {m.column: m.allowed for m in PLAN.masks}
    assert by_col == {
        "email": ("jobs", "sales-data"),
        "margin": ("jobs", "sales-data"),
        "inspector": ("jobs", "quality-data"),
    }


MODEL_ERRORS = {
    "group name with capitals": {"groups": [{"name": "Sales", "layers": ["gold"]}]},
    "unknown layer": {"groups": [{"name": "a", "layers": ["platinum"]}]},
    "unknown protected access": {"groups": [{"name": "a", "layers": ["gold"], "protected": "clear"}]},
    "group declared twice": {"groups": [{"name": "a", "layers": ["gold"]}, {"name": "a", "layers": ["silver"]}]},
    "row filter names an unknown group": {
        "groups": [{"name": "a", "layers": ["gold"]}],
        "row_filters": [{"table": "gold.s.t", "column": "c", "unrestricted": ["b"]}],
    },
}


@pytest.mark.parametrize("case", MODEL_ERRORS)
def test_model_from_doc_refuses(case):
    with pytest.raises(AccessError):
        model_from_doc(MODEL_ERRORS[case])


def test_row_filter_on_unpublished_table_refused():
    doc = {
        "groups": [{"name": "a", "layers": ["gold"]}],
        "row_filters": [{"table": "gold.sales.nope", "column": "c", "unrestricted": ["a"]}],
    }
    with pytest.raises(AccessError):
        plan_access(model_from_doc(doc), TABLES)


def test_plan_is_deterministic():
    assert plan_access(model_from_doc(MODEL), list(reversed(TABLES))) == PLAN


# case -> (group, table, can read)
READS = {
    "layer grant covers every table in the layer": ("analysts", FACT, True),
    "table grant covers only its table": ("sales-data", DEALER, True),
    "no grant in that layer": ("analysts", DEALER, False),
}


@pytest.mark.parametrize("case", READS)
def test_can_read(case):
    group, table, want = READS[case]
    assert can_read(PLAN, group, table) is want
