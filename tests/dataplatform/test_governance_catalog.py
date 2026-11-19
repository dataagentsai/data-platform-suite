"""Governance (DPC-0075, CAT-01, SEC-01, SERV-01): contracts -> catalog entries -> a sync plan against the catalog.

Table-driven. The CatalogState port has one contract table (M5, ADR-020) run against every adapter: the memory
adapter, and the Unity Catalog adapter over a fake information schema (the same rows the system tables return).
"""


import pytest

from dataplatform.adapters.memory_catalog import MemoryCatalogState
from dataplatform.adapters.uc_catalog import COLUMN_TAGS, COLUMNS, TABLE_TAGS, TABLES, UcCatalogState
from dataplatform.governance.catalog import Action, apply, desired_entries, plan_sync
from dataplatform.governance.port import Entry, ObjectRef
from dataplatform.governance.tables import (
    SHARED,
    GovernanceError,
    compute_tags,
    consumers,
    owner_findings,
    published_tables,
    table_from_contract,
)


def contract(
    *,
    name="dealer",
    physical="silver_crm.dealer",
    domain="sales",
    owner="sales@x",
    team="sales-data",
    props=None,
    cid=None,
):
    return {
        "id": cid or f"c-{name}",
        "version": "0.1.0",
        "domain": domain,
        "description": {"purpose": "Dealers.", "limitations": "Tokenised contacts."},
        "schema": [
            {
                "name": name,
                "physicalName": physical,
                "properties": props
                if props is not None
                else [
                    {"name": "code", "logicalType": "string", "description": "dealer code"},
                    {"name": "email", "logicalType": "string", "classification": "restricted"},
                    {"name": "margin", "logicalType": "number", "classification": "confidential"},
                ],
            }
        ],
        "team": {"name": team, "members": [{"username": owner, "role": "owner"}]},
    }


GOLD = {
    "fact_a": {
        "kind": "gold_fact",
        "inputs": [{"dataset": "crm.dealer"}, {"dataset": "erp.order"}],
        "dimensions": [{"name": "dim_m"}],
    },
    "fact_b": {"kind": "gold_fact", "inputs": [{"dataset": "erp.order"}], "dimensions": [{"name": "dim_m"}]},
    "dim_m": {"kind": "gold_dimension", "inputs": [{"dataset": "erp.material"}]},
}


# ---------------------------------------------------------------------------------------------------- tables


def test_table_from_contract_reads_names_owner_and_classifications():
    t = table_from_contract(contract(), consumers(GOLD), "crm.dealer")
    assert (t.layer, t.schema, t.name, t.qualified("dev")) == ("silver", "crm", "dealer", "dev_silver.crm.dealer")
    assert (t.owner, t.team, t.contract, t.description) == (
        "sales@x",
        "sales-data",
        "c-dealer@0.1.0",
        "Dealers. Tokenised contacts.",
    )
    assert [(c.name, c.classification, c.protected, c.tokenised) for c in t.columns] == [
        ("code", "internal", False, False),  # SEC-01: unclassified means internal
        ("email", "restricted", True, True),
        ("margin", "confidential", True, False),
    ]
    assert (t.data_product, t.used_by) == ("a", ("a",))


CONTRACT_ERRORS = {
    "physical name without layer": contract(physical="crm.dealer"),
    "unknown layer": contract(physical="platinum_crm.dealer"),
    "unknown classification": contract(props=[{"name": "x", "logicalType": "string", "classification": "secret"}]),
}


@pytest.mark.parametrize("case", CONTRACT_ERRORS)
def test_table_from_contract_refuses(case):
    with pytest.raises(GovernanceError):
        table_from_contract(CONTRACT_ERRORS[case], {}, "crm.dealer")


# dataset key -> the products that read it (directly or through a dimension)
CONSUMERS = {
    "crm.dealer": ("a",),
    "erp.order": ("a", "b"),
    "erp.material": ("a", "b"),  # through dim_m
    "gold.dim_m": ("a", "b"),
    "gold.fact_a": ("a",),
}


@pytest.mark.parametrize("key", CONSUMERS)
def test_consumers(key):
    assert consumers(GOLD)[key] == CONSUMERS[key]


def test_shared_input_is_tagged_shared():
    t = table_from_contract(contract(), consumers(GOLD), "erp.order")
    assert (t.data_product, t.used_by) == (SHARED, ("a", "b"))


def test_compute_tags():
    assert compute_tags("dev", "sales", source="erp", dataset="order", cost_center="cc") == {
        "env": "dev",
        "domain": "sales",
        "source": "erp",
        "dataset": "order",
        "cost_center": "cc",
    }
    assert (
        compute_tags("dev", "sales", source="gold", dataset="fact_a", cost_center="cc", data_product="a")[
            "data_product"
        ]
        == "a"
    )


OWNERS = {
    "same owner": ((contract(), {"owners": {"dataset": "sales@x"}}), []),
    "different owner reported": (
        (contract(), {"owners": {"dataset": "other@x"}}),
        ["c-dealer: contract owner sales@x but manifest owner other@x"],
    ),
    "manifest silent": ((contract(), {}), []),
}


@pytest.mark.parametrize("case", OWNERS)
def test_owner_findings(case):
    pair, want = OWNERS[case]
    assert owner_findings([pair]) == want


# ---------------------------------------------------------------------------------------------------- desired entries

SPECS = published_tables([contract()], [], GOLD)
DESIRED = desired_entries(SPECS)
T = ObjectRef("silver", "crm", "dealer")


def col(c):
    return ObjectRef("silver", "crm", "dealer", c)


DESIRED_CASES = {
    "table owner, contract, domain, layer, product": (
        T,
        {
            "owner": "sales@x",
            "contract": "c-dealer@0.1.0",
            "domain": "sales",
            "layer": "silver",
            "data_product": "a",
            "used_by": "a",
        },
    ),
    "column classification": (col("code"), {"classification": "internal"}),
    "restricted column is tokenised": (col("email"), {"classification": "restricted", "protection": "tokenised"}),
    "confidential column is masked": (col("margin"), {"classification": "confidential", "protection": "masked"}),
}


@pytest.mark.parametrize("case", DESIRED_CASES)
def test_desired_entries(case):
    ref, tags = DESIRED_CASES[case]
    assert DESIRED[ref].tag_map() == tags


def test_desired_comments_come_from_the_contract():
    assert DESIRED[T].comment == "Dealers. Tokenised contacts."
    assert DESIRED[col("code")].comment == "dealer code"
    assert DESIRED[col("email")].comment is None  # no text in the contract: never cleared, never invented


# ---------------------------------------------------------------------------------------------------- the plan


def entry(comment=None, kind="table", **tags):
    return Entry(kind, comment, tuple(sorted(tags.items())))


WANT = {T: entry("Dealers.", owner="a@x", domain="sales")}

# case -> (current, expected actions as (op, column, payload))
PLAN = {
    "empty catalog: write everything": (
        {},
        [("comment", None, "Dealers."), ("set_tags", None, (("domain", "sales"), ("owner", "a@x")))],
    ),
    "in step: nothing to do": ({T: entry("Dealers.", owner="a@x", domain="sales")}, []),
    "owner changed by hand: reset": (
        {T: entry("Dealers.", owner="b@x", domain="sales")},
        [("set_tags", None, (("owner", "a@x"),))],
    ),
    "comment drifted": ({T: entry("old", owner="a@x", domain="sales")}, [("comment", None, "Dealers.")]),
    "someone else's tag stays": ({T: entry("Dealers.", owner="a@x", domain="sales", team_note="x")}, []),
    "our stale tag goes": (
        {T: entry("Dealers.", owner="a@x", domain="sales", certification="draft")},
        [("unset_tags", None, ("certification",))],
    ),
    "undeclared table loses only our tags": (
        {T: WANT[T], ObjectRef("silver", "crm", "old"): entry(owner="a@x", team_note="x")},
        [("unset_tags", None, ("owner",))],
    ),
    "bronze is never touched": ({T: WANT[T], ObjectRef("bronze", "crm", "dealer"): entry(owner="z@x")}, []),
}


@pytest.mark.parametrize("case", PLAN)
def test_plan_sync(case):
    current, want = PLAN[case]
    got = [
        (a.op, a.ref.column, a.comment if a.op == "comment" else (a.tags if a.op == "set_tags" else a.keys))
        for a in plan_sync(WANT, current)
    ]
    assert got == want


@pytest.mark.parametrize("case", PLAN)
def test_plan_converges(case):
    """Applying the plan leaves nothing to do: the realm's statements, run once, close every gap."""
    current, _ = PLAN[case]
    assert plan_sync(WANT, apply(current, plan_sync(WANT, current))) == []


def test_plan_converges_on_real_shapes():
    assert plan_sync(DESIRED, apply({}, plan_sync(DESIRED, {}))) == []


def test_apply_refuses_unknown_action():
    with pytest.raises(ValueError, match="unknown catalog action"):
        apply({}, [Action("drop", T, "table")])


# ---------------------------------------------------------------------------------------------------- port contract


def _uc_rows(by_env):
    """Fake system.information_schema rows for the entries, as the four constant queries return them."""
    tables, columns, ttags, ctags = [], [], [], []
    for env, entries in by_env.items():
        for ref, e in entries.items():
            cat = f"{env}_{ref.layer}"
            if ref.column is None:
                ttype = {"table": "MANAGED", "view": "VIEW", "metric_view": "METRIC_VIEW"}[e.kind]
                tables.append((cat, ref.schema, ref.name, ttype, e.comment))
                ttags += [(cat, ref.schema, ref.name, k, v) for k, v in e.tags]
            else:
                columns.append((cat, ref.schema, ref.name, ref.column, e.comment))
                ctags += [(cat, ref.schema, ref.name, ref.column, k, v) for k, v in e.tags]
    by_sql = {TABLES: tables, COLUMNS: columns, TABLE_TAGS: ttags, COLUMN_TAGS: ctags}

    def query(sql, params):
        wanted = set(params.values())
        return [r for r in by_sql[sql] if r[0] in wanted]

    return query


ADAPTERS = {
    "memory": MemoryCatalogState,
    "unity catalog": lambda by_env: UcCatalogState(_uc_rows(by_env)),
}

V = ObjectRef("gold", "sales", "mv_a")
# case -> (seed {env: entries}, env asked, expected entries)
PORT = {
    "empty": ({}, "dev", {}),
    "table with comment and tags": ({"dev": {T: entry("c", owner="a")}}, "dev", {T: entry("c", owner="a")}),
    "column with tags, no comment": (
        {"dev": {T: entry("c"), col("email"): entry(classification="restricted")}},
        "dev",
        {T: entry("c"), col("email"): entry(classification="restricted")},
    ),
    "metric view keeps its kind": (
        {"dev": {V: entry("v", kind="metric_view", certification="certified")}},
        "dev",
        {V: entry("v", kind="metric_view", certification="certified")},
    ),
    "another environment is not seen": ({"prod": {T: entry("c")}}, "dev", {}),
    "unsynced layers are not returned": ({"dev": {ObjectRef("bronze", "crm", "x"): entry("b")}}, "dev", {}),
}


@pytest.mark.parametrize("adapter", ADAPTERS)
@pytest.mark.parametrize("case", PORT)
def test_catalog_state_port(adapter, case):
    seed, env, want = PORT[case]
    assert dict(ADAPTERS[adapter](seed).entries(env)) == want


@pytest.mark.parametrize("adapter", ADAPTERS)
def test_sync_against_each_adapter_reports_drift(adapter):
    """CAT-01's test: change the catalog by hand; the plan (the drift report) names exactly that change."""
    drifted = dict(apply({}, plan_sync(DESIRED, {})))
    drifted[col("email")] = entry(classification="internal")
    plan = plan_sync(DESIRED, ADAPTERS[adapter]({"dev": drifted}).entries("dev"))
    assert [(a.op, a.ref.column, a.tags) for a in plan if a.op != "comment"] == [
        ("set_tags", "email", (("classification", "restricted"), ("protection", "tokenised")))
    ]


def test_memory_adapter_apply():
    s = MemoryCatalogState()
    s.apply("dev", plan_sync(DESIRED, {}))
    assert plan_sync(DESIRED, s.entries("dev")) == []
