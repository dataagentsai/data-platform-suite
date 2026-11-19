"""The Databricks rendering of the semantic layer, BI, catalog sync, access model and audit (golden files).

The generated files are committed; the first test fails when they differ from the generator (run generate.py).
The rest are table-driven checks on the renderer and on what the tyre slice produces.
"""

import importlib.util
import json
import re
import sys
from fractions import Fraction
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO = Path(__file__).parents[2]
HERE = REPO / "realms/databricks/semantic_governance"
SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
sys.path.insert(0, str(HERE))


def _load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod  # dataclasses look their module up while the class is built
    spec.loader.exec_module(mod)
    return mod


render = _load("render")
gen = _load("generate")

from dataplatform.access.model import ColumnMask  # noqa: E402
from dataplatform.governance.catalog import Action  # noqa: E402
from dataplatform.governance.port import ObjectRef  # noqa: E402

needs_slice = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre slice)")


@pytest.fixture(scope="module")
def files():
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog (the tyre slice)")
    return gen.generate(SLICE)


@pytest.fixture(scope="module")
def built():
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog (the tyre slice)")
    sl = gen.load_slice(SLICE)
    return gen.build(sl, yaml.safe_load((HERE / "access/tyre.yaml").read_text()))


# ---------------------------------------------------------------------------------------------------- golden files


@needs_slice
def test_committed_files_are_up_to_date(files):
    out = HERE / "generated"
    committed = {str(p.relative_to(out)) for p in out.rglob("*") if p.is_file()}
    assert committed == set(files), "run realms/databricks/semantic_governance/generate.py"
    for name, text in files.items():
        assert (out / name).read_text() == text, f"stale {name}: run the generator"


@needs_slice
def test_generation_is_deterministic(files):
    assert gen.generate(SLICE) == files


# ---------------------------------------------------------------------------------------------------- renderer

QUOTING = {
    "plain identifier": (lambda: render.ident("dev_gold", "sales", "mv_otif"), "`dev_gold`.`sales`.`mv_otif`"),
    "identifier with a quote refused": (lambda: render.ident("x`; DROP"), render.RenderError),
    "identifier with a space refused": (lambda: render.ident("a b"), render.RenderError),
    "string with quote and backslash escaped": (lambda: render.lit("it's a\\b"), "'it\\'s a\\\\b'"),
    "group back-quoted": (lambda: render.principal("sales-data"), "`sales-data`"),
    "group with capitals refused": (lambda: render.principal("Sales"), render.RenderError),
    "field name back-quoted": (lambda: render.label("OTIF %"), "`OTIF %`"),
    "field name with back quote refused": (lambda: render.label("a`b"), render.RenderError),
}


@pytest.mark.parametrize("case", QUOTING)
def test_quoting(case):
    fn, want = QUOTING[case]
    if isinstance(want, type):
        with pytest.raises(want):
            fn()
        return
    assert fn() == want


T = ObjectRef("silver", "crm", "dealer")
C = ObjectRef("silver", "crm", "dealer", "email")
V = ObjectRef("gold", "sales", "mv_a")
ACTIONS = {
    "table comment": (
        Action("comment", T, "table", comment="Dealers."),
        "COMMENT ON TABLE `dev_silver`.`crm`.`dealer` IS 'Dealers.';",
    ),
    "column comment": (
        Action("comment", C, "table", comment="mail"),
        "ALTER TABLE `dev_silver`.`crm`.`dealer` ALTER COLUMN `email` COMMENT 'mail';",
    ),
    "view comment comes from its definition": (
        Action("comment", V, "metric_view", comment="x"),
        "-- mv_a: the view's comment is set by its definition",
    ),
    "table tags": (
        Action("set_tags", T, "table", tags=(("owner", "a@x"),)),
        "ALTER TABLE `dev_silver`.`crm`.`dealer` SET TAGS ('owner' = 'a@x');",
    ),
    "column tags": (
        Action("set_tags", C, "table", tags=(("classification", "restricted"),)),
        "ALTER TABLE `dev_silver`.`crm`.`dealer` ALTER COLUMN `email` SET TAGS ('classification' = 'restricted');",
    ),
    "metric view tags": (
        Action("set_tags", V, "metric_view", tags=(("certification", "certified"),)),
        "ALTER VIEW `dev_gold`.`sales`.`mv_a` SET TAGS ('certification' = 'certified');",
    ),
    "unset tags": (
        Action("unset_tags", T, "table", keys=("owner", "used_by")),
        "ALTER TABLE `dev_silver`.`crm`.`dealer` UNSET TAGS ('owner', 'used_by');",
    ),
}


@pytest.mark.parametrize("case", ACTIONS)
def test_catalog_action_sql(case):
    action, want = ACTIONS[case]
    assert render.catalog_action_sql(action, "dev") == want


MASKS = {
    "string restricted, tokenised": (
        ColumnMask("silver", "crm", "dealer", "email", "string", "restricted", True, ("jobs", "sales-data")),
        [
            "RETURNS STRING",
            "is_account_group_member('jobs') OR is_account_group_member('sales-data') THEN v ELSE '***' END",
            "the token (never detokenised)",
            "ALTER COLUMN `email` SET MASK `dev_ops`.`access`.`mask_silver_crm_dealer_email`",
        ],
    ),
    "number confidential masks to NULL": (
        ColumnMask("silver", "crm", "dealer", "margin", "number", "confidential", False, ("sales-data",)),
        ["RETURNS DOUBLE", "ELSE CAST(NULL AS DOUBLE) END", "the stored value"],
    ),
    "nobody allowed": (
        ColumnMask("silver", "crm", "dealer", "x", "string", "restricted", True, ()),
        ["CASE WHEN FALSE THEN v ELSE '***' END"],
    ),
}


@pytest.mark.parametrize("case", MASKS)
def test_mask_sql(case):
    mask, fragments = MASKS[case]
    sql = render._mask_sql(mask, "dev")
    for f in fragments:
        assert f in sql


# ---------------------------------------------------------------------------------------------------- the tyre slice


@needs_slice
@pytest.mark.parametrize("env", ["dev", "prod"])
def test_genie_reads_certified_views_only(files, built, env):
    space = json.loads(files[f"tyre/{env}/bi/genie_space.json"])["space"]
    tables = {t["identifier"] for t in space["data_sources"]["tables"]}
    certified = {f"{env}_gold.{v.domain}.{v.name}" for v in built.published if v.certified_only}
    assert tables == certified
    assert not any(t.endswith("_preview") for t in tables)
    text = " ".join(c for i in space["instructions"]["text_instructions"] for c in i["content"])
    assert "Net sales value" in text
    assert "Tyres cured" in text


@needs_slice
@pytest.mark.parametrize("env", ["dev", "prod"])
def test_dashboards_read_certified_metric_views_only(files, env):
    for name in ("sales_metrics", "manufacturing_metrics"):
        doc = json.loads(files[f"tyre/{env}/bi/{name}.lvdash.json"])
        sources = {m for d in doc["datasets"] for q in d["queryLines"] for m in re.findall(r"FROM (\S+)", q)}
        assert sources
        assert all(re.fullmatch(rf"`{env}_gold`\.`\w+`\.`mv_[a-z_]+`", s) for s in sources)
        assert not any("_preview" in s for s in sources)
        names = [d["name"] for d in doc["datasets"]]
        assert len(names) == len(set(names))


@needs_slice
def test_every_certified_known_answer_is_queried(files, built):
    sql = files["tyre/dev/semantic/known_answers.sql"]
    for v in built.views:
        for m in v.measures:
            if m.certification == "certified":
                assert f"MEASURE(`{m.name}`)" in sql
                assert repr(m.known_answer.value) in sql


@needs_slice
def test_every_metric_view_is_created_with_metrics_yaml(files, built):
    for v in built.published:
        sql = files[f"tyre/dev/semantic/{v.name}.sql"]
        body = sql.split("AS $$\n", 1)[1].rsplit("$$;", 1)[0]
        doc = yaml.safe_load(body)
        assert doc["version"] == 1.1
        assert [m["name"] for m in doc["measures"]] == [m.name for m in v.measures]
        assert doc["source"] == f"dev_gold.{v.source_domain}.{v.source}"


@needs_slice
def test_every_restricted_contract_column_is_masked(files, built):
    sql = files["tyre/dev/access/access.sql"]
    restricted = [(t, c) for t in built.tables for c in t.columns if c.classification == "restricted"]
    assert len(restricted) == 3  # dealer contact email and phone, lab inspector id
    for t, c in restricted:
        assert f"ALTER TABLE {render.table('dev', t.layer, t.schema, t.name)} ALTER COLUMN `{c.name}` SET MASK" in sql


@needs_slice
def test_analysts_get_gold_only(files):
    grants = [line for line in files["tyre/dev/access/access.sql"].splitlines() if line.endswith("TO `analysts`;")]
    assert grants == ["GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG `dev_gold` TO `analysts`;"]


@needs_slice
def test_no_grant_to_a_named_person(files):
    """ENV-02: access to groups only."""
    for env in ("dev", "prod"):
        for line in files[f"tyre/{env}/access/access.sql"].splitlines():
            if line.startswith("GRANT"):
                assert "@" not in line.split(" TO ")[1]


@needs_slice
def test_catalog_sync_tags_certification_and_terms_on_metric_views(files):
    sql = files["tyre/dev/governance/catalog_sync.sql"]
    assert "ALTER VIEW `dev_gold`.`sales`.`mv_otif` SET TAGS ('certification' = 'certified'" in sql
    assert "'glossary_terms' = 'OTIF % | Order lines'" in sql
    assert "ALTER VIEW `dev_gold`.`sales`.`mv_sales_preview` SET TAGS ('certification' = 'draft'" in sql


@needs_slice
def test_findings_report_the_owner_mismatch(files):
    assert "tyre-lims-inspection-result" in files["tyre/findings.md"]


# view -> its identifier in dev, as known_answers.sql queries it (every published view has a known-answer test)
KNOWN_ANSWER_VIEWS = {
    "mv_otif": "`dev_gold`.`sales`.`mv_otif`",
    "mv_sales": "`dev_gold`.`sales`.`mv_sales`",
    "mv_sales_preview": "`dev_gold`.`sales`.`mv_sales_preview`",
    "mv_scrap": "`dev_gold`.`manufacturing`.`mv_scrap`",
    "mv_scrap_preview": "`dev_gold`.`manufacturing`.`mv_scrap_preview`",
}


@needs_slice
@pytest.mark.parametrize("view", KNOWN_ANSWER_VIEWS)
def test_each_published_view_has_known_answer_queries(files, built, view):
    assert view in {v.name for v in built.published}
    assert f"FROM {KNOWN_ANSWER_VIEWS[view]} WHERE" in files["tyre/dev/semantic/known_answers.sql"]


def test_metric_view_joins_declare_cardinality(files):
    """SL-SEM-04: a join to a dimension by surrogate key is many to one, so no measure is counted twice."""
    for name, text in files.items():
        if "/semantic/mv_" in name:
            for j in yaml.safe_load(text.split("AS $$\n", 1)[1].rsplit("$$;", 1)[0]).get("joins", []):
                assert j["cardinality"] == "many_to_one"


def test_no_cost_files_are_generated(files):
    """Cost per data product belongs to the separate cost tool (4 Nov 2026): nothing under <domain>/cost/ is generated."""
    assert not [n for n in files if "/cost/" in n]
