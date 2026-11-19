"""Unity Catalog table names (UcIcebergStore.name): <env>_<layer>.<schema>.<table>, as the contracts' physicalName
says (ADR-012); a schema that repeats the layer loses the prefix, so gold lands where the metric views look."""

import pytest

pytest.importorskip("pyspark")  # the store module imports Spark types at load; the CI test job runs without Spark

from dataplatform.adapters.table_store import UcIcebergStore, cluster_clause, layer_properties


def store(env="dev"):
    s = UcIcebergStore.__new__(UcIcebergStore)  # naming needs no session
    s.env = env
    return s


@pytest.mark.parametrize(
    ("case", "layer", "table", "expect"),
    [
        ("gold drops its repeated prefix", "gold", "gold_sales__fact_otif", "`dev_gold`.`sales`.`fact_otif`"),
        ("gold manufacturing", "gold", "gold_manufacturing__dim_press", "`dev_gold`.`manufacturing`.`dim_press`"),
        ("listed name round-trips", "gold", "sales__fact_otif", "`dev_gold`.`sales`.`fact_otif`"),
        ("silver keeps its source schema", "silver", "erp__sales_order_line", "`dev_silver`.`erp`.`sales_order_line`"),
        ("a schema that only starts like the layer", "silver", "silverline__x", "`dev_silver`.`silverline`.`x`"),
        ("bronze", "bronze", "press_stream__press_reading", "`dev_bronze`.`press_stream`.`press_reading`"),
    ],
)
def test_uc_names(case, layer, table, expect):
    assert store().name(layer, table) == expect


@pytest.mark.parametrize("table", ["no_separator", "bad-schema__x", "sales__bad name", "sales__x;drop"])
def test_uc_names_refused(table):
    with pytest.raises(ValueError, match=r"not a (safe table name|schema__table name)"):
        store().name("gold", table)


@pytest.mark.parametrize(
    ("case", "layer", "versions", "want"),
    [
        ("bronze is v3: deletion vectors and row tracking stay on", "bronze", None, {"format-version": "3"}),
        ("silver is v3", "silver", None, {"format-version": "3"}),
        ("gold stays v2 with the clustering workaround", "gold", None,
         {"format-version": "2", "delta.enableDeletionVectors": "false", "delta.enableRowTracking": "false"}),
        ("a binding can move gold to v3", "gold", {"gold": "3"}, {"format-version": "3"}),
    ],
)
def test_layer_properties(case, layer, versions, want):
    assert layer_properties(layer, versions) == want


def test_an_unknown_format_version_is_refused():
    with pytest.raises(ValueError, match="not 2 or 3"):
        layer_properties("silver", {"silver": "4"})


@pytest.mark.parametrize(
    ("case", "layer", "columns", "want"),
    [
        ("bronze clusters by batch key", "bronze", ["a", "_batch_key"], "(`_batch_key`)"),
        ("silver clusters by the business-key hash", "silver", ["_key_hash", "v"], "(`_key_hash`)"),
        ("gold lets the platform choose", "gold", ["promised_date", "dealer_sk"], "AUTO"),
        ("a missing key falls back to AUTO", "silver", ["v"], "AUTO"),
        ("ops is not clustered", "ops", ["x"], None),
    ],
)
def test_cluster_clause(case, layer, columns, want):
    assert cluster_clause(layer, columns) == want
