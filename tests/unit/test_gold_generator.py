"""Unit tests for the Gold dbt-project generator (Phase 1).

Covers:

- Validation paths (every error message exercised at least once).
- Emitted YAML / SQL contents — top-level shape, key fields,
  watermark template assembly.
- Determinism + idempotence — re-running with the same config writes
  byte-identical files (golden-diff CI relies on this).
- YAML config loader — required keys, unknown-key rejection, defaults.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.gold import (
    GoldModelConfig,
    GoldProjectConfig,
    GoldSnapshotConfig,
    GoldSource,
    GoldStrategy,
    Materialization,
    SnapshotStrategy,
    UnsupportedGoldFeatureError,
    generate_dbt_project,
    load_project_config,
)

# ──────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ──────────────────────────────────────────────────────────────────────


def _model(**overrides: object) -> GoldModelConfig:
    """Sensible defaults; override per-test."""
    base: dict[str, object] = {
        "name": "fct_orders",
        "domain": "finance",
        "silver_sources": ("orders",),
        "business_keys": ("order_id",),
        "materialization": Materialization.INCREMENTAL,
        "incremental_watermark_column": "updated_at",
    }
    base.update(overrides)
    return GoldModelConfig(**base)  # type: ignore[arg-type]


def _project(**overrides: object) -> GoldProjectConfig:
    base: dict[str, object] = {
        "project_name": "acme_gold",
        "sources": (GoldSource(name="orders"),),
        "models": (_model(),),
    }
    base.update(overrides)
    return GoldProjectConfig(**base)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# generate_dbt_project — file layout
# ──────────────────────────────────────────────────────────────────────


def test_writes_full_project_skeleton(tmp_path: Path) -> None:
    stats = generate_dbt_project(_project(), tmp_path)

    assert (tmp_path / "dbt_project.yml").is_file()
    assert (tmp_path / "profiles.yml").is_file()
    assert (tmp_path / "packages.yml").is_file()
    assert (tmp_path / "macros" / "gold_meta.sql").is_file()
    assert (tmp_path / "models" / "sources.yml").is_file()
    assert (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").is_file()
    assert (tmp_path / "models" / "marts" / "finance" / "fct_orders.yml").is_file()
    assert stats.sources_count == 1
    assert stats.models_count == 1
    # 5 project-level files (dbt_project, profiles, packages, sources, gold_meta macro)
    # + 2 per model (.sql + .yml)
    assert len(stats.files_written) == 7


def test_files_written_are_sorted(tmp_path: Path) -> None:
    project = _project(
        models=(
            _model(name="fct_orders", domain="finance"),
            _model(
                name="dim_customer",
                domain="product",
                silver_sources=("customers",),
                materialization=Materialization.TABLE,
                incremental_watermark_column=None,
                business_keys=("customer_id",),
            ),
        ),
        sources=(GoldSource(name="orders"), GoldSource(name="customers")),
    )
    stats = generate_dbt_project(project, tmp_path)
    names = [str(p.relative_to(tmp_path)) for p in stats.files_written]
    assert names == sorted(names)


# ──────────────────────────────────────────────────────────────────────
# Determinism / idempotence
# ──────────────────────────────────────────────────────────────────────


def test_regenerating_produces_byte_identical_output(tmp_path: Path) -> None:
    project = _project()
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate_dbt_project(project, out_a)
    generate_dbt_project(project, out_b)

    files = sorted(p.relative_to(out_a) for p in out_a.rglob("*") if p.is_file())
    for rel in files:
        assert (out_a / rel).read_bytes() == (
            out_b / rel
        ).read_bytes(), f"Determinism broken for {rel}"


def test_source_order_is_independent_of_input_order(tmp_path: Path) -> None:
    """Sorting in sources.yml means input order doesn't change the output."""
    p1 = _project(
        sources=(GoldSource(name="zebra"), GoldSource(name="apple")),
        models=(_model(silver_sources=("zebra",)),),
    )
    p2 = _project(
        sources=(GoldSource(name="apple"), GoldSource(name="zebra")),
        models=(_model(silver_sources=("zebra",)),),
    )
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    generate_dbt_project(p1, out1)
    generate_dbt_project(p2, out2)
    assert (out1 / "models" / "sources.yml").read_bytes() == (
        out2 / "models" / "sources.yml"
    ).read_bytes()


# ──────────────────────────────────────────────────────────────────────
# dbt_project.yml
# ──────────────────────────────────────────────────────────────────────


def test_dbt_project_yml_has_correct_top_level_keys(tmp_path: Path) -> None:
    generate_dbt_project(_project(project_name="my_proj"), tmp_path)
    parsed = yaml.safe_load((tmp_path / "dbt_project.yml").read_text())
    assert parsed["name"] == "my_proj"
    assert parsed["profile"] == "my_proj"  # profile name == project name
    assert parsed["config-version"] == 2
    assert "models" in parsed["model-paths"]


# ──────────────────────────────────────────────────────────────────────
# profiles.yml
# ──────────────────────────────────────────────────────────────────────


def test_profiles_yml_uses_env_var_for_secrets(tmp_path: Path) -> None:
    """Connection params route through env_var so the file is safe to commit.

    YAML escapes embedded single quotes as ``''`` on the wire — verify
    the parsed value, which is what dbt sees after YAML load.
    """
    generate_dbt_project(_project(project_name="my_proj"), tmp_path)
    parsed = yaml.safe_load((tmp_path / "profiles.yml").read_text())
    output = parsed["my_proj"]["outputs"]["prod"]
    assert output["token"] == "{{ env_var('DATABRICKS_TOKEN') }}"
    assert output["host"] == "{{ env_var('DATABRICKS_HOST') }}"
    assert output["http_path"] == "{{ env_var('DATABRICKS_HTTP_PATH') }}"


def test_profiles_yml_target_databricks(tmp_path: Path) -> None:
    generate_dbt_project(_project(project_name="my_proj"), tmp_path)
    parsed = yaml.safe_load((tmp_path / "profiles.yml").read_text())
    output = parsed["my_proj"]["outputs"]["prod"]
    assert output["type"] == "databricks"
    assert output["threads"] == 8


# ──────────────────────────────────────────────────────────────────────
# sources.yml
# ──────────────────────────────────────────────────────────────────────


def test_sources_yml_lists_silver_sources(tmp_path: Path) -> None:
    project = _project(
        sources=(
            GoldSource(name="orders", description="Stripe orders"),
            GoldSource(name="customers", description="Salesforce accounts"),
        ),
        models=(_model(silver_sources=("orders",)),),
    )
    generate_dbt_project(project, tmp_path)
    parsed = yaml.safe_load((tmp_path / "models" / "sources.yml").read_text())

    assert parsed["version"] == 2
    silver_group = parsed["sources"][0]
    assert silver_group["name"] == "silver"
    table_names = [t["name"] for t in silver_group["tables"]]
    assert table_names == sorted(table_names) == ["customers", "orders"]


def test_sources_yml_freshness_uses_configured_thresholds(tmp_path: Path) -> None:
    """Freshness lives under ``config:`` per dbt 1.11+ schema."""
    project = _project(
        sources=(
            GoldSource(
                name="orders",
                freshness_warn_after_hours=2,
                freshness_error_after_hours=12,
            ),
        ),
    )
    generate_dbt_project(project, tmp_path)
    parsed = yaml.safe_load((tmp_path / "models" / "sources.yml").read_text())
    table = parsed["sources"][0]["tables"][0]
    assert table["config"]["loaded_at_field"] == "_silver_loaded_at"
    assert table["config"]["freshness"]["warn_after"] == {"count": 2, "period": "hour"}
    assert table["config"]["freshness"]["error_after"] == {"count": 12, "period": "hour"}


def test_sources_yml_uses_var_references_for_catalog_and_schema(tmp_path: Path) -> None:
    """Hard-coding catalog/schema would prevent multi-env reuse."""
    generate_dbt_project(
        _project(silver_catalog_var="my_cat", silver_schema_var="my_sch"),
        tmp_path,
    )
    parsed = yaml.safe_load((tmp_path / "models" / "sources.yml").read_text())
    silver_group = parsed["sources"][0]
    assert silver_group["database"] == "{{ var('my_cat') }}"
    assert silver_group["schema"] == "{{ var('my_sch') }}"


# ──────────────────────────────────────────────────────────────────────
# Model SQL — incremental
# ──────────────────────────────────────────────────────────────────────


def test_incremental_model_emits_config_block(tmp_path: Path) -> None:
    generate_dbt_project(_project(), tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "{{ config(" in sql
    assert "materialized='incremental'" in sql
    assert "incremental_strategy='merge'" in sql
    assert "unique_key='order_id'" in sql


def test_incremental_model_emits_watermark_template(tmp_path: Path) -> None:
    generate_dbt_project(_project(), tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "{% if is_incremental() %}" in sql
    assert "WHERE src.updated_at > (SELECT MAX(updated_at) FROM {{ this }})" in sql
    assert "INTERVAL 3 DAYS" in sql


def test_incremental_zero_lookback_omits_interval(tmp_path: Path) -> None:
    project = _project(models=(_model(incremental_lookback_days=0),))
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "INTERVAL" not in sql


def test_incremental_with_compound_business_keys_uses_list_unique_key(tmp_path: Path) -> None:
    project = _project(
        models=(_model(business_keys=("order_id", "line_item_id")),),
    )
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "unique_key=['order_id', 'line_item_id']" in sql


def test_custom_sql_overrides_default_select(tmp_path: Path) -> None:
    custom = "SELECT order_id, custom_metric FROM {{ source('silver', 'orders') }} WHERE foo = 1"
    project = _project(models=(_model(custom_sql=custom),))
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "custom_metric" in sql
    # The default SELECT * shouldn't be present.
    assert "SELECT *" not in sql


# ──────────────────────────────────────────────────────────────────────
# Model SQL — view / table
# ──────────────────────────────────────────────────────────────────────


def test_view_model_skips_incremental_block(tmp_path: Path) -> None:
    project = _project(
        models=(
            _model(
                materialization=Materialization.VIEW,
                incremental_watermark_column=None,  # not needed for view
            ),
        ),
    )
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "materialized='view'" in sql
    assert "is_incremental" not in sql
    assert "unique_key" not in sql


def test_table_model_simple_select(tmp_path: Path) -> None:
    project = _project(
        models=(
            _model(
                materialization=Materialization.TABLE,
                incremental_watermark_column=None,
            ),
        ),
    )
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "materialized='table'" in sql
    assert "src.*" in sql
    assert "is_incremental" not in sql


# ──────────────────────────────────────────────────────────────────────
# Model schema YAML
# ──────────────────────────────────────────────────────────────────────


def test_schema_yml_emits_not_null_and_unique_for_single_key(tmp_path: Path) -> None:
    generate_dbt_project(_project(), tmp_path)
    parsed = yaml.safe_load(
        (tmp_path / "models" / "marts" / "finance" / "fct_orders.yml").read_text()
    )
    model = parsed["models"][0]
    column = model["columns"][0]
    assert column["name"] == "order_id"
    assert sorted(column["tests"]) == ["not_null", "unique"]


def test_schema_yml_compound_keys_get_not_null_each_no_unique(tmp_path: Path) -> None:
    """unique on compound keys needs dbt_utils; deferred to tests-dedupe phase.

    Business-key columns must each carry only ``not_null``. Framework
    metadata cols also appear in the schema, but they're checked
    separately in test_schema_yml_includes_gold_metadata_cols.
    """
    project = _project(models=(_model(business_keys=("a", "b")),))
    generate_dbt_project(project, tmp_path)
    parsed = yaml.safe_load(
        (tmp_path / "models" / "marts" / "finance" / "fct_orders.yml").read_text()
    )
    columns = parsed["models"][0]["columns"]
    business_cols = [c for c in columns if not c["name"].startswith("_gold_")]
    assert {c["name"] for c in business_cols} == {"a", "b"}
    for c in business_cols:
        assert c["tests"] == ["not_null"]


# ──────────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────────


def test_empty_sources_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="sources must be non-empty"):
        generate_dbt_project(_project(sources=()), tmp_path)


def test_empty_models_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="models must be non-empty"):
        generate_dbt_project(_project(models=()), tmp_path)


def test_model_referencing_unknown_source_rejected(tmp_path: Path) -> None:
    project = _project(
        sources=(GoldSource(name="orders"),),
        models=(_model(silver_sources=("totally_made_up",)),),
    )
    with pytest.raises(ValidationError, match="silver source 'totally_made_up'"):
        generate_dbt_project(project, tmp_path)


# ──────────────────────────────────────────────────────────────────────
# Multi-source models — Phase 1.5
# ──────────────────────────────────────────────────────────────────────


def test_multi_source_without_custom_sql_rejected(tmp_path: Path) -> None:
    """Default templated SELECT can't render JOINs — must come from custom_sql."""
    project = _project(
        sources=(GoldSource(name="orders"), GoldSource(name="order_lines")),
        models=(_model(silver_sources=("orders", "order_lines"), custom_sql=None),),
    )
    with pytest.raises(ValidationError, match="2 silver_sources but no custom_sql"):
        generate_dbt_project(project, tmp_path)


def test_multi_source_with_custom_sql_works(tmp_path: Path) -> None:
    """Custom SQL is the supported path for multi-source facts."""
    custom = (
        "SELECT o.order_id, o.amount, p.payment_method\n"
        "FROM {{ source('silver', 'orders') }} o\n"
        "LEFT JOIN {{ source('silver', 'payments') }} p USING (order_id)"
    )
    project = _project(
        sources=(GoldSource(name="orders"), GoldSource(name="payments")),
        models=(
            _model(
                silver_sources=("orders", "payments"),
                custom_sql=custom,
                materialization=Materialization.TABLE,
                incremental_watermark_column=None,
            ),
        ),
    )
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "LEFT JOIN {{ source('silver', 'payments') }}" in sql


def test_empty_silver_sources_rejected(tmp_path: Path) -> None:
    project = _project(models=(_model(silver_sources=()),))
    with pytest.raises(ValidationError, match="at least one entry in silver_sources"):
        generate_dbt_project(project, tmp_path)


def test_duplicate_silver_sources_rejected(tmp_path: Path) -> None:
    project = _project(
        sources=(GoldSource(name="orders"),),
        models=(
            _model(
                silver_sources=("orders", "orders"),
                custom_sql="SELECT 1",  # multi-source needs custom_sql; duplicate is the error
            ),
        ),
    )
    with pytest.raises(ValidationError, match="more than once"):
        generate_dbt_project(project, tmp_path)


def test_invalid_dbt_name_rejected(tmp_path: Path) -> None:
    """Project / model / source names must be lower snake_case."""
    with pytest.raises(ValidationError, match="dbt names must match"):
        generate_dbt_project(_project(project_name="My-Project"), tmp_path)


def test_invalid_business_key_identifier_rejected(tmp_path: Path) -> None:
    project = _project(models=(_model(business_keys=("order id",)),))  # space — invalid
    with pytest.raises(ValidationError, match="business_key"):
        generate_dbt_project(project, tmp_path)


def test_incremental_requires_watermark_column(tmp_path: Path) -> None:
    project = _project(models=(_model(incremental_watermark_column=None),))
    with pytest.raises(ValidationError, match="incremental_watermark_column"):
        generate_dbt_project(project, tmp_path)


def test_invalid_watermark_identifier_rejected(tmp_path: Path) -> None:
    project = _project(
        models=(_model(incremental_watermark_column="updated at"),),  # space — invalid
    )
    with pytest.raises(ValidationError, match="watermark"):
        generate_dbt_project(project, tmp_path)


def test_negative_lookback_rejected(tmp_path: Path) -> None:
    project = _project(models=(_model(incremental_lookback_days=-1),))
    with pytest.raises(ValidationError, match="lookback_days"):
        generate_dbt_project(project, tmp_path)


def test_missing_business_keys_rejected(tmp_path: Path) -> None:
    project = _project(models=(_model(business_keys=()),))
    with pytest.raises(ValidationError, match="at least one business_key"):
        generate_dbt_project(project, tmp_path)


def test_snapshot_materialization_unsupported(tmp_path: Path) -> None:
    project = _project(
        models=(
            _model(
                materialization=Materialization.SNAPSHOT,
                incremental_watermark_column=None,
            ),
        ),
    )
    with pytest.raises(ValidationError, match="snapshots' field"):
        generate_dbt_project(project, tmp_path)


def test_pyspark_workflow_strategy_unsupported(tmp_path: Path) -> None:
    project = _project(models=(_model(strategy=GoldStrategy.PYSPARK_WORKFLOW),))
    with pytest.raises(UnsupportedGoldFeatureError, match="not supported in Phase 1"):
        generate_dbt_project(project, tmp_path)


# ──────────────────────────────────────────────────────────────────────
# Phase 5 — per-record metadata cols + macro + packages.yml
# ──────────────────────────────────────────────────────────────────────


def test_packages_yml_pins_dbt_utils(tmp_path: Path) -> None:
    generate_dbt_project(_project(), tmp_path)
    parsed = yaml.safe_load((tmp_path / "packages.yml").read_text())
    assert parsed["packages"][0]["package"] == "dbt-labs/dbt_utils"
    assert ">=1.1.0" in parsed["packages"][0]["version"]


def test_gold_meta_macro_emitted(tmp_path: Path) -> None:
    """The macro file must declare gold_meta_cols and use the gold_run_id var."""
    generate_dbt_project(_project(), tmp_path)
    macro = (tmp_path / "macros" / "gold_meta.sql").read_text()
    assert "{% macro gold_meta_cols()" in macro
    assert "_gold_run_id" in macro
    assert "_gold_loaded_at" in macro
    # Falls back to 'manual' so ad-hoc dbt runs still produce well-formed rows.
    assert 'var("gold_run_id", "manual")' in macro


def test_default_template_calls_gold_meta_cols(tmp_path: Path) -> None:
    """Every Gold model gets _gold_run_id + _gold_loaded_at via the macro."""
    generate_dbt_project(_project(), tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "{{ gold_meta_cols() }}" in sql


def test_custom_sql_does_not_auto_inject_gold_meta(tmp_path: Path) -> None:
    """Custom SQL is taken verbatim — customers add the macro themselves.

    Auto-wrapping their SQL would change its shape unexpectedly (e.g.,
    nested SELECT could break ORDER BY positional refs).
    """
    custom = "SELECT order_id, custom_metric FROM {{ source('silver', 'orders') }}"
    project = _project(models=(_model(custom_sql=custom),))
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "{{ gold_meta_cols() }}" not in sql
    assert "custom_metric" in sql


def test_schema_yml_includes_gold_metadata_cols(tmp_path: Path) -> None:
    generate_dbt_project(_project(), tmp_path)
    parsed = yaml.safe_load(
        (tmp_path / "models" / "marts" / "finance" / "fct_orders.yml").read_text()
    )
    columns = parsed["models"][0]["columns"]
    names = [c["name"] for c in columns]
    assert "_gold_run_id" in names
    assert "_gold_loaded_at" in names
    # Both metadata cols must be not_null (the macro guarantees them).
    for c in columns:
        if c["name"] in ("_gold_run_id", "_gold_loaded_at"):
            assert "not_null" in c["tests"]
            assert c["description"]  # non-empty


def test_phase5_idempotence_holds(tmp_path: Path) -> None:
    """Re-running with the new Phase 5 emitters still produces byte-identical output."""
    project = _project()
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate_dbt_project(project, out_a)
    generate_dbt_project(project, out_b)
    for path in out_a.rglob("*"):
        if path.is_file():
            rel = path.relative_to(out_a)
            assert (
                path.read_bytes() == (out_b / rel).read_bytes()
            ), f"Phase 5 broke determinism for {rel}"


# ──────────────────────────────────────────────────────────────────────
# YAML loader
# ──────────────────────────────────────────────────────────────────────


_VALID_YAML = """\
project_name: my_proj
sources:
  - name: orders
    description: Orders source
models:
  - name: fct_orders
    domain: finance
    silver_sources: [orders]
    business_keys: [order_id]
    materialization: incremental
    incremental_watermark_column: updated_at
"""


def test_load_project_config_round_trips(tmp_path: Path) -> None:
    cfg_path = tmp_path / "gold.yaml"
    cfg_path.write_text(_VALID_YAML)
    project = load_project_config(cfg_path)
    assert project.project_name == "my_proj"
    assert len(project.sources) == 1
    assert project.sources[0].name == "orders"
    assert len(project.models) == 1
    assert project.models[0].silver_sources == ("orders",)
    assert project.models[0].business_keys == ("order_id",)
    assert project.models[0].materialization == Materialization.INCREMENTAL


def test_load_project_config_missing_required_key(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text("sources: []\nmodels: []\n")
    with pytest.raises(ValidationError, match="project_name"):
        load_project_config(cfg_path)


def test_load_project_config_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(_VALID_YAML + "\nbogus_key: 42\n")
    with pytest.raises(ValidationError, match="bogus_key"):
        load_project_config(cfg_path)


def test_load_project_config_rejects_unknown_model_key(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(
        _VALID_YAML.replace(
            "    incremental_watermark_column: updated_at",
            "    incremental_watermark_column: updated_at\n    not_a_field: 42",
        )
    )
    with pytest.raises(ValidationError, match="not_a_field"):
        load_project_config(cfg_path)


def test_load_project_config_rejects_non_mapping(tmp_path: Path) -> None:
    cfg_path = tmp_path / "list.yaml"
    cfg_path.write_text("- 1\n- 2\n")
    with pytest.raises(ValidationError, match="must be a YAML mapping"):
        load_project_config(cfg_path)


def test_load_project_config_then_generate_works(tmp_path: Path) -> None:
    """End-to-end: YAML → GoldProjectConfig → dbt project."""
    cfg_path = tmp_path / "gold.yaml"
    cfg_path.write_text(_VALID_YAML)
    project = load_project_config(cfg_path)

    out = tmp_path / "out"
    stats = generate_dbt_project(project, out)
    assert stats.models_count == 1
    assert (out / "models" / "marts" / "finance" / "fct_orders.sql").is_file()


def test_load_project_config_requires_silver_sources_key(tmp_path: Path) -> None:
    """Models without silver_sources fail loader-time, not deeper."""
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(_VALID_YAML.replace("    silver_sources: [orders]\n", ""))
    with pytest.raises(ValidationError, match="silver_sources"):
        load_project_config(cfg_path)


def test_load_project_config_silver_sources_must_be_list(tmp_path: Path) -> None:
    """Catch the ergonomic mistake of passing a string instead of a list."""
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(
        _VALID_YAML.replace("    silver_sources: [orders]\n", "    silver_sources: orders\n")
    )
    with pytest.raises(ValidationError, match="silver_sources must be a list"):
        load_project_config(cfg_path)


def test_load_project_config_multi_source_with_custom_sql(tmp_path: Path) -> None:
    """End-to-end multi-source path: YAML → loader → generator."""
    cfg_path = tmp_path / "multi.yaml"
    cfg_path.write_text(
        """\
project_name: my_proj
sources:
  - name: orders
  - name: payments
models:
  - name: fct_orders
    domain: finance
    silver_sources: [orders, payments]
    business_keys: [order_id]
    materialization: table
    custom_sql: |
      SELECT o.order_id, o.amount, p.payment_method
      FROM {{ source('silver', 'orders') }} o
      LEFT JOIN {{ source('silver', 'payments') }} p USING (order_id)
"""
    )
    project = load_project_config(cfg_path)
    assert project.models[0].silver_sources == ("orders", "payments")
    assert project.models[0].custom_sql is not None

    out = tmp_path / "out"
    generate_dbt_project(project, out)
    sql = (out / "models" / "marts" / "finance" / "fct_orders.sql").read_text()
    assert "{{ source('silver', 'orders') }}" in sql
    assert "{{ source('silver', 'payments') }}" in sql


# ──────────────────────────────────────────────────────────────────────
# Phase 6 — snapshots (SCD2 at Gold)
# ──────────────────────────────────────────────────────────────────────


def _snapshot(**overrides: object) -> GoldSnapshotConfig:
    """Defaults for a timestamp-strategy snapshot wrapping fct_orders."""
    base: dict[str, object] = {
        "name": "snap_orders_daily",
        "target_schema": "gold_snapshots",
        "source_model": "fct_orders",
        "unique_key": "order_id",
        "strategy": SnapshotStrategy.TIMESTAMP,
        "updated_at_column": "_gold_loaded_at",
    }
    base.update(overrides)
    return GoldSnapshotConfig(**base)  # type: ignore[arg-type]


def test_no_snapshots_means_no_snapshot_files(tmp_path: Path) -> None:
    """Empty snapshots tuple — no ``snapshots/`` directory created."""
    stats = generate_dbt_project(_project(), tmp_path)
    assert stats.snapshots_count == 0
    assert not (tmp_path / "snapshots").exists()


def test_timestamp_snapshot_emitted(tmp_path: Path) -> None:
    project = _project(snapshots=(_snapshot(),))
    stats = generate_dbt_project(project, tmp_path)
    snap_path = tmp_path / "snapshots" / "snap_orders_daily.sql"
    assert snap_path.is_file()
    sql = snap_path.read_text()
    assert "{% snapshot snap_orders_daily %}" in sql
    assert "{% endsnapshot %}" in sql
    assert "strategy='timestamp'" in sql
    assert "updated_at='_gold_loaded_at'" in sql
    assert "target_schema='gold_snapshots'" in sql
    assert "unique_key='order_id'" in sql
    assert "SELECT * FROM {{ ref('fct_orders') }}" in sql
    assert stats.snapshots_count == 1


def test_check_snapshot_emits_check_cols(tmp_path: Path) -> None:
    project = _project(
        snapshots=(
            _snapshot(
                name="snap_orders_check",
                strategy=SnapshotStrategy.CHECK,
                updated_at_column=None,
                check_cols=("status", "amount"),
            ),
        ),
    )
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "snapshots" / "snap_orders_check.sql").read_text()
    assert "strategy='check'" in sql
    assert "check_cols=['status', 'amount']" in sql
    assert "updated_at" not in sql  # timestamp-only key shouldn't leak in


def test_compound_unique_key_renders_as_list(tmp_path: Path) -> None:
    project = _project(snapshots=(_snapshot(unique_key=("a", "b")),))
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "snapshots" / "snap_orders_daily.sql").read_text()
    assert "unique_key=['a', 'b']" in sql


def test_snapshot_custom_sql_overrides_default_select(tmp_path: Path) -> None:
    custom = "SELECT order_id, status FROM {{ ref('fct_orders') }} WHERE region = 'EU'"
    project = _project(snapshots=(_snapshot(custom_sql=custom),))
    generate_dbt_project(project, tmp_path)
    sql = (tmp_path / "snapshots" / "snap_orders_daily.sql").read_text()
    assert "WHERE region = 'EU'" in sql
    assert "SELECT * FROM {{ ref(" not in sql


# ── Validation ────────────────────────────────────────────────────────


def test_snapshot_referencing_unknown_model_rejected(tmp_path: Path) -> None:
    project = _project(snapshots=(_snapshot(source_model="not_a_real_model"),))
    with pytest.raises(ValidationError, match=r"not in project\.models"):
        generate_dbt_project(project, tmp_path)


def test_timestamp_strategy_requires_updated_at_column(tmp_path: Path) -> None:
    project = _project(
        snapshots=(_snapshot(strategy=SnapshotStrategy.TIMESTAMP, updated_at_column=None),),
    )
    with pytest.raises(ValidationError, match="updated_at_column"):
        generate_dbt_project(project, tmp_path)


def test_check_strategy_requires_check_cols(tmp_path: Path) -> None:
    project = _project(
        snapshots=(
            _snapshot(
                strategy=SnapshotStrategy.CHECK,
                updated_at_column=None,
                check_cols=None,
            ),
        ),
    )
    with pytest.raises(ValidationError, match="check_cols"):
        generate_dbt_project(project, tmp_path)


def test_unsafe_target_schema_rejected(tmp_path: Path) -> None:
    project = _project(snapshots=(_snapshot(target_schema="gold; DROP TABLE"),))
    with pytest.raises(ValidationError, match="target_schema"):
        generate_dbt_project(project, tmp_path)


def test_unsafe_check_col_rejected(tmp_path: Path) -> None:
    project = _project(
        snapshots=(
            _snapshot(
                strategy=SnapshotStrategy.CHECK,
                updated_at_column=None,
                check_cols=("status; DROP TABLE",),
            ),
        ),
    )
    with pytest.raises(ValidationError, match="check_cols"):
        generate_dbt_project(project, tmp_path)


def test_duplicate_snapshot_names_rejected(tmp_path: Path) -> None:
    project = _project(
        snapshots=(_snapshot(), _snapshot()),  # both default to snap_orders_daily
    )
    with pytest.raises(ValidationError, match="Duplicate snapshot name"):
        generate_dbt_project(project, tmp_path)


# ── Determinism with snapshots ────────────────────────────────────────


def test_phase6_idempotence_holds(tmp_path: Path) -> None:
    project = _project(snapshots=(_snapshot(),))
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate_dbt_project(project, out_a)
    generate_dbt_project(project, out_b)
    for path in out_a.rglob("*"):
        if path.is_file():
            rel = path.relative_to(out_a)
            assert (
                path.read_bytes() == (out_b / rel).read_bytes()
            ), f"Snapshot determinism broken for {rel}"


# ── YAML loader for snapshots ─────────────────────────────────────────


_VALID_YAML_WITH_SNAPSHOT = """\
project_name: my_proj
sources:
  - name: orders
models:
  - name: fct_orders
    domain: finance
    silver_sources: [orders]
    business_keys: [order_id]
    materialization: incremental
    incremental_watermark_column: updated_at
snapshots:
  - name: snap_orders_daily
    target_schema: gold_snapshots
    source_model: fct_orders
    unique_key: order_id
    strategy: timestamp
    updated_at_column: _gold_loaded_at
    description: Daily snapshot of fct_orders
"""


def test_load_project_config_round_trips_snapshot(tmp_path: Path) -> None:
    cfg_path = tmp_path / "with_snap.yaml"
    cfg_path.write_text(_VALID_YAML_WITH_SNAPSHOT)
    project = load_project_config(cfg_path)
    assert len(project.snapshots) == 1
    snap = project.snapshots[0]
    assert snap.name == "snap_orders_daily"
    assert snap.strategy == SnapshotStrategy.TIMESTAMP
    assert snap.updated_at_column == "_gold_loaded_at"
    assert snap.unique_key == "order_id"


def test_load_project_config_snapshot_unique_key_as_list(tmp_path: Path) -> None:
    cfg_path = tmp_path / "compound_snap.yaml"
    cfg_path.write_text(
        _VALID_YAML_WITH_SNAPSHOT.replace(
            "    unique_key: order_id", "    unique_key: [order_id, line_item_id]"
        )
    )
    project = load_project_config(cfg_path)
    assert project.snapshots[0].unique_key == ("order_id", "line_item_id")


def test_load_project_config_snapshot_check_strategy(tmp_path: Path) -> None:
    cfg_path = tmp_path / "check_snap.yaml"
    cfg_path.write_text(
        _VALID_YAML_WITH_SNAPSHOT.replace(
            "    strategy: timestamp\n    updated_at_column: _gold_loaded_at\n",
            "    strategy: check\n    check_cols: [status, amount]\n",
        )
    )
    project = load_project_config(cfg_path)
    snap = project.snapshots[0]
    assert snap.strategy == SnapshotStrategy.CHECK
    assert snap.check_cols == ("status", "amount")


def test_load_project_config_snapshots_must_be_list(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(
        _VALID_YAML_WITH_SNAPSHOT.replace("snapshots:\n", "snapshots: bogus_string\n").split(
            "  - name: snap_orders_daily"
        )[0]
    )
    with pytest.raises(ValidationError, match="snapshots' must be a list"):
        load_project_config(cfg_path)


def test_load_project_config_snapshot_unknown_key_rejected(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(
        _VALID_YAML_WITH_SNAPSHOT.replace(
            "    description: Daily snapshot of fct_orders",
            "    description: Daily snapshot of fct_orders\n    bogus_field: 1",
        )
    )
    with pytest.raises(ValidationError, match="bogus_field"):
        load_project_config(cfg_path)


def test_load_project_config_then_generate_with_snapshot(tmp_path: Path) -> None:
    cfg_path = tmp_path / "gold.yaml"
    cfg_path.write_text(_VALID_YAML_WITH_SNAPSHOT)
    project = load_project_config(cfg_path)

    out = tmp_path / "out"
    stats = generate_dbt_project(project, out)
    assert stats.snapshots_count == 1
    assert (out / "snapshots" / "snap_orders_daily.sql").is_file()
