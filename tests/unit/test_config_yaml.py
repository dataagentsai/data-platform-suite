"""Unit tests for the customer-repo config YAML schemas."""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework.core.config_yaml import (
    DatasetConfigFile,
    SourceConfigFile,
    load_dataset_yaml,
    load_platform_config_yaml,
    load_source_yaml,
)
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.types.enums import PiiClass, SourceType


def _write(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────────────
# platform_config
# ──────────────────────────────────────────────────────────────────────


def test_platform_config_yaml_stringifies_scalars(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "dev.yaml",
        """
default_realm: adf-adls
default_batch_count: 10
auto_retry_on_partial: true
        """.strip(),
    )
    out = load_platform_config_yaml(p)
    assert out.kv == {
        "default_realm": "adf-adls",
        "default_batch_count": "10",
        "auto_retry_on_partial": "true",
    }
    assert out.processor_groups == {}
    assert out.pii_policy == ()


def test_platform_config_yaml_empty_file(tmp_path: Path) -> None:
    p = _write(tmp_path / "dev.yaml", "")
    out = load_platform_config_yaml(p)
    assert out.kv == {}
    assert out.processor_groups == {}
    assert out.pii_policy == ()


def test_platform_config_rejects_non_mapping(tmp_path: Path) -> None:
    p = _write(tmp_path / "dev.yaml", "- 1\n- 2\n")
    with pytest.raises(ValidationError):
        load_platform_config_yaml(p)


# ──────────────────────────────────────────────────────────────────────
# source YAML
# ──────────────────────────────────────────────────────────────────────


def test_load_source_yaml_minimal(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "orders.yaml",
        """
source_name: orders_db
source_type: azure_sql_db
config:
  connection_pattern: azure_basic
  auth_method: managed_identity
        """.strip(),
    )
    src = load_source_yaml(p)
    assert isinstance(src, SourceConfigFile)
    assert src.source_name == "orders_db"
    assert src.source_env == "prod"
    assert src.source_type is SourceType.AZURE_SQL_DB
    assert src.is_enabled is True
    assert src.config == {
        "connection_pattern": "azure_basic",
        "auth_method": "managed_identity",
    }


def test_load_source_yaml_rejects_extra_field(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "orders.yaml",
        """
source_name: orders_db
source_type: azure_sql_db
mystery_field: surprise
        """.strip(),
    )
    with pytest.raises(ValidationError):
        load_source_yaml(p)


def test_load_source_yaml_rejects_bad_source_env(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "orders.yaml",
        """
source_name: orders_db
source_env: PROD!
source_type: azure_sql_db
        """.strip(),
    )
    with pytest.raises(ValidationError):
        load_source_yaml(p)


# ──────────────────────────────────────────────────────────────────────
# dataset YAML
# ──────────────────────────────────────────────────────────────────────


def test_load_dataset_yaml_with_columns(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "orders" / "customers.yaml",
        """
source_name: orders_db
dataset_name: customers
is_enabled: true
config:
  realm: adf-adls
  dataset_type: TABLE
  load_type: INCREMENTAL
  target_path_template: raw/{source}/{object}
  schema_name: dbo
  object_name: customers
  watermark_column: modified_at
columns:
  - column_name: id
    data_type: int
    is_nullable: false
    ordinal_position: 1
    is_primary_key: true
  - column_name: email
    data_type: varchar(255)
    ordinal_position: 2
    pii_class: direct
        """.strip(),
    )
    ds = load_dataset_yaml(p)
    assert isinstance(ds, DatasetConfigFile)
    assert ds.dataset_name == "customers"
    assert len(ds.columns) == 2
    # pii_class is now a free-form string (matches the underlying
    # TEXT column + supports both PiiClass tier values and fine-grained
    # classes like 'email' / 'ssn' for the Wave 2 PII processor).
    assert ds.columns[1].pii_class == PiiClass.DIRECT.value
    assert ds.columns[1].pii_class == "direct"


def test_load_dataset_yaml_rejects_duplicate_column(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "ds.yaml",
        """
source_name: x
dataset_name: y
columns:
  - column_name: a
    data_type: int
    ordinal_position: 1
  - column_name: a
    data_type: int
    ordinal_position: 2
        """.strip(),
    )
    with pytest.raises(ValidationError, match="Duplicate column_name"):
        load_dataset_yaml(p)


def test_load_dataset_yaml_no_columns(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "ds.yaml",
        """
source_name: x
dataset_name: y
        """.strip(),
    )
    ds = load_dataset_yaml(p)
    assert ds.columns == ()


def test_load_dataset_yaml_stringifies_list_value(tmp_path: Path) -> None:
    p = _write(
        tmp_path / "ds.yaml",
        """
source_name: x
dataset_name: y
config:
  redaction_extra_patterns:
    - "pwd="
    - "secret="
        """.strip(),
    )
    ds = load_dataset_yaml(p)
    # JSON-encoded; whatever the registry sees it can re-parse.
    assert ds.config["redaction_extra_patterns"] == '["pwd=","secret="]'


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        load_source_yaml(tmp_path / "nope.yaml")
