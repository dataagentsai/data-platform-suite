"""Unit tests for the customer profile parser + validator."""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.profile import CustomerProfile, load_profile
from lake_ingestion_framework.core.types.enums import SourceType

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILES_DIR = REPO_ROOT / "templates" / "profiles"


@pytest.mark.parametrize(
    "profile_name", ["dummycustomera.yaml", "dummycustomerb.yaml", "dummycustomerc.yaml"]
)
def test_reference_profiles_load(profile_name: str) -> None:
    """All three reference profiles parse cleanly."""
    profile = load_profile(PROFILES_DIR / profile_name)
    assert isinstance(profile, CustomerProfile)
    assert profile.customer.name == profile_name.replace(".yaml", "")


def test_dummycustomera_shape() -> None:
    """Pin the shape of dummycustomera as the canonical example."""
    profile = load_profile(PROFILES_DIR / "dummycustomera.yaml")

    assert profile.customer.name == "dummycustomera"
    assert profile.customer.display_name == "DummyCustomer A, Inc."

    env_names = [e.name for e in profile.environments]
    assert env_names == ["dev", "uat", "prod"]

    source_types = {s.source_type for s in profile.sources}
    assert source_types == {SourceType.SQL_SERVER, SourceType.SFTP}

    assert profile.features.segmented_loads is True
    assert profile.features.flattening is False
    assert profile.features.document_dataset_type is False

    assert profile.constraints.no_azure_functions is True
    assert profile.constraints.private_endpoint_required is True


def test_dummycustomerb_rag_shape() -> None:
    """dummycustomerb is the RAG profile — DOCUMENT + flattening on."""
    profile = load_profile(PROFILES_DIR / "dummycustomerb.yaml")

    source_types = {s.source_type for s in profile.sources}
    assert SourceType.SHAREPOINT in source_types
    assert SourceType.SALESFORCE in source_types

    assert profile.features.flattening is True
    assert profile.features.document_dataset_type is True
    assert profile.features.saas_apis is True


def test_missing_profile_raises() -> None:
    with pytest.raises(ValidationError, match="not found"):
        load_profile(Path("/tmp/definitely-not-a-real-file-xyzzy.yaml"))


def test_unknown_field_rejected(tmp_path: Path) -> None:
    """`extra='forbid'` enforces the schema — unknown blocks fail loudly."""
    profile = tmp_path / "bad.yaml"
    profile.write_text(
        """
customer:
  name: "test"
  display_name: "Test"
environments:
  - name: dev
    storage_account: testdevsa
    container: lake
    metadata_db_server: x.database.windows.net
    metadata_db_database: meta
    region: westeurope
sources:
  - source_type: sql_server
    connection_patterns: [onprem_basic]
totally_unknown_top_level_block: 42
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="invalid"):
        load_profile(profile)


def test_invalid_customer_name_rejected(tmp_path: Path) -> None:
    """customer.name must match ^[a-z][a-z0-9-]{1,49}$."""
    profile = tmp_path / "bad.yaml"
    profile.write_text(
        """
customer:
  name: "Has Capitals"
  display_name: "Bad"
environments:
  - name: dev
    storage_account: x
    container: lake
    metadata_db_server: x
    metadata_db_database: meta
    region: westeurope
sources:
  - source_type: sql_server
    connection_patterns: [onprem_basic]
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError):
        load_profile(profile)


def test_duplicate_environment_names_rejected(tmp_path: Path) -> None:
    profile = tmp_path / "bad.yaml"
    profile.write_text(
        """
customer:
  name: test
  display_name: Test
environments:
  - { name: dev, storage_account: aaadev, container: lake, metadata_db_server: x.foo,
      metadata_db_database: m, region: r }
  - { name: dev, storage_account: bbbdev, container: lake, metadata_db_server: x.foo,
      metadata_db_database: m, region: r }
sources:
  - source_type: sql_server
    connection_patterns: [onprem_basic]
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="Duplicate"):
        load_profile(profile)


def test_unknown_source_type_rejected(tmp_path: Path) -> None:
    profile = tmp_path / "bad.yaml"
    profile.write_text(
        """
customer:
  name: test
  display_name: Test
environments:
  - { name: dev, storage_account: aaadev, container: lake, metadata_db_server: x.foo,
      metadata_db_database: m, region: r }
sources:
  - source_type: definitely_not_a_source
    connection_patterns: [foo]
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError):
        load_profile(profile)


def test_yaml_root_must_be_mapping(tmp_path: Path) -> None:
    profile = tmp_path / "bad.yaml"
    profile.write_text("- a\n- b\n")
    with pytest.raises(ValidationError, match="mapping"):
        load_profile(profile)


def test_malformed_yaml_rejected(tmp_path: Path) -> None:
    profile = tmp_path / "bad.yaml"
    profile.write_text("customer:\n  name: [unclosed\n")
    with pytest.raises(ValidationError, match="invalid"):
        load_profile(profile)
