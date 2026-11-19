"""Which bronze-to-silver engine a dataset runs on, and what each engine refuses (ADR-035, DAC ENG-02)."""

import shutil
from pathlib import Path

import pytest
import yaml

from dataplatform.manifest import ManifestError
from dataplatform.meta import registry
from dataplatform.meta.loader import compile_domain
from dataplatform.silver.engines import EngineCaps, engine_of, engine_problems, history_type

SCD2 = {"history": {"kind": "master"}}
SNAPSHOT = {"history": {"kind": "snapshot"}}
SOURCE_VERSIONED = {"history": {"kind": "master", "type": "source_versioned"}}
EVENT = {"history": {"kind": "event"}}


@pytest.mark.parametrize(
    ("case", "settings", "platform", "expect"),
    [
        ("nothing set: job", {}, {}, "job"),
        ("platform default", {}, {"silver_engine": "declarative"}, "declarative"),
        ("dataset overrides platform", {"silver_engine": "job"}, {"silver_engine": "declarative"}, "job"),
    ],
)
def test_engine_of(case, settings, platform, expect):
    assert engine_of(settings, platform) == expect


@pytest.mark.parametrize(
    ("case", "manifest", "expect"),
    [
        ("master dataset -> scd2", SCD2, "scd2"),
        ("transaction -> scd1", {"history": {"kind": "transaction"}}, "scd1"),
        ("explicit type wins", SOURCE_VERSIONED, "source_versioned"),
        ("no history block", {}, ""),
    ],
)
def test_history_type(case, manifest, expect):
    assert history_type(manifest) == expect


@pytest.mark.parametrize(
    ("case", "manifest", "settings", "platform", "says"),
    [
        ("job takes everything", SNAPSHOT, {"table_format": "iceberg"}, {}, []),
        ("declarative takes scd2", SCD2, {"silver_engine": "declarative", "table_format": "delta"}, {}, []),
        ("declarative takes append", EVENT, {"silver_engine": "declarative"}, {}, []),
        ("declarative refuses snapshot history", SNAPSHOT, {"silver_engine": "declarative"}, {},
         ["history 'snapshot'"]),
        ("declarative refuses source-versioned history", SOURCE_VERSIONED, {"silver_engine": "declarative"}, {},
         ["history 'source_versioned'"]),
        ("declarative refuses a native format it does not write", SCD2, {"silver_engine": "declarative"},
         {"table_format": "iceberg"}, ["table_format 'iceberg'"]),
        ("both problems reported", SNAPSHOT, {"silver_engine": "declarative", "table_format": "iceberg"}, {},
         ["history 'snapshot'", "table_format 'iceberg'"]),
        ("declarative refuses full-copy deletes", {**SCD2, "load": {"deletes": {"detect": "key_compare"}}},
         {"silver_engine": "declarative"}, {}, ["comparing full copies"]),
        ("job takes full-copy deletes", {**SCD2, "load": {"deletes": {"detect": "key_compare"}}}, {}, {}, []),
        ("unknown engine", SCD2, {"silver_engine": "notebook"}, {}, ["silver_engine 'notebook' is not one of"]),
    ],
)
def test_engine_problems(case, manifest, settings, platform, says):
    got = engine_problems("erp.x", manifest, settings, platform)
    assert len(got) == len(says)
    assert all(s in g for s, g in zip(says, got, strict=True))


def test_a_binding_can_widen_an_engine():
    wide = {"declarative": EngineCaps(frozenset({"scd1", "scd2", "append"}), frozenset({"delta", "iceberg"}))}
    assert engine_problems("erp.x", SCD2, {"silver_engine": "declarative", "table_format": "iceberg"}, {}, wide) == []


SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"


@pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre manifests)")
@pytest.mark.parametrize(
    ("case", "dataset", "engine", "refused"),
    [
        ("an SCD2 dataset may move to declarative", "erp.sales_order_line.yaml", "declarative", False),
        ("the job engine is always accepted", "erp.material.yaml", "job", False),
    ],
)
def test_loader_with_the_tyre_manifests(tmp_path, case, dataset, engine, refused):
    man = tmp_path / "manifest"
    shutil.copytree(SLICE / "manifest", man)
    shutil.copytree(SLICE / "contracts", tmp_path / "contracts")
    p = man / "datasets" / dataset
    doc = yaml.safe_load(p.read_text())
    doc.setdefault("settings", {})["silver_engine"] = engine
    p.write_text(yaml.safe_dump(doc, sort_keys=False))
    defs = registry.load()
    if refused:
        with pytest.raises(ManifestError):
            compile_domain(man, defs)
    else:
        compile_domain(man, defs)
