"""The validating loader and the settings registry (M3, M4, DPC-0001, DPC-0034): the tyre manifest fills the store,
bad manifests are refused before anything is written, and the registry stays in step with the old key registry.

Reads the tyre slice from ~/data-platform-catalog (CI checks it out beside the repo); skipped when it is missing.
"""

import json
import shutil
from pathlib import Path

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.manifest import ManifestError
from dataplatform.meta import loader, registry

yaml = pytest.importorskip("yaml")

CATALOG = Path.home() / "data-platform-catalog"
TYRE = CATALOG / "slices/02-tyre/manifest"
pytestmark = pytest.mark.skipif(not TYRE.exists(), reason="spec catalog not beside the repo")


@pytest.fixture(scope="module")
def tyre():
    store = MemoryMetaStore()
    return store, loader.load(store, TYRE)


@pytest.mark.parametrize(
    ("source", "dataset", "ingest", "history", "primary", "dedup"),
    [
        ("dealer_api", "dealer", "pull", "master", ["dealer_code"], ["dealer_code"]),
        ("erp", "delivery", "push", "transaction", ["delivery_id"], ["delivery_id"]),
        ("erp", "material", "pull", "master", ["material_code"], ["material_code"]),
        ("erp", "sales_order_line", "push", "transaction", ["order_id", "line_no"], ["order_id", "line_no"]),
        ("lims", "inspection_result", "push", "event", ["inspection_id"], ["inspection_id"]),
        ("mes", "press", "pull", "master", ["press_id"], ["press_id"]),
        ("mes", "production", "push", "event", ["production_id"], ["production_id"]),
        ("partner", "dealer_sellout", "push", "snapshot", ["dealer_code", "sale_month", "material_code"],
         ["dealer_code", "sale_month", "material_code"]),
        ("press_stream", "press_reading", "stream", "event", ["reading_id"], ["reading_id"]),
    ],
)
def test_tyre_dataset_loaded(tyre, source, dataset, ingest, history, primary, dedup):
    store, _ = tyre
    view = store.dataset_view(source, dataset)
    contract = yaml.safe_load(next((CATALOG / "slices/02-tyre/contracts").glob(f"{dataset}.odcs.yaml")).read_text())
    assert (view["ingest_mode"], view["history_kind"]) == (ingest, history)
    assert store.business_keys(source, dataset) == primary
    assert store.business_keys(source, dataset, "dedup") == dedup
    assert [c.name for c in store.columns(source, dataset)] == [p["name"] for p in contract["schema"][0]["properties"]]
    assert view["contract_version"] == str(contract["version"])
    assert all(view["origins"][k] == "dataset" for k in view["manifest"]["config"])


@pytest.mark.parametrize(
    ("check", "got", "expect"),
    [
        ("every dataset file loaded", lambda r, s: len(r.loaded), 9),
        ("domain is tyre", lambda r, s: r.domain, "tyre"),
        ("nothing in the store outside the manifest", lambda r, s: r.not_in_manifest, []),
        ("dedup block stored as registry keys",
         lambda r, s: {k: v for k, v in s.dataset_view("erp", "sales_order_line")["settings"].items()
                       if k.startswith("dedup.")},
         {"dedup.mode": "business_key_latest", "dedup.business_keys": "order_id,line_no",
          "dedup.order_by": "change_seq"}),
        ("a registry default fills an unset key",
         lambda r, s: next((x.value, x.origin) for x in s.settings("erp", "delivery") if x.key == "csv_has_header"),
         ("true", "default")),
        ("proposed keys are accepted",
         lambda r, s: s.dataset_view("erp", "sales_order_line")["settings"]["sla_freshness_error_hours"], "2"),
        # values that do not fit the old registry's types: reported, not refused (spec gaps, see the W1a report)
        # W1b: the source file format is file_format_data (file_format is the sink's), and full copies say so with
        # load_type FULL, not extraction_mode (catalog e20d9b1), so the tyre manifests load without warnings.
        ("no type mismatches", lambda r, s: r.warnings, []),
        ("source file format under its own key",
         lambda r, s: s.dataset_view("lims", "inspection_result")["settings"].get("file_format_data"), "csv"),
        ("restricted columns carry their class",
         lambda r, s: sorted(c.name for c in s.columns("dealer_api", "dealer") if c.classification == "restricted"),
         ["contact_email", "contact_phone"]),
    ],
)
def test_tyre_load_report(tyre, check, got, expect):
    store, report = tyre
    assert got(report, store) == expect


def test_reload_is_idempotent(tyre):
    store, _ = tyre
    before = {(d.source, d.dataset): d.fingerprint for d in store.datasets("tyre")}
    loader.load(store, TYRE)
    assert {(d.source, d.dataset): d.fingerprint for d in store.datasets("tyre")} == before


def test_dataset_dropped_from_manifest_is_reported(tmp_path):
    store = MemoryMetaStore()
    loader.load(store, TYRE)
    shutil.copytree(TYRE.parent, tmp_path / "slice")
    (tmp_path / "slice/manifest/datasets/mes.press.yaml").unlink()
    assert loader.load(store, tmp_path / "slice/manifest").not_in_manifest == ["mes.press"]


def _break(path: Path, change):
    doc = yaml.safe_load(path.read_text())
    change(doc)
    path.write_text(yaml.safe_dump(doc))


@pytest.mark.parametrize(
    ("case", "file", "change", "message"),
    [
        ("unregistered config key", "datasets/erp.delivery.yaml", lambda d: d["config"].update(retry_cuont="3"),
         "setting 'retry_cuont' is not registered"),
        ("contract version not the contract's", "datasets/mes.press.yaml", lambda d: d.update(contract_version="9.9.9"),
         "manifest pins contract 9.9.9"),
        ("required field missing", "datasets/lims.inspection_result.yaml", lambda d: d.pop("owners"),
         "missing field: owners"),
        ("history ordered by a column the contract lacks", "datasets/erp.sales_order_line.yaml",
         lambda d: d["load"].update(history_order_by="change_no"), "load.history_order_by 'change_no'"),
        ("unregistered domain setting", "platform.yaml", lambda d: d.update(settings={"timezone": "UTC"}),
         "platform.yaml: setting 'timezone' is not registered"),
    ],
)
def test_bad_manifest_refused_before_any_write(tmp_path, case, file, change, message):
    shutil.copytree(TYRE.parent, tmp_path / "slice")
    _break(tmp_path / "slice/manifest" / file, change)
    store = MemoryMetaStore()
    with pytest.raises(ManifestError, match=message.replace(".", r"\.")):
        loader.load(store, tmp_path / "slice/manifest")
    assert (store.domains, store.ds, store.registry) == ({}, {}, {})


@pytest.mark.parametrize(
    ("key", "value", "problem"),
    [
        ("load_type", "INCREMENTAL", None),
        ("load_type", "incremental", "load_type='incremental' is not one of FULL, INCREMENTAL"),
        ("retry_count", "3", None),
        ("retry_count", "three", "retry_count='three' is not a whole number"),
        ("csv_has_header", "TRUE", None),
        ("csv_has_header", "yes", "csv_has_header='yes' is not true or false"),
        ("cost_center", "anything", None),
    ],
)
def test_registry_type_check(key, value, problem):
    assert registry.type_problem(registry.by_key(registry.load(), "dataset")[key], value) == problem


def test_registry_matches_the_old_key_registry():
    """The JSON the kernel reads is exactly what the exporter builds from LakeIngestionFramework's registry."""
    pytest.importorskip("pydantic")
    import importlib.util

    script = Path(__file__).parents[2] / "scripts/export_settings_registry.py"
    spec = importlib.util.spec_from_file_location("export", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert json.loads(registry.PATH.read_text()) == mod.build(CATALOG / "dpm/keys.yaml")


def test_tyre_view_same_on_postgres(tyre):
    """M5 end to end: the loader fills both adapters and every dataset view agrees."""
    from test_meta_store import fresh_postgres  # same folder, on the path (rootdir import mode)

    memory, _ = tyre
    pg = fresh_postgres()
    loader.load(pg, TYRE)
    assert [d.__dict__ for d in pg.datasets("tyre")] == [d.__dict__ for d in memory.datasets("tyre")]
    for d in memory.datasets("tyre"):
        assert pg.dataset_view(d.source, d.dataset) == memory.dataset_view(d.source, d.dataset), d.dataset


DQ_CASES = [
    ("a column rule from the contract, critical stops", "erp", "sales_order_line", "ordered_qty-sql-1",
     ("critical", "stop", "ordered_qty")),
    ("a major rule quarantines", "erp", "sales_order_line", "line_status-invalidValues-1",
     ("major", "quarantine", "line_status")),
    ("a table rule has no column", "erp", "sales_order_line", "table-duplicateValues-1", ("critical", "stop", "")),
]


@pytest.mark.skipif(not TYRE.exists(), reason="needs ~/data-platform-catalog")
@pytest.mark.parametrize(("name", "source", "dataset", "rule_id", "want"), DQ_CASES, ids=[c[0] for c in DQ_CASES])
def test_loader_writes_contract_quality_rules(name: str, source: str, dataset: str, rule_id: str,
                                              want: tuple[str, str, str]) -> None:
    from dataplatform.adapters.memory_meta import MemoryMetaStore

    store = MemoryMetaStore()
    loader.load(store, TYRE)
    rule = next(r for r in store.dq_rules(source, dataset) if r.rule_id == rule_id)
    assert (rule.severity, rule.action, rule.column) == want
