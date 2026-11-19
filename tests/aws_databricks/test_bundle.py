"""The Databricks on AWS bundle is generated from the compiled domain summaries (M4), never hand-written.

Table-driven: each row is a domain summary and what the generator must make of it, or the error that stops it.
"""
import importlib.util
import json
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO = Path(__file__).parents[2]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, REPO / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen = _load("generate_bundle", "realms/aws-databricks/generate_bundle.py")
compiler = _load("dp_compile_domain", "scripts/dp_compile_domain.py")

TYRE = json.loads((REPO / "realms/databricks/compiled/domains/tyre.json").read_text())


def test_committed_bundle_is_up_to_date():
    for name, text in gen.generate().items():
        assert (REPO / "realms/aws-databricks/bundle" / name).read_text() == text, f"stale {name}: run the generator"


def test_generation_is_deterministic():
    assert gen.generate() == gen.generate()


def domain(**over):
    d = json.loads(json.dumps(TYRE))
    d.update(over)
    return d


ONE = [{"source": "erp", "dataset": "material", "is_enabled": True}]

# name -> (domain summary, the error that stops the generation or None)
CASES = {
    "the tyre domain": (domain(), None),
    "one dataset is enough": (domain(datasets=ONE), None),
    "a disabled dataset beside an enabled one": (
        domain(datasets=[*ONE, {**ONE[0], "dataset": "old", "is_enabled": False}]), None),
    "domain name breaks ADR-012": (domain(domain="Tyre-EU"), "not lower snake case"),
    "dataset name breaks ADR-012": (domain(datasets=[{**ONE[0], "dataset": "Material-EU"}]), "not lower snake case"),
    "no business timezone": (domain(business_timezone=""), "no business_timezone"),
    "every dataset disabled": (domain(datasets=[{**ONE[0], "is_enabled": False}]), "no enabled dataset"),
    "no dataset at all": (domain(datasets=[]), "no enabled dataset"),
}


@pytest.mark.parametrize("case", CASES)
def test_platform_job(case):
    d, error = CASES[case]
    if error:
        with pytest.raises(gen.BundleError, match=error):
            gen.platform_job(d)
        return
    job = gen.platform_job(d)["resources"]["jobs"][f"{d['domain']}__platform"]
    assert [t["task_key"] for t in job["tasks"]] == ["ingest", "silver", "gold"]
    assert [t.get("depends_on") for t in job["tasks"]] == [None, [{"task_key": "ingest"}], [{"task_key": "silver"}]]
    assert job["max_concurrent_runs"] == 1                       # two runs never load one domain at once
    assert job["tags"]["env"] == "${var.env}"                    # cost split by environment
    assert job["parameters"] == [{"name": "window", "default": ""}]
    assert job["schedule"] == {"quartz_cron_expression": "0 0 1 * * ?", "timezone_id": d["business_timezone"],
                               "pause_status": "PAUSED"}         # declared, and never fires until someone unpauses it
    for task in job["tasks"]:
        params = task["spark_python_task"]["parameters"]
        assert params[params.index("--hop") + 1] == task["task_key"]
        assert params[params.index("--business-tz") + 1] == d["business_timezone"]
        assert params[params.index("--env") + 1] == "${var.env}"
        assert "token" not in " ".join(params).lower()            # the key comes from the secret scope


@pytest.mark.parametrize("case", [c for c, (_, error) in CASES.items() if not error])
def test_proof_job_drives_the_platform_job(case):
    d, _ = CASES[case]
    job = gen.proof_job(d)["resources"]["jobs"][f"{d['domain']}__proof"]
    params = job["tasks"][0]["spark_python_task"]["parameters"]
    assert params[params.index("--platform-job-id") + 1] == f"${{resources.jobs.{d['domain']}__platform.id}}"
    assert [p["name"] for p in job["parameters"]] == ["days", "calendar", "housekeeping_days"]
    assert "schedule" not in job                                  # a proof run is started on purpose
    assert "trigger" not in job


# target -> (env it deploys, workspace host it is pinned to; None = from DATABRICKS_HOST / the CLI profile)
TARGETS = {
    "dev": ("dev", None),
    "prod": ("prod", None),
    "free-dev": ("dev", gen.FREE_EDITION_HOST),
}


@pytest.mark.parametrize("name", TARGETS)
def test_target_env_and_workspace(name):
    env, host = TARGETS[name]
    t = yaml.safe_load(gen.generate()["databricks.yml"])["targets"][name]
    assert t["variables"] == {"env": env, "meta_instance": f"dp-meta-{env}", "secret_scope": f"dp-{env}"}
    assert t["workspace"].get("host") == host
    assert t["mode"] == "production"


def test_the_proof_never_shares_the_live_metadata_database():
    variables = yaml.safe_load(gen.generate()["databricks.yml"])["variables"]
    assert variables["meta_database"]["default"] == "dp_meta_proof"   # the proof run empties its database


# ---------------------------------------------------------------- the domain summary the generator reads
def manifest(tmp_path, platform, datasets, gold=()):
    (tmp_path / "datasets").mkdir(parents=True)
    (tmp_path / "gold").mkdir()
    if platform is not None:
        (tmp_path / "platform.yaml").write_text(yaml.safe_dump(platform))
    for d in datasets:
        (tmp_path / "datasets" / f"{d['source_name']}.{d['dataset_name']}.yaml").write_text(yaml.safe_dump(d))
    for g in gold:
        (tmp_path / "gold" / f"{g}.yaml").write_text("kind: gold\n")
    return tmp_path


PLATFORM = {"kind": "platform", "domain": "tyre", "business_timezone": "Asia/Kolkata"}
MATERIAL = {"source_name": "erp", "dataset_name": "material", "is_enabled": True,
            "config": {"schedule_trigger": "cron", "schedule_cron": "0 1 * * *", "load_type": "FULL"}}
SELLOUT = {"source_name": "partner", "dataset_name": "dealer_sellout", "config": {"schedule_trigger": "on_delivery"}}

# name -> (platform, datasets, gold files, expected summary or the error)
SUMMARY_CASES = {
    "schedules kept, other settings left out": (
        PLATFORM, [MATERIAL], ["fact_otif", "dim_dealer"],
        {"domain": "tyre", "slice": "s", "business_timezone": "Asia/Kolkata", "gold": ["dim_dealer", "fact_otif"],
         "datasets": [{"source": "erp", "dataset": "material", "is_enabled": True, "schedule_trigger": "cron",
                       "schedule_cron": "0 1 * * *"}]}),
    "enabled unless the manifest says otherwise; datasets sorted by file name": (
        PLATFORM, [SELLOUT, MATERIAL], [],
        {"domain": "tyre", "slice": "s", "business_timezone": "Asia/Kolkata", "gold": [],
         "datasets": [{"source": "erp", "dataset": "material", "is_enabled": True, "schedule_trigger": "cron",
                       "schedule_cron": "0 1 * * *"},
                      {"source": "partner", "dataset": "dealer_sellout", "is_enabled": True,
                       "schedule_trigger": "on_delivery"}]}),
    "no platform file": (None, [MATERIAL], [], "no platform.yaml"),
    "no business timezone": ({"kind": "platform", "domain": "tyre"}, [MATERIAL], [], "`business_timezone` is missing"),
    "no dataset": (PLATFORM, [], [], "holds no dataset"),
}


@pytest.mark.parametrize("case", SUMMARY_CASES)
def test_domain_summary(tmp_path, case):
    platform, datasets, gold, want = SUMMARY_CASES[case]
    folder = manifest(tmp_path, platform, datasets, gold)
    if isinstance(want, str):
        with pytest.raises(compiler.ManifestError, match=want):
            compiler.summarise(folder, "s")
        return
    assert compiler.summarise(folder, "s") == want


def test_committed_summary_is_up_to_date_with_the_catalog():
    slice_dir = Path.home() / "data-platform-catalog" / "slices" / TYRE["slice"] / "manifest"
    if not slice_dir.exists():
        pytest.skip("the spec catalog is not beside the repo")
    assert compiler.summarise(slice_dir, TYRE["slice"]) == TYRE, "stale: run scripts/dp_compile_domain.py"
