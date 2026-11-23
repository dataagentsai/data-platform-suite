"""Offline checks of the Azure Databricks realm: names, layers, secrets, budget, isolation, pinned versions, bundle.

Table-driven: every check is a parametrised table, one row per case. Nothing here touches Azure or Databricks; the
plan itself is checked by `terraform test` against mocked providers (infra/platform/tests).
"""
import importlib.util
import os
import re
import sys
from datetime import date
from pathlib import Path

import pytest

hcl2 = pytest.importorskip("hcl2")
yaml = pytest.importorskip("yaml")

REALM = Path(__file__).resolve().parents[1]
REPO = REALM.parents[1]
INFRA = REALM / "infra"
sys.path.insert(0, str(REPO / "src"))

from dataplatform.adapters.azure_blob import account_name  # noqa: E402
from dataplatform.adapters.secrets import key_vault_name  # noqa: E402
from dataplatform.cloud_ports import LAYERS  # noqa: E402

spec = importlib.util.spec_from_file_location("generate_bundle", REALM / "generate_bundle.py")
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)

SKIP_DIRS = {".terraform", "__pycache__", ".databricks", ".pytest_cache", ".complexipy_cache"}


def files(*suffixes):
    return sorted(p for p in REALM.rglob("*") if p.is_file() and not (SKIP_DIRS & set(p.parts))
                  and (not suffixes or p.suffix in suffixes or p.name.endswith(suffixes)))


def text(p):
    return p.read_text()


def clean(x):
    """python-hcl2 keeps the quotes of strings and wraps expressions in ${...}; undo both."""
    if isinstance(x, dict):
        return {clean(k): clean(v) for k, v in x.items() if k not in ("__is_block__", "__comments__")}
    if isinstance(x, list):
        return [clean(v) for v in x]
    if isinstance(x, str):
        if x.startswith("${") and x.endswith("}"):
            return x[2:-1]
        if len(x) >= 2 and x[0] == x[-1] == '"':
            return x[1:-1]
    return x


def parse(path):
    with open(path) as f:
        return clean(hcl2.load(f))


def variables():
    out = {}
    for block in parse(INFRA / "platform/variables.tf").get("variable", []):
        (name, body), = block.items()
        out[name] = body
    return out


def example(env):
    return parse(INFRA / f"envs/{env}.tfvars.example")


def backend(env):
    return parse(INFRA / f"envs/{env}.backend.hcl.example")


def prefix_rule():
    cond = variables()["name_prefix"]["validation"][0]["condition"]
    return re.compile(re.search(r'regex\("([^"]+)"', cond).group(1))


ENVS = ["dev", "prod"]
KV_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9-]{1,22}[A-Za-z0-9]$")


# ----------------------------------------------------------------------------------------------------- naming rules
@pytest.mark.parametrize("prefix,accepted", [
    ("acme", True), ("a1", True), ("changeme", True), ("abcdefghijklmn", True),
    ("Acme", False), ("acme-dp", False), ("acme_dp", False), ("1acme", False), ("a", False), ("", False),
    ("abcdefghijklmno", False), ("acme.dp", False),
])
def test_prefix_rule_matches_what_azure_allows(prefix, accepted):
    """The rule in variables.tf accepts exactly the prefixes whose every derived name is legal in Azure."""
    assert bool(prefix_rule().fullmatch(prefix)) is accepted
    if accepted:
        for env in ENVS:
            assert account_name(prefix, env)                       # <prefix><env>: the platform adapter agrees
            assert account_name(prefix, f"{env}pi")                # personal-data account
            assert KV_NAME.match(f"{prefix}-{env}-kv") and "--" not in f"{prefix}-{env}-kv"


@pytest.mark.parametrize("env", ENVS)
def test_example_names_are_legal(env):
    prefix = example(env)["name_prefix"]
    names = {
        "storage": account_name(prefix, env),
        "personal": account_name(prefix, env + "pi"),
        "state": backend(env)["storage_account_name"],
    }
    assert all(re.fullmatch(r"[a-z0-9]{3,24}", n) for n in names.values()), names
    assert KV_NAME.match(f"{prefix}-{env}-kv")


@pytest.mark.parametrize("secret_ref,vault_name", [
    ("sources/erp", "sources--erp"), ("token-key", "token-key"), ("a/b/c", "a--b--c"),
])
def test_secret_references_map_to_key_vault_names(secret_ref, vault_name):
    assert key_vault_name(secret_ref) == vault_name
    assert KV_NAME.match(f"x{vault_name}") or re.fullmatch(r"[A-Za-z0-9-]{1,127}", vault_name)


# ------------------------------------------------------------------------------------- every layer has both halves
def test_default_layers_are_the_platforms_layers():
    assert tuple(variables()["layers"]["default"]) == LAYERS


@pytest.mark.parametrize("layer", LAYERS)
def test_every_layer_has_a_container_and_an_external_location(layer):
    assert layer in variables()["layers"]["default"]
    storage, catalog = text(INFRA / "modules/storage/main.tf"), text(INFRA / "modules/unity_catalog/main.tf")
    assert re.search(r'azurerm_storage_container" "layer" \{\s*for_each\s*=\s*toset\(var\.layers\)', storage)
    assert re.search(r'databricks_external_location" "layer" \{\s*for_each\s*=\s*local\.url', catalog)
    assert "for l in var.layers" in catalog


@pytest.mark.parametrize("layer", ["landing", "bronze", "silver", "gold", "ops"])
def test_every_catalog_layer_has_a_container_behind_it(layer):
    v = variables()
    assert layer in v["catalog_layers"]["default"] and layer in v["layers"]["default"]
    assert re.search(r'name\s*=\s*"\$\{var\.env\}_\$\{each\.key\}"', text(INFRA / "modules/unity_catalog/main.tf"))


@pytest.mark.parametrize("needle", [
    'resource "databricks_volume" "files"', 'volume_type      = "EXTERNAL"', '"landing"',
    'securable_type = "catalog"', 'securable_type = "external_location"', 'securable_type = "storage_credential"',
])
def test_unity_catalog_module_has_the_pieces_the_engine_job_needs(needle):
    assert needle in text(INFRA / "modules/unity_catalog/main.tf")


def test_personal_data_has_its_own_account_with_a_purge():
    s = text(INFRA / "modules/storage/main.tf")
    assert 'azurerm_storage_account" "personal"' in s and 'azurerm_storage_management_policy" "personal_purge"' in s
    assert variables().get("layers") and "personal_purge_days" in s


# ---------------------------------------------------------------------------------- no ids, emails or secrets
GUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
ZERO_GUID = re.compile(r"^0{8}-0{4}-0{4}-0{4}-0{11}[0-9]$")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
# The words that must never appear are kept in a file outside the repository (EXPLAINER_FORBIDDEN_FILE), so the list
# itself is not shipped. Without the file this check matches nothing; the repository's release script runs it with the file.
_WORDS = os.environ.get("EXPLAINER_FORBIDDEN_FILE")
FORBIDDEN = re.compile(Path(_WORDS).expanduser().read_text().strip() if _WORDS else r"(?!x)x", re.I)


@pytest.mark.parametrize("name,pattern,allow", [
    ("a real GUID (subscription, tenant, account)", GUID, lambda m: bool(ZERO_GUID.match(m))),
    ("an email address", EMAIL, lambda m: False),
    ("a storage key or SAS token", re.compile(r"AccountKey=|SharedAccessKey=|[?&]sig=|sv=20\d\d-"), lambda m: False),
    ("a password or client secret", re.compile(r"(password|client_secret|secret_key)\s*=\s*\"[^\"]+\"", re.I),
     lambda m: False),
    ("a databricks token", re.compile(r"\bdapi[0-9a-f]{20,}"), lambda m: False),
    ("a workspace host id", re.compile(r"adb-\d{6,}"), lambda m: False),
    ("a forbidden name", FORBIDDEN, lambda m: False),
])
def test_no_hard_coded_ids_emails_or_secrets(name, pattern, allow):
    hits = []
    for p in files():
        if p.name == "test_realm.py" or p.suffix in {".pyc"}:
            continue
        for m in pattern.finditer(text(p)):
            if not allow(m.group(0)):
                hits.append(f"{p.relative_to(REALM)}: {m.group(0)}")
    assert not hits, f"{name}: {hits}"


@pytest.mark.parametrize("var", ["subscription_id", "budget_emails"])
def test_owner_values_have_no_default_in_tracked_files(var):
    default = variables()[var].get("default")
    assert default in (None, []), f"{var} must come from the environment or the untracked tfvars"
    for env in ENVS:
        assert var not in example(env) or example(env)[var] == []


# -------------------------------------------------------------------------------------------------------- budget
@pytest.mark.parametrize("needle", [
    'resource "azurerm_consumption_budget_resource_group" "env"', 'time_grain        = "Monthly"',
    'threshold_type = notification.value.kind', 'contact_emails = var.emails', 'contact_roles',
])
def test_budget_module_has_an_alert(needle):
    assert needle in text(INFRA / "modules/budget/main.tf")


@pytest.mark.parametrize("env", ENVS)
def test_budget_is_set_in_the_billing_currency_and_wired(env):
    assert 0 < example(env)["budget_amount"] <= 5000  # the billing currency, not dollars: 20 in rupees was 20 rupees
    assert 'module "budget"' in text(INFRA / "platform/main.tf")


# ------------------------------------------------------------------------------------------ dev and prod isolated
def test_dev_and_prod_have_their_own_state_and_settings():
    d, p, bd, bp = example("dev"), example("prod"), backend("dev"), backend("prod")
    assert (d["env"], p["env"]) == ("dev", "prod")
    assert bd["key"] != bp["key"] and "dev" in bd["key"] and "prod" in bp["key"]
    assert d["force_destroy"] is True and p["force_destroy"] is False
    assert p["key_vault_purge_protection"] is True


@pytest.mark.parametrize("module,needle", [
    ("resource_group", '"${var.name_prefix}-${var.env}-rg"'),
    ("storage", '"${var.name_prefix}${var.env}"'),
    ("storage", '"${var.name_prefix}${var.env}pi"'),
    ("key_vault", '"${var.name_prefix}-${var.env}-kv"'),
    ("access_connector", '"${var.name_prefix}-${var.env}-uc"'),
    ("workspace", '"${var.name_prefix}-${var.env}-dbx"'),
    ("budget", '"${var.name_prefix}-${var.env}"'),
    ("unity_catalog", '"${var.env}_layers"'),
    ("lakebase", '"dp-meta-${var.env}"'),
])
def test_every_resource_name_carries_the_environment(module, needle):
    assert needle in text(INFRA / f"modules/{module}/main.tf")


@pytest.mark.parametrize("securable,needle", [
    ("catalog", 'isolation_mode = "ISOLATED"'),
    ("external location", 'isolation_mode  = "ISOLATION_MODE_ISOLATED"'),
    ("credential", 'isolation_mode = "ISOLATION_MODE_ISOLATED"'),
])
def test_unity_catalog_objects_are_isolated_to_their_workspace(securable, needle):
    assert needle in text(INFRA / "modules/unity_catalog/main.tf"), securable


def test_connector_reaches_only_its_own_environments_storage():
    s = text(INFRA / "modules/access_connector/main.tf")
    assert s.count('resource "azurerm_role_assignment"') == 1 and '"Storage Blob Data Contributor"' in s
    assert "scope                = var.storage_account_id" in s


# ------------------------------------------------------------------------------------------ pinned providers
PUBLISHED = {"azurerm": ("5.12.0", date(2026, 10, 29)), "databricks": ("1.141.0", date(2026, 11, 2))}


@pytest.mark.parametrize("provider", sorted(PUBLISHED))
def test_provider_is_pinned_exactly_at_a_release_older_than_two_weeks(provider):
    version, published = PUBLISHED[provider]
    assert f'version = "= {version}"' in text(INFRA / "platform/versions.tf")
    assert (date(2026, 11, 19) - published).days >= 14


def test_backend_is_azure_and_driven_by_a_file():
    assert 'backend "azurerm" {}' in text(INFRA / "platform/versions.tf")
    assert all(backend(e)["use_azuread_auth"] is True and "access_key" not in backend(e) for e in ENVS)


# --------------------------------------------------------------------------------------------------------- bundle
def test_committed_bundle_is_up_to_date():
    assert (REALM / "bundle/databricks.yml").read_text() == gen.render()


@pytest.mark.parametrize("target,env", [("azure-dev", "dev"), ("azure-prod", "prod")])
def test_bundle_targets_pick_the_environment_and_no_host(target, env):
    t = yaml.safe_load(gen.render())["targets"][target]
    assert t["variables"] == {"env": env, "meta_instance": f"dp-meta-{env}"}
    assert "host" not in t["workspace"] and t["mode"] == "production"


def test_bundle_has_the_engine_job_and_its_files_exist():
    doc = yaml.safe_load(gen.render())
    assert doc["resources"]["jobs"]["engine_days"]["name"] == "dp-${var.env}-engine-days"
    base = REALM / "bundle"
    for job in doc["resources"]["jobs"].values():
        for task in job["tasks"]:
            assert (base / task["spark_python_task"]["python_file"]).resolve().exists()
    for p in doc["sync"]["paths"]:
        assert (base / p).resolve().exists(), p


@pytest.mark.parametrize("secret", ["host:", "token", "client_secret"])
def test_bundle_file_has_no_credentials(secret):
    assert secret not in (REALM / "bundle/databricks.yml").read_text()
