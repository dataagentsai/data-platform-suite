"""Install the metadata store (ADR-010) on a Lakebase instance and load a domain's manifest into it.

    DATABRICKS_CONFIG_PROFILE=free uv run --no-project --python 3.12 --with 'psycopg[binary]' --with pyyaml \
        python realms/aws-databricks/meta_setup.py --instance dp-meta-dev \
        --manifest ~/data-platform-catalog/slices/02-tyre/manifest

Steps, all idempotent: a short-lived OAuth token from `databricks database generate-database-credential` (held in
this process's PGPASSWORD only, never written anywhere), CREATE DATABASE <database> if missing, install the schemas
and functions (adapters/sql/postgres_meta.sql via postgres_meta.install), run the W1a validating loader, then read
back through the read procedures (config.list_datasets, config.dataset_settings) as the check.

Prints the DP_META_DSN to use (no password in it: callers mint their own token the same way).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess  # nosec B404 - runs the Databricks CLI with fixed arguments, no shell
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from dataplatform.adapters.postgres_meta import PostgresMetaStore  # noqa: E402
from dataplatform.meta.loader import load  # noqa: E402


def cli(*args: str) -> dict:
    # S603/S607: the Databricks CLI from PATH with fixed arguments built here, no shell (nosec B603 B607 likewise)
    out = subprocess.run(["databricks", *args, "-o", "json"],  # noqa: S603, S607  # nosec B603 B607
                         check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def say(line: str) -> None:
    sys.stdout.write(line + "\n")


def dsn(instance: str, database: str) -> str:
    host = cli("database", "get-database-instance", instance)["read_write_dns"]
    user = cli("current-user", "me")["userName"]
    return f"host={host} port=5432 dbname={database} user={user} sslmode=require"


def token(instance: str) -> str:
    body = json.dumps({"instance_names": [instance], "request_id": str(uuid.uuid4())})
    return cli("database", "generate-database-credential", "--json", body)["token"]


def wake(instance: str) -> None:
    """Lakebase moved to Autoscaling (projects, branches, endpoints): an instance made on 4 Nov is now project
    <instance> with endpoint production/primary, and an endpoint can be disabled (seen 13 Nov: connections refused
    with "The endpoint has been disabled"). Enable it if so; an idle endpoint wakes on connect by itself."""
    endpoint = f"projects/{instance}/branches/production/endpoints/primary"
    try:
        status = cli("postgres", "get-endpoint", endpoint).get("status") or {}
    except subprocess.CalledProcessError:
        return  # not an Autoscaling project (an older provisioned instance): nothing to enable
    if status.get("disabled"):
        cli("postgres", "update-endpoint", endpoint, "spec.disabled", "--json", '{"spec": {"disabled": false}}',
            "--timeout", "5m")
        say(f"enabled the disabled endpoint {endpoint}")


def binding_settings(manifest: Path, chosen: dict[str, str | None]) -> dict[str, dict[str, str]]:
    """{"source.dataset": {key: value}} for every dataset file of the manifest: the settings the caller chose (a None is
    left out), added on top of the manifest by the loader. Empty when nothing was chosen."""
    import yaml  # noqa: PLC0415 -- only this command needs it

    values = {k: v for k, v in chosen.items() if v}
    if not values:
        return {}
    out: dict[str, dict[str, str]] = {}
    for f in sorted((manifest / "datasets").glob("*.yaml")):
        d = yaml.safe_load(f.read_text())
        out[f"{d['source_name']}.{d['dataset_name']}"] = dict(values)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--instance", required=True)
    p.add_argument("--database", default="dp_meta")
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--table-format", choices=["iceberg-uc", "delta-uc-uniform", "s3tables"],
                   help="record this table format on every dataset (a binding-level setting, in the run's fingerprint)")
    p.add_argument("--silver-engine", choices=["job", "declarative"], help="record this silver engine on every dataset")
    a = p.parse_args()

    wake(a.instance)
    meta_dsn = dsn(a.instance, a.database)
    os.environ["PGPASSWORD"] = token(a.instance)  # libpq reads it; lives only in this process
    store = PostgresMetaStore.connect(meta_dsn, create=True, install_schema=True)
    report = load(store, a.manifest.expanduser(), extra_settings=binding_settings(
        a.manifest.expanduser(), {"table_format": a.table_format, "silver_engine": a.silver_engine}))
    say(f"loaded domain {report.domain}: {len(report.loaded)} datasets; warnings {len(report.warnings)}; "
          f"in store but not in manifest {report.not_in_manifest}")
    for w in report.warnings:
        say(f"  warning: {w}")

    datasets = store.datasets(report.domain)
    say(f"read back config.list_datasets('{report.domain}'): {len(datasets)} rows")
    for d in datasets:
        say(f"  {d.source}.{d.dataset}: {len(store.settings(d.source, d.dataset))} settings")
    say(f"DP_META_DSN='{meta_dsn}'  (password: a token from generate-database-credential, ~1 hour)")
    return 0 if len(datasets) == len(report.loaded) else 1


if __name__ == "__main__":
    sys.exit(main())
