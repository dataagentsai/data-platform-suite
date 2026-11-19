"""Integration test on a deployed environment (CI: after deploy dev, before the release gate).

Runs each domain's generated proof job once and fails if it fails. The proof job plays the client: it feeds the
simulated world's days into the landing volume (on AWS an external volume over the S3 landing bucket), triggers the
deployed platform job once per day, and judges every layer against the world's known answers. So a green run means
the deployed jobs loaded the days correctly, with every planted fault handled.

    python realms/aws-databricks/integration.py --env dev [--days 3]

Needs Databricks auth (DATABRICKS_HOST + github-oidc in CI, or a CLI profile locally).
"""
from __future__ import annotations

import argparse
import json
import subprocess  # nosec B404 - runs the databricks CLI with a fixed argument list
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
REALM = REPO / "realms" / "aws-databricks"
DOMAINS = REPO / "realms" / "databricks" / "compiled" / "domains"


def proof_jobs(domains_dir: Path = DOMAINS) -> list[str]:
    """The proof job of every compiled domain, by its key in the generated bundle."""
    return [f"{json.loads(p.read_text())['domain']}__proof" for p in sorted(domains_dir.glob("*.json"))]


def command(env: str, job: str, days: int) -> list[str]:
    return ["databricks", "bundle", "run", "-t", env, job, "--params", f"days={days}"]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env", required=True, choices=["dev", "prod", "free-dev"])
    p.add_argument("--days", type=int, default=3)
    a = p.parse_args()
    for job in proof_jobs():
        sys.stdout.write(f"running {job} in {a.env} for {a.days} day(s)\n")
        subprocess.run(command(a.env, job, a.days), cwd=REALM / "bundle", check=True)  # noqa: S603  # nosec B603
    return 0


if __name__ == "__main__":
    sys.exit(main())
