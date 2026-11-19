"""Write explainer/data/coverage.json: for every cross-cutting concern, its rules and whether each has a test, from this
repository and the specification catalog (the same logic as scripts/dac_coverage.py). Run with pyyaml available:

    uv run --no-project --with pyyaml python explainer/make_coverage.py        (DP_CATALOG names the catalog checkout)
"""

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import yaml

suite = Path(__file__).resolve().parents[1]
catalog = Path(
    os.environ.get("DP_CATALOG")
    or next(
        (c for c in (suite.parent / "data-platform-catalog", Path.home() / "data-platform-catalog") if c.exists()),
        suite.parent / "data-platform-catalog",
    )
)
sys.path.insert(0, str(suite / "src"))
from dataplatform.governance.coverage import coverage, rules_from  # noqa: E402

rules = rules_from(yaml.safe_load(p.read_text()) for p in sorted((catalog / "dac/obligations").glob("*.yaml")))


def texts(paths):
    return {str(p.relative_to(suite)): p.read_text(errors="ignore") for p in paths}


tests = texts(sorted((suite / "tests").rglob("*.py")))
code = texts(
    sorted((suite / "src/dataplatform").rglob("*.py"))
    + sorted((suite / "realms").rglob("*.py"))
    + sorted((suite / "scripts").rglob("*.py"))
)
cov = coverage(rules, tests, code, [], date.today())
out: dict = {}
for c in cov:
    out.setdefault(c.rule.concern, []).append(
        {"id": c.rule.id, "title": c.rule.title, "level": c.rule.level, "state": c.state}
    )
def sha(folder):
    cmd = ["git", "-C", str(folder), "rev-parse", "--short", "HEAD"]
    return subprocess.run(cmd, capture_output=True, text=True, check=False).stdout.strip()  # noqa: S603


Path(__file__).resolve().parent.joinpath("data/coverage.json").write_text(
    json.dumps(
        {"date": str(date.today()), "suite": sha(suite), "catalog": sha(catalog), "rules": out},
        indent=1,
        sort_keys=True,
    )
)
sys.stdout.write(str({k: len(v) for k, v in sorted(out.items(), key=lambda kv: int(kv[0][1:]))}) + "\n")
