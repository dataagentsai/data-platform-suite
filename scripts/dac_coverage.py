"""DAC coverage report (B20 C20): which platform rules have a passing test, a waiver, or neither.

    python scripts/dac_coverage.py [--catalog ~/data-platform-catalog] [--out build/dac_coverage.md]

Reads the catalog's obligation files, every test and code file of this repo, and docs/dac-waivers.yaml; writes a
Markdown report and prints the counts. The test files cite the rule they prove by id.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dataplatform.governance.coverage import coverage, left, rules_from, summary  # noqa: E402


def texts(paths: list[Path]) -> dict[str, str]:
    return {str(p.relative_to(ROOT)): p.read_text(errors="ignore") for p in paths}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", type=Path, default=Path.home() / "data-platform-catalog")
    ap.add_argument("--out", type=Path, default=ROOT / "build/dac_coverage.md")
    a = ap.parse_args()
    rules = rules_from(yaml.safe_load(p.read_text()) for p in sorted((a.catalog / "dac/obligations").glob("*.yaml")))
    # a CI job that fails the build is a test too (e.g. the secret scan proves REL-05)
    tests = texts(sorted((ROOT / "tests").rglob("*.py")) + sorted((ROOT / ".github/workflows").glob("*.yml")))
    code = texts(sorted((ROOT / "src/dataplatform").rglob("*.py")) + sorted((ROOT / "realms").rglob("*.py"))
                 + sorted((ROOT / "scripts").glob("*.py")))
    waiver_file = ROOT / "docs/dac-waivers.yaml"
    waivers = (yaml.safe_load(waiver_file.read_text()) or []) if waiver_file.exists() else []
    covered = coverage(rules, tests, code, waivers, datetime.now(UTC).date())
    counts = summary(covered)
    done = counts["tested"] + counts["waived"]
    lines = [f"# DAC coverage: {done} of {len(rules)} rules tested or waived", "",
             " ".join(f"{k}: {v}." for k, v in counts.items()), "", "## Left (MUST first)", "",
             "| Rule | Level | Concern | Title | State | Cited in code |", "|---|---|---|---|---|---|"]
    lines += [f"| {c.rule.id} | {c.rule.level} | {c.rule.concern} | {c.rule.title} | {c.state} | "
              f"{', '.join(c.where[:3])} |" for c in left(covered)]
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text("\n".join(lines) + "\n")
    sys.stdout.write(f"DAC coverage: {done} of {len(rules)} tested or waived ({counts}); report {a.out}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
