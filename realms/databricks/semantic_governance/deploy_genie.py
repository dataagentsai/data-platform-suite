"""Deploy the generated Genie space (B12, SERV-06): create it, or update the space with the same title.

    python realms/databricks/semantic_governance/deploy_genie.py --env dev --warehouse <id> [--profile free]

The space is generated (render.py: certified metric views only, instructions, sample questions); this only ships it
through the Databricks CLI, so the workspace never holds a hand-made copy. Found on the first deploy (15 Nov): the
API refuses unsorted lists, so render.py sorts tables and questions.
"""

from __future__ import annotations

import argparse
import json
import subprocess  # nosec B404 - runs the databricks CLI with fixed arguments, no shell
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).parent


def payload(doc: dict[str, Any], warehouse: str) -> dict[str, Any]:
    """The create/update request body from the generated file (warehouse id filled in per environment)."""
    return {"warehouse_id": warehouse, "serialized_space": json.dumps(doc["space"]), "title": doc["title"],
            "description": doc["description"]}


def existing(spaces: list[dict[str, Any]], title: str) -> str | None:
    found = [s["space_id"] for s in spaces if s.get("title") == title]
    if len(found) > 1:
        raise SystemExit(f"{len(found)} Genie spaces are titled {title!r}; remove the extras by hand first")
    return found[0] if found else None


def cli(args: list[str], profile: str) -> Any:
    cmd = ["databricks", *args, "-p", profile, "-o", "json"]
    # the databricks CLI on PATH with fixed arguments and no shell; the only inputs are ids the CLI itself returned
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout  # noqa: S603  # nosec B603 B607
    return json.loads(out) if out.strip() else {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default="dev")
    ap.add_argument("--domain", default="tyre")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--profile", default="free")
    a = ap.parse_args()
    doc = json.loads((HERE / "generated" / a.domain / a.env / "bi" / "genie_space.json").read_text())
    body = payload(doc, a.warehouse)
    space = existing(cli(["genie", "list-spaces"], a.profile).get("spaces", []), doc["title"])
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(body, f)
    if space:
        cli(["genie", "update-space", space, "--json", f"@{f.name}"], a.profile)
        sys.stdout.write(f"updated Genie space {space} ({doc['title']})\n")
    else:
        made = cli(["genie", "create-space", "--json", f"@{f.name}"], a.profile)
        sys.stdout.write(f"created Genie space {made.get('space_id')} ({doc['title']})\n")
    Path(f.name).unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
