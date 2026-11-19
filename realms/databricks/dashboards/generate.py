"""Write (or check) the ops dashboards, the ops metric views and the adopted native set (ADR-024).

    <id>.lvdash.json      observability's dashboards (dataplatform.obs.dashboards) and DES's (dataplatform.des)
    views/mv_ops_*.sql    the ops metric views the dashboards read (dataplatform.obs.views); run before publishing
    native.yml            platform-native screens and notifications adopted, not rebuilt (dataplatform.obs.native)
    resources.yml         the bundle's dashboard resources

    PYTHONPATH=src python realms/databricks/dashboards/generate.py            # write
    PYTHONPATH=src python realms/databricks/dashboards/generate.py --check    # exit 1 if a file is out of date

Catalog and schema of the ops tables: --catalog / --schema (defaults dev_ops / ops, the Free Edition dev environment).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dataplatform.adapters.databricks_native import render_native_yaml
from dataplatform.adapters.databricks_ops_views import view_sql
from dataplatform.adapters.lakeview import bundle_resources, render_file
from dataplatform.obs.dashboards import every
from dataplatform.obs.views import VIEWS

HERE = Path(__file__).parent


def files(catalog: str, schema: str) -> dict[Path, str]:
    boards = every()
    out = {HERE / f"{d.id}.lvdash.json": render_file(d, catalog, schema) for d in boards}
    out.update({HERE / "views" / f"{v.name}.sql": view_sql(v, catalog, schema) for v in VIEWS})
    native = tuple(sorted(p.name.removesuffix(".lvdash.json") for p in (HERE / "native").glob("*.lvdash.json")))
    out[HERE / "resources.yml"] = bundle_resources(boards, native)  # native/: the platform's own, unchanged
    out[HERE / "native.yml"] = render_native_yaml()
    return out


def stale_files() -> list[Path]:
    """Generated files no definition produces any more (a dashboard or view removed)."""
    keep = set(files("x", "x"))
    found = [*HERE.glob("*.lvdash.json"), *(HERE / "views").glob("*.sql")]
    return sorted(p for p in found if p not in keep)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="dev_ops")
    ap.add_argument("--schema", default="ops")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    stale = []
    for path, text in files(args.catalog, args.schema).items():
        if args.check:
            if not path.exists() or path.read_text() != text:
                stale.append(path.name)
        else:
            path.parent.mkdir(exist_ok=True)
            path.write_text(text)
    for path in stale_files():
        if args.check:
            stale.append(f"{path.name} (no longer generated)")
        else:
            path.unlink()
    if stale:
        print("out of date: " + ", ".join(stale) + " (run generate.py)", file=sys.stderr)  # noqa: T201
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
