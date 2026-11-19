"""Compile a slice's DPM manifest, ODCS contract and DWD world into the JSON the platform loads.

    uv run --with pyyaml python scripts/dp_compile.py [catalog_dir] [slice]

Writes realms/databricks/compiled/<slice>.json. The file is committed so the bundle ships it, and a test checks it
is up to date with the catalog.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dataplatform.manifest import compile_slice  # noqa: E402

catalog = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / "data-platform-catalog"
slice_name = sys.argv[2] if len(sys.argv) > 2 else "01-orders"
out = ROOT / "realms" / "databricks" / "compiled" / f"{slice_name}.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(compile_slice(catalog / "slices" / slice_name), indent=1, sort_keys=True) + "\n")
print(f"compiled {slice_name} -> {out.relative_to(ROOT)}")
