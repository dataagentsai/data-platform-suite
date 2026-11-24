"""Write explainer/data/sources.json: the source families and the source profiles (products and their routes, each with a
proof level), read from the specification catalog. Run with pyyaml available:

    uv run --no-project --with pyyaml python explainer/make_sources.py        (DP_CATALOG names the catalog checkout)
"""

import json
import os
from pathlib import Path

import yaml

catalog = Path(os.environ.get("DP_CATALOG") or Path.home() / "data-platform-catalog").expanduser()
fam = yaml.safe_load((catalog / "dpc/source_families.yaml").read_text())
prof = yaml.safe_load((catalog / "dbs/sources/source_profiles.yaml").read_text())
out = {
    "as_of": prof["as_of"],
    "arrival_shapes": fam["arrival_shapes"],
    "initiators": fam["initiators"],
    "capabilities": fam["capabilities"],
    "proof_levels": {str(k): v for k, v in fam["proof_levels"].items()},
    "families": fam["families"],
    "products": prof["products"],
}
path = Path(__file__).resolve().parent / "data/sources.json"
path.write_text(json.dumps(out, indent=1, ensure_ascii=False, default=str) + "\n")
print(f"{len(out['families'])} families, {len(out['products'])} products -> {path}")
