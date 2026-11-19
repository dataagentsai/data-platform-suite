"""Turn on the platform's anomaly detection (freshness and completeness, limits learned per table) on every schema of
the silver and gold catalogs: the adopted half of "freshness" (obs/native.py `anomaly_detection`). Idempotent: a
schema already watched is left alone.

    python realms/databricks/dashboards/enable_anomaly_detection.py --profile free --env dev
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

SKIP = {"information_schema", "default"}


def cli(profile: str, *args: str) -> object:
    out = subprocess.run(["databricks", *args, "-p", profile, "-o", "json"],  # noqa: S603, S607 - our own CLI call
                         capture_output=True, text=True, check=False)
    if out.returncode:
        raise RuntimeError(out.stderr.strip() or out.stdout.strip())
    return json.loads(out.stdout or "null")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="free")
    ap.add_argument("--env", default="dev")
    a = ap.parse_args()
    for catalog in (f"{a.env}_silver", f"{a.env}_gold"):
        for schema in cli(a.profile, "schemas", "list", catalog) or []:
            if schema["name"] in SKIP:
                continue
            sid, name = schema["schema_id"], schema["full_name"]
            try:
                cli(a.profile, "api", "get", f"/api/data-quality/v1/monitors/schema/{sid}")
                print(f"{name}: already watched")  # noqa: T201
                continue
            except RuntimeError:
                pass
            body = json.dumps({"object_type": "schema", "object_id": sid, "anomaly_detection_config": {}})
            cli(a.profile, "api", "post", "/api/data-quality/v1/monitors", "--json", body)
            print(f"{name}: anomaly detection on")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
