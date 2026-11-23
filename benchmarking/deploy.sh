#!/bin/bash
# Deploy and run the benchmark on the office Databricks. Nothing here generates data on this machine.
#   benchmarking/deploy.sh <databricks-profile> [validate|deploy|maintenance|world|all]
# `validate` is safe anywhere (checks the scenario files and both bundles); the others need the workspace.
set -euo pipefail
PROFILE="${1:?databricks profile name}"; STEP="${2:-validate}"
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/.." && pwd)"
SLICE="${SLICE:-02-tyre}"; CATALOG="${DP_CATALOG:-$HOME/data-platform-catalog}"
FORMATS=(iceberg-uc delta-uc-uniform)          # s3tables runs on its own cluster job, see README
SCALE="${SCALE:-0.1}"; DAYS="${DAYS:-3}"

validate() {
  (cd "$HERE" && python3 -m bench.plan --check)
  (cd "$HERE/databricks" && databricks bundle validate -p "$PROFILE")
  (cd "$ROOT/realms/databricks/engine" && databricks bundle validate -p "$PROFILE")
}
deploy() {
  mkdir -p "$ROOT/realms/databricks/engine/_catalog/slices"
  rsync -a --delete "$CATALOG/slices/$SLICE" "$ROOT/realms/databricks/engine/_catalog/slices/"
  (cd "$ROOT/realms/databricks/engine" && databricks bundle deploy -p "$PROFILE")
  (cd "$HERE/databricks" && databricks bundle deploy -p "$PROFILE")
}
maintenance() {
  for f in "${FORMATS[@]}"; do
    (cd "$HERE/databricks" && databricks bundle run bench_maintenance -p "$PROFILE" --params "table_format=$f")
  done
  (cd "$HERE/databricks" && databricks bundle run bench_maintenance_s3tables -p "$PROFILE")
}
world() {
  for f in "${FORMATS[@]}"; do
    (cd "$ROOT/realms/databricks/engine" && databricks bundle run engine_days -p "$PROFILE" \
      --params "table_format=$f,days=$DAYS,scale=$SCALE")
  done
}
case "$STEP" in
  validate) validate ;;
  deploy) deploy ;;
  maintenance) maintenance ;;
  world) world ;;
  all) validate; deploy; maintenance; world ;;
  *) echo "unknown step $STEP" >&2; exit 2 ;;
esac
