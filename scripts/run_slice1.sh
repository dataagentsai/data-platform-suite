#!/usr/bin/env bash
# Run slice 1 end to end on Databricks: a clean start, then one job run per world day, then the evidence.
#
#   scripts/run_slice1.sh [profile] [days]      defaults: free 7
#
# Clean start: destroying and redeploying the bundle replaces the silver pipeline; the reset job then drops every
# dp_<env>_* schema, silver included, because the old pipeline's tables stay registered to it and would block the new one.
set -euo pipefail
PROFILE="${1:-free}"; DAYS="${2:-7}"
cd "$(dirname "$0")/../realms/databricks"
B="databricks bundle -p $PROFILE"

uv run --no-project --python 3.12 --with pyyaml python ../../scripts/dp_compile.py >/dev/null
$B destroy --auto-approve || true
$B deploy
$B run dp_slice1_reset
for d in $(seq 1 "$DAYS"); do
  echo "=== world day $d ==="
  for attempt in 1 2 3; do               # retry only when the CLI could not reach the workspace
    if out=$($B run dp_slice1_day --params day="$d" 2>&1); then echo "$out"; break; fi
    echo "$out"
    echo "$out" | grep -q "TERMINATED\|INTERNAL_ERROR" && { echo "day $d: checks failed (see evidence)"; break; }
    echo "day $d: could not reach the workspace (attempt $attempt); retrying"; sleep 20
  done
done
