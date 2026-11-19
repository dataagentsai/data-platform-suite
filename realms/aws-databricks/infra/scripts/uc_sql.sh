#!/usr/bin/env bash
# Run one SQL statement on a Databricks SQL warehouse through the statement execution API and fail unless it
# SUCCEEDED. Used by modules/free_edition_catalogs: on Databricks Free Edition the REST "create catalog" call needs a
# storage root the workspace does not have, while `CREATE CATALOG x` in SQL uses the default storage.
#
#   uc_sql.sh <warehouse_id> "<statement>"
#
# Auth: the Databricks CLI's own (DATABRICKS_CONFIG_PROFILE locally, DATABRICKS_HOST + OIDC in CI). Prints no token.
set -euo pipefail
wh="$1"
stmt="$2"
body=$(python3 -c 'import json,sys; print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2], "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}))' "$wh" "$stmt")
resp=$(databricks api post /api/2.0/sql/statements --json "$body")
state() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d["status"]["state"], d["statement_id"])' "$1"; }
read -r st id <<<"$(state "$resp")"
for _ in $(seq 1 30); do
  case "$st" in
    PENDING | RUNNING)
      sleep 5
      resp=$(databricks api get "/api/2.0/sql/statements/$id")
      read -r st id <<<"$(state "$resp")"
      ;;
    *) break ;;
  esac
done
if [ "$st" != "SUCCEEDED" ]; then
  echo "uc_sql: '$stmt' ended $st" >&2
  python3 -c 'import json,sys; print(json.loads(sys.argv[1])["status"].get("error",{}).get("message",""))' "$resp" >&2
  exit 1
fi
echo "uc_sql: $stmt -> SUCCEEDED"
