#!/usr/bin/env bash
# ADR-035 spike, step 1: SCD Type 2 silver with open-source Spark Declarative Pipelines (AUTO CDC) on Iceberg.
#   PY=<python with pyspark[pipelines]==4.3.0>  JAR=<iceberg-spark-runtime jar>  WORK=<scratch folder>  ./run.sh
# Writes spark-pipeline.yml here (git-ignored), feeds batch 1, runs, feeds batch 2, runs again, prints the table.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
: "${PY:?python with pyspark 4.3}" "${JAR:?iceberg runtime jar}" "${WORK:?scratch folder}"
BIN="$(dirname "$PY")"
mkdir -p "$WORK"
cat > "$HERE/spark-pipeline.yml" <<EOF
name: sdp_silver_spike
storage: file://$WORK/checkpoints
catalog: local
database: silver
libraries:
  - glob:
      include: transformations/**
configuration:
  spike.source: file://$WORK/changes
EOF
CONF=(--jars "$JAR" --driver-memory 1g
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=$WORK/warehouse"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.shuffle.partitions=2 --conf spark.ui.enabled=false)
export PYTHONPATH="$REPO/src" PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY" TZ=UTC
cd "$HERE"
"$PY" make_changes.py "$WORK/changes" 1
"$BIN/spark-pipelines" run --spec spark-pipeline.yml "${CONF[@]}"
"$PY" make_changes.py "$WORK/changes" 2
"$BIN/spark-pipelines" run --spec spark-pipeline.yml "${CONF[@]}"
"$PY" "$HERE/show.py" "$WORK" "$JAR"
