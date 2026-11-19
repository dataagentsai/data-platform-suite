# Data platform suite

A metadata-driven data platform: one metadata store says what to ingest and how to treat it, and the same code runs
every dataset from the source to the semantic layer.

**The guided explanation, from one picture down to the code, is part of this repository.** Run `make explainer` and open
`build/explainer/index.html`. It is built from this repository alone, works offline, and links every code box to its file
and line, so it cannot drift from the code without the build saying so. For plain text instead of pages, `scripts/explain.py`
explains a run, a dataset's manifest, the checks a change passes, and any function and its callers, with no model and no
network.

## What is here

| Folder | What it holds |
|---|---|
| `src/dataplatform/meta` | The metadata store port, the validating loader and the settings registry |
| `src/dataplatform/adapters/sql` | The metadata store itself: PostgreSQL tables and functions |
| `src/dataplatform/ingest` | Ingestion: the runner, the five pipelines and their entry steps |
| `src/dataplatform/silver`, `src/dataplatform/stages` | Bronze to silver: the plan, the thirteen stages, the two engines |
| `src/dataplatform/gold` | Silver to gold: the harness that builds dimensions and facts |
| `src/dataplatform/semantic` | Metrics and known answers |
| `src/dataplatform/obs` | The run log, freshness, alerts, lineage and the operations page |
| `src/dataplatform/world` | The simulated company and its oracle, used as test data with known answers |
| `src/dataplatform/adapters` | One adapter per product behind each port (Spark, Delta, DuckDB, PostgreSQL, Databricks, AWS) |
| `realms/` | Deployable bundles for Databricks and AWS with Databricks |
| `tests/dataplatform` | Table-driven tests, one file per part |
| `explainer` | The generator of the guided explanation (`make explainer`) |
| `src/lake_ingestion_framework` | The earlier ingestion framework, kept for its Azure Data Factory generator |

The specifications this code follows (obligations, ports, decision records, the example domain's manifests and
contracts) are in <https://github.com/dataagentsai/data-platform-catalog>.

## Run it on a laptop

```bash
uv sync
uv run python scripts/run_platform.py --engine duckdb --db sqlite --meta memory --calendar compressed
```

This runs three simulated days through every layer and compares each layer with the oracle.

## Status

This repository is a published snapshot of a working repository. It is refreshed as the platform moves; it does not
take pull requests yet. Documents for the earlier framework are not included.

Licence: not chosen yet. Until a licence file is added, the code is here to read and learn from, with all rights
reserved.
