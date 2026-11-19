# End-to-end smoke tests

These tests prove the full local-mode loop works against real source databases:

```
Docker postgres ─→ PostgresConnector.read_query ─→ ADF Mimic
                                                       │
                                                       ▼
                                              LocalDeltaWriter
                                                       │
                                                       ▼
                            ~/.lake-ingest/lakes/.../bronze/<table>/_delta_log/
```

The headline test (`test_postgres_to_delta.py`) is the demo arc described in
[`docs/adf_mimic_design.md` §12.3](../../docs/adf_mimic_design.md).

## Prerequisites

### 1. Docker Desktop

The smoke test spins up a real `postgres:16` container, so Docker must be
installed and running.

**macOS** — install Docker Desktop:

```sh
brew install --cask docker
```

Then **launch Docker Desktop** from `/Applications/Docker.app` (or via
Spotlight). Wait until the whale icon in the menu bar shows "Docker Desktop
is running."

Verify:

```sh
docker info | head
```

Should print server version + status; if it says "Cannot connect to the
Docker daemon", the daemon isn't running yet.

**Linux**:

```sh
# Ubuntu / Debian
sudo apt-get install docker.io
sudo systemctl start docker
sudo usermod -aG docker $USER  # log out + back in for group to take effect
```

**Windows**: install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/).

### 2. Python extras

The framework lazy-loads connector + writer dependencies. Install the two
extras the smoke test needs:

```sh
source .venv/bin/activate
pip install -e '.[postgres,local]'
```

(`postgres` brings `psycopg`; `local` brings `deltalake` + `pyarrow`.)

## Running

```sh
make smoke-test
```

Or directly:

```sh
source .venv/bin/activate
pytest -m smoke -v tests/smoke/
```

The first run pulls the `postgres:16` image (~150 MB; ~30s on a fast
connection). Subsequent runs reuse the cached image.

## What it does, step by step

1. Spin up `postgres:16` on port `55432` (avoids clashing with a developer's
   local postgres on 5432).
2. Wait for `pg_isready` (up to 60 seconds).
3. Connect via psycopg, create `public.customers`, insert 5 rows.
4. Hand-roll a minimal ADF artifact tree (LinkedService + 2 Datasets + Pipeline)
   into `tmp_path/adf/`.
5. Seed a SQLite metadata DB with the `run_log` table the mimic emulates.
6. Call `run_pipeline_local("PL_INGEST_POSTGRES", ...)` which:
   - Loads the artifact registry
   - Walks the DAG
   - Resolves LinkedService + Dataset parameters
   - Builds a `PostgresConnector` and runs `read_query("SELECT * FROM public.customers")`
   - Pumps the `QueryBatch` stream into a `LocalDeltaWriter`
   - Writes Delta files to `tmp_path/lakes/bronze/postgres/customers/`
7. Read back the Delta table via `deltalake.DeltaTable` and assert:
   - 5 rows
   - All 5 expected emails present
   - Framework metadata columns (`_run_id`, `_loaded_at`, `_record_hash`) added
   - All rows share the same `_run_id`
   - Each row has a distinct `_record_hash`
8. Stop and remove the postgres container.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Test skipped: "Docker CLI not on PATH" | Docker Desktop not installed | Install Docker Desktop |
| Test skipped: "Docker daemon isn't reachable" | Docker Desktop installed but not running | Launch Docker Desktop, wait for green status |
| `port is already allocated` | Another postgres on 55432 | Stop it (`docker ps`, `docker stop <name>`) or change `_PG_PORT` in the test |
| Test runs but "deltalake not installed" | Missing `[local]` extra | `pip install -e '.[postgres,local]'` |
| Test runs but "psycopg not installed" | Missing `[postgres]` extra | `pip install -e '.[postgres,local]'` |

## Inspecting actual Delta rows

The pytest smoke test asserts and tears the lake down (it runs under
`tmp_path`). To see the **actual rows** that landed — useful for demos and
debugging — run the companion inspection script:

```sh
source .venv/bin/activate
python scripts/show_postgres_delta.py
```

Same flow (Docker postgres → mimic → Delta) but writes to
`/tmp/lake-ingest-demo/` and dumps:

- file listing (`_delta_log/*.json`, `*.snappy.parquet`)
- the pyarrow schema
- every row as a Python dict (source columns + `_run_id` + `_loaded_at` + `_record_hash`)

The lake is preserved on disk after the script exits, so you can poke around
with `ls -la /tmp/lake-ingest-demo/lakes/` or load the Delta table from any
other tool.

## Why smoke tests are gated separately from integration tests

- **unit tests** (default): no external deps; sub-second; run on every commit
- **integration tests** (`-m integration`): require `LAKE_INGEST_TEST_*_URL` env
  vars pointing at developer-managed databases
- **smoke tests** (`-m smoke`): require Docker; bring up containers themselves

The smoke tests are the **demo path** — what a CTO sees during a 5-minute
"prove it works" session — not what runs on every PR.
