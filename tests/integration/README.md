# Integration tests

These tests require external services (a real Postgres instance,
later Azure SQL DB, etc.). They are **skipped by default**.

## How to run

### Postgres connector tests

Bring up Postgres any way you like — Docker is the simplest:

```bash
docker run --rm -d --name lake-ingest-test-pg \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=test \
  -p 5432:5432 \
  postgres:16
```

Then point the tests at it:

```bash
export LAKE_INGEST_TEST_PG_URL=postgresql://test:test@localhost:5432/test
pytest -m integration tests/integration/
```

When done:

```bash
docker stop lake-ingest-test-pg
```

If `LAKE_INGEST_TEST_PG_URL` isn't set, the Postgres tests skip
themselves with a clear reason.

### SQL Server / Azure SQL DB connector tests

Two prerequisites:

**1. ODBC Driver 18 for SQL Server (system-level install)**

```bash
# macOS
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18

# Ubuntu / Debian
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo apt-get install -y unixodbc-dev msodbcsql18
```

**2. A SQL Server instance** — Docker is the simplest:

```bash
docker run --rm -d --name lake-ingest-test-mssql \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='Test1234!Strong' \
  -e MSSQL_PID=Developer \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# SQL Server takes ~20-30 seconds to be ready on first start
sleep 30
```

Then point the tests at it (note `Encrypt=no&TrustServerCertificate=yes`
for local non-TLS):

```bash
uv sync --extra dev --extra azure_sql_db
export LAKE_INGEST_TEST_MSSQL_URL='mssql://sa:Test1234%21Strong@localhost:1433/master?Encrypt=no&TrustServerCertificate=yes'
pytest -m integration tests/integration/test_mssql_connector.py
```

The `!` in the password is URL-encoded as `%21`.

When done:

```bash
docker stop lake-ingest-test-mssql
```

If `LAKE_INGEST_TEST_MSSQL_URL` isn't set or pyodbc / ODBC Driver
isn't installed, the SQL Server tests skip themselves cleanly.

### SFTP connector tests

The simplest free SFTP server for local testing is the `atmoz/sftp`
Docker image. Bring up an SFTP server with one user and a writable
home directory:

```bash
docker run --rm -d --name lake-ingest-test-sftp \
  -p 2222:22 \
  atmoz/sftp:alpine \
  testuser:Test1234!:1001
```

The image exposes user `testuser` with password `Test1234!`,
listening on container port 22 (mapped to host 2222 above). The
user's home directory is `/home/testuser` and writable to that user.

Then point the tests at it:

```bash
uv sync --extra dev --extra sftp
export LAKE_INGEST_TEST_SFTP_URL='sftp://testuser:Test1234%21@localhost:2222/upload'
pytest -m integration tests/integration/test_sftp_connector.py
```

The tests upload a small fixture tree (`lake_ingest_test/orders/`
and `lake_ingest_test/events/`) under `root_path`, exercise list +
describe, then clean up. The `!` in the password is URL-encoded as
`%21`. We use `/upload` as the writable directory inside the
container's home.

When done:

```bash
docker stop lake-ingest-test-sftp
```

### MySQL connector tests

Bring up MySQL via Docker:

```bash
docker run --rm -d --name lake-ingest-test-mysql \
  -e MYSQL_ROOT_PASSWORD='Test1234!' \
  -e MYSQL_DATABASE=test \
  -e MYSQL_USER=test \
  -e MYSQL_PASSWORD='Test1234!' \
  -p 3306:3306 \
  mysql:lts
sleep 20  # MySQL takes ~15 seconds to be ready on first start
```

Then point the tests at it:

```bash
uv sync --extra dev --extra mysql
export LAKE_INGEST_TEST_MYSQL_URL='mysql://test:Test1234%21@localhost:3306/test'
pytest -m integration tests/integration/test_mysql_connector.py
```

When done:

```bash
docker stop lake-ingest-test-mysql
```

### Oracle connector tests

Use Oracle's free `gvenzl/oracle-free` image (community-maintained,
based on Oracle Database 23c Free):

```bash
docker run --rm -d --name lake-ingest-test-oracle \
  -e ORACLE_PASSWORD='Test1234' \
  -e APP_USER=ingestor \
  -e APP_USER_PASSWORD='Test1234' \
  -p 1521:1521 \
  gvenzl/oracle-free:23-slim-faststart
```

**First boot takes 5-10 minutes.** Watch the logs to know when ready:

```bash
docker logs -f lake-ingest-test-oracle
# Wait for "DATABASE IS READY TO USE!" then Ctrl-C out of logs
```

Subsequent boots are fast (~30 seconds).

The image creates an `APP_USER=ingestor` and a pluggable database
`FREEPDB1`. Connect as that user:

```bash
uv sync --extra dev --extra oracle
export LAKE_INGEST_TEST_ORACLE_URL='oracle://ingestor:Test1234@localhost:1521/FREEPDB1'
pytest -m integration tests/integration/test_oracle_connector.py
```

When done:

```bash
docker stop lake-ingest-test-oracle
```

The tests use `oracledb` thin mode — no Oracle Instant Client install
needed. Pure Python end-to-end.

## Why this shape

We deliberately avoid `testcontainers-python` to keep the dev-dep
footprint minimal and let developers provision their own Postgres
however they want (Docker, Postgres.app on macOS, native install,
remote dev DB). One env var is the contract.
