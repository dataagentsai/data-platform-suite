"""One engine session for every frame test in the run, stopped at the end.

Engine: `--engine spark` (default) or `--engine duckdb` (or DP_ENGINE=duckdb). Spark is a local Spark (local[2], 1 GB
driver); its tests skip when pyspark or delta-spark is not installed (run them with --with 'pyspark==3.5.9' --with
'delta-spark==3.2.0'; Java 17). DuckDB is the same PySpark DataFrame API through SQLFrame (docs/DUCKDB-SPIKE.md):
SQLFrame takes over the `pyspark` imports for the whole process, so the engine is chosen per pytest process, before
any test module imports pyspark, and the same test tables run on both (`make engine-conformance` runs the two
processes). The `spark` fixture keeps its name: the stage and build code only sees a session and its frames.
"""

from __future__ import annotations

import os

import pytest

ENGINES = ("spark", "duckdb")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--engine", choices=ENGINES, default=os.environ.get("DP_ENGINE", "spark"),
                     help="the frame engine for the stage and gold-build tests (docs/DUCKDB-SPIKE.md)")


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--engine") == "duckdb":
        from dataplatform.adapters.duckdb_frames import activate

        activate()  # before collection: every `from pyspark.sql import ...` below now gets SQLFrame on DuckDB


@pytest.fixture(scope="session")
def engine_name(pytestconfig: pytest.Config) -> str:
    return str(pytestconfig.getoption("--engine"))


@pytest.fixture(scope="session")
def spark(engine_name: str, tmp_path_factory: pytest.TempPathFactory):
    if engine_name == "duckdb":
        from dataplatform.adapters.duckdb_frames import local_session as duck_session

        session = duck_session()
        yield session
        session.stop()
        return
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from dataplatform.adapters.spark_silver import local_session
    from dataplatform.adapters.table_store import iceberg_jar

    # an Iceberg folder catalog "ice" beside Delta when the runtime jar is on this machine (ADR-027 contract tests)
    warehouse = tmp_path_factory.mktemp("iceberg") if iceberg_jar() else None
    session = local_session("dataplatform-tests", iceberg_warehouse=warehouse)
    yield session
    session.stop()
