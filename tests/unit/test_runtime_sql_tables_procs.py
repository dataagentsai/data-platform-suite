"""Phase 6b-1a — runtime SQL log tables + stored procs.

Three layers:

1. **Loader** — exposes the right tables / procs in the right dialect.
2. **SQLite end-to-end** — apply log tables to the in-memory metadata
   DB and assert they accept the rows the procs would write.
3. **Deploy integration** — ``plan_deploy(include_procs=True)`` emits
   the SQL artifacts under runtime_sql/.

The T-SQL stored procs are not executed in unit tests (SQLite has no
proc support). sqlfluff lints their syntax; Phase 8 runs them against
a real Azure SQL DB.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from lake_ingestion_framework.core.metadata.repository import (
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.realms.adf_adls.runtime_sql import (
    PROC_NAMES,
    TABLE_NAMES,
    all_procs,
    all_tables,
    bootstrap_sqlite_tables,
    proc_sql,
    table_sql,
)

# ──────────────────────────────────────────────────────────────────────
# Loader
# ──────────────────────────────────────────────────────────────────────


class TestLoader:
    def test_table_names(self) -> None:
        # FK-safe order: master_run_log first (run_log references it).
        assert TABLE_NAMES == ("master_run_log", "run_log", "watermark_audit")

    def test_proc_names(self) -> None:
        assert set(PROC_NAMES) == {
            "sp_master_run_log_started",
            "sp_master_run_log_finished",
            "sp_run_log_started",
            "sp_run_log_finished",
            "sp_advance_watermark",
        }

    def test_azure_sql_db_table_uses_idempotent_create(self) -> None:
        sql = table_sql("run_log", dialect="azure_sql_db")
        # IF OBJECT_ID(..) IS NULL guard + meta. schema + IDENTITY column
        assert "IF OBJECT_ID" in sql
        assert "IS NULL" in sql
        assert "meta.run_log" in sql
        assert "IDENTITY" in sql

    def test_sqlite_table_uses_create_if_not_exists(self) -> None:
        sql = table_sql("run_log", dialect="sqlite")
        assert "CREATE TABLE IF NOT EXISTS run_log" in sql
        # SQLite has no `meta.` schema
        non_comment = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        assert "meta." not in non_comment

    def test_proc_uses_create_or_alter(self) -> None:
        sql = proc_sql("sp_advance_watermark")
        assert "CREATE OR ALTER PROCEDURE meta.sp_advance_watermark" in sql

    def test_run_log_table_has_observability_columns(self) -> None:
        """Phase 1 observability adds cost/perf metric columns to
        run_log (T-SQL + SQLite stand-in). See docs/operability.md.
        """
        for dialect in ("azure_sql_db", "sqlite"):
            sql = table_sql("run_log", dialect=dialect)  # type: ignore[arg-type]
            for col in (
                "bytes_read",
                "used_parallel_copies",
                "used_dius",
                "throughput_kbps",
            ):
                assert col in sql, f"{dialect}/run_log missing column {col!r}"

    def test_sp_run_log_finished_accepts_observability_params(self) -> None:
        """sp_run_log_finished accepts the new cost/perf metrics so
        the ADF RunLogFinishedSuccess activity can pass them through.
        """
        sql = proc_sql("sp_run_log_finished")
        for param in (
            "@bytes_read",
            "@used_parallel_copies",
            "@used_dius",
            "@throughput_kbps",
        ):
            assert param in sql, f"sp_run_log_finished missing param {param}"

    def test_unknown_table_name_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown table"):
            table_sql("v_does_not_exist", dialect="sqlite")

    def test_unknown_proc_name_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown proc"):
            proc_sql("sp_nope")

    def test_all_tables_returns_in_fk_order(self) -> None:
        out = all_tables(dialect="sqlite")
        assert [n for n, _ in out] == list(TABLE_NAMES)

    def test_all_procs_returns_in_observability_order(self) -> None:
        # master open → child open → child close → master close → watermark
        out = all_procs()
        names = [n for n, _ in out]
        assert names.index("sp_master_run_log_started") < names.index("sp_run_log_started")
        assert names.index("sp_run_log_started") < names.index("sp_run_log_finished")
        assert names.index("sp_run_log_finished") < names.index("sp_master_run_log_finished")


# ──────────────────────────────────────────────────────────────────────
# SQLite end-to-end — bootstrap tables + INSERT a row mimicking proc behavior
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo() -> Iterator[SqliteMetadataRepository]:
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    conn = r._connect()
    bootstrap_sqlite_tables(conn)
    try:
        yield r
    finally:
        r.close()


class TestLogTables:
    def test_master_run_log_accepts_proc_shape_insert(self, repo: SqliteMetadataRepository) -> None:
        """Mirror the INSERT sp_master_run_log_started would do."""
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        conn = repo._connect()
        conn.execute(
            "INSERT INTO master_run_log "
            "(source_id, source_env, run_type, started_at, status) "
            "VALUES (?, 'dev', 'INCREMENTAL', '2026-04-30T00:00:00Z', 'RUNNING')",
            (sid,),
        )
        conn.commit()
        rows = list(
            conn.execute(
                "SELECT master_run_id, status FROM master_run_log WHERE source_id = ?",
                (sid,),
            )
        )
        assert len(rows) == 1
        assert rows[0][1] == "RUNNING"

    def test_run_log_fk_to_master_run_log(self, repo: SqliteMetadataRepository) -> None:
        """run_log.master_run_id references master_run_log.master_run_id."""
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        did = repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=True,
            config={"realm": "adf-adls", "dataset_type": "TABLE"},
        )
        conn = repo._connect()
        # Open a master run, then a child run pointing at it.
        cur = conn.execute(
            "INSERT INTO master_run_log "
            "(source_id, source_env, run_type, started_at, status) "
            "VALUES (?, 'dev', 'INCREMENTAL', '2026-04-30T00:00:00Z', 'RUNNING')",
            (sid,),
        )
        master_run_id = cur.lastrowid
        conn.execute(
            "INSERT INTO run_log "
            "(dataset_id, master_run_id, source_env, realm, run_type, "
            " started_at, status) "
            "VALUES (?, ?, 'dev', 'adf-adls', 'INCREMENTAL', "
            "'2026-04-30T00:01:00Z', 'STARTED')",
            (did, master_run_id),
        )
        conn.commit()
        rows = list(
            conn.execute(
                "SELECT rl.run_id, rl.status, mrl.status "
                "FROM run_log rl JOIN master_run_log mrl "
                "ON rl.master_run_id = mrl.master_run_id "
                "WHERE rl.dataset_id = ?",
                (did,),
            )
        )
        assert len(rows) == 1


# ──────────────────────────────────────────────────────────────────────
# Deploy integration — --include-procs emits runtime SQL files
# ──────────────────────────────────────────────────────────────────────


class TestDeployIncludeProcs:
    def _seeded_repo(self) -> SqliteMetadataRepository:
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x.database.windows.net", "database": "orders"},
        )
        return r

    def test_default_does_not_emit_runtime_sql(self, tmp_path: Path) -> None:
        from lake_ingestion_framework.core.use_cases.deploy import plan_deploy

        repo = self._seeded_repo()
        try:
            plan = plan_deploy(
                realm="adf-adls",
                env="dev",
                repo=repo,
                customer_repo=tmp_path,
            )
            assert all(not pf.kind.startswith("runtime_sql_") for pf in plan.files)
        finally:
            repo.close()

    def test_include_procs_emits_views_tables_procs(self, tmp_path: Path) -> None:
        from lake_ingestion_framework.core.use_cases.deploy import plan_deploy

        repo = self._seeded_repo()
        try:
            plan = plan_deploy(
                realm="adf-adls",
                env="dev",
                repo=repo,
                customer_repo=tmp_path,
                include_procs=True,
            )
            kinds = {pf.kind for pf in plan.files}
            assert "runtime_sql_table" in kinds
            assert "runtime_sql_view" in kinds
            assert "runtime_sql_proc" in kinds
            # Specific filenames present
            paths = {pf.target_path.name for pf in plan.files}
            assert "master_run_log.sql" in paths
            assert "run_log.sql" in paths
            assert "v_enabled_sources.sql" in paths
            assert "sp_advance_watermark.sql" in paths
        finally:
            repo.close()

    def test_include_procs_apply_writes_files_to_disk(self, tmp_path: Path) -> None:
        from lake_ingestion_framework.core.use_cases.deploy import (
            apply_deploy,
            plan_deploy,
        )

        repo = self._seeded_repo()
        try:
            plan = plan_deploy(
                realm="adf-adls",
                env="dev",
                repo=repo,
                customer_repo=tmp_path,
                include_procs=True,
            )
            apply_deploy(plan)
            runtime_dir = tmp_path / "generated" / "adf-adls" / "dev" / "runtime_sql"
            assert (runtime_dir / "tables" / "master_run_log.sql").is_file()
            assert (runtime_dir / "views" / "v_source_config.sql").is_file()
            assert (runtime_dir / "procs" / "sp_run_log_started.sql").is_file()
        finally:
            repo.close()

    def test_include_procs_is_byte_identical_on_rerun(self, tmp_path: Path) -> None:
        """Per CLAUDE.md "Idempotent deploy" — same metadata → same bytes."""
        from lake_ingestion_framework.core.use_cases.deploy import plan_deploy

        repo = self._seeded_repo()
        try:
            first = plan_deploy(
                realm="adf-adls",
                env="dev",
                repo=repo,
                customer_repo=tmp_path,
                include_procs=True,
            )
            second = plan_deploy(
                realm="adf-adls",
                env="dev",
                repo=repo,
                customer_repo=tmp_path,
                include_procs=True,
            )
            assert len(first.files) == len(second.files)
            for a, b in zip(first.files, second.files, strict=True):
                assert a.target_path == b.target_path
                assert a.rendered == b.rendered
        finally:
            repo.close()
