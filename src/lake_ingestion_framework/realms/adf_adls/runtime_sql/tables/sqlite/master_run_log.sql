-- SQLite stand-in for meta.master_run_log.
-- Same logical shape as the T-SQL canonical (../azure_sql_db/...);
-- differences are dialect-only (no `meta.` schema, no IDENTITY, no
-- inline INDEX clauses).

CREATE TABLE IF NOT EXISTS master_run_log (
    master_run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id              INTEGER NOT NULL REFERENCES source_master(source_id),
    source_env             TEXT    NOT NULL,
    trigger_name           TEXT,
    trigger_type           TEXT,
    requested_dataset_list TEXT,
    run_type               TEXT    NOT NULL,
    retry_of_master_run_id INTEGER REFERENCES master_run_log(master_run_id),
    started_at             TEXT    NOT NULL,
    ended_at               TEXT,
    status                 TEXT    NOT NULL,
    total_attempted        INTEGER,
    succeeded              INTEGER,
    failed                 INTEGER,
    skipped                INTEGER,
    adf_pipeline_run_id    TEXT,
    runtime_environment    TEXT
);

CREATE INDEX IF NOT EXISTS ix_master_run_log_source_started
    ON master_run_log (source_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_master_run_log_status_started
    ON master_run_log (status, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_master_run_log_env_started
    ON master_run_log (source_env, started_at DESC);
