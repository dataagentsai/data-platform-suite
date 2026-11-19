-- SQLite stand-in for meta.run_log.
-- Same logical shape as the T-SQL canonical (../azure_sql_db/...);
-- differences are dialect-only.

CREATE TABLE IF NOT EXISTS run_log (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id          INTEGER NOT NULL REFERENCES dataset_master(dataset_id),
    master_run_id       INTEGER REFERENCES master_run_log(master_run_id),
    source_env          TEXT    NOT NULL,
    realm               TEXT    NOT NULL,
    pipeline_name       TEXT,
    run_type            TEXT    NOT NULL,
    trigger_type        TEXT,
    correlation_id      TEXT,
    started_at          TEXT    NOT NULL,
    ended_at            TEXT,
    status              TEXT    NOT NULL,
    rows_extracted      INTEGER,
    rows_written        INTEGER,
    bytes_written       INTEGER,
    bytes_read          INTEGER,
    used_parallel_copies INTEGER,
    used_dius           INTEGER,
    throughput_kbps     REAL,
    file_count          INTEGER,
    target_path         TEXT,
    watermark_before    TEXT,
    watermark_after     TEXT,
    source_duration_ms  INTEGER,
    sink_duration_ms    INTEGER,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT,
    runtime_environment TEXT
);

CREATE INDEX IF NOT EXISTS ix_run_log_dataset_started
    ON run_log (dataset_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_run_log_status_started
    ON run_log (status, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_run_log_master_run
    ON run_log (master_run_id) WHERE master_run_id IS NOT NULL;
