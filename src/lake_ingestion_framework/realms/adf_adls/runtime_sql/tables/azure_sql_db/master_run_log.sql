-- meta.master_run_log — one row per per-source-driver invocation.
--
-- Frames the per-dataset run_log rows for a single source's pipeline
-- run. Per-Source Driver INSERTs at start (status=RUNNING, captures
-- master_run_id back to the pipeline) and UPDATEs at end (status =
-- SUCCESS | PARTIAL | FAILED, plus aggregate counts).
--
-- Restart paths key off master_run_id — TR_RERUN_FAILED finds
-- the originally-failed run and replays only the FAILED datasets.
--
-- See docs/metadata_schema.md for the full column spec.

IF OBJECT_ID('meta.master_run_log', 'U') IS NULL
    CREATE TABLE meta.master_run_log (
        master_run_id BIGINT IDENTITY (1, 1) PRIMARY KEY,
        source_id INT NOT NULL REFERENCES meta.source_master (source_id),
        source_env VARCHAR(20) NOT NULL,
        trigger_name VARCHAR(100) NULL,
        trigger_type VARCHAR(20) NULL,
        requested_dataset_list NVARCHAR(MAX) NULL,
        run_type VARCHAR(20) NOT NULL,
        retry_of_master_run_id BIGINT NULL REFERENCES meta.master_run_log (master_run_id),
        started_at DATETIME2 NOT NULL,
        ended_at DATETIME2 NULL,
        status VARCHAR(20) NOT NULL,
        total_attempted INT NULL,
        succeeded INT NULL,
        failed INT NULL,
        skipped INT NULL,
        adf_pipeline_run_id VARCHAR(100) NULL,
        runtime_environment VARCHAR(50) NULL,
        INDEX ix_master_run_log_source_started (source_id, started_at DESC),
        INDEX ix_master_run_log_status_started (status, started_at DESC),
        INDEX ix_master_run_log_env_started (source_env, started_at DESC)
    );
