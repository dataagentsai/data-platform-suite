-- meta.run_log — one row per per-dataset Copy Activity execution.
--
-- Per-dataset rows live under a master_run_log row. Lifecycle:
-- INSERT at start (status=STARTED, watermark_before captured) →
-- UPDATE at end (status=SUCCESS | FAILED | SKIPPED, plus row counts,
-- target_path, watermark_after, error_message redacted).
--
-- Idempotent ADF retries reuse the same run_id and bump retry_count
-- (handled by sp_run_log_started's @retry_of_run_id parameter).
--
-- See docs/metadata_schema.md for the full column spec.

IF OBJECT_ID('meta.run_log', 'U') IS NULL
    CREATE TABLE meta.run_log (
        run_id BIGINT IDENTITY (1, 1) PRIMARY KEY,
        dataset_id INT NOT NULL REFERENCES meta.dataset_master (dataset_id),
        master_run_id BIGINT NULL REFERENCES meta.master_run_log (master_run_id),
        source_env VARCHAR(20) NOT NULL,
        realm VARCHAR(50) NOT NULL,
        pipeline_name VARCHAR(100) NULL,
        run_type VARCHAR(20) NOT NULL,
        trigger_type VARCHAR(20) NULL,
        correlation_id VARCHAR(100) NULL,
        started_at DATETIME2 NOT NULL,
        ended_at DATETIME2 NULL,
        status VARCHAR(20) NOT NULL,
        rows_extracted BIGINT NULL,
        rows_written BIGINT NULL,
        bytes_written BIGINT NULL,
        bytes_read BIGINT NULL,
        used_parallel_copies INT NULL,
        used_dius INT NULL,
        throughput_kbps DECIMAL(18, 2) NULL,
        file_count INT NULL,
        target_path NVARCHAR(500) NULL,
        watermark_before NVARCHAR(MAX) NULL,
        watermark_after NVARCHAR(MAX) NULL,
        source_duration_ms BIGINT NULL,
        sink_duration_ms BIGINT NULL,
        retry_count INT NOT NULL DEFAULT 0,
        error_message NVARCHAR(MAX) NULL,
        runtime_environment VARCHAR(50) NULL,
        INDEX ix_run_log_dataset_started (dataset_id, started_at DESC),
        INDEX ix_run_log_status_started (status, started_at DESC),
        INDEX ix_run_log_master_run (master_run_id)
        WHERE master_run_id IS NOT NULL
    );
