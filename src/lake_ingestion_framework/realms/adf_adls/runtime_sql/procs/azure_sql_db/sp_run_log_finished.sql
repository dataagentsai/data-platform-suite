-- meta.sp_run_log_finished — close a per-dataset run.
--
-- Called by the ingest pipeline as the final activity. Wired twice:
-- once on success path (status=SUCCESS, full metrics) and once on
-- failure path (status=FAILED, error_message captured).
--
-- error_message is redacted by the runner (Password=, AccountKey=,
-- bearer tokens, SAS) and capped at 4 KB before reaching this proc;
-- the proc trusts the caller. See docs/security.md.
--
-- Idempotent: re-running this DDL replaces the proc.

CREATE OR ALTER PROCEDURE meta.sp_run_log_finished
    @run_id BIGINT,
    @status VARCHAR(20),
    @rows_extracted BIGINT = NULL,
    @rows_written BIGINT = NULL,
    @bytes_written BIGINT = NULL,
    @bytes_read BIGINT = NULL,
    @used_parallel_copies INT = NULL,
    @used_dius INT = NULL,
    @throughput_kbps DECIMAL(18, 2) = NULL,
    @file_count INT = NULL,
    @target_path NVARCHAR(500) = NULL,
    @watermark_after NVARCHAR(MAX) = NULL,
    @source_duration_ms BIGINT = NULL,
    @sink_duration_ms BIGINT = NULL,
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @status NOT IN ('SUCCESS', 'FAILED', 'SKIPPED')
        BEGIN
            THROW 51002, 'sp_run_log_finished: status must be SUCCESS, FAILED, or SKIPPED.', 1;
        END;

    -- bytes_read / used_parallel_copies / used_dius / throughput_kbps
    -- come from ADF Copy Activity output (.dataRead, .usedParallelCopies,
    -- .usedDataIntegrationUnits, .throughput) — the cost-attribution
    -- inputs (DIU-hours = used_dius * source_duration_ms / 3.6e6).
    UPDATE meta.run_log
    SET
        ended_at = SYSUTCDATETIME(),
        status = @status,
        rows_extracted = @rows_extracted,
        rows_written = @rows_written,
        bytes_written = @bytes_written,
        bytes_read = @bytes_read,
        used_parallel_copies = @used_parallel_copies,
        used_dius = @used_dius,
        throughput_kbps = @throughput_kbps,
        file_count = @file_count,
        target_path = @target_path,
        watermark_after = @watermark_after,
        source_duration_ms = @source_duration_ms,
        sink_duration_ms = @sink_duration_ms,
        error_message = @error_message
    WHERE run_id = @run_id;
END;
