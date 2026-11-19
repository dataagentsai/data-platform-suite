-- meta.sp_master_run_log_finished — close a per-source-driver run.
--
-- Called by PL_PER_SOURCE_DRIVER_<TYPE> as the final activity (depends
-- on the ForEach completing). Aggregates from run_log children:
--   total_attempted = COUNT(*)
--   succeeded       = COUNT WHERE status = 'SUCCESS'
--   failed          = COUNT WHERE status = 'FAILED'
--   skipped         = COUNT WHERE status = 'SKIPPED'
-- and derives the final status:
--   FAILED  if no children were INSERTed (controller-level error;
--           pipeline never enumerated datasets)
--   SUCCESS if every attempted child = SUCCESS
--   PARTIAL otherwise
--
-- Idempotent: re-running this DDL replaces the proc.

CREATE OR ALTER PROCEDURE meta.sp_master_run_log_finished
    @master_run_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @attempted INT, @succ INT, @fail INT, @skip INT;

    SELECT
        @attempted = COUNT(*),
        @succ = SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END),
        @fail = SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END),
        @skip = SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END)
    FROM meta.run_log
    WHERE master_run_id = @master_run_id;

    DECLARE @final_status VARCHAR(20);
    SET @final_status = CASE
        WHEN @attempted IS NULL OR @attempted = 0 THEN 'FAILED'
        WHEN @fail = 0 THEN 'SUCCESS'
        ELSE 'PARTIAL'
    END;

    UPDATE meta.master_run_log
    SET
        ended_at = SYSUTCDATETIME(),
        status = @final_status,
        total_attempted = @attempted,
        succeeded = @succ,
        failed = @fail,
        skipped = @skip
    WHERE master_run_id = @master_run_id;
END;
