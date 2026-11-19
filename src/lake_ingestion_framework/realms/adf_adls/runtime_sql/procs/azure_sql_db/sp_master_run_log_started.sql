-- meta.sp_master_run_log_started — open a per-source-driver run.
--
-- Called by PL_PER_SOURCE_DRIVER_<TYPE> via a Lookup activity (so the
-- pipeline captures master_run_id back from the result set). Inserts
-- one row in status=RUNNING and SELECTs master_run_id for the caller.
--
-- The Per-Source Driver SetVariables master_run_id and forwards it to
-- each ingest call's run_log row, so dashboards can group all
-- per-dataset runs under their parent master run.
--
-- Idempotent: re-running this DDL replaces the proc.

CREATE OR ALTER PROCEDURE meta.sp_master_run_log_started
    @source_id INT,
    @run_type VARCHAR(20),
    @trigger_type VARCHAR(20) = NULL,
    @adf_pipeline_run_id VARCHAR(100) = NULL,
    @runtime_environment VARCHAR(50) = NULL,
    @retry_of_master_run_id BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_env VARCHAR(20);
    SELECT @source_env = source_env
    FROM meta.source_master
    WHERE source_id = @source_id;

    IF @source_env IS NULL
        BEGIN
            THROW 51000, 'sp_master_run_log_started: source_id not found in meta.source_master.', 1;
        END;

    INSERT INTO meta.master_run_log (
        source_id, source_env, run_type, trigger_type,
        adf_pipeline_run_id, runtime_environment, retry_of_master_run_id,
        started_at, status
    )
    VALUES (
        @source_id, @source_env, @run_type, @trigger_type,
        @adf_pipeline_run_id, @runtime_environment, @retry_of_master_run_id,
        SYSUTCDATETIME(), 'RUNNING'
    );

    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS master_run_id;
END;
