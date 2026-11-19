-- meta.sp_run_log_started — open a per-dataset run.
--
-- Called by the ingest pipeline as the first activity (Lookup, so the
-- pipeline captures run_id back). Inserts one row in status=STARTED
-- with watermark_before captured from meta.watermark, and SELECTs
-- run_id for the caller.
--
-- realm and source_env are denormalised from dataset_config /
-- source_master so dashboards filter without joining KV.
--
-- Idempotent: re-running this DDL replaces the proc.

CREATE OR ALTER PROCEDURE meta.sp_run_log_started
    @dataset_id INT,
    @master_run_id BIGINT = NULL,
    @run_type VARCHAR(20),
    @pipeline_name VARCHAR(100) = NULL,
    @trigger_type VARCHAR(20) = NULL,
    @correlation_id VARCHAR(100) = NULL,
    @runtime_environment VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_env VARCHAR(20), @realm VARCHAR(50), @watermark_before NVARCHAR(MAX);

    SELECT @source_env = sm.source_env
    FROM meta.dataset_master AS dm
    INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
    WHERE dm.dataset_id = @dataset_id;

    IF @source_env IS NULL
        BEGIN
            THROW 51001, 'sp_run_log_started: dataset_id not found in meta.dataset_master.', 1;
        END;

    SELECT @realm = value
    FROM meta.dataset_config
    WHERE dataset_id = @dataset_id AND [key] = 'realm';

    IF @realm IS NULL
        BEGIN
            SET @realm = 'adf-adls';
        END;

    SELECT @watermark_before = watermark_value
    FROM meta.watermark
    WHERE dataset_id = @dataset_id;

    INSERT INTO meta.run_log (
        dataset_id, master_run_id, source_env, realm,
        pipeline_name, run_type, trigger_type, correlation_id,
        started_at, status, watermark_before, runtime_environment
    )
    VALUES (
        @dataset_id, @master_run_id, @source_env, @realm,
        @pipeline_name, @run_type, @trigger_type, @correlation_id,
        SYSUTCDATETIME(), 'STARTED', @watermark_before, @runtime_environment
    );

    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS run_id;
END;
