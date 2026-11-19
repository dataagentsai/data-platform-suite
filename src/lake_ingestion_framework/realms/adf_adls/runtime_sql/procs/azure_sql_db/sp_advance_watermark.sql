-- meta.sp_advance_watermark — atomically advance the high-water mark.
--
-- Called by the ingest pipeline ONLY on success path (after Copy
-- completes and sp_run_log_finished records SUCCESS). Per CLAUDE.md
-- "watermarks are sacred" — advance only on confirmed write.
--
-- UPSERT semantics: first run for a dataset INSERTs; subsequent
-- successful runs UPDATE. last_loaded_at always set to SYSUTCDATETIME().
--
-- @new_watermark may be NULL for FULL-mode runs that don't track an
-- incremental cursor (the watermark row still exists, but holds the
-- snapshot timestamp via last_loaded_at).
--
-- This proc does NOT write to watermark_audit — that's reserved for
-- operator-driven resets (lake-ingest reset-watermark, Phase 6b-3).
--
-- Idempotent: re-running this DDL replaces the proc.

CREATE OR ALTER PROCEDURE meta.sp_advance_watermark
    @dataset_id INT,
    @new_watermark NVARCHAR(MAX) = NULL,
    @rows_loaded BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE locks the row; safe under concurrent advances against the
    -- same dataset (which shouldn't happen under our orchestration but
    -- defends against retry storms).
    -- @new_watermark = NULL means "leave watermark_value alone, just
    -- bump last_loaded_at and rows_loaded". Phase 6b-1 ingest pipelines
    -- don't yet compute the new max watermark from source data; they
    -- pass NULL here. Phase 6b-2 adds a max-watermark Lookup and
    -- starts passing real values.
    MERGE meta.watermark AS tgt
    USING (SELECT @dataset_id AS dataset_id) AS src
        ON tgt.dataset_id = src.dataset_id
    WHEN MATCHED
        THEN
        UPDATE
            SET
                watermark_value = COALESCE(@new_watermark, tgt.watermark_value),
                last_loaded_at = SYSUTCDATETIME(),
                rows_loaded = @rows_loaded
    WHEN NOT MATCHED
        THEN
        INSERT (dataset_id, watermark_value, last_loaded_at, rows_loaded)
        VALUES (@dataset_id, @new_watermark, SYSUTCDATETIME(), @rows_loaded);
END;
