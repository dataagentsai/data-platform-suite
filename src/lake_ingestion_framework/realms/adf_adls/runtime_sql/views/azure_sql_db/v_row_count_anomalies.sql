-- meta.v_row_count_anomalies — SUCCESS runs in the last 7 days where
-- the source row count and the sink row count disagree. The framework
-- does no transformation in Bronze, so rows_extracted (Copy.rowsRead)
-- and rows_written (Copy.rowsCopied) should match exactly; a delta
-- means rows were dropped or duplicated somewhere in the pipeline.
--
-- Common causes for a non-zero delta:
--   - Type conversion failures with skip-on-error enabled (sink drop)
--   - Source produced more rows than sink could process (back-pressure)
--   - Connector / driver bug (rare; investigate fast)
--
-- The view is the data-quality "early warning" — a row here means
-- "look at this run before signing off the day's load". Pair with
-- the dataset's downstream parity tests for the actual investigation.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_row_count_anomalies AS
SELECT
    rl.run_id,
    rl.master_run_id,
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    rl.source_env,
    rl.started_at,
    rl.ended_at,
    rl.rows_extracted,
    rl.rows_written,
    (rl.rows_extracted - rl.rows_written) AS row_count_delta,
    CAST(
        ABS(rl.rows_extracted - rl.rows_written) * 100.0
        / NULLIF(rl.rows_extracted, 0) AS DECIMAL(10, 2)
    ) AS delta_pct
FROM meta.run_log AS rl
INNER JOIN meta.dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.rows_extracted IS NOT NULL
    AND rl.rows_written IS NOT NULL
    AND rl.rows_extracted <> rl.rows_written
    AND rl.started_at >= DATEADD(DAY, -7, SYSUTCDATETIME());
