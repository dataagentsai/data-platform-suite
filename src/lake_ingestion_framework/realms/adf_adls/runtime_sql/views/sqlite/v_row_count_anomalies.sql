-- SQLite stand-in for meta.v_row_count_anomalies.
-- Same column shape + filter rule as the T-SQL canonical
-- (../azure_sql_db/v_row_count_anomalies.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DECIMAL(10,2) -> REAL
--   - DATEADD(DAY, -7, ...) -> datetime('now', '-7 days')

DROP VIEW IF EXISTS v_row_count_anomalies;
CREATE VIEW v_row_count_anomalies AS
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
        / NULLIF(rl.rows_extracted, 0) AS REAL
    ) AS delta_pct
FROM run_log AS rl
INNER JOIN dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.rows_extracted IS NOT NULL
    AND rl.rows_written IS NOT NULL
    AND rl.rows_extracted <> rl.rows_written
    AND rl.started_at >= datetime('now', '-7 days');
