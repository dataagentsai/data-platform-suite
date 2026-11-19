-- SQLite stand-in for meta.v_data_volume_trend.
-- Same column shape as the T-SQL canonical
-- (../azure_sql_db/v_data_volume_trend.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - CAST(x AS DATE) -> DATE(x)
--   - DATEADD(DAY, -7, ...) -> datetime('now', '-7 days')

DROP VIEW IF EXISTS v_data_volume_trend;
CREATE VIEW v_data_volume_trend AS
SELECT
    rl.dataset_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    DATE(rl.started_at) AS run_date,
    SUM(rl.rows_extracted) AS daily_rows,
    SUM(rl.bytes_read) AS daily_bytes_read,
    SUM(rl.bytes_written) AS daily_bytes_written,
    SUM(rl.file_count) AS daily_files,
    COUNT(*) AS daily_runs
FROM run_log AS rl
INNER JOIN dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.started_at >= datetime('now', '-7 days')
GROUP BY
    rl.dataset_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    DATE(rl.started_at);
