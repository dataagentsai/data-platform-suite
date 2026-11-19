-- SQLite stand-in for meta.v_recent_failures.
-- Same column shape as the T-SQL canonical
-- (../azure_sql_db/v_recent_failures.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DATEADD(DAY, -7, SYSUTCDATETIME()) -> datetime('now', '-7 days')

DROP VIEW IF EXISTS v_recent_failures;
CREATE VIEW v_recent_failures AS
SELECT
    rl.run_id,
    rl.master_run_id,
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    sm.source_env,
    rl.pipeline_name,
    rl.started_at,
    rl.ended_at,
    rl.retry_count,
    rl.error_message,
    rl.target_path
FROM run_log AS rl
INNER JOIN dataset_master AS dm ON dm.dataset_id = rl.dataset_id
INNER JOIN source_master AS sm ON sm.source_id = dm.source_id
WHERE rl.status = 'FAILED'
    AND rl.started_at >= datetime('now', '-7 days');
