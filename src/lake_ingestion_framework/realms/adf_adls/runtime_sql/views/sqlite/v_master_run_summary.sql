-- SQLite stand-in for meta.v_master_run_summary.
-- Same column shape as the T-SQL canonical
-- (../azure_sql_db/v_master_run_summary.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DATEDIFF(SECOND, a, b) -> (julianday(b) - julianday(a)) * 86400
--   - DATEADD(DAY, -7, ...) -> datetime('now', '-7 days')
--   - CAST(x AS DECIMAL(5,2)) -> CAST(x AS REAL)

DROP VIEW IF EXISTS v_master_run_summary;
CREATE VIEW v_master_run_summary AS
SELECT
    mrl.master_run_id,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    mrl.source_env,
    mrl.run_type,
    mrl.trigger_name,
    mrl.trigger_type,
    mrl.started_at,
    mrl.ended_at,
    mrl.status,
    mrl.total_attempted,
    mrl.succeeded,
    mrl.failed,
    mrl.skipped,
    mrl.retry_of_master_run_id,
    mrl.adf_pipeline_run_id,
    CAST(
        (julianday(mrl.ended_at) - julianday(mrl.started_at)) * 86400 AS INTEGER
    ) AS duration_seconds,
    CASE
        WHEN mrl.total_attempted > 0
            THEN CAST(mrl.succeeded AS REAL)
                / mrl.total_attempted * 100
    END AS success_rate_pct
FROM master_run_log AS mrl
INNER JOIN source_master AS sm ON mrl.source_id = sm.source_id
WHERE mrl.started_at >= datetime('now', '-7 days');
