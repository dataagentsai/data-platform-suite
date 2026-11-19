-- SQLite stand-in for meta.v_slow_runs.
-- Same column shape + slow-run rule as the T-SQL canonical
-- (../azure_sql_db/v_slow_runs.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DATEADD(DAY, -N, ...) -> datetime('now', '-N days')
--   - DECIMAL -> REAL

DROP VIEW IF EXISTS v_slow_runs;
CREATE VIEW v_slow_runs AS
WITH stats AS (
    SELECT
        dataset_id,
        AVG(CAST(source_duration_ms AS REAL)) AS avg_duration_ms,
        COUNT(*) AS sample_size
    FROM run_log
    WHERE
        status = 'SUCCESS'
        AND source_duration_ms IS NOT NULL
        AND started_at >= datetime('now', '-14 days')
    GROUP BY dataset_id
)

SELECT
    rl.run_id,
    rl.master_run_id,
    rl.dataset_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    rl.started_at,
    rl.ended_at,
    rl.status,
    rl.source_duration_ms,
    s.avg_duration_ms AS dataset_avg_duration_ms,
    s.sample_size AS dataset_sample_size,
    CAST(
        rl.source_duration_ms / NULLIF(s.avg_duration_ms, 0) AS REAL
    ) AS slowness_ratio
FROM run_log AS rl
INNER JOIN dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
INNER JOIN stats AS s ON rl.dataset_id = s.dataset_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.source_duration_ms IS NOT NULL
    AND s.sample_size >= 3
    AND rl.source_duration_ms > 3 * s.avg_duration_ms
    AND rl.started_at >= datetime('now', '-7 days');
