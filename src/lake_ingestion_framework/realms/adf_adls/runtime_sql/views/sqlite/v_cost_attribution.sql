-- SQLite stand-in for meta.v_cost_attribution.
-- Same column shape + DIU-hour formula as the T-SQL canonical
-- (../azure_sql_db/v_cost_attribution.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - bare key/value (T-SQL uses [key]/[value])
--   - DECIMAL -> REAL; CAST AS DATE -> DATE(...)
--   - DATEADD(DAY, -30, ...) -> datetime('now', '-30 days')

DROP VIEW IF EXISTS v_cost_attribution;
CREATE VIEW v_cost_attribution AS
WITH cost_center_lookup AS (
    SELECT
        dataset_id,
        value AS cost_center
    FROM dataset_config
    WHERE key = 'cost_center'
)

SELECT
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    rl.source_env,
    rl.realm,
    cc.cost_center,
    DATE(rl.started_at) AS run_date,
    CAST(
        SUM(
            CAST(rl.used_dius AS REAL)
            * rl.source_duration_ms
            / 3600000.0
        ) AS REAL
    ) AS diu_hours,
    SUM(rl.source_duration_ms) AS total_duration_ms,
    SUM(rl.bytes_read) AS total_bytes_read,
    SUM(rl.bytes_written) AS total_bytes_written,
    COUNT(*) AS run_count
FROM run_log AS rl
INNER JOIN dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
LEFT JOIN cost_center_lookup AS cc ON rl.dataset_id = cc.dataset_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.used_dius IS NOT NULL
    AND rl.source_duration_ms IS NOT NULL
    AND rl.started_at >= datetime('now', '-30 days')
GROUP BY
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    rl.source_env,
    rl.realm,
    cc.cost_center,
    DATE(rl.started_at);
