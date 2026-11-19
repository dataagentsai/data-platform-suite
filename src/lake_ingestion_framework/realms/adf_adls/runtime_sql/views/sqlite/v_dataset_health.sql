-- SQLite stand-in for meta.v_dataset_health.
-- Same column shape + health rule as the T-SQL canonical
-- (../azure_sql_db/v_dataset_health.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - bare `key`/`value` (T-SQL uses [key]/[value])
--   - DATEDIFF -> julianday math (julianday returns days; *24 = hours,
--     *1440 = minutes); SYSUTCDATETIME() -> datetime('now')
--   - INT -> INTEGER

DROP VIEW IF EXISTS v_dataset_health;
CREATE VIEW v_dataset_health AS
WITH ranked AS (
    SELECT
        run_id,
        dataset_id,
        status,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY started_at DESC) AS rn,
        MAX(CASE WHEN status = 'SUCCESS' THEN started_at END)
            OVER (PARTITION BY dataset_id) AS last_success_at,
        MAX(started_at) OVER (PARTITION BY dataset_id) AS last_attempt_at
    FROM run_log
),

sla AS (
    SELECT
        dataset_id,
        CAST(value AS INTEGER) AS sla_freshness_hours
    FROM dataset_config
    WHERE key = 'sla_freshness_hours'
)

SELECT
    dm.dataset_id,
    dm.source_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    lr.last_success_at,
    lr.last_attempt_at,
    lr.status AS last_status,
    lr.run_id AS last_run_id,
    COALESCE(sla.sla_freshness_hours, 24) AS sla_freshness_hours,
    -- minutes / hours since last success (julianday returns days)
    CAST((julianday('now') - julianday(lr.last_success_at)) * 1440 AS INTEGER)
        AS minutes_since_success,
    CAST((julianday('now') - julianday(lr.last_success_at)) * 24 AS INTEGER)
        AS hours_since_success,
    CASE
        WHEN lr.last_success_at IS NULL THEN 'red'
        WHEN lr.status = 'FAILED' THEN 'red'
        WHEN (julianday('now') - julianday(lr.last_success_at)) * 24
            > COALESCE(sla.sla_freshness_hours, 24) THEN 'red'
        WHEN (julianday('now') - julianday(lr.last_success_at)) * 24
            > COALESCE(sla.sla_freshness_hours, 24) * 0.75 THEN 'yellow'
        ELSE 'green'
    END AS health
FROM dataset_master AS dm
INNER JOIN source_master AS sm ON sm.source_id = dm.source_id
LEFT JOIN ranked AS lr ON lr.dataset_id = dm.dataset_id AND lr.rn = 1
LEFT JOIN sla ON sla.dataset_id = dm.dataset_id
WHERE dm.is_enabled = 1;
