-- SQLite stand-in for meta.v_watermark_status.
-- Same column shape + health rule as the T-SQL canonical
-- (../azure_sql_db/v_watermark_status.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DATEDIFF -> julianday math (julianday returns days; *24 = hours,
--     *1440 = minutes); SYSUTCDATETIME() -> datetime('now')

DROP VIEW IF EXISTS v_watermark_status;
CREATE VIEW v_watermark_status AS
WITH last_reset AS (
    SELECT
        dataset_id,
        old_watermark_value,
        new_watermark_value,
        change_type,
        changed_by,
        reason,
        changed_at,
        ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY changed_at DESC) AS rn
    FROM watermark_audit
)

SELECT
    dm.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_env,
    w.watermark_value AS current_watermark,
    w.last_loaded_at,
    w.rows_loaded AS last_rows_loaded,
    lr.old_watermark_value AS last_reset_from_value,
    lr.new_watermark_value AS last_reset_to_value,
    lr.change_type AS last_reset_change_type,
    lr.changed_by AS last_reset_changed_by,
    lr.reason AS last_reset_reason,
    lr.changed_at AS last_reset_at,
    CAST((julianday('now') - julianday(w.last_loaded_at)) * 1440 AS INTEGER)
        AS minutes_since_advance,
    CAST((julianday('now') - julianday(w.last_loaded_at)) * 24 AS INTEGER)
        AS hours_since_advance,
    CASE
        WHEN w.last_loaded_at IS NULL THEN 'never_loaded'
        WHEN (julianday('now') - julianday(w.last_loaded_at)) * 24 > 24 THEN 'possibly_stuck'
        ELSE 'healthy'
    END AS health
FROM dataset_master AS dm
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
LEFT JOIN watermark AS w ON dm.dataset_id = w.dataset_id
LEFT JOIN last_reset AS lr ON dm.dataset_id = lr.dataset_id AND lr.rn = 1
WHERE dm.is_enabled = 1;
