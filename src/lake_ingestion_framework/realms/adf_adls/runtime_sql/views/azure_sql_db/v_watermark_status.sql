-- meta.v_watermark_status — one row per enabled dataset showing the
-- current watermark, time since last advance, and (when present) the
-- most recent operator-driven reset.
--
-- Reliability concern view. Answers two questions in one place:
--   1. "Has this dataset's watermark advanced recently?"
--      → minutes_since_advance / hours_since_advance + health label
--   2. "Was this dataset reset, by whom, when, why?"
--      → last_reset_* columns (NULL when no resets logged)
--
-- Health labels:
--   never_loaded   = no row in meta.watermark (first run hasn't happened)
--   possibly_stuck = last_loaded_at older than 24h (incremental
--                    datasets should advance at least daily; per-
--                    dataset thresholds land in a later phase)
--   healthy        = last advance < 24h ago
--
-- Reset history comes from meta.watermark_audit, which sp_advance_
-- watermark deliberately does NOT write to (advances would bloat the
-- audit log). Phase 6b-3 CLIs (`reset-watermark`, `backfill`) populate
-- the table; until then last_reset_* stays NULL.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_watermark_status AS
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
    FROM meta.watermark_audit
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
    DATEDIFF(MINUTE, w.last_loaded_at, SYSUTCDATETIME()) AS minutes_since_advance,
    DATEDIFF(HOUR, w.last_loaded_at, SYSUTCDATETIME()) AS hours_since_advance,
    CASE
        WHEN w.last_loaded_at IS NULL THEN 'never_loaded'
        WHEN DATEDIFF(HOUR, w.last_loaded_at, SYSUTCDATETIME()) > 24 THEN 'possibly_stuck'
        ELSE 'healthy'
    END AS health
FROM meta.dataset_master AS dm
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
LEFT JOIN meta.watermark AS w ON dm.dataset_id = w.dataset_id
LEFT JOIN last_reset AS lr ON dm.dataset_id = lr.dataset_id AND lr.rn = 1
WHERE dm.is_enabled = 1;
