-- SQLite stand-in for meta.v_volume_drift.
-- Same column shape + drift rule as the T-SQL canonical
-- (../azure_sql_db/v_volume_drift.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - DECIMAL -> REAL; DATEADD -> datetime('now', '-N days')
--   - CAST(x AS DATE) -> DATE(x)

DROP VIEW IF EXISTS v_volume_drift;
CREATE VIEW v_volume_drift AS
WITH daily_totals AS (
    SELECT
        rl.dataset_id,
        DATE(rl.started_at) AS run_date,
        SUM(rl.rows_extracted) AS daily_rows,
        SUM(rl.bytes_written) AS daily_bytes
    FROM run_log AS rl
    WHERE
        rl.status = 'SUCCESS'
        AND rl.started_at >= datetime('now', '-14 days')
    GROUP BY rl.dataset_id, DATE(rl.started_at)
),

with_baseline AS (
    SELECT
        dataset_id,
        run_date,
        daily_rows,
        daily_bytes,
        AVG(CAST(daily_rows AS REAL)) OVER (
            PARTITION BY dataset_id ORDER BY run_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg_rows,
        AVG(CAST(daily_bytes AS REAL)) OVER (
            PARTITION BY dataset_id ORDER BY run_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg_bytes,
        COUNT(*) OVER (
            PARTITION BY dataset_id ORDER BY run_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_sample_size
    FROM daily_totals
),

scored AS (
    SELECT
        dataset_id,
        run_date,
        daily_rows,
        daily_bytes,
        baseline_avg_rows,
        baseline_avg_bytes,
        baseline_sample_size,
        CAST(
            (daily_rows - baseline_avg_rows) * 100.0
            / NULLIF(baseline_avg_rows, 0) AS REAL
        ) AS rows_delta_pct,
        CAST(
            (daily_bytes - baseline_avg_bytes) * 100.0
            / NULLIF(baseline_avg_bytes, 0) AS REAL
        ) AS bytes_delta_pct
    FROM with_baseline
)

SELECT
    s.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    s.run_date,
    s.daily_rows,
    s.daily_bytes,
    s.baseline_avg_rows,
    s.baseline_avg_bytes,
    s.baseline_sample_size,
    s.rows_delta_pct,
    s.bytes_delta_pct,
    CASE
        WHEN ABS(s.rows_delta_pct) > 50 THEN 'anomaly'
        ELSE 'warning'
    END AS drift_severity
FROM scored AS s
INNER JOIN dataset_master AS dm ON s.dataset_id = dm.dataset_id
INNER JOIN source_master AS sm ON dm.source_id = sm.source_id
WHERE
    s.baseline_sample_size >= 3
    AND s.run_date >= DATE(datetime('now', '-7 days'))
    AND ABS(s.rows_delta_pct) > 25;
