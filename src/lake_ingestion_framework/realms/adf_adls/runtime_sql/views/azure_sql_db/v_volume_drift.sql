-- meta.v_volume_drift — flag datasets whose recent daily volume
-- (rows or bytes) deviates significantly from their own 7-day
-- baseline. Catches:
--   - source data outages / corruption (sudden drop to zero / near-zero)
--   - doubled-rows / dedup regressions (sudden 2× spike)
--   - silent schema changes (rows or bytes shift without a failure)
--
-- The view returns one row per (dataset_id, run_date) in the last
-- 7 days where drift_severity IN ('warning', 'anomaly') AND there
-- are at least 3 prior baseline days (otherwise we can't tell drift
-- from natural variance). Rows where the dataset is in steady state
-- (drift_severity='normal') are filtered out so the dashboard stays
-- focused on actionable signals.
--
-- Severity buckets (per docs/operability.md):
--   normal   = |rows_delta_pct| <= 25%
--   warning  = 25% < |rows_delta_pct| <= 50%
--   anomaly  = |rows_delta_pct| > 50%
--
-- Baseline = AVG of the previous 7 days' daily_rows / daily_bytes
-- via window function with frame `ROWS BETWEEN 7 PRECEDING AND 1
-- PRECEDING`. Same approach as v_slow_runs (no median in SQLite, so
-- AVG keeps T-SQL + SQLite parity).
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_volume_drift AS
WITH daily_totals AS (
    -- One row per (dataset_id, run_date) summing all SUCCESS runs
    -- that day. 14-day reach so the windowing has data for the
    -- 7-day baseline + 7-day evaluation window.
    SELECT
        rl.dataset_id,
        CAST(rl.started_at AS DATE) AS run_date,
        SUM(rl.rows_extracted) AS daily_rows,
        SUM(rl.bytes_written) AS daily_bytes
    FROM meta.run_log AS rl
    WHERE
        rl.status = 'SUCCESS'
        AND rl.started_at >= DATEADD(DAY, -14, SYSUTCDATETIME())
    GROUP BY rl.dataset_id, CAST(rl.started_at AS DATE)
),

with_baseline AS (
    SELECT
        dataset_id,
        run_date,
        daily_rows,
        daily_bytes,
        AVG(CAST(daily_rows AS DECIMAL(18, 2))) OVER (
            PARTITION BY dataset_id ORDER BY run_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg_rows,
        AVG(CAST(daily_bytes AS DECIMAL(18, 2))) OVER (
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
            / NULLIF(baseline_avg_rows, 0) AS DECIMAL(10, 2)
        ) AS rows_delta_pct,
        CAST(
            (daily_bytes - baseline_avg_bytes) * 100.0
            / NULLIF(baseline_avg_bytes, 0) AS DECIMAL(10, 2)
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
INNER JOIN meta.dataset_master AS dm ON s.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
WHERE
    s.baseline_sample_size >= 3
    AND s.run_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
    AND ABS(s.rows_delta_pct) > 25;
