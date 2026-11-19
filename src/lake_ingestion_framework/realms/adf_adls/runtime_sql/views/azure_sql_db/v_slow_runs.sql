-- meta.v_slow_runs — runs in the last 7 days that took > 3× the
-- dataset's recent (last 14 days) average source-side duration.
--
-- Perf-investigation view. Outputs a row per slow run with the
-- baseline average and a slowness_ratio so dashboards can sort by
-- magnitude.
--
-- Why AVG (not median): SQLite has no PERCENTILE_CONT; AVG works in
-- both dialects with the same expression. With a 3× multiplier and
-- 14-day baseline AVG is robust enough to spot regressions; if a
-- dataset's distribution is too skewed for AVG, the operator can
-- still inspect raw run_log with a sharper percentile query.
--
-- Datasets with fewer than 3 successful runs in the baseline window
-- are excluded — flagging "slow" without a real baseline is noise.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_slow_runs AS
WITH stats AS (
    SELECT
        dataset_id,
        AVG(CAST(source_duration_ms AS DECIMAL(18, 2))) AS avg_duration_ms,
        COUNT(*) AS sample_size
    FROM meta.run_log
    WHERE
        status = 'SUCCESS'
        AND source_duration_ms IS NOT NULL
        AND started_at >= DATEADD(DAY, -14, SYSUTCDATETIME())
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
        rl.source_duration_ms / NULLIF(s.avg_duration_ms, 0) AS DECIMAL(10, 2)
    ) AS slowness_ratio
FROM meta.run_log AS rl
INNER JOIN meta.dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
INNER JOIN stats AS s ON rl.dataset_id = s.dataset_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.source_duration_ms IS NOT NULL
    AND s.sample_size >= 3
    AND rl.source_duration_ms > 3 * s.avg_duration_ms
    AND rl.started_at >= DATEADD(DAY, -7, SYSUTCDATETIME());
