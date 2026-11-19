-- meta.v_cost_attribution — DIU-hours attributed per source / dataset
-- / realm / cost_center over a 30-day window. The cost concern's
-- finance-facing view; consumers (Power BI, Cost Management, weekly
-- reports) slice by any column.
--
-- Formula: DIU-hours = used_dius * source_duration_ms / 3.6e6
--   used_dius            : ADF Copy.usedDataIntegrationUnits (run_log)
--   source_duration_ms   : Copy.copyDuration * 1000 (run_log)
--   3.6e6                : ms in 1 hour (1000 * 60 * 60)
--
-- DIU-hours is the dominant cost driver per docs/cost.md
-- ("DIU-hours dominate at any non-trivial volume"). At ~$0.25/DIU-hour
-- for AutoResolve IR, this view × $0.25 yields a defensible cost
-- estimate per row.
--
-- Daily granularity (run_date) so consumers can chart trends or
-- sum/group at coarser intervals. cost_center comes from
-- dataset_config (per-dataset KV); NULL when unset.
--
-- 30-day window is wider than the operability views (7d) since cost
-- analysis is typically monthly. Only SUCCESS runs contribute (FAILED
-- runs may have NULL metrics + skewed durations from partial work).
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_cost_attribution AS
WITH cost_center_lookup AS (
    SELECT
        dataset_id,
        [value] AS cost_center
    FROM meta.dataset_config
    WHERE [key] = 'cost_center'
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
    CAST(rl.started_at AS DATE) AS run_date,
    CAST(
        SUM(
            CAST(rl.used_dius AS DECIMAL(18, 4))
            * rl.source_duration_ms
            / 3600000.0
        ) AS DECIMAL(18, 4)
    ) AS diu_hours,
    SUM(rl.source_duration_ms) AS total_duration_ms,
    SUM(rl.bytes_read) AS total_bytes_read,
    SUM(rl.bytes_written) AS total_bytes_written,
    COUNT(*) AS run_count
FROM meta.run_log AS rl
INNER JOIN meta.dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
LEFT JOIN cost_center_lookup AS cc ON rl.dataset_id = cc.dataset_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.used_dius IS NOT NULL
    AND rl.source_duration_ms IS NOT NULL
    AND rl.started_at >= DATEADD(DAY, -30, SYSUTCDATETIME())
GROUP BY
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    rl.source_env,
    rl.realm,
    cc.cost_center,
    CAST(rl.started_at AS DATE);
