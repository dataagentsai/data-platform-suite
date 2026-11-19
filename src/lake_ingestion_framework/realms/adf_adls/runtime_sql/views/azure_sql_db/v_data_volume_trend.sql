-- meta.v_data_volume_trend — daily aggregate of bytes / rows / files
-- per dataset over the last 7 days. Capacity-planning + drift view.
--
-- Only SUCCESS runs contribute (failed runs have NULL volume metrics
-- and would skew the rollup). Datasets with no successful runs in
-- the window are absent.
--
-- Daily granularity; consumers chart day-over-day or week-over-week
-- diffs. Pair with v_dataset_health for traffic-light + trend on the
-- same dashboard tile.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_data_volume_trend AS
SELECT
    rl.dataset_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    CAST(rl.started_at AS DATE) AS run_date,
    SUM(rl.rows_extracted) AS daily_rows,
    SUM(rl.bytes_read) AS daily_bytes_read,
    SUM(rl.bytes_written) AS daily_bytes_written,
    SUM(rl.file_count) AS daily_files,
    COUNT(*) AS daily_runs
FROM meta.run_log AS rl
INNER JOIN meta.dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
WHERE
    rl.status = 'SUCCESS'
    AND rl.started_at >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY
    rl.dataset_id,
    dm.dataset_name,
    sm.source_name,
    sm.source_env,
    CAST(rl.started_at AS DATE);
