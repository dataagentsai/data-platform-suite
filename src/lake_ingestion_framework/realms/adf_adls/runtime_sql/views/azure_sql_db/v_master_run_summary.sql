-- meta.v_master_run_summary — trigger-level rollup of the last 7 days
-- of master runs. Joins master_run_log with source context and adds
-- computed duration + success-rate columns.
--
-- Release-train view: one row per fired trigger / orchestration run.
-- adf_pipeline_run_id supports drill-through to the ADF Studio UI.
-- retry_of_master_run_id surfaces auto-retry chains.
--
-- The succeeded / failed / skipped counts are stored on master_run_log
-- (sp_master_run_log_finished writes them); this view just adds the
-- enrichment ops actually consume (source_name, source_env, duration,
-- success rate %).
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_master_run_summary AS
SELECT
    mrl.master_run_id,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    mrl.source_env,
    mrl.run_type,
    mrl.trigger_name,
    mrl.trigger_type,
    mrl.started_at,
    mrl.ended_at,
    mrl.status,
    mrl.total_attempted,
    mrl.succeeded,
    mrl.failed,
    mrl.skipped,
    mrl.retry_of_master_run_id,
    mrl.adf_pipeline_run_id,
    DATEDIFF(SECOND, mrl.started_at, mrl.ended_at) AS duration_seconds,
    CASE
        WHEN mrl.total_attempted > 0 THEN CAST(mrl.succeeded AS DECIMAL(5, 2)) / mrl.total_attempted * 100  -- noqa: LT05
    END AS success_rate_pct
FROM meta.master_run_log AS mrl
INNER JOIN meta.source_master AS sm ON mrl.source_id = sm.source_id
WHERE mrl.started_at >= DATEADD(DAY, -7, SYSUTCDATETIME());
