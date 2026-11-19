-- meta.v_dataset_health — one row per enabled dataset, traffic-light
-- status (green / yellow / red) based on last run + SLA breach.
--
-- Read by Power BI / Grafana / Synapse Studio dashboards as the
-- top-level "what's broken right now?" view. Each row carries
-- enough context to drill into v_recent_failures (via last_run_id)
-- or sp_run_log_finished history.
--
-- Health rule (load-bearing):
--   green  = last status SUCCESS AND time-since-success <= 75% of SLA
--   yellow = last status SUCCESS AND time-since-success in (75%, 100%] of SLA
--   red    = (no successful run yet) OR (last status FAILED) OR
--            (time-since-success > SLA)
--
-- sla_freshness_hours comes from dataset_config (per-dataset KV);
-- defaults to 24h when unset. Customers tune per dataset for ELT
-- vs. CDC-flavored cadences.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_dataset_health AS
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
    FROM meta.run_log
),

sla AS (
    -- Per-dataset SLA override; rows missing default to 24h in the
    -- outer SELECT via COALESCE.
    SELECT
        dataset_id,
        CAST([value] AS INT) AS sla_freshness_hours
    FROM meta.dataset_config
    WHERE [key] = 'sla_freshness_hours'
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
    DATEDIFF(MINUTE, lr.last_success_at, SYSUTCDATETIME()) AS minutes_since_success,
    DATEDIFF(HOUR, lr.last_success_at, SYSUTCDATETIME()) AS hours_since_success,
    -- Health rule: red on no-success / last-failed / SLA breach;
    -- yellow when last success is in the warning band (75–100% of SLA);
    -- green otherwise.
    CASE
        WHEN lr.last_success_at IS NULL THEN 'red'
        WHEN lr.status = 'FAILED' THEN 'red'
        WHEN DATEDIFF(HOUR, lr.last_success_at, SYSUTCDATETIME()) > COALESCE(sla.sla_freshness_hours, 24) THEN 'red'  -- noqa: LT05
        WHEN DATEDIFF(HOUR, lr.last_success_at, SYSUTCDATETIME()) > COALESCE(sla.sla_freshness_hours, 24) * 0.75 THEN 'yellow'  -- noqa: LT05
        ELSE 'green'
    END AS health
FROM meta.dataset_master AS dm
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
LEFT JOIN ranked AS lr ON dm.dataset_id = lr.dataset_id AND lr.rn = 1
LEFT JOIN sla ON dm.dataset_id = sla.dataset_id
WHERE dm.is_enabled = 1;
