-- meta.v_recent_failures — last 7 days of FAILED runs, ordered most
-- recent first. Triage view: each row carries enough context to
-- correlate back to the master run + the source row + the dataset
-- without further lookups.
--
-- error_message is shown verbatim — the runner already redacted it
-- (Password=, AccountKey=, bearer tokens, SAS) and capped at 4 KB
-- per docs/security.md. Consumers can safely render this column in
-- a dashboard.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_recent_failures AS
SELECT
    rl.run_id,
    rl.master_run_id,
    rl.dataset_id,
    dm.dataset_name,
    sm.source_id,
    sm.source_name,
    sm.source_type,
    sm.source_env,
    rl.pipeline_name,
    rl.started_at,
    rl.ended_at,
    rl.retry_count,
    rl.error_message,
    rl.target_path
FROM meta.run_log AS rl
INNER JOIN meta.dataset_master AS dm ON rl.dataset_id = dm.dataset_id
INNER JOIN meta.source_master AS sm ON dm.source_id = sm.source_id
WHERE
    rl.status = 'FAILED'
    AND rl.started_at >= DATEADD(DAY, -7, SYSUTCDATETIME());
