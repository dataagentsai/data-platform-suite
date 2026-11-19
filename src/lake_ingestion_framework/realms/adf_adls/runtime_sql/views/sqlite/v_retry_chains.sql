-- SQLite stand-in for meta.v_retry_chains.
-- Same column shape + chain rule as the T-SQL canonical
-- (../azure_sql_db/v_retry_chains.sql); differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - WITH RECURSIVE (SQLite requires the keyword; T-SQL implicit)
--   - DATEDIFF(SECOND, a, b) -> (julianday(b) - julianday(a)) * 86400

DROP VIEW IF EXISTS v_retry_chains;
CREATE VIEW v_retry_chains AS
WITH RECURSIVE chain_walk AS (
    SELECT
        master_run_id AS chain_root_id,
        master_run_id AS current_id,
        1 AS chain_position,
        started_at,
        ended_at,
        status,
        total_attempted,
        succeeded,
        failed
    FROM master_run_log
    WHERE retry_of_master_run_id IS NULL

    UNION ALL

    SELECT
        cw.chain_root_id,
        m.master_run_id AS current_id,
        cw.chain_position + 1 AS chain_position,
        m.started_at,
        m.ended_at,
        m.status,
        m.total_attempted,
        m.succeeded,
        m.failed
    FROM master_run_log AS m
    INNER JOIN chain_walk AS cw ON m.retry_of_master_run_id = cw.current_id
),

leaf AS (
    SELECT
        chain_root_id,
        current_id AS final_master_run_id,
        status AS final_status,
        ended_at AS final_ended_at,
        ROW_NUMBER() OVER (PARTITION BY chain_root_id ORDER BY chain_position DESC) AS rn
    FROM chain_walk
),

agg AS (
    SELECT
        chain_root_id,
        COUNT(*) AS chain_length,
        SUM(total_attempted) AS total_attempts_across_chain,
        SUM(succeeded) AS total_succeeded_across_chain,
        SUM(failed) AS total_failed_across_chain,
        MIN(started_at) AS chain_started_at,
        MAX(ended_at) AS chain_ended_at
    FROM chain_walk
    GROUP BY chain_root_id
)

SELECT
    a.chain_root_id,
    a.chain_length,
    a.total_attempts_across_chain,
    a.total_succeeded_across_chain,
    a.total_failed_across_chain,
    l.final_master_run_id,
    l.final_status,
    a.chain_started_at,
    a.chain_ended_at,
    CAST((julianday(a.chain_ended_at) - julianday(a.chain_started_at)) * 86400 AS INTEGER)
        AS chain_duration_seconds,
    CASE
        WHEN l.final_status = 'SUCCESS' THEN 'eventually_succeeded'
        WHEN l.final_status = 'RUNNING' THEN 'in_progress'
        ELSE 'still_failing'
    END AS chain_outcome
FROM agg AS a
INNER JOIN leaf AS l ON a.chain_root_id = l.chain_root_id AND l.rn = 1
WHERE a.chain_length >= 2;
