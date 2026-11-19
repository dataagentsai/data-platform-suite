-- meta.v_retry_chains — one row per master-run retry chain (length ≥ 2).
-- A "chain" is a connected sequence of master_run_log rows linked
-- by retry_of_master_run_id. Single-attempt runs are not retry
-- chains by definition and are filtered out (use v_master_run_summary
-- for those).
--
-- Reliability concern view. Answers:
--   - Did the L4 auto-retry actually help? (chain_outcome)
--   - How many attempts did this chain take? (chain_length)
--   - How long from first attempt to last? (chain_duration_seconds)
--   - Which datasets were left failing at the leaf? (run_log
--     correlation via final_master_run_id)
--
-- chain_outcome values:
--   eventually_succeeded — leaf has status='SUCCESS' (auto-retry healed)
--   still_failing        — leaf has status='FAILED' or 'PARTIAL'
--                          (chain exhausted; manual intervention)
--   in_progress          — leaf has status='RUNNING' (active chain)
--
-- T-SQL recursive CTE: implicit (no RECURSIVE keyword); SQLite
-- requires `WITH RECURSIVE` (see ../sqlite/v_retry_chains.sql).
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_retry_chains AS
WITH chain_walk AS (
    -- Anchor: every chain root (master runs with no parent).
    -- Each anchor is "position 1" in its own chain.
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
    FROM meta.master_run_log
    WHERE retry_of_master_run_id IS NULL

    UNION ALL

    -- Recursive: walk forward to children
    -- (m.retry_of_master_run_id = parent.master_run_id).
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
    FROM meta.master_run_log AS m
    INNER JOIN chain_walk AS cw ON m.retry_of_master_run_id = cw.current_id
),

leaf AS (
    -- The latest run in each chain (highest chain_position).
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
    DATEDIFF(SECOND, a.chain_started_at, a.chain_ended_at) AS chain_duration_seconds,
    CASE
        WHEN l.final_status = 'SUCCESS' THEN 'eventually_succeeded'
        WHEN l.final_status = 'RUNNING' THEN 'in_progress'
        ELSE 'still_failing'
    END AS chain_outcome
FROM agg AS a
INNER JOIN leaf AS l ON a.chain_root_id = l.chain_root_id AND l.rn = 1
WHERE a.chain_length >= 2;
