-- SQLite bootstrap DDL — the v0.x metadata DB stand-in for local
-- development and testing. A subset of the canonical Azure SQL DB
-- schema in docs/metadata_schema.md, scoped to what Python touches:
--   source_master, source_config, dataset_master, dataset_config,
--   dataset_config_columns, watermark.
--
-- ADF runtime tables (run_log, master_run_log, segment_run_log,
-- watermark_audit, dataset_segments, platform_config) are out of
-- scope here — they're written by ADF stored procs at runtime, not
-- by Python. When we build the Azure SQL DB realm, the canonical
-- schema lands as schema_azure_sql.sql alongside this file.
--
-- Differences from the canonical schema:
--   - INTEGER PRIMARY KEY AUTOINCREMENT instead of IDENTITY(1,1)
--   - DATETIME stored as ISO-8601 TEXT (SQLite doesn't have DATETIME2)
--   - INTEGER for BIT (SQLite has no BIT type; 0/1 conventionally)
--   - TEXT for NVARCHAR (SQLite is dynamically typed anyway)
--   - No CHECK constraints on enums (validated in Python)

-- ─── source_master ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS source_master (
    source_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name    TEXT    NOT NULL,
    source_env     TEXT    NOT NULL DEFAULT 'prod',
    source_type    TEXT    NOT NULL,
    is_enabled     INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name, source_env)
);

-- ─── source_config (KV) ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS source_config (
    source_id   INTEGER NOT NULL REFERENCES source_master(source_id),
    key         TEXT    NOT NULL,
    value       TEXT,
    updated_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, key)
);

-- ─── dataset_master ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dataset_master (
    dataset_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id     INTEGER NOT NULL REFERENCES source_master(source_id),
    dataset_name  TEXT    NOT NULL,
    is_enabled    INTEGER NOT NULL DEFAULT 0,    -- new datasets default disabled
    created_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_id, dataset_name)
);

-- ─── dataset_config (KV) ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dataset_config (
    dataset_id  INTEGER NOT NULL REFERENCES dataset_master(dataset_id),
    key         TEXT    NOT NULL,
    value       TEXT,
    updated_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, key)
);

-- ─── dataset_config_columns ───────────────────────────────────────────
-- column_name is the BRONZE column name (the natural key against the
-- source's INFORMATION_SCHEMA / discovered shape).
-- silver_column_name (Wave 1, schema v3) is the rename target — when
-- NULL, Silver inherits the bronze name. silver_data_type is the
-- target type for the type_safety processor's safe_cast (NULL =
-- inherit). null_replacement / transforms_json / skip_groups_json /
-- skip_processors_json carry per-column processor configuration.
CREATE TABLE IF NOT EXISTS dataset_config_columns (
    dataset_id        INTEGER NOT NULL REFERENCES dataset_master(dataset_id),
    column_name       TEXT    NOT NULL,
    data_type         TEXT    NOT NULL,
    is_nullable       INTEGER NOT NULL,
    ordinal_position  INTEGER NOT NULL,
    is_primary_key    INTEGER NOT NULL DEFAULT 0,
    is_excluded       INTEGER NOT NULL DEFAULT 0,
    pii_class         TEXT    NOT NULL DEFAULT 'none',
    source_path       TEXT,
    -- Wave 1 (schema v3) — column-transform metadata
    silver_column_name    TEXT,                       -- rename target (NULL = inherit)
    silver_data_type      TEXT,                       -- safe_cast target (NULL = inherit)
    cast_safety           TEXT NOT NULL DEFAULT 'strict',  -- 'strict' | 'try_cast'
    null_replacement      TEXT,                       -- literal substituted for NULL
    transforms_json       TEXT,                       -- JSON list of opt-in processor names
    skip_groups_json      TEXT,                       -- JSON list of group names to bypass
    skip_processors_json  TEXT,                       -- JSON list of processor names to bypass
    column_role           TEXT,                       -- e.g. 'code', 'identifier' — drives case_normalize
    PRIMARY KEY (dataset_id, column_name)
);

-- ─── watermark ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS watermark (
    dataset_id       INTEGER PRIMARY KEY REFERENCES dataset_master(dataset_id),
    watermark_value  TEXT,
    last_loaded_at   TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rows_loaded      INTEGER
);

-- ─── platform_config (KV) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS platform_config (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    description TEXT,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ─── dq_rules (typed; v1 — Inline Checks, sql tech only) ──────────────
-- Per docs/dq_design.md §12.2 + the v1 addendum at the top of that doc.
-- Authored as YAML in customer/config/dq/<layer>_<dataset>.yaml; pushed
-- here by `lake-ingest config sync`. Pipelines read from this table at
-- runtime — they NEVER open the YAML files directly.
CREATE TABLE IF NOT EXISTS dq_rules (
    rule_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name            TEXT    NOT NULL,
    dataset_id           INTEGER NOT NULL REFERENCES dataset_master(dataset_id),
    layer                TEXT    NOT NULL,                  -- bronze | silver | gold
    rule_type            TEXT    NOT NULL DEFAULT 'predicate',
    tech                 TEXT    NOT NULL DEFAULT 'sql',    -- v1: sql only
    container            TEXT    NOT NULL DEFAULT 'duckdb', -- duckdb (local) | spark (prod)
    predicate            TEXT    NOT NULL,                  -- SQL expression; rows where TRUE = violators
    injection_point      TEXT    NOT NULL,                  -- post_reader | post_processor | post_write
    severity             TEXT    NOT NULL,                  -- info | warning | error
    action               TEXT    NOT NULL,                  -- stop_pipeline | continue_log | quarantine_record
    quarantine_strategy  TEXT,                              -- side_table | log_only; NULL when action != quarantine_record
    sample_cap           INTEGER NOT NULL DEFAULT 100,      -- max violator rows to capture per run
    is_enabled           INTEGER NOT NULL DEFAULT 1,
    source               TEXT    NOT NULL DEFAULT 'user_authored',  -- user_authored | auto_recommended | framework_invariant
    authored_by          TEXT,
    description          TEXT,
    created_at           TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_id, layer, rule_name)
);
CREATE INDEX IF NOT EXISTS idx_dq_rules_dataset_layer
    ON dq_rules (dataset_id, layer, is_enabled);

-- ─── dq_results (typed; one row per rule × run) ───────────────────────
-- Audit trail of every rule evaluation: when, what, pass/fail,
-- violator count, action taken. Customer queries via SQL view
-- meta.v_dq_health (built later).
CREATE TABLE IF NOT EXISTS dq_results (
    result_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id          INTEGER NOT NULL REFERENCES dq_rules(rule_id),
    run_id           TEXT    NOT NULL,                  -- pipeline run identifier
    evaluated_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    passed           INTEGER NOT NULL,                  -- 0 | 1
    violators_total  INTEGER NOT NULL DEFAULT 0,        -- total violator rows; -1 = not counted (cheap-eval bool)
    sample_captured  INTEGER NOT NULL DEFAULT 0,        -- how many violator rows landed in dq_violations
    action_taken     TEXT    NOT NULL,                  -- echoes rules.action; future: may differ if framework escalates
    duration_ms      INTEGER NOT NULL DEFAULT 0,
    error_message    TEXT                               -- non-null when evaluator itself failed
);
CREATE INDEX IF NOT EXISTS idx_dq_results_run     ON dq_results (run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_rule    ON dq_results (rule_id, evaluated_at);

-- ─── dq_violations (typed; PKs of violator rows for log_only quarantine) ──
-- For action=quarantine_record + quarantine_strategy=side_table, the
-- violator rows go to <main>__quarantine Delta tables (not here).
-- For quarantine_strategy=log_only OR for any action that captures a
-- sample of violators, the PKs land here for audit.
CREATE TABLE IF NOT EXISTS dq_violations (
    violation_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id          INTEGER NOT NULL REFERENCES dq_rules(rule_id),
    result_id        INTEGER NOT NULL REFERENCES dq_results(result_id),
    pk_columns_json  TEXT    NOT NULL,                  -- {"order_id": "X1", ...} — JSON for portability
    violated_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dq_violations_result ON dq_violations (result_id);
CREATE INDEX IF NOT EXISTS idx_dq_violations_rule   ON dq_violations (rule_id, violated_at);

-- ─── pii_policy (typed; v4 — per pii_class × env action) ─────────────
-- Customer-managed table: each row picks the action a PII column with
-- the given pii_class should receive in a given env. ``salt_kv_ref``
-- is a Key Vault reference (``kv://<vault>/<secret>``) for the
-- per-customer salt used by hash / tokenize. For Wave 2, salt may also
-- be a literal value (resolved as-is); KV resolution lands in a
-- follow-up commit. See docs/standard_processors.md §6 (pii group).
CREATE TABLE IF NOT EXISTS pii_policy (
    policy_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    pii_class     TEXT NOT NULL,           -- 'email' | 'ssn' | 'phone' | ...
    env           TEXT NOT NULL,           -- 'dev' | 'uat' | 'prod' | ...
    action        TEXT NOT NULL,           -- 'pass_through' | 'hash' | 'mask' | 'tokenize' | 'redact'
    salt_kv_ref   TEXT,                    -- KV ref OR literal salt (Wave 2 v1)
    redact_value  TEXT,                    -- literal substitute for 'redact' (default '[redacted]')
    mask_visible  INTEGER NOT NULL DEFAULT 4,  -- last-N chars kept by 'mask'
    updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pii_class, env)
);
CREATE INDEX IF NOT EXISTS idx_pii_policy_env ON pii_policy (env);

-- ─── schema version row ───────────────────────────────────────────────
INSERT OR IGNORE INTO platform_config (key, value, description)
VALUES ('metadata_schema_version', '4', 'Bumped on each breaking schema migration.');

-- 2026-05-05 — schema v2: add dq_rules / dq_results / dq_violations
-- 2026-05-05 — schema v3: extend dataset_config_columns with column-transform fields
--                          (silver_column_name / silver_data_type / cast_safety /
--                           null_replacement / transforms_json / skip_*_json /
--                           column_role)
-- 2026-05-05 — schema v4: add pii_policy (per pii_class × env → action + salt_kv_ref)
