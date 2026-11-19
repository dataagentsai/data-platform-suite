-- The metadata store on PostgreSQL (ADR-010). Any PostgreSQL 14+: a local server, a managed one, or the engine's
-- managed PostgreSQL; the binding only picks the connection string. Re-running this file is safe (idempotent).
--
-- Two schemas:
--   config    what to load and how: the settings registry, domains, sources, datasets, their settings, columns
--             and business keys. Written only by config.load_* (called by the validating loader, REL-01).
--   runtime   what happened: runs, dataset runs, steps, files, errors (all keyed by run id), config snapshots,
--             watermarks, bookmarks and the watermark reset audit.
--
-- The functions below are the only way in (DPC-0033): they are SECURITY DEFINER, tables are revoked from PUBLIC,
-- and a service role gets USAGE on the schemas and EXECUTE on the functions only (see the end of this file).
-- Refusals use our own SQLSTATEs, which the adapter maps to the kernel's exceptions:
--   DPM01 unknown setting   DPM02 not found   DPM03 invalid argument   DPM04 run out of order
--
-- Reused from LakeIngestionFramework (realms/adf_adls/runtime_sql, docs/metadata_schema.md), reshaped for PostgreSQL:
-- source_master -> config.source, dataset_master -> config.dataset, dataset_config (KV) -> config.dataset_setting,
-- dataset_config_columns -> config.dataset_column, master_run_log -> runtime.run, run_log -> runtime.dataset_run,
-- watermark -> runtime.watermark, watermark_audit -> runtime.watermark_audit; sp_run_log_started/finished,
-- sp_master_run_log_started/finished and sp_advance_watermark -> the runtime functions below.

CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS runtime;

-- ---------------------------------------------------------------------------------------------------- config tables

CREATE TABLE IF NOT EXISTS config.setting_def (
    scope TEXT NOT NULL CHECK (scope IN ('platform', 'source', 'dataset')),
    key TEXT NOT NULL,
    value_type TEXT NOT NULL,
    default_value TEXT,
    allowed TEXT [],
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'registered' CHECK (status IN ('registered', 'proposed')),
    PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS config.domain (
    domain TEXT PRIMARY KEY,
    business_timezone TEXT NOT NULL,
    environments TEXT [] NOT NULL DEFAULT '{}',
    manifest JSONB NOT NULL,
    fingerprint TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS config.domain_setting (
    domain TEXT NOT NULL REFERENCES config.domain (domain) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (domain, key),
    FOREIGN KEY (scope, key) REFERENCES config.setting_def (scope, key)
);

CREATE TABLE IF NOT EXISTS config.source (
    source_id BIGSERIAL PRIMARY KEY,
    domain TEXT NOT NULL REFERENCES config.domain (domain),
    source_name TEXT NOT NULL UNIQUE,
    source_env TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS config.dataset (
    dataset_id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES config.source (source_id),
    dataset_name TEXT NOT NULL,
    is_enabled BOOLEAN NOT NULL,
    kind TEXT NOT NULL,
    archetypes TEXT [] NOT NULL DEFAULT '{}',
    ingest_mode TEXT NOT NULL DEFAULT '',
    ingest_read TEXT NOT NULL DEFAULT '',
    history_kind TEXT NOT NULL DEFAULT '',
    load_pattern TEXT NOT NULL DEFAULT '',
    contract_id TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    manifest JSONB NOT NULL,
    fingerprint TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, dataset_name)
);

CREATE TABLE IF NOT EXISTS config.dataset_setting (
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id) ON DELETE CASCADE,
    scope TEXT NOT NULL DEFAULT 'dataset' CHECK (scope = 'dataset'),
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (dataset_id, key),
    FOREIGN KEY (scope, key) REFERENCES config.setting_def (scope, key)
);

CREATE TABLE IF NOT EXISTS config.dataset_column (
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id) ON DELETE CASCADE,
    ordinal INT NOT NULL,
    column_name TEXT NOT NULL,
    logical_type TEXT NOT NULL,
    is_required BOOLEAN NOT NULL DEFAULT FALSE,
    primary_key_position INT,
    description TEXT NOT NULL DEFAULT '',
    classification TEXT NOT NULL DEFAULT '',     -- the contract's personal-data class (restricted: tokenised, DPC-0070)
    PRIMARY KEY (dataset_id, ordinal),
    UNIQUE (dataset_id, column_name)
);
ALTER TABLE config.dataset_column ADD COLUMN IF NOT EXISTS classification TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS config.business_key (
    dataset_id BIGINT NOT NULL,
    key_kind TEXT NOT NULL CHECK (key_kind IN ('primary', 'dedup')),
    ordinal INT NOT NULL,
    column_name TEXT NOT NULL,
    PRIMARY KEY (dataset_id, key_kind, ordinal),
    FOREIGN KEY (dataset_id, column_name) REFERENCES config.dataset_column (dataset_id, column_name)
    ON DELETE CASCADE
);

-- --------------------------------------------------------------------------------------------------- runtime tables

CREATE TABLE IF NOT EXISTS runtime.config_snapshot (
    fingerprint TEXT PRIMARY KEY,
    config JSONB NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.run (
    run_id TEXT PRIMARY KEY,                     -- the trace id once tracing lands (W3)
    domain TEXT NOT NULL REFERENCES config.domain (domain),
    env TEXT NOT NULL,
    trigger TEXT NOT NULL,
    run_type TEXT NOT NULL CHECK (run_type IN ('normal', 'reload', 'backfill')),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'partial')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS runtime.dataset_run (
    seq BIGSERIAL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runtime.run (run_id),
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    config_fingerprint TEXT NOT NULL REFERENCES runtime.config_snapshot (fingerprint),
    watermark_before TEXT,
    watermark_after TEXT,
    counts JSONB NOT NULL DEFAULT '{}',
    message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS runtime.step_log (
    step_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_id BIGINT NOT NULL,
    step TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'skipped', 'warned')),
    counts JSONB NOT NULL DEFAULT '{}',
    message TEXT NOT NULL DEFAULT '',
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, dataset_id) REFERENCES runtime.dataset_run (run_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS runtime.file_log (
    file_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_id BIGINT NOT NULL,
    batch_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    delivery_window TEXT NOT NULL,
    checksum TEXT NOT NULL,
    declared_rows BIGINT,
    row_count BIGINT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, dataset_id) REFERENCES runtime.dataset_run (run_id, dataset_id)
);
CREATE INDEX IF NOT EXISTS ix_file_log_batch ON runtime.file_log (dataset_id, batch_key, file_id DESC);
-- B8 (15 Nov): a full copy is a rebase point for rebuilds and housekeeping; added in place on an older store
ALTER TABLE runtime.file_log ADD COLUMN IF NOT EXISTS load_type TEXT NOT NULL DEFAULT 'incremental'
    CHECK (load_type IN ('incremental', 'full', 'baseline'));

CREATE TABLE IF NOT EXISTS runtime.error_log (
    error_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runtime.run (run_id),
    dataset_id BIGINT REFERENCES config.dataset (dataset_id),
    step TEXT NOT NULL,
    error_class TEXT NOT NULL,
    message TEXT NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.watermark (
    dataset_id BIGINT PRIMARY KEY REFERENCES config.dataset (dataset_id),
    watermark_value TEXT,
    last_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.bookmark (
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    bookmark_key TEXT NOT NULL,                  -- a shard, a feed, a cursor name
    bookmark_value TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, bookmark_key)
);

CREATE TABLE IF NOT EXISTS runtime.watermark_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    old_value TEXT,
    new_value TEXT,
    reason TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Corrections (ADR-017, DPC-0082): a correction says an old value was wrong. It is a row here, never an edit of
-- silver; a backfill plan activates it on a date and names the windows the gold recompute rebuilds.
CREATE TABLE IF NOT EXISTS runtime.backfill_plan (
    plan_id TEXT PRIMARY KEY,
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    windows TEXT [] NOT NULL CHECK (cardinality(windows) > 0),
    activate_on DATE NOT NULL,
    reason TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.correction (
    correction_id BIGSERIAL PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES runtime.backfill_plan (plan_id),
    key_values JSONB NOT NULL,                   -- the business key: {column: value}
    field TEXT NOT NULL,
    value TEXT,
    was TEXT,
    applies_from DATE,
    applies_to DATE
);

-- Data quality (DPC-0010, DPC-0042, ADR-009): a dataset's rules, written by the loader from the contract, and what
-- each rule found in each dataset run. Severity sets the action; results are counts, never the failing rows.
CREATE TABLE IF NOT EXISTS config.dq_rule (
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    rule_id TEXT NOT NULL,
    column_name TEXT NOT NULL DEFAULT '',        -- empty: a rule on the whole row or batch
    check_name TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}',
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'major', 'minor')),
    action TEXT NOT NULL CHECK (action IN ('quarantine', 'stop', 'warn')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (dataset_id, rule_id)
);

CREATE TABLE IF NOT EXISTS runtime.dq_result (
    result_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runtime.run (run_id),
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    rule_id TEXT NOT NULL,
    checked BIGINT NOT NULL CHECK (checked >= 0),
    failed BIGINT NOT NULL CHECK (failed >= 0 AND failed <= checked),
    action TEXT NOT NULL CHECK (action IN ('quarantine', 'stop', 'warn')),
    detail TEXT NOT NULL DEFAULT '',
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (dataset_id, rule_id) REFERENCES config.dq_rule (dataset_id, rule_id)
);

-- Voided batches (ADR-025): a loaded batch that must not count. The file log stays append-only; a void is its own
-- audited row, and every read of a batch's status sees it.
CREATE TABLE IF NOT EXISTS runtime.batch_void (
    dataset_id BIGINT NOT NULL REFERENCES config.dataset (dataset_id),
    batch_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    approved_by TEXT NOT NULL,
    voided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, batch_key)
);

-- Profiles (DPC-0051, B9): one per dataset run that loaded something; the newest is what drift compares with.
CREATE TABLE IF NOT EXISTS runtime.dq_profile (
    profile_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_id BIGINT NOT NULL,
    profile JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, dataset_id) REFERENCES runtime.dataset_run (run_id, dataset_id)
);

-- Writer leases (ORCH-04): one row per table while a run writes it. A second run finds the table busy and fails
-- clearly; a lease past its expiry (a crashed run) is taken over. Not tied to a dataset: gold and shared tables too.
CREATE TABLE IF NOT EXISTS runtime.table_lease (
    table_name TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

REVOKE ALL ON ALL TABLES IN SCHEMA config, runtime FROM PUBLIC;

-- ------------------------------------------------------------------------------------------------- config functions

CREATE OR REPLACE FUNCTION config._dataset_id(p_source TEXT, p_dataset TEXT) RETURNS BIGINT
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE v BIGINT;
BEGIN
    SELECT d.dataset_id INTO v FROM config.dataset d JOIN config.source s USING (source_id)
    WHERE s.source_name = p_source AND d.dataset_name = p_dataset;
    IF v IS NULL THEN
        RAISE EXCEPTION 'dataset %.% is not loaded', p_source, p_dataset USING ERRCODE = 'DPM02';
    END IF;
    RETURN v;
END $$;

CREATE OR REPLACE FUNCTION config.register_settings(p_defs JSONB) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE r JSONB; n INT := 0;
BEGIN
    FOR r IN SELECT * FROM jsonb_array_elements(p_defs) LOOP
        IF NOT (r ->> 'scope') = ANY (ARRAY['platform', 'source', 'dataset']) THEN
            RAISE EXCEPTION 'scope % is not platform, source or dataset', r ->> 'scope' USING ERRCODE = 'DPM03';
        END IF;
        INSERT INTO config.setting_def (scope, key, value_type, default_value, allowed, description, status)
        VALUES (r ->> 'scope', r ->> 'key', r ->> 'value_type', r ->> 'default',
                CASE WHEN jsonb_typeof(r -> 'allowed') = 'array'
                     THEN ARRAY(SELECT jsonb_array_elements_text(r -> 'allowed')) END,
                COALESCE(r ->> 'description', ''), COALESCE(r ->> 'status', 'registered'))
        ON CONFLICT (scope, key) DO UPDATE SET
            value_type = EXCLUDED.value_type, default_value = EXCLUDED.default_value, allowed = EXCLUDED.allowed,
            description = EXCLUDED.description, status = EXCLUDED.status;
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

CREATE OR REPLACE FUNCTION config.load_domain(p_doc JSONB) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE k TEXT; v TEXT; sc TEXT;
BEGIN
    INSERT INTO config.domain (domain, business_timezone, environments, manifest, fingerprint, loaded_at)
    VALUES (p_doc ->> 'domain', p_doc ->> 'business_timezone',
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(p_doc -> 'environments', '[]'))),
            p_doc -> 'manifest', p_doc ->> 'fingerprint', now())
    ON CONFLICT (domain) DO UPDATE SET
        business_timezone = EXCLUDED.business_timezone, environments = EXCLUDED.environments,
        manifest = EXCLUDED.manifest, fingerprint = EXCLUDED.fingerprint, loaded_at = now();
    DELETE FROM config.domain_setting WHERE domain = p_doc ->> 'domain';
    FOR k, v IN SELECT * FROM jsonb_each_text(COALESCE(p_doc -> 'settings', '{}')) LOOP
        SELECT scope INTO sc FROM config.setting_def WHERE key = k AND scope IN ('dataset', 'platform')
        ORDER BY scope LIMIT 1;
        IF sc IS NULL THEN
            RAISE EXCEPTION 'setting ''%'' is not registered', k USING ERRCODE = 'DPM01';
        END IF;
        INSERT INTO config.domain_setting (domain, scope, key, value) VALUES (p_doc ->> 'domain', sc, k, v);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION config.load_dataset(p_doc JSONB) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE src BIGINT; ds BIGINT; k TEXT; v TEXT; kind TEXT; cols JSONB; i INT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM config.domain WHERE domain = p_doc ->> 'domain') THEN
        RAISE EXCEPTION 'domain % is not loaded', p_doc ->> 'domain' USING ERRCODE = 'DPM02';
    END IF;
    FOR k IN SELECT jsonb_object_keys(COALESCE(p_doc -> 'settings', '{}')) LOOP
        IF NOT EXISTS (SELECT 1 FROM config.setting_def WHERE scope = 'dataset' AND key = k) THEN
            RAISE EXCEPTION 'setting ''%'' is not registered', k USING ERRCODE = 'DPM01';
        END IF;
    END LOOP;
    FOR kind, cols IN SELECT * FROM jsonb_each(COALESCE(p_doc -> 'business_keys', '{}')) LOOP
        IF kind NOT IN ('primary', 'dedup') THEN
            RAISE EXCEPTION 'business key kind % is not primary or dedup', kind USING ERRCODE = 'DPM03';
        END IF;
        IF EXISTS (SELECT 1 FROM jsonb_array_elements_text(cols) c
                   WHERE c NOT IN (SELECT x ->> 'name' FROM jsonb_array_elements(p_doc -> 'columns') x)) THEN
            RAISE EXCEPTION 'business key % names a column the dataset does not have', kind USING ERRCODE = 'DPM03';
        END IF;
    END LOOP;

    INSERT INTO config.source (domain, source_name, source_env)
    VALUES (p_doc ->> 'domain', p_doc ->> 'source_name', COALESCE(p_doc ->> 'source_env', ''))
    ON CONFLICT (source_name) DO UPDATE SET source_env = EXCLUDED.source_env
    RETURNING source_id INTO src;

    INSERT INTO config.dataset (source_id, dataset_name, is_enabled, kind, archetypes, ingest_mode, ingest_read,
                                history_kind, load_pattern, contract_id, contract_version, manifest, fingerprint)
    VALUES (src, p_doc ->> 'dataset_name', (p_doc ->> 'is_enabled')::BOOLEAN, p_doc ->> 'kind',
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(p_doc -> 'archetypes', '[]'))),
            COALESCE(p_doc ->> 'ingest_mode', ''), COALESCE(p_doc ->> 'ingest_read', ''),
            COALESCE(p_doc ->> 'history_kind', ''), COALESCE(p_doc ->> 'load_pattern', ''),
            p_doc ->> 'contract_id', p_doc ->> 'contract_version', p_doc -> 'manifest', p_doc ->> 'fingerprint')
    ON CONFLICT (source_id, dataset_name) DO UPDATE SET
        is_enabled = EXCLUDED.is_enabled, kind = EXCLUDED.kind, archetypes = EXCLUDED.archetypes,
        ingest_mode = EXCLUDED.ingest_mode, ingest_read = EXCLUDED.ingest_read,
        history_kind = EXCLUDED.history_kind, load_pattern = EXCLUDED.load_pattern,
        contract_id = EXCLUDED.contract_id, contract_version = EXCLUDED.contract_version,
        manifest = EXCLUDED.manifest, fingerprint = EXCLUDED.fingerprint, loaded_at = now()
    RETURNING dataset_id INTO ds;

    -- the dataset's settings, columns and keys are replaced whole, in this transaction
    DELETE FROM config.business_key WHERE dataset_id = ds;
    DELETE FROM config.dataset_column WHERE dataset_id = ds;
    DELETE FROM config.dataset_setting WHERE dataset_id = ds;
    FOR k, v IN SELECT * FROM jsonb_each_text(COALESCE(p_doc -> 'settings', '{}')) LOOP
        INSERT INTO config.dataset_setting (dataset_id, key, value) VALUES (ds, k, v);
    END LOOP;
    INSERT INTO config.dataset_column (dataset_id, ordinal, column_name, logical_type, is_required,
                                       primary_key_position, description, classification)
    SELECT ds, c.ord, c.x ->> 'name', c.x ->> 'logical_type', COALESCE((c.x ->> 'required')::BOOLEAN, FALSE),
           (c.x ->> 'primary_key_position')::INT, COALESCE(c.x ->> 'description', ''),
           COALESCE(c.x ->> 'classification', '')
    FROM jsonb_array_elements(COALESCE(p_doc -> 'columns', '[]')) WITH ORDINALITY AS c (x, ord);
    FOR kind, cols IN SELECT * FROM jsonb_each(COALESCE(p_doc -> 'business_keys', '{}')) LOOP
        i := 0;
        FOR k IN SELECT jsonb_array_elements_text(cols) LOOP
            i := i + 1;
            INSERT INTO config.business_key (dataset_id, key_kind, ordinal, column_name) VALUES (ds, kind, i, k);
        END LOOP;
    END LOOP;
    RETURN ds;
END $$;

CREATE OR REPLACE FUNCTION config.settings_registry()
RETURNS TABLE (scope TEXT, key TEXT, value_type TEXT, default_value TEXT, allowed TEXT [], description TEXT,
               status TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT scope, key, value_type, default_value, allowed, description, status FROM config.setting_def
    ORDER BY scope COLLATE "C", key COLLATE "C"
$$;

CREATE OR REPLACE FUNCTION config.list_datasets(p_domain TEXT)
RETURNS TABLE (domain TEXT, source_name TEXT, dataset_name TEXT, is_enabled BOOLEAN, kind TEXT, ingest_mode TEXT,
               fingerprint TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT s.domain, s.source_name, d.dataset_name, d.is_enabled, d.kind, d.ingest_mode, d.fingerprint
    FROM config.dataset d JOIN config.source s USING (source_id)
    WHERE s.domain = p_domain
    ORDER BY s.source_name COLLATE "C", d.dataset_name COLLATE "C"
$$;

-- The merged view (DPC-0002): every registered dataset key, the narrowest value winning, with where it came from.
CREATE OR REPLACE FUNCTION config.dataset_settings(p_source TEXT, p_dataset TEXT)
RETURNS TABLE (key TEXT, value TEXT, origin TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY
    SELECT r.key, COALESCE(dv.value, gv.value, r.default_value),
           CASE WHEN dv.value IS NOT NULL THEN 'dataset' WHEN gv.value IS NOT NULL THEN 'domain' ELSE 'default' END
    FROM config.setting_def r
    LEFT JOIN config.dataset_setting dv ON dv.dataset_id = ds AND dv.key = r.key
    LEFT JOIN config.domain_setting gv ON gv.key = r.key AND gv.domain = (
        SELECT s.domain FROM config.dataset d JOIN config.source s USING (source_id) WHERE d.dataset_id = ds)
    WHERE r.scope = 'dataset'
    ORDER BY r.key COLLATE "C";
END $$;

DROP FUNCTION IF EXISTS config.dataset_columns(TEXT, TEXT);  -- W1b added classification to its result
CREATE OR REPLACE FUNCTION config.dataset_columns(p_source TEXT, p_dataset TEXT)
RETURNS TABLE (column_name TEXT, logical_type TEXT, is_required BOOLEAN, primary_key_position INT, description TEXT,
               classification TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT c.column_name, c.logical_type, c.is_required, c.primary_key_position, c.description,
                        c.classification
    FROM config.dataset_column c WHERE c.dataset_id = ds ORDER BY c.ordinal;
END $$;

CREATE OR REPLACE FUNCTION config.business_keys(p_source TEXT, p_dataset TEXT, p_kind TEXT)
RETURNS TABLE (column_name TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    IF p_kind NOT IN ('primary', 'dedup') THEN
        RAISE EXCEPTION 'business key kind % is not primary or dedup', p_kind USING ERRCODE = 'DPM03';
    END IF;
    RETURN QUERY SELECT b.column_name FROM config.business_key b
    WHERE b.dataset_id = ds AND b.key_kind = p_kind ORDER BY b.ordinal;
END $$;

CREATE OR REPLACE FUNCTION config.dataset_view(p_source TEXT, p_dataset TEXT) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset); out JSONB;
BEGIN
    SELECT jsonb_build_object(
        'domain', s.domain, 'source', s.source_name, 'dataset', d.dataset_name, 'source_env', s.source_env,
        'is_enabled', d.is_enabled, 'kind', d.kind, 'archetypes', to_jsonb(d.archetypes),
        'ingest_mode', d.ingest_mode, 'ingest_read', d.ingest_read, 'history_kind', d.history_kind,
        'load_pattern', d.load_pattern, 'contract_id', d.contract_id, 'contract_version', d.contract_version,
        'fingerprint', d.fingerprint,
        'settings', (SELECT COALESCE(jsonb_object_agg(m.key, m.value), '{}')
                     FROM config.dataset_settings(p_source, p_dataset) m WHERE m.value IS NOT NULL),
        'origins', (SELECT COALESCE(jsonb_object_agg(m.key, m.origin), '{}')
                    FROM config.dataset_settings(p_source, p_dataset) m WHERE m.value IS NOT NULL),
        'columns', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'name', c.column_name, 'logical_type', c.logical_type, 'required', c.is_required,
                        'primary_key_position', c.primary_key_position, 'description', c.description,
                        'classification', c.classification)
                        ORDER BY c.ordinal), '[]')
                    FROM config.dataset_column c WHERE c.dataset_id = ds),
        'business_keys', jsonb_build_object(
            'primary', (SELECT COALESCE(jsonb_agg(b.column_name ORDER BY b.ordinal), '[]') FROM config.business_key b
                        WHERE b.dataset_id = ds AND b.key_kind = 'primary'),
            'dedup', (SELECT COALESCE(jsonb_agg(b.column_name ORDER BY b.ordinal), '[]') FROM config.business_key b
                      WHERE b.dataset_id = ds AND b.key_kind = 'dedup')),
        'manifest', d.manifest)
    INTO out
    FROM config.dataset d JOIN config.source s USING (source_id) WHERE d.dataset_id = ds;
    RETURN out;
END $$;

-- ------------------------------------------------------------------------------------------------ runtime functions

-- Secrets out, then capped at 4000 characters (kept in step with dataplatform.meta.port.redact).
CREATE OR REPLACE FUNCTION runtime._redact(p_message TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT left(regexp_replace(
        regexp_replace(COALESCE(p_message, ''),
                       '(password|pwd|accountkey|sharedaccesskey|sig|token|secret)=[^;&[:space:]]+', '\1=***', 'gi'),
        'bearer[[:space:]]+[A-Za-z0-9._~+/=-]+', 'Bearer ***', 'gi'), 4000)
$$;

CREATE OR REPLACE FUNCTION runtime._check_run(p_run_id TEXT) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE st TEXT;
BEGIN
    SELECT status INTO st FROM runtime.run WHERE run_id = p_run_id;
    IF st IS NULL THEN
        RAISE EXCEPTION 'run % was never started', p_run_id USING ERRCODE = 'DPM02';
    END IF;
    RETURN st;
END $$;

-- The dataset id of a dataset run that exists (the run must exist, the dataset must have started in it).
CREATE OR REPLACE FUNCTION runtime._dataset_run(p_run_id TEXT, p_source TEXT, p_dataset TEXT) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT;
BEGIN
    PERFORM runtime._check_run(p_run_id);
    ds := config._dataset_id(p_source, p_dataset);
    IF NOT EXISTS (SELECT 1 FROM runtime.dataset_run WHERE run_id = p_run_id AND dataset_id = ds) THEN
        RAISE EXCEPTION '%.% was not started in run %', p_source, p_dataset, p_run_id USING ERRCODE = 'DPM04';
    END IF;
    RETURN ds;
END $$;

CREATE OR REPLACE FUNCTION runtime.start_run(p_run_id TEXT, p_domain TEXT, p_env TEXT, p_trigger TEXT,
                                             p_run_type TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF p_run_type NOT IN ('normal', 'reload', 'backfill') THEN
        RAISE EXCEPTION 'run type % is not normal, reload or backfill', p_run_type USING ERRCODE = 'DPM03';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM config.domain WHERE domain = p_domain) THEN
        RAISE EXCEPTION 'domain % is not loaded', p_domain USING ERRCODE = 'DPM02';
    END IF;
    IF EXISTS (SELECT 1 FROM runtime.run WHERE run_id = p_run_id) THEN
        RAISE EXCEPTION 'run % was already started', p_run_id USING ERRCODE = 'DPM04';
    END IF;
    INSERT INTO runtime.run (run_id, domain, env, trigger, run_type, status)
    VALUES (p_run_id, p_domain, p_env, p_trigger, p_run_type, 'running');
END $$;

CREATE OR REPLACE FUNCTION runtime.start_dataset_run(p_run_id TEXT, p_source TEXT, p_dataset TEXT,
                                                     p_fingerprint TEXT, p_config JSONB) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT; before TEXT;
BEGIN
    IF runtime._check_run(p_run_id) <> 'running' THEN
        RAISE EXCEPTION 'run % is already finished', p_run_id USING ERRCODE = 'DPM04';
    END IF;
    ds := config._dataset_id(p_source, p_dataset);
    IF EXISTS (SELECT 1 FROM runtime.dataset_run WHERE run_id = p_run_id AND dataset_id = ds) THEN
        RAISE EXCEPTION '%.% was already started in run %', p_source, p_dataset, p_run_id USING ERRCODE = 'DPM04';
    END IF;
    INSERT INTO runtime.config_snapshot (fingerprint, config) VALUES (p_fingerprint, p_config)
    ON CONFLICT (fingerprint) DO NOTHING;
    SELECT watermark_value INTO before FROM runtime.watermark WHERE dataset_id = ds;
    INSERT INTO runtime.dataset_run (run_id, dataset_id, status, config_fingerprint, watermark_before)
    VALUES (p_run_id, ds, 'running', p_fingerprint, before);
    RETURN before;
END $$;

CREATE OR REPLACE FUNCTION runtime.log_step(p_run_id TEXT, p_source TEXT, p_dataset TEXT, p_step TEXT,
                                            p_status TEXT, p_counts JSONB, p_message TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT;
BEGIN
    IF p_status NOT IN ('succeeded', 'failed', 'skipped', 'warned') THEN
        RAISE EXCEPTION 'step status % is not succeeded, failed, skipped or warned', p_status USING ERRCODE = 'DPM03';
    END IF;
    ds := runtime._dataset_run(p_run_id, p_source, p_dataset);
    INSERT INTO runtime.step_log (run_id, dataset_id, step, status, counts, message)
    VALUES (p_run_id, ds, p_step, p_status, COALESCE(p_counts, '{}'), runtime._redact(p_message));
END $$;

DROP FUNCTION IF EXISTS runtime.log_file(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION runtime.log_file(p_run_id TEXT, p_source TEXT, p_dataset TEXT, p_batch_key TEXT,
                                            p_file_name TEXT, p_window TEXT, p_checksum TEXT,
                                            p_declared_rows BIGINT, p_rows BIGINT, p_status TEXT,
                                            p_reason TEXT, p_load_type TEXT DEFAULT 'incremental') RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := runtime._dataset_run(p_run_id, p_source, p_dataset);
BEGIN
    IF p_status NOT IN ('loaded', 'skipped_duplicate', 'rejected_contract', 'rejected_volume', 'held_mass_delete') THEN
        RAISE EXCEPTION 'file status % is not a lander outcome', p_status USING ERRCODE = 'DPM03';
    END IF;
    IF p_load_type NOT IN ('incremental', 'full', 'baseline') THEN
        RAISE EXCEPTION 'load type % is not incremental, full or baseline', p_load_type USING ERRCODE = 'DPM03';
    END IF;
    INSERT INTO runtime.file_log (run_id, dataset_id, batch_key, file_name, delivery_window, checksum,
                                  declared_rows, row_count, status, reason, load_type)
    VALUES (p_run_id, ds, p_batch_key, p_file_name, p_window, p_checksum, p_declared_rows, p_rows, p_status,
            runtime._redact(p_reason), p_load_type);
END $$;

CREATE OR REPLACE FUNCTION runtime.batch_status(p_source TEXT, p_dataset TEXT, p_batch_key TEXT) RETURNS TEXT
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    IF EXISTS (SELECT 1 FROM runtime.batch_void v WHERE v.dataset_id = ds AND v.batch_key = p_batch_key) THEN
        RETURN 'void';
    END IF;
    RETURN (SELECT f.status FROM runtime.file_log f WHERE f.dataset_id = ds AND f.batch_key = p_batch_key
            ORDER BY f.file_id DESC LIMIT 1);
END $$;

CREATE OR REPLACE FUNCTION runtime.void_batch(p_source TEXT, p_dataset TEXT, p_batch_key TEXT, p_reason TEXT,
                                              p_approved_by TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset); st TEXT;
BEGIN
    IF length(btrim(COALESCE(p_reason, ''))) < 10 OR btrim(COALESCE(p_approved_by, '')) = '' THEN
        RAISE EXCEPTION 'voiding a batch needs who approved it and a reason of at least 10 characters'
            USING ERRCODE = 'DPM03';
    END IF;
    st := runtime.batch_status(p_source, p_dataset, p_batch_key);
    IF st = 'void' THEN
        RAISE EXCEPTION 'batch % is already void', p_batch_key USING ERRCODE = 'DPM04';
    END IF;
    IF st IS DISTINCT FROM 'loaded' THEN
        RAISE EXCEPTION 'batch % never loaded (status %); only a loaded batch is voided', p_batch_key, st
            USING ERRCODE = 'DPM03';
    END IF;
    INSERT INTO runtime.batch_void (dataset_id, batch_key, reason, approved_by)
    VALUES (ds, p_batch_key, p_reason, p_approved_by);
END $$;

CREATE OR REPLACE FUNCTION runtime.log_error(p_run_id TEXT, p_source TEXT, p_dataset TEXT, p_step TEXT,
                                             p_error_class TEXT, p_message TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM runtime._check_run(p_run_id);
    INSERT INTO runtime.error_log (run_id, dataset_id, step, error_class, message)
    VALUES (p_run_id,
            CASE WHEN p_source IS NULL OR p_dataset IS NULL THEN NULL
                 ELSE config._dataset_id(p_source, p_dataset) END,
            p_step, p_error_class, runtime._redact(p_message));
END $$;

-- Close a dataset run. The watermark and bookmarks move only on success, in this same transaction as the run record
-- (kept from sp_run_log_finished + sp_advance_watermark, now one call so they cannot drift apart).
CREATE OR REPLACE FUNCTION runtime.finish_dataset_run(p_run_id TEXT, p_source TEXT, p_dataset TEXT, p_status TEXT,
                                                      p_counts JSONB, p_watermark TEXT, p_bookmarks JSONB,
                                                      p_message TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT; st TEXT; after TEXT;
BEGIN
    IF p_status NOT IN ('succeeded', 'failed', 'skipped') THEN
        RAISE EXCEPTION 'dataset run status % is not succeeded, failed or skipped', p_status USING ERRCODE = 'DPM03';
    END IF;
    ds := runtime._dataset_run(p_run_id, p_source, p_dataset);
    SELECT status INTO st FROM runtime.dataset_run WHERE run_id = p_run_id AND dataset_id = ds FOR UPDATE;
    IF st <> 'running' THEN
        RAISE EXCEPTION '%.% in run % is already %', p_source, p_dataset, p_run_id, st USING ERRCODE = 'DPM04';
    END IF;
    IF p_status = 'succeeded' THEN
        IF p_watermark IS NOT NULL THEN
            INSERT INTO runtime.watermark (dataset_id, watermark_value, last_run_id, updated_at)
            VALUES (ds, p_watermark, p_run_id, now())
            ON CONFLICT (dataset_id) DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value, last_run_id = EXCLUDED.last_run_id, updated_at = now();
        END IF;
        INSERT INTO runtime.bookmark (dataset_id, bookmark_key, bookmark_value, last_run_id)
        SELECT ds, b.key, b.value, p_run_id FROM jsonb_each_text(COALESCE(p_bookmarks, '{}')) b
        ON CONFLICT (dataset_id, bookmark_key) DO UPDATE SET
            bookmark_value = EXCLUDED.bookmark_value, last_run_id = EXCLUDED.last_run_id, updated_at = now();
    END IF;
    SELECT watermark_value INTO after FROM runtime.watermark WHERE dataset_id = ds;
    UPDATE runtime.dataset_run SET status = p_status, counts = COALESCE(p_counts, '{}'), watermark_after = after,
        message = runtime._redact(p_message), ended_at = now()
    WHERE run_id = p_run_id AND dataset_id = ds;
END $$;

-- Close a run; its status follows its dataset runs (kept from sp_master_run_log_finished).
CREATE OR REPLACE FUNCTION runtime.finish_run(p_run_id TEXT) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE st TEXT; n INT; failed INT; running INT; final TEXT;
BEGIN
    PERFORM runtime._check_run(p_run_id);
    SELECT status INTO st FROM runtime.run WHERE run_id = p_run_id FOR UPDATE;
    IF st <> 'running' THEN
        RAISE EXCEPTION 'run % is already %', p_run_id, st USING ERRCODE = 'DPM04';
    END IF;
    SELECT count(*), count(*) FILTER (WHERE status = 'failed'), count(*) FILTER (WHERE status = 'running')
    INTO n, failed, running FROM runtime.dataset_run WHERE run_id = p_run_id;
    IF running > 0 THEN
        RAISE EXCEPTION 'run % still has datasets running', p_run_id USING ERRCODE = 'DPM04';
    END IF;
    final := CASE WHEN n = 0 THEN 'failed' WHEN failed = 0 THEN 'succeeded' WHEN failed = n THEN 'failed'
                  ELSE 'partial' END;
    UPDATE runtime.run SET status = final, ended_at = now() WHERE run_id = p_run_id;
    RETURN final;
END $$;

CREATE OR REPLACE FUNCTION runtime.get_watermark(p_source TEXT, p_dataset TEXT) RETURNS TEXT
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN (SELECT w.watermark_value FROM runtime.watermark w WHERE w.dataset_id = ds);
END $$;

CREATE OR REPLACE FUNCTION runtime.get_bookmarks(p_source TEXT, p_dataset TEXT)
RETURNS TABLE (bookmark_key TEXT, bookmark_value TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT b.bookmark_key, b.bookmark_value FROM runtime.bookmark b WHERE b.dataset_id = ds
    ORDER BY b.bookmark_key COLLATE "C";
END $$;

-- An operator reset (backfill, correction): audited with who and why (kept from watermark_audit's rules).
CREATE OR REPLACE FUNCTION runtime.reset_watermark(p_source TEXT, p_dataset TEXT, p_value TEXT, p_reason TEXT,
                                                   p_changed_by TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset); old TEXT;
BEGIN
    IF length(btrim(COALESCE(p_reason, ''))) < 10 THEN
        RAISE EXCEPTION 'a watermark reset needs a reason of at least 10 characters' USING ERRCODE = 'DPM03';
    END IF;
    IF btrim(COALESCE(p_changed_by, '')) = '' THEN
        RAISE EXCEPTION 'a watermark reset needs who made it' USING ERRCODE = 'DPM03';
    END IF;
    SELECT watermark_value INTO old FROM runtime.watermark WHERE dataset_id = ds FOR UPDATE;
    INSERT INTO runtime.watermark_audit (dataset_id, old_value, new_value, reason, changed_by)
    VALUES (ds, old, p_value, p_reason, p_changed_by);
    INSERT INTO runtime.watermark (dataset_id, watermark_value, last_run_id, updated_at)
    VALUES (ds, p_value, NULL, now())
    ON CONFLICT (dataset_id) DO UPDATE SET watermark_value = EXCLUDED.watermark_value, updated_at = now();
END $$;

-- Everything recorded for one run, for the evidence view and the alert triage agent.
CREATE OR REPLACE FUNCTION runtime.run_report(p_run_id TEXT) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM runtime._check_run(p_run_id);
    RETURN jsonb_build_object(
        'run', (SELECT jsonb_build_object('run_id', r.run_id, 'domain', r.domain, 'env', r.env, 'trigger', r.trigger,
                                          'run_type', r.run_type, 'status', r.status)
                FROM runtime.run r WHERE r.run_id = p_run_id),
        'datasets', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'source', s.source_name, 'dataset', d.dataset_name, 'status', x.status,
                        'config_fingerprint', x.config_fingerprint, 'watermark_before', x.watermark_before,
                        'watermark_after', x.watermark_after, 'counts', x.counts, 'message', x.message)
                        ORDER BY x.seq), '[]')
                     FROM runtime.dataset_run x JOIN config.dataset d USING (dataset_id)
                     JOIN config.source s USING (source_id) WHERE x.run_id = p_run_id),
        'steps', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                     'source', s.source_name, 'dataset', d.dataset_name, 'step', x.step, 'status', x.status,
                     'counts', x.counts, 'message', x.message) ORDER BY x.step_id), '[]')
                  FROM runtime.step_log x JOIN config.dataset d USING (dataset_id)
                  JOIN config.source s USING (source_id) WHERE x.run_id = p_run_id),
        'files', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                     'source', s.source_name, 'dataset', d.dataset_name, 'batch_key', x.batch_key,
                     'file_name', x.file_name, 'window', x.delivery_window, 'checksum', x.checksum,
                     'declared_rows', x.declared_rows, 'rows', x.row_count, 'status', x.status,
                     'reason', x.reason, 'load_type', x.load_type) ORDER BY x.file_id), '[]')
                  FROM runtime.file_log x JOIN config.dataset d USING (dataset_id)
                  JOIN config.source s USING (source_id) WHERE x.run_id = p_run_id),
        'errors', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                      'source', s.source_name, 'dataset', d.dataset_name, 'step', x.step,
                      'error_class', x.error_class, 'message', x.message) ORDER BY x.error_id), '[]')
                   FROM runtime.error_log x LEFT JOIN config.dataset d USING (dataset_id)
                   LEFT JOIN config.source s USING (source_id) WHERE x.run_id = p_run_id));
END $$;

-- Every batch recorded for a dataset, optionally one window, oldest first (the lander's "already handled" list and
-- where a reload finds a window's batches again).
DROP FUNCTION IF EXISTS runtime.dataset_files(TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION runtime.dataset_files(p_source TEXT, p_dataset TEXT, p_window TEXT)
RETURNS TABLE (run_id TEXT, batch_key TEXT, file_name TEXT, delivery_window TEXT, checksum TEXT,
               declared_rows BIGINT, row_count BIGINT, status TEXT, reason TEXT, load_type TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT f.run_id, f.batch_key, f.file_name, f.delivery_window, f.checksum, f.declared_rows,
                        f.row_count, CASE WHEN v.batch_key IS NULL THEN f.status ELSE 'void' END, f.reason,
                        f.load_type
                 FROM runtime.file_log f
                 LEFT JOIN runtime.batch_void v ON v.dataset_id = f.dataset_id AND v.batch_key = f.batch_key
                 WHERE f.dataset_id = ds AND (p_window IS NULL OR f.delivery_window = p_window)
                 ORDER BY f.file_id;
END $$;

-- Record a backfill plan and its corrections in one call. Refused: a plan id seen before, no reason or no author,
-- no windows, a key that is not the dataset's primary key, a field that is not one of its columns.
CREATE OR REPLACE FUNCTION runtime.record_corrections(p_plan JSONB, p_rows JSONB) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_plan ->> 'source', p_plan ->> 'dataset'); pk TEXT []; r JSONB; n INT := 0;
BEGIN
    IF EXISTS (SELECT 1 FROM runtime.backfill_plan WHERE plan_id = p_plan ->> 'plan_id') THEN
        RAISE EXCEPTION 'backfill plan % was already recorded', p_plan ->> 'plan_id' USING ERRCODE = 'DPM04';
    END IF;
    IF length(btrim(COALESCE(p_plan ->> 'reason', ''))) < 10 OR btrim(COALESCE(p_plan ->> 'recorded_by', '')) = '' THEN
        RAISE EXCEPTION 'a backfill plan needs who recorded it and a reason of at least 10 characters'
            USING ERRCODE = 'DPM03';
    END IF;
    IF jsonb_array_length(COALESCE(p_plan -> 'windows', '[]')) = 0 THEN
        RAISE EXCEPTION 'a backfill plan names the windows it rebuilds' USING ERRCODE = 'DPM03';
    END IF;
    SELECT array_agg(column_name ORDER BY column_name COLLATE "C") INTO pk
    FROM config.business_key WHERE dataset_id = ds AND key_kind = 'primary';
    INSERT INTO runtime.backfill_plan (plan_id, dataset_id, windows, activate_on, reason, recorded_by)
    VALUES (p_plan ->> 'plan_id', ds, ARRAY(SELECT jsonb_array_elements_text(p_plan -> 'windows')),
            (p_plan ->> 'activate_on')::DATE, p_plan ->> 'reason', p_plan ->> 'recorded_by');
    FOR r IN SELECT * FROM jsonb_array_elements(COALESCE(p_rows, '[]')) LOOP
        IF ARRAY(SELECT k FROM jsonb_object_keys(r -> 'key') k ORDER BY k COLLATE "C") IS DISTINCT FROM pk THEN
            RAISE EXCEPTION 'a correction''s key must be the primary key %', pk USING ERRCODE = 'DPM03';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM config.dataset_column WHERE dataset_id = ds AND column_name = r ->> 'field') THEN
            RAISE EXCEPTION '% is not a column of the dataset', r ->> 'field' USING ERRCODE = 'DPM03';
        END IF;
        INSERT INTO runtime.correction (plan_id, key_values, field, value, was, applies_from, applies_to)
        VALUES (p_plan ->> 'plan_id', r -> 'key', r ->> 'field', r ->> 'value', r ->> 'was',
                (r ->> 'applies_from')::DATE, (r ->> 'applies_to')::DATE);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

-- A dataset's corrections with their status on a business date: active once the plan's date is reached.
CREATE OR REPLACE FUNCTION runtime.corrections(p_source TEXT, p_dataset TEXT, p_as_of DATE)
RETURNS TABLE (key_values JSONB, field TEXT, value TEXT, was TEXT, applies_from TEXT, applies_to TEXT,
               plan_id TEXT, status TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT c.key_values, c.field, c.value, c.was, c.applies_from::TEXT, c.applies_to::TEXT, c.plan_id,
                        CASE WHEN p_as_of IS NOT NULL AND p_as_of >= p.activate_on THEN 'active' ELSE 'recorded' END
                 FROM runtime.correction c JOIN runtime.backfill_plan p USING (plan_id)
                 WHERE p.dataset_id = ds ORDER BY c.correction_id;
END $$;

CREATE OR REPLACE FUNCTION runtime.backfill_plans(p_source TEXT, p_dataset TEXT)
RETURNS TABLE (plan_id TEXT, windows TEXT [], activate_on TEXT, reason TEXT, recorded_by TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT b.plan_id, b.windows, b.activate_on::TEXT, b.reason, b.recorded_by
                 FROM runtime.backfill_plan b WHERE b.dataset_id = ds ORDER BY b.plan_id COLLATE "C";
END $$;

-- ------------------------------------------------------------------------------------------------- data quality

-- Replace a dataset's rules in one call. Refused: a rule id twice, an empty id, a column the dataset does not have,
-- an unknown severity or action. Results already logged keep their rules: a rule with results is updated, not
-- dropped (the foreign key refuses the delete), so the history of what a rule found stays readable.
CREATE OR REPLACE FUNCTION config.set_dq_rules(p_source TEXT, p_dataset TEXT, p_rules JSONB) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset); r JSONB; n INT := 0;
BEGIN
    IF (SELECT count(*) FROM jsonb_array_elements(COALESCE(p_rules, '[]')))
       <> (SELECT count(DISTINCT x ->> 'rule_id') FROM jsonb_array_elements(COALESCE(p_rules, '[]')) x) THEN
        RAISE EXCEPTION 'a rule id appears twice' USING ERRCODE = 'DPM03';
    END IF;
    FOR r IN SELECT * FROM jsonb_array_elements(COALESCE(p_rules, '[]')) LOOP
        IF btrim(COALESCE(r ->> 'rule_id', '')) = '' THEN
            RAISE EXCEPTION 'a rule needs an id' USING ERRCODE = 'DPM03';
        END IF;
        IF COALESCE(r ->> 'column', '') <> '' AND NOT EXISTS (
            SELECT 1 FROM config.dataset_column WHERE dataset_id = ds AND column_name = r ->> 'column') THEN
            RAISE EXCEPTION '% is not a column of %.%', r ->> 'column', p_source, p_dataset USING ERRCODE = 'DPM03';
        END IF;
        IF r ->> 'severity' NOT IN ('critical', 'major', 'minor') OR r ->> 'action' NOT IN ('quarantine', 'stop', 'warn') THEN
            RAISE EXCEPTION 'rule %: severity % or action % unknown', r ->> 'rule_id', r ->> 'severity', r ->> 'action'
                USING ERRCODE = 'DPM03';
        END IF;
    END LOOP;
    DELETE FROM config.dq_rule q WHERE q.dataset_id = ds
        AND q.rule_id NOT IN (SELECT x ->> 'rule_id' FROM jsonb_array_elements(COALESCE(p_rules, '[]')) x)
        AND NOT EXISTS (SELECT 1 FROM runtime.dq_result d WHERE d.dataset_id = ds AND d.rule_id = q.rule_id);
    FOR r IN SELECT * FROM jsonb_array_elements(COALESCE(p_rules, '[]')) LOOP
        INSERT INTO config.dq_rule (dataset_id, rule_id, column_name, check_name, params, severity, action, enabled)
        VALUES (ds, r ->> 'rule_id', COALESCE(r ->> 'column', ''), r ->> 'check', COALESCE(r -> 'params', '{}'),
                r ->> 'severity', r ->> 'action', COALESCE((r ->> 'enabled')::BOOLEAN, TRUE))
        ON CONFLICT (dataset_id, rule_id) DO UPDATE SET column_name = EXCLUDED.column_name,
            check_name = EXCLUDED.check_name, params = EXCLUDED.params, severity = EXCLUDED.severity,
            action = EXCLUDED.action, enabled = EXCLUDED.enabled;
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

CREATE OR REPLACE FUNCTION config.dq_rules(p_source TEXT, p_dataset TEXT)
RETURNS TABLE (rule_id TEXT, column_name TEXT, check_name TEXT, params JSONB, severity TEXT, action TEXT,
               enabled BOOLEAN)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN QUERY SELECT q.rule_id, q.column_name, q.check_name, q.params, q.severity, q.action, q.enabled
                 FROM config.dq_rule q WHERE q.dataset_id = ds ORDER BY q.rule_id COLLATE "C";
END $$;

-- Log what rules found, all rows or none. Refused: a dataset run never started, a rule the dataset does not have,
-- impossible counts, an unknown action.
CREATE OR REPLACE FUNCTION runtime.log_dq_results(p_rows JSONB) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE r JSONB; ds BIGINT; n INT := 0;
BEGIN
    FOR r IN SELECT * FROM jsonb_array_elements(COALESCE(p_rows, '[]')) LOOP
        ds := runtime._dataset_run(r ->> 'run_id', r ->> 'source', r ->> 'dataset');
        IF NOT EXISTS (SELECT 1 FROM config.dq_rule WHERE dataset_id = ds AND rule_id = r ->> 'rule_id') THEN
            RAISE EXCEPTION '% is not a rule of %.%', r ->> 'rule_id', r ->> 'source', r ->> 'dataset'
                USING ERRCODE = 'DPM03';
        END IF;
        IF (r ->> 'checked')::BIGINT < 0 OR (r ->> 'failed')::BIGINT < 0
           OR (r ->> 'failed')::BIGINT > (r ->> 'checked')::BIGINT THEN
            RAISE EXCEPTION 'rule %: failed % of % is not possible', r ->> 'rule_id', r ->> 'failed', r ->> 'checked'
                USING ERRCODE = 'DPM03';
        END IF;
        IF r ->> 'action' NOT IN ('quarantine', 'stop', 'warn') THEN
            RAISE EXCEPTION 'rule %: action % unknown', r ->> 'rule_id', r ->> 'action' USING ERRCODE = 'DPM03';
        END IF;
        INSERT INTO runtime.dq_result (run_id, dataset_id, rule_id, checked, failed, action, detail)
        VALUES (r ->> 'run_id', ds, r ->> 'rule_id', (r ->> 'checked')::BIGINT, (r ->> 'failed')::BIGINT,
                r ->> 'action', runtime._redact(COALESCE(r ->> 'detail', '')));
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

CREATE OR REPLACE FUNCTION runtime.dq_results(p_run_id TEXT, p_source TEXT, p_dataset TEXT)
RETURNS TABLE (source TEXT, dataset TEXT, rule_id TEXT, checked BIGINT, failed BIGINT, action TEXT, detail TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM runtime._check_run(p_run_id);
    RETURN QUERY SELECT s.source_name, d.dataset_name, q.rule_id, q.checked, q.failed, q.action, q.detail
                 FROM runtime.dq_result q JOIN config.dataset d USING (dataset_id) JOIN config.source s USING (source_id)
                 WHERE q.run_id = p_run_id AND (p_source IS NULL OR s.source_name = p_source)
                   AND (p_dataset IS NULL OR d.dataset_name = p_dataset)
                 ORDER BY q.result_id;
END $$;

-- ------------------------------------------------------------------------------------------------- profiles

CREATE OR REPLACE FUNCTION runtime.save_profile(p_run_id TEXT, p_source TEXT, p_dataset TEXT, p_profile JSONB)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := runtime._dataset_run(p_run_id, p_source, p_dataset);
BEGIN
    INSERT INTO runtime.dq_profile (run_id, dataset_id, profile) VALUES (p_run_id, ds, p_profile);
END $$;

CREATE OR REPLACE FUNCTION runtime.last_profile(p_source TEXT, p_dataset TEXT) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE ds BIGINT := config._dataset_id(p_source, p_dataset);
BEGIN
    RETURN (SELECT q.profile FROM runtime.dq_profile q WHERE q.dataset_id = ds ORDER BY q.profile_id DESC LIMIT 1);
END $$;

-- ------------------------------------------------------------------------------------------------- writer leases

-- NULL when this run holds the lease now (new, re-entered or taken over after expiry); else the run holding it.
CREATE OR REPLACE FUNCTION runtime.acquire_lease(p_table TEXT, p_run_id TEXT, p_ttl_seconds INTEGER) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE holder TEXT;
BEGIN
    IF p_ttl_seconds IS NULL OR p_ttl_seconds <= 0 THEN
        RAISE EXCEPTION 'a lease needs a positive time to live' USING ERRCODE = 'DPM03';
    END IF;
    INSERT INTO runtime.table_lease AS l (table_name, run_id, expires_at)
    VALUES (p_table, p_run_id, now() + make_interval(secs => p_ttl_seconds))
    ON CONFLICT (table_name) DO UPDATE
        SET run_id = EXCLUDED.run_id, acquired_at = now(), expires_at = EXCLUDED.expires_at
        WHERE l.expires_at < now() OR l.run_id = EXCLUDED.run_id;
    SELECT l.run_id INTO holder FROM runtime.table_lease l WHERE l.table_name = p_table;
    RETURN CASE WHEN holder = p_run_id THEN NULL ELSE holder END;
END $$;

CREATE OR REPLACE FUNCTION runtime.release_lease(p_table TEXT, p_run_id TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    DELETE FROM runtime.table_lease l WHERE l.table_name = p_table AND l.run_id = p_run_id;
END $$;
