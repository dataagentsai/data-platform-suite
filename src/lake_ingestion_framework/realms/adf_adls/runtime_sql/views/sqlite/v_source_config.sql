-- SQLite stand-in for meta.v_source_config.
-- Same logical projection as the T-SQL canonical (../azure_sql_db/...);
-- differences:
--   - no `meta.` schema; no CREATE OR ALTER (DROP+CREATE for idempotency)
--   - `key`/`value` are bare identifiers (T-SQL uses [key]/[value])
--   - SUBSTRING/CHARINDEX/LEN -> substr/instr/length

DROP VIEW IF EXISTS v_source_config;
CREATE VIEW v_source_config AS
WITH cfg AS (
    SELECT
        source_id,
        MAX(CASE WHEN key = 'server' THEN value END)                  AS server,
        MAX(CASE WHEN key = 'database' THEN value END)                AS "database",
        MAX(CASE WHEN key = 'service_name' THEN value END)            AS service_name,
        MAX(CASE WHEN key = 'connection_user' THEN value END)         AS connection_user,
        MAX(CASE WHEN key = 'connection_secret_name' THEN value END)  AS connection_secret_name,
        MAX(CASE WHEN key = 'base_url' THEN value END)                AS base_url,
        MAX(CASE WHEN key = 'account' THEN value END)                 AS account,
        MAX(CASE WHEN key = 'warehouse' THEN value END)               AS warehouse,
        MAX(CASE WHEN key = 'role' THEN value END)                    AS role,
        MAX(CASE WHEN key = 'project_id' THEN value END)              AS project_id,
        MAX(CASE WHEN key = 'access_key_id' THEN value END)           AS access_key_id,
        MAX(CASE WHEN key = 'storage_account' THEN value END)         AS storage_account,
        MAX(CASE WHEN key = 'environment_url' THEN value END)         AS environment_url,
        MAX(CASE WHEN key = 'client_id' THEN value END)               AS client_id
    FROM source_config
    GROUP BY source_id
)
SELECT
    sm.source_id,
    sm.source_type,
    cfg.server            AS p_server,
    cfg."database"        AS p_database,
    cfg.service_name      AS p_service_name,
    cfg.connection_user   AS p_user,
    cfg.account           AS p_account,
    cfg.warehouse         AS p_warehouse,
    cfg.role              AS p_role,
    cfg.project_id        AS p_project_id,
    cfg.access_key_id     AS p_access_key_id,
    cfg.storage_account   AS p_storage_account,
    cfg.environment_url   AS p_environment_url,
    cfg.client_id         AS p_client_id,
    -- Strip "kv://<vault>/" prefix; pass the bare secret name.
    -- substr(s, 6) starts at the char after "kv://"; instr finds the
    -- next '/' (between vault and secret); 6 + instr is the position
    -- in the original string after that '/'.
    -- Both p_password_secret and p_key_file_secret project the same
    -- bare secret name; the consuming LS knows what kind it is
    -- (BigQuery uses it as a JSON key file; others as a password).
    CASE
        WHEN cfg.connection_secret_name LIKE 'kv://%/%'
            THEN substr(
                cfg.connection_secret_name,
                6 + instr(substr(cfg.connection_secret_name, 6), '/')
            )
        ELSE cfg.connection_secret_name
    END                   AS p_password_secret,
    CASE
        WHEN cfg.connection_secret_name LIKE 'kv://%/%'
            THEN substr(
                cfg.connection_secret_name,
                6 + instr(substr(cfg.connection_secret_name, 6), '/')
            )
        ELSE cfg.connection_secret_name
    END                   AS p_key_file_secret,
    CASE
        WHEN cfg.connection_secret_name LIKE 'kv://%/%'
            THEN substr(
                cfg.connection_secret_name,
                6 + instr(substr(cfg.connection_secret_name, 6), '/')
            )
        ELSE cfg.connection_secret_name
    END                   AS p_secret_access_key_secret,
    CASE
        WHEN cfg.connection_secret_name LIKE 'kv://%/%'
            THEN substr(
                cfg.connection_secret_name,
                6 + instr(substr(cfg.connection_secret_name, 6), '/')
            )
        ELSE cfg.connection_secret_name
    END                   AS p_client_secret_secret,
    cfg.base_url          AS p_base_url
FROM source_master sm
LEFT JOIN cfg ON sm.source_id = cfg.source_id
WHERE sm.is_enabled = 1;
