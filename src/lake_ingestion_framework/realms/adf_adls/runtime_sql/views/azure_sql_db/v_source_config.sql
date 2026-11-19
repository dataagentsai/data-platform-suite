-- meta.v_source_config — flattens source_config (KV) into per-source-type
-- connection columns named to match the parameterised LinkedService
-- parameters across all 7 source types:
--
--   p_server, p_database, p_service_name, p_user, p_password_secret,
--   p_base_url, p_account, p_warehouse, p_role,
--   p_project_id, p_key_file_secret,
--   p_access_key_id, p_secret_access_key_secret,
--   p_storage_account,
--   p_environment_url, p_client_id, p_client_secret_secret
--
-- Read by PL_PER_SOURCE_DRIVER_<TYPE>. Each driver Lookups only the
-- columns its source type needs (Azure SQL DB pulls p_server/p_database;
-- Postgres pulls p_server/p_database/p_user/p_password_secret; Oracle
-- pulls p_server/p_service_name/...; Snowflake pulls
-- p_account/p_database/p_warehouse/p_user/p_password_secret/p_role; etc).
-- Irrelevant columns return NULL and are ignored.
--
-- Why a single wide view: keeps the driver SQL identical across types
-- (just SELECT a different column subset), and isolates KV→column
-- pivoting in one place.
--
-- p_password_secret strips the "kv://<vault>/" prefix so the LS hands
-- the bare secret name to LS_KEY_VAULT. Stripping in the view (not
-- ADF) keeps the parsing logic in one place; full kv:// URIs that
-- don't match the prefix pattern are passed through unchanged so
-- malformed entries surface clearly at the Lookup result.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_source_config AS
WITH cfg AS (
    SELECT
        source_id,
        -- Internal CTE columns use suffixed names (server_, db_name)
        -- because sqlfluff RF04 rejects keyword identifiers (server,
        -- database) even when bracket-quoted. The outer SELECT renames
        -- them to the LS parameter names (p_server, p_database).
        -- [key] stays bracket-quoted because KEY is a reserved word.
        MAX(CASE WHEN [key] = 'server' THEN value END) AS server_,
        MAX(CASE WHEN [key] = 'database' THEN value END) AS db_name,
        MAX(CASE WHEN [key] = 'service_name' THEN value END) AS service_name,
        MAX(CASE WHEN [key] = 'connection_user' THEN value END) AS connection_user,
        MAX(CASE WHEN [key] = 'connection_secret_name' THEN value END) AS connection_secret_name,
        MAX(CASE WHEN [key] = 'base_url' THEN value END) AS base_url,
        MAX(CASE WHEN [key] = 'account' THEN value END) AS account_,
        MAX(CASE WHEN [key] = 'warehouse' THEN value END) AS warehouse_,
        MAX(CASE WHEN [key] = 'role' THEN value END) AS role_,
        MAX(CASE WHEN [key] = 'project_id' THEN value END) AS project_id_,
        MAX(CASE WHEN [key] = 'access_key_id' THEN value END) AS access_key_id_,
        MAX(CASE WHEN [key] = 'storage_account' THEN value END) AS storage_account_,
        MAX(CASE WHEN [key] = 'environment_url' THEN value END) AS environment_url_,
        MAX(CASE WHEN [key] = 'client_id' THEN value END) AS client_id_
    FROM meta.source_config
    GROUP BY source_id
),

stripped_secret AS (
    -- Strip "kv://<vault>/" prefix once; both p_password_secret and
    -- p_key_file_secret project the same bare secret name. The LS that
    -- consumes it knows what kind of secret it is (BigQuery treats the
    -- KV value as a JSON service-account key file; others as a password
    -- or private key).
    SELECT
        cfg.source_id,
        CASE
            WHEN cfg.connection_secret_name LIKE 'kv://%/%'
                THEN SUBSTRING(
                    cfg.connection_secret_name,
                    CHARINDEX('/', cfg.connection_secret_name, 6) + 1,
                    8000
                )
            ELSE cfg.connection_secret_name
        END AS bare_secret
    FROM cfg
)

SELECT
    sm.source_id,
    sm.source_type,
    cfg.server_ AS p_server,
    cfg.db_name AS p_database,
    cfg.service_name AS p_service_name,
    cfg.connection_user AS p_user,
    cfg.base_url AS p_base_url,
    cfg.account_ AS p_account,
    cfg.warehouse_ AS p_warehouse,
    cfg.role_ AS p_role,
    cfg.project_id_ AS p_project_id,
    cfg.access_key_id_ AS p_access_key_id,
    cfg.storage_account_ AS p_storage_account,
    cfg.environment_url_ AS p_environment_url,
    cfg.client_id_ AS p_client_id,
    ss.bare_secret AS p_password_secret,
    ss.bare_secret AS p_key_file_secret,
    ss.bare_secret AS p_secret_access_key_secret,
    ss.bare_secret AS p_client_secret_secret
FROM meta.source_master AS sm
LEFT JOIN cfg ON sm.source_id = cfg.source_id
LEFT JOIN stripped_secret AS ss ON sm.source_id = ss.source_id
WHERE sm.is_enabled = 1;
