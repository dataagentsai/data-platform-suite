-- meta.v_enabled_datasets — flattens dataset_config (KV) + watermark
-- (typed) into one row per enabled dataset, projected to match the
-- parameter set Phase 5d ingest pipelines expect.
--
-- Read by PL_PER_SOURCE_DRIVER_<TYPE> (Phase 5d) — the second of the
-- two Lookups (after v_source_config). Result is iterated via
-- ForEach; each iteration ExecutePipelines the matching ingest
-- pipeline with @item().<column>.
--
-- Column responsibilities:
--   schema_name       — RDBMS schema (T-SQL: dbo; PG: public; Oracle owner)
--   table_name        — RDBMS table or view name
--   watermark_column  — incremental cursor column
--   watermark_value   — last successful high-water mark (from typed
--                       meta.watermark; NULL on first run)
--   container         — ADLS container (e.g. "lake")
--   folder_path       — for SFTP, source folder; for RDBMS/REST,
--                       target ADLS folder. Path templates with
--                       {yyyy}/{mm}/{dd} are returned raw — runtime
--                       date substitution is deferred to Phase 6b.
--   file_format       — SFTP discriminator (csv | json_lines).
--                       Sources from dataset_config.file_format_data
--                       (the source-side file format, distinct from the
--                       sink-side dataset_config.file_format which is
--                       always PARQUET on the lake).
--   relative_url      — REST API endpoint path
--   file_name         — single SFTP file (wildcards land later)
--   folder_path_sink  — target ADLS folder (used when source folder
--                       and sink folder differ — e.g. SFTP)
--
-- Datasets without explicit overrides return NULL; the ingest
-- pipeline either ignores irrelevant columns or substitutes defaults.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_enabled_datasets AS
WITH cfg AS (
    SELECT
        dataset_id,
        -- [key] is mandatory (KEY reserved); value is identifier-safe.
        MAX(CASE WHEN [key] = 'schema_name' THEN value END) AS schema_name,
        MAX(CASE WHEN [key] = 'object_name' THEN value END) AS object_name,
        MAX(CASE WHEN [key] = 'watermark_column' THEN value END) AS watermark_column,
        MAX(CASE WHEN [key] = 'target_container' THEN value END) AS target_container,
        MAX(CASE WHEN [key] = 'target_path_template' THEN value END) AS target_path_template,
        MAX(CASE WHEN [key] = 'file_format_data' THEN value END) AS file_format_data,
        MAX(CASE WHEN [key] = 'relative_url' THEN value END) AS relative_url,
        MAX(CASE WHEN [key] = 'file_name' THEN value END) AS file_name,
        MAX(CASE WHEN [key] = 'sftp_folder_path' THEN value END) AS sftp_folder_path,
        MAX(CASE WHEN [key] = 's3_bucket' THEN value END) AS s3_bucket,
        MAX(CASE WHEN [key] = 'azure_blob_container' THEN value END) AS azure_blob_container
    FROM meta.dataset_config
    GROUP BY dataset_id
)

SELECT
    dm.dataset_id,
    dm.source_id,
    dm.dataset_name,
    cfg.schema_name,
    cfg.object_name AS table_name,
    cfg.watermark_column,
    w.watermark_value,
    cfg.target_container AS container,
    cfg.file_format_data AS [file_format],  -- noqa: RF06
    cfg.relative_url,
    cfg.file_name,
    cfg.target_path_template AS folder_path_sink,
    COALESCE(cfg.s3_bucket, cfg.azure_blob_container) AS bucket,
    COALESCE(cfg.sftp_folder_path, cfg.target_path_template) AS folder_path
FROM meta.dataset_master AS dm
LEFT JOIN cfg ON dm.dataset_id = cfg.dataset_id
LEFT JOIN meta.watermark AS w ON dm.dataset_id = w.dataset_id
WHERE dm.is_enabled = 1;
