-- SQLite stand-in for meta.v_enabled_datasets.
-- Same logical projection as the T-SQL canonical (../azure_sql_db/...);
-- differences are dialect-only (no `meta.` schema, no CREATE OR ALTER,
-- bare key/value identifiers, COALESCE same).

DROP VIEW IF EXISTS v_enabled_datasets;
CREATE VIEW v_enabled_datasets AS
WITH cfg AS (
    SELECT
        dataset_id,
        MAX(CASE WHEN key = 'schema_name' THEN value END)          AS schema_name,
        MAX(CASE WHEN key = 'object_name' THEN value END)          AS object_name,
        MAX(CASE WHEN key = 'watermark_column' THEN value END)     AS watermark_column,
        MAX(CASE WHEN key = 'target_container' THEN value END)     AS target_container,
        MAX(CASE WHEN key = 'target_path_template' THEN value END) AS target_path_template,
        MAX(CASE WHEN key = 'file_format_data' THEN value END)     AS file_format_data,
        MAX(CASE WHEN key = 'relative_url' THEN value END)         AS relative_url,
        MAX(CASE WHEN key = 'file_name' THEN value END)            AS file_name,
        MAX(CASE WHEN key = 'sftp_folder_path' THEN value END)     AS sftp_folder_path,
        MAX(CASE WHEN key = 's3_bucket' THEN value END)            AS s3_bucket,
        MAX(CASE WHEN key = 'azure_blob_container' THEN value END) AS azure_blob_container
    FROM dataset_config
    GROUP BY dataset_id
)
SELECT
    dm.dataset_id,
    dm.source_id,
    dm.dataset_name,
    cfg.schema_name                                          AS schema_name,
    cfg.object_name                                          AS table_name,
    cfg.watermark_column                                     AS watermark_column,
    w.watermark_value                                        AS watermark_value,
    cfg.target_container                                     AS container,
    COALESCE(cfg.sftp_folder_path, cfg.target_path_template) AS folder_path,
    cfg.file_format_data                                     AS file_format,
    cfg.relative_url                                         AS relative_url,
    cfg.file_name                                            AS file_name,
    cfg.target_path_template                                 AS folder_path_sink,
    COALESCE(cfg.s3_bucket, cfg.azure_blob_container)        AS bucket
FROM dataset_master dm
LEFT JOIN cfg ON dm.dataset_id = cfg.dataset_id
LEFT JOIN watermark w ON dm.dataset_id = w.dataset_id
WHERE dm.is_enabled = 1;
