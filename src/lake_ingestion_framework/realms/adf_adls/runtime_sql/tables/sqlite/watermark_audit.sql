-- SQLite stand-in for meta.watermark_audit.
-- Same logical shape as the T-SQL canonical (../azure_sql_db/...).
-- Schema canonical: docs/metadata_schema.md "meta.watermark_audit".

CREATE TABLE IF NOT EXISTS watermark_audit (
    audit_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id          INTEGER NOT NULL REFERENCES dataset_master(dataset_id),
    old_watermark_value TEXT,
    new_watermark_value TEXT,
    change_type         TEXT    NOT NULL,
    reason              TEXT    NOT NULL,
    changed_by          TEXT    NOT NULL,
    changed_at          TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    affected_partitions TEXT,
    master_run_id       INTEGER REFERENCES master_run_log(master_run_id)
);

CREATE INDEX IF NOT EXISTS ix_watermark_audit_dataset_at
    ON watermark_audit (dataset_id, changed_at DESC);
