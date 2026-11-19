-- meta.watermark_audit — append-only log of operator-driven watermark
-- resets. NOT written to by sp_advance_watermark (that's per-run
-- bookkeeping; would bloat the audit log). Captures the rare,
-- high-stakes events: backfills, corrections, reseeds.
--
-- Rows land here from `lake-ingest reset-watermark` / `backfill`
-- CLIs (Phase 6b-3). The table ships now so that v_watermark_status
-- can join against it from day 1; it stays empty in current deploys.
--
-- Per CLAUDE.md ("Watermarks are sacred"): every reset is logged
-- here so forensic replay is always possible. ``reason`` and
-- ``changed_by`` are required when the proc writes — operators must
-- justify the reset; the proc rejects empty / single-character /
-- placeholder reasons.
--
-- Schema canonical: docs/metadata_schema.md "meta.watermark_audit".
-- See docs/recovery.md for the CLI flows.

IF OBJECT_ID('meta.watermark_audit', 'U') IS NULL
    CREATE TABLE meta.watermark_audit (
        audit_id BIGINT IDENTITY (1, 1) PRIMARY KEY,
        dataset_id INT NOT NULL REFERENCES meta.dataset_master (dataset_id),
        old_watermark_value NVARCHAR(MAX) NULL,
        new_watermark_value NVARCHAR(MAX) NULL,
        change_type VARCHAR(20) NOT NULL,
        reason NVARCHAR(MAX) NOT NULL,
        changed_by VARCHAR(200) NOT NULL,
        changed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        affected_partitions NVARCHAR(MAX) NULL,
        master_run_id BIGINT NULL REFERENCES meta.master_run_log (master_run_id),
        INDEX ix_watermark_audit_dataset_at (dataset_id, changed_at DESC)
    );
