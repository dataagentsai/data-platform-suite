-- meta.v_enabled_sources — one row per enabled source.
--
-- Read by PL_MASTER_DRIVER (Phase 5d). The Lookup activity returns
-- (source_id, source_type) rows; the master driver's Switch dispatches
-- on source_type into the matching PL_PER_SOURCE_DRIVER_<TYPE>.
--
-- Idempotent: re-running this DDL replaces the view definition.

CREATE OR ALTER VIEW meta.v_enabled_sources AS
SELECT
    source_id,
    source_type
FROM meta.source_master
WHERE is_enabled = 1;
