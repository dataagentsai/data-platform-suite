-- SQLite stand-in for meta.v_enabled_sources.
-- Same logical projection as the T-SQL canonical (../azure_sql_db/...);
-- differences are dialect-only (no `meta.` schema, no CREATE OR ALTER).

DROP VIEW IF EXISTS v_enabled_sources;
CREATE VIEW v_enabled_sources AS
SELECT
    source_id,
    source_type
FROM source_master
WHERE is_enabled = 1;
