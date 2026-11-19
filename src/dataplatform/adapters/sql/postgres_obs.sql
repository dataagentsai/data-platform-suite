-- The run log, read back for operations (W3, DPC-0057): one read-only procedure over the run tables of
-- postgres_meta.sql. Installed after that file; changes none of its tables or functions. Same rules as the store:
-- callers reach the tables only through this function (DPC-0033), which runs as its owner.

CREATE OR REPLACE FUNCTION runtime.ops_run_log(p_domain TEXT, p_since TIMESTAMPTZ, p_until TIMESTAMPTZ)
RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    WITH r AS (
        SELECT run_id, domain, env, trigger, run_type, status, started_at, ended_at
        FROM runtime.run
        WHERE domain = p_domain AND started_at >= p_since AND started_at < p_until
    )
    SELECT jsonb_build_object(
        'runs', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'run_id', r.run_id, 'domain', r.domain, 'env', r.env, 'trigger', r.trigger,
                    'run_type', r.run_type, 'status', r.status, 'started_at', r.started_at,
                    'ended_at', r.ended_at) ORDER BY r.started_at, r.run_id), '[]') FROM r),
        'datasets', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'run_id', x.run_id, 'source', s.source_name, 'dataset', d.dataset_name,
                        'status', x.status, 'config_fingerprint', x.config_fingerprint,
                        'watermark_before', x.watermark_before, 'watermark_after', x.watermark_after,
                        'counts', x.counts, 'message', x.message, 'started_at', x.started_at,
                        'ended_at', x.ended_at) ORDER BY r.started_at, r.run_id, x.seq), '[]')
                     FROM r JOIN runtime.dataset_run x USING (run_id)
                     JOIN config.dataset d USING (dataset_id) JOIN config.source s USING (source_id)),
        'steps', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                     'run_id', x.run_id, 'source', s.source_name, 'dataset', d.dataset_name, 'step', x.step,
                     'status', x.status, 'counts', x.counts, 'message', x.message, 'at', x.logged_at)
                     ORDER BY r.started_at, r.run_id, x.step_id), '[]')
                  FROM r JOIN runtime.step_log x USING (run_id)
                  JOIN config.dataset d USING (dataset_id) JOIN config.source s USING (source_id)),
        'files', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                     'run_id', x.run_id, 'source', s.source_name, 'dataset', d.dataset_name,
                     'batch_key', x.batch_key, 'file_name', x.file_name, 'window', x.delivery_window,
                     'rows', x.row_count, 'status', x.status, 'reason', x.reason, 'at', x.recorded_at)
                     ORDER BY r.started_at, r.run_id, x.file_id), '[]')
                  FROM r JOIN runtime.file_log x USING (run_id)
                  JOIN config.dataset d USING (dataset_id) JOIN config.source s USING (source_id)),
        'errors', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                      'run_id', x.run_id, 'source', s.source_name, 'dataset', d.dataset_name, 'step', x.step,
                      'error_class', x.error_class, 'message', x.message, 'at', x.logged_at)
                      ORDER BY r.started_at, r.run_id, x.error_id), '[]')
                   FROM r JOIN runtime.error_log x USING (run_id)
                   LEFT JOIN config.dataset d USING (dataset_id) LEFT JOIN config.source s USING (source_id))
    );
$$;

REVOKE ALL ON FUNCTION runtime.ops_run_log(TEXT, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
