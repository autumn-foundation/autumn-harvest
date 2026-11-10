-- Result identity. Run every per-decision lookup for every run of the fixture
-- and hash the full ordered result. The id breaks every tie.
\set ON_ERROR_STOP 1
SET plan_cache_mode = force_generic_plan;
SELECT 'ingest' AS q, count(*) AS rows, md5(string_agg(r::text, ',' ORDER BY r.workflow_exec_id, r.fires_at, r.timer_id, r.id)) AS digest
FROM harvest_workflow_executions e
CROSS JOIN LATERAL (SELECT t.* FROM harvest_timers t WHERE t.workflow_exec_id = e.id AND t.fired = false AND t.fires_at <= now() ORDER BY t.fires_at, t.timer_id) r
UNION ALL
SELECT 'ingest_fired_true', count(*), md5(string_agg(r::text, ',' ORDER BY r.id))
FROM harvest_workflow_executions e
CROSS JOIN LATERAL (SELECT t.* FROM harvest_timers t WHERE t.workflow_exec_id = e.id AND t.fired = true AND t.fires_at <= now()) r
WHERE e.workflow_name <> 'long_lived'
UNION ALL
SELECT 'by_id_open', count(*), md5(string_agg(r::text, ',' ORDER BY r.id))
FROM harvest_workflow_executions e
CROSS JOIN LATERAL (SELECT t.* FROM harvest_timers t WHERE t.workflow_exec_id = e.id AND t.timer_id = 'timer-' || CASE WHEN e.workflow_name = 'long_lived' THEN 10000 ELSE 100 END AND t.fired = false LIMIT 1) r
UNION ALL
SELECT 'by_ids_lag', count(*), md5(string_agg(r::text, ',' ORDER BY r.id))
FROM harvest_workflow_executions e
CROSS JOIN LATERAL (SELECT t.* FROM harvest_timers t WHERE t.workflow_exec_id = e.id AND t.timer_id = ANY (ARRAY['lag-1','lag-2','lag-3','timer-1','timer-4']) AND t.fired = false ORDER BY t.fires_at, t.id) r
UNION ALL
SELECT 'no_such_run', count(*), md5(coalesce(string_agg(t::text, ','), ''))
FROM harvest_timers t WHERE t.workflow_exec_id = '00000000-0000-4000-8000-ffffffffffff' AND t.fired = false;
