-- plan_cache_mode = auto, the default. A pooled connection runs a prepared
-- statement many times. After five custom plans, Postgres may keep a generic
-- plan. Run the lookup six times for a mix of runs, then explain it.
\set ON_ERROR_STOP 1
\pset pager off
PREPARE by_id(uuid, text, boolean, bigint) AS
SELECT "harvest_timers"."id", "harvest_timers"."workflow_exec_id", "harvest_timers"."timer_id", "harvest_timers"."fires_at", "harvest_timers"."fired" FROM "harvest_timers" WHERE ((("harvest_timers"."workflow_exec_id" = $1) AND ("harvest_timers"."timer_id" = $2)) AND ("harvest_timers"."fired" = $3)) LIMIT $4;
SET plan_cache_mode = auto;
EXECUTE by_id('00000000-0000-4000-8000-00000000138c', 'timer-4', false, 1) \g /dev/null
EXECUTE by_id('00000000-0000-4000-8000-0000000001f4', 'timer-100', false, 1) \g /dev/null
EXECUTE by_id('00000000-0000-4000-8000-00000000138d', 'timer-1', false, 1) \g /dev/null
EXECUTE by_id('00000000-0000-4000-8000-000000000002', 'timer-10000', false, 1) \g /dev/null
EXECUTE by_id('00000000-0000-4000-8000-00000000138e', 'timer-2', false, 1) \g /dev/null
EXECUTE by_id('00000000-0000-4000-8000-0000000001f5', 'timer-100', false, 1) \g /dev/null
SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'by_id';
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) EXECUTE by_id('00000000-0000-4000-8000-000000000001', 'timer-10000', false, 1);
SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'by_id';
