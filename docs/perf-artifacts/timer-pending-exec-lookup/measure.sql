-- Per-decision timer lookups of the worker, as Diesel sends them.
-- Shapes: long-lived run (10,000 timers), medium (100), short (4).
-- Each statement is prepared, so the plan is the one a pooled connection runs.
\set ON_ERROR_STOP 1
\pset pager off
PREPARE ingest(uuid, boolean) AS
SELECT "harvest_timers"."id", "harvest_timers"."workflow_exec_id", "harvest_timers"."timer_id", "harvest_timers"."fires_at", "harvest_timers"."fired" FROM "harvest_timers" WHERE ((("harvest_timers"."workflow_exec_id" = $1) AND ("harvest_timers"."fired" = $2)) AND ("harvest_timers"."fires_at" <= NOW())) ORDER BY "harvest_timers"."fires_at" ASC, "harvest_timers"."timer_id" ASC;
PREPARE by_id(uuid, text, boolean, bigint) AS
SELECT "harvest_timers"."id", "harvest_timers"."workflow_exec_id", "harvest_timers"."timer_id", "harvest_timers"."fires_at", "harvest_timers"."fired" FROM "harvest_timers" WHERE ((("harvest_timers"."workflow_exec_id" = $1) AND ("harvest_timers"."timer_id" = $2)) AND ("harvest_timers"."fired" = $3)) LIMIT $4;
PREPARE by_ids(uuid, text[], boolean) AS
SELECT "harvest_timers"."id", "harvest_timers"."workflow_exec_id", "harvest_timers"."timer_id", "harvest_timers"."fires_at", "harvest_timers"."fired" FROM "harvest_timers" WHERE ((("harvest_timers"."workflow_exec_id" = $1) AND ("harvest_timers"."timer_id" = ANY($2))) AND ("harvest_timers"."fired" = $3)) ORDER BY "harvest_timers"."fires_at" ASC, "harvest_timers"."id" ASC;

\echo '##### plan_cache_mode = force_custom_plan'
SET plan_cache_mode = force_custom_plan;
\i shapes.sql
\echo '##### plan_cache_mode = force_generic_plan'
SET plan_cache_mode = force_generic_plan;
\i shapes.sql
