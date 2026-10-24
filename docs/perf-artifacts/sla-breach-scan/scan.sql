BEGIN;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
UPDATE "harvest_workflow_executions" SET "sla_breached" = true, "sla_breached_at" = now()
WHERE ((("harvest_workflow_executions"."state" != 'PAUSED') AND ("harvest_workflow_executions"."sla_deadline_at" IS NOT NULL)) AND ("harvest_workflow_executions"."sla_breached" = false)) AND sla_deadline_at < COALESCE(completed_at, NOW())
RETURNING "harvest_workflow_executions"."id", "harvest_workflow_executions"."workflow_name", "harvest_workflow_executions"."queue_name";
ROLLBACK;
