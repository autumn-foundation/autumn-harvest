BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
PREPARE r(text, int4, int8) AS WITH batch AS ( SELECT t.id FROM harvest_workflow_executions e JOIN harvest_task_queue t ON t.workflow_exec_id = e.id WHERE e.workflow_name = $1 AND e.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'CONTINUED_AS_NEW', 'TERMINATED') AND t.task_type = 'workflow' AND t.state IN ('PENDING', 'RUNNING') AND t.concurrency_key IS NOT NULL AND t.concurrency_cap < $2 ORDER BY t.id LIMIT $3 FOR UPDATE OF t SKIP LOCKED ) UPDATE harvest_task_queue AS t SET concurrency_cap = $2 FROM batch WHERE t.id = batch.id;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) EXECUTE r('wf1', 20, 500);
DEALLOCATE r;
ROLLBACK;
