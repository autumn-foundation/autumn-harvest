-- The four statements of issue #2367, each on one execution, rolled back.
BEGIN;
UPDATE harvest_signals SET workflow_exec_id = '00000000-0000-4000-8000-000000000fa1',
       consumed = false
 WHERE workflow_exec_id = '00000000-0000-4000-8000-000000000001';
ROLLBACK;
BEGIN;
UPDATE harvest_signals SET payload = '{"_harvest_erased": true}'::jsonb
 WHERE workflow_exec_id = '00000000-0000-4000-8000-000000000002';
ROLLBACK;
SELECT COALESCE(jsonb_agg(to_jsonb(s)), '[]'::jsonb) IS NOT NULL AS copied
  FROM harvest_signals s WHERE s.workflow_exec_id = '00000000-0000-4000-8000-000000000003';
BEGIN;
DELETE FROM harvest_workflow_executions WHERE id = '00000000-0000-4000-8000-000000000004';
ROLLBACK;
