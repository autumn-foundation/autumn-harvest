-- The statements of issue #2367, each on one execution, rolled back.
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
BEGIN;
DELETE FROM harvest_signals WHERE workflow_exec_id = '00000000-0000-4000-8000-000000000005';
ROLLBACK;
-- The unkeyed signal check of backup_verify::probes::effect_verdict, for a
-- name that no signal has. That is the case that reads the most rows.
WITH RECURSIVE chain(id) AS (
    SELECT '00000000-0000-4000-8000-000000000006'::uuid
  UNION
    SELECT w.id FROM harvest_workflow_executions w
      JOIN chain c ON w.continued_from_exec_id = c.id OR w.retry_of_exec_id = c.id
)
SELECT EXISTS (
    SELECT 1 FROM harvest_signals s JOIN chain c ON s.workflow_exec_id = c.id
     WHERE s.signal_name = 'absent'
) OR EXISTS (
    SELECT 1 FROM harvest_events e JOIN chain c ON e.workflow_exec_id = c.id
     WHERE e.event_type = 'SignalReceived'
       AND e.event_data->'data'->>'signal_name' = 'absent'
) AS present;
