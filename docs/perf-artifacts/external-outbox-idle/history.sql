-- Resolved request history for g in [:lo, :hi]: one ExternalSignalRequested
-- row and its ExternalSignalDelivered marker each. 80% on terminal runs.
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, 1000 + k, 'ExternalSignalRequested',
       jsonb_build_object('type','ExternalSignalRequested','data',jsonb_build_object('signal_id','h-'||e.workflow_id||'-'||k)),
       e.started_at + (k||' seconds')::interval
FROM (SELECT g, CASE WHEN g % 5 = 0 THEN md5('r'||(1 + (g/5) % 20000))::uuid ELSE md5('t'||(1 + g % 100000))::uuid END AS eid, g / 100000 AS k
      FROM generate_series(:lo, :hi) g) s
JOIN harvest_workflow_executions e ON e.id = s.eid
ON CONFLICT DO NOTHING;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT r.workflow_exec_id, r.event_id + 500000, 'ExternalSignalDelivered',
       jsonb_build_object('type','ExternalSignalDelivered','data',jsonb_build_object('signal_id', r.event_data->'data'->>'signal_id')),
       r.timestamp + interval '1 second'
FROM harvest_events r
WHERE r.event_type='ExternalSignalRequested' AND r.event_id BETWEEN 1000 AND 499999
  AND NOT EXISTS (SELECT 1 FROM harvest_events d WHERE d.workflow_exec_id=r.workflow_exec_id AND d.event_id=r.event_id+500000);
ANALYZE harvest_events; ANALYZE harvest_workflow_executions;
