-- The base fixture: 20,000 RUNNING runs with 50 events each, and 100,000
-- COMPLETED runs inside the retention window with 20 events each.
SELECT setseed(0.42);
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at, completed_at)
SELECT md5('r'||gs)::uuid, 'probe_wf', 'run_'||gs, md5('rr'||gs)::uuid, 0, 'RUNNING', '{}'::jsonb, 'default', NOW()-interval '3 days', NOW()-interval '3 days', NULL
FROM generate_series(1,20000) gs;
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at, completed_at)
SELECT md5('t'||gs)::uuid, 'probe_wf', 'done_'||gs, md5('tt'||gs)::uuid, 0, 'COMPLETED', '{}'::jsonb, 'default',
       NOW()-interval '7 days' + (gs||' seconds')::interval, NOW()-interval '7 days' + (gs||' seconds')::interval, NOW()-interval '7 days' + ((gs+60)||' seconds')::interval
FROM generate_series(1,100000) gs;
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT e.id, gs,
       (ARRAY['ActivityScheduled','ActivityStarted','ActivityCompleted','WorkflowTaskScheduled'])[1 + (gs % 4)],
       jsonb_build_object('type','ActivityScheduled','data',jsonb_build_object('activity_id','a'||gs,'input',repeat('x',120))),
       e.started_at + (gs||' seconds')::interval
FROM harvest_workflow_executions e, generate_series(1, CASE WHEN e.state='RUNNING' THEN 50 ELSE 20 END) gs;
