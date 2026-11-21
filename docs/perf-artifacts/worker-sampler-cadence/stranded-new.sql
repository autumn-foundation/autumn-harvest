SET statement_timeout = '300s';
SET jit = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT tq.queue_name, tq.required_capabilities, tq.required_build_id,
  CASE WHEN tq.sticky_worker_id IS NOT NULL AND tq.sticky_until IS NOT NULL AND tq.sticky_until > NOW() THEN tq.sticky_worker_id ELSE NULL END AS sticky_owner,
  tq.activity_name, COUNT(*)::BIGINT AS cnt
FROM harvest_task_queue tq
WHERE tq.state = 'PENDING' AND tq.scheduled_at <= NOW()
  AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name = tq.queue_name)
  AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap WHERE ap.activity_name = tq.activity_name AND tq.task_type = 'activity')
  AND (tq.schedule_to_close_at IS NULL OR tq.schedule_to_close_at > NOW())
  AND (tq.task_type <> 'workflow' OR tq.workflow_exec_id IS NULL OR NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e WHERE e.id = tq.workflow_exec_id AND e.state = 'PAUSED'))
  AND (tq.concurrency_key IS NULL OR tq.concurrency_cap IS NULL OR (SELECT COUNT(*) FROM harvest_task_queue inner_q WHERE inner_q.concurrency_key = tq.concurrency_key AND inner_q.task_type = tq.task_type AND inner_q.state = 'RUNNING' AND inner_q.worker_id IS NOT NULL) < tq.concurrency_cap)
  AND (tq.rate_limit_key IS NULL OR tq.activity_name = ANY(ARRAY[]::text[]) OR EXISTS (SELECT 1 FROM harvest_rate_limit_buckets b WHERE b.key = tq.rate_limit_key AND LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + GREATEST(0, EXTRACT(EPOCH FROM (LEAST(COALESCE(b.override_expires_at, b.last_refilled_at), NOW()) - b.last_refilled_at))) * COALESCE(b.override_refill_rate, b.refill_rate) + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - GREATEST(COALESCE(b.override_expires_at, b.last_refilled_at), b.last_refilled_at)))) * b.refill_rate) >= 1.0))
GROUP BY tq.queue_name, tq.required_capabilities, tq.required_build_id, CASE WHEN tq.sticky_worker_id IS NOT NULL AND tq.sticky_until IS NOT NULL AND tq.sticky_until > NOW() THEN tq.sticky_worker_id ELSE NULL END, tq.activity_name;
