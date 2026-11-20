SET enable_bitmapscan = off; SET enable_sort = off; SET jit = off;
SET plan_cache_mode = force_generic_plan;
PREPARE p(text[]) AS SELECT DISTINCT concurrency_key, task_type FROM harvest_task_queue WHERE queue_name = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW() AND concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) EXECUTE p('{q0,q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19}');
