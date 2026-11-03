-- Issue #2401 fixture. 100,000 dead letters in 20 queues, over 7 days.
-- One row in 10 is a callback. Each original_task_id is random, as a task id
-- is. Row 50000 has the fixed id that statements.sql deletes.
-- The 5,000 newest rows belong to the activity `refund`. tradeoff.sql reads
-- them oldest first.
INSERT INTO harvest_dead_letters
    (original_task_id, queue_name, task_type, workflow_exec_id, activity_name, input, error,
     attempts, failed_at)
SELECT CASE WHEN n = 50000 THEN '00000000-0000-4000-8000-00000000c350'::uuid
            ELSE gen_random_uuid() END,
       'queue_' || (n % 20),
       CASE WHEN n % 10 = 0 THEN 'CALLBACK' ELSE 'activity' END,
       gen_random_uuid(), CASE WHEN n <= 5000 THEN 'refund' ELSE 'charge_card' END,
       jsonb_build_object('n', n, 'pad', repeat('x', 200)),
       'upstream returned 503 for request ' || n || ': ' || repeat('y', 60),
       3, NOW() - (n * INTERVAL '6 seconds')
FROM generate_series(1, 100000) AS n;
VACUUM ANALYZE harvest_dead_letters;
