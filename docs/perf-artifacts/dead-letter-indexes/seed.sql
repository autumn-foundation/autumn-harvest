-- Issue #2401 fixture. 100,000 dead letters in 20 queues, over 7 days.
-- One row in 10 is a callback. Row n has original_task_id ...-<n in hex>.
INSERT INTO harvest_dead_letters
    (original_task_id, queue_name, task_type, workflow_exec_id, activity_name, input, error,
     attempts, failed_at)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
       'queue_' || (n % 20),
       CASE WHEN n % 10 = 0 THEN 'CALLBACK' ELSE 'activity' END,
       gen_random_uuid(), 'charge_card',
       jsonb_build_object('n', n, 'pad', repeat('x', 200)),
       'upstream returned 503 for request ' || n || ': ' || repeat('y', 60),
       3, NOW() - (n * INTERVAL '6 seconds')
FROM generate_series(1, 100000) AS n;
VACUUM ANALYZE harvest_dead_letters;
