-- Claim 9,000 random background rows in place, with 40 other keys, as a
-- live queue does. The running rows then sit across the whole heap.
SELECT setseed(0.42);
UPDATE harvest_task_queue
SET state = 'RUNNING', worker_id = 'w' || (abs(hashtext(id::text)) % 50),
    concurrency_key = 'bgk' || (abs(hashtext(id::text)) % 40), concurrency_cap = 10,
    started_at = now()
WHERE id IN (
    SELECT id FROM harvest_task_queue
    WHERE state = 'PENDING' AND queue_name LIKE 'bg%' AND concurrency_key IS NULL
    ORDER BY random() LIMIT 9000
);
VACUUM ANALYZE harvest_task_queue;
