-- The statements of issue #2401. Each write is rolled back.
-- 1. dlq::list_dead_letters, one page.
SELECT id, original_task_id, queue_name, failed_at FROM harvest_dead_letters
 ORDER BY failed_at DESC LIMIT 50;
-- 2. The newest-age probe of the plugin status summary.
SELECT CEIL(EXTRACT(EPOCH FROM (NOW() - MAX(failed_at))))::BIGINT AS age
  FROM harvest_dead_letters;
-- 3. The callback redrive delete of completion_callback::redrive_delivery_at.
BEGIN;
DELETE FROM harvest_dead_letters
 WHERE original_task_id = '00000000-0000-4000-8000-00000000c350' AND task_type = 'CALLBACK';
ROLLBACK;
-- 4. The queue count of a redrive or a bulk replay.
SELECT COUNT(*) FROM harvest_dead_letters WHERE queue_name = 'queue_7';
-- 5. The queue read of a redrive, oldest first, up to the default cap.
SELECT id, original_task_id, queue_name, failed_at FROM harvest_dead_letters
 WHERE queue_name = 'queue_7' ORDER BY failed_at ASC LIMIT 100;
-- 6. The error filter of a redrive. No btree index serves it.
SELECT COUNT(*) FROM harvest_dead_letters WHERE error ILIKE '%request 4242:%';
