-- Revert: drop the owner-mode switch and the claim guard (issue #2252).
SET LOCAL lock_timeout = '5s';
DROP TRIGGER IF EXISTS harvest_task_queue_owner_claim_guard_trg ON harvest_task_queue;
DROP FUNCTION IF EXISTS harvest_task_queue_owner_claim_guard();
DROP TABLE IF EXISTS harvest_decision_owner_mode;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS decision_partition;
