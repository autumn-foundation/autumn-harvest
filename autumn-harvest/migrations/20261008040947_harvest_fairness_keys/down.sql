SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_fairness_weights;
DROP TABLE IF EXISTS harvest_fairness_state;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS fairness_key;
