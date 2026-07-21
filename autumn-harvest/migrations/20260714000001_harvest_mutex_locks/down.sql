DROP SEQUENCE IF EXISTS harvest_mutex_lock_seq;
DROP INDEX IF EXISTS idx_harvest_mutex_waiters_key_id;
DROP TABLE IF EXISTS harvest_mutex_waiters;
DROP INDEX IF EXISTS idx_harvest_mutex_locks_lease;
DROP INDEX IF EXISTS idx_harvest_mutex_locks_holder;
DROP TABLE IF EXISTS harvest_mutex_locks;
