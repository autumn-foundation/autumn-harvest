-- Reverse of the waiter-leading index for the mutex terminal sweep.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_mutex_waiters_waiter;
