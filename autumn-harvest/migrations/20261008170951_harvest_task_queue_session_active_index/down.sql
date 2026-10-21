-- Revert: drop the session-keyed seek index (issue #2069).
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_tq_session_active;
