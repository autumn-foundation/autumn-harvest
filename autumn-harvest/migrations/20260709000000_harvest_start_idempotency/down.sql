-- Revert request-scoped start idempotency (issue #808).
DROP TABLE IF EXISTS harvest_start_idempotency;
