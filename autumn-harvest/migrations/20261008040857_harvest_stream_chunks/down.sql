-- Rollback: durable workflow output streams (issue #1974).
--
-- Dropping the table drops its foreign key, which locks the hot executions
-- table for a moment. A held lock fails the rollback after 5 s.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS harvest_stream_chunks_exec_offset;

DROP TABLE IF EXISTS harvest_stream_chunks;
