-- Rollback: durable workflow output streams (issue #1974).

DROP INDEX IF EXISTS harvest_stream_chunks_exec_offset;

DROP TABLE IF EXISTS harvest_stream_chunks;
