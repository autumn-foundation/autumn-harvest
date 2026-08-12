-- Rollback: durable per-execution workflow logs (issue #790)

DROP INDEX IF EXISTS harvest_workflow_logs_exec_seq;

DROP TABLE IF EXISTS harvest_workflow_logs;
