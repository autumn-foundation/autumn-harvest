-- autumn-harvest/migrations/20260409000000_harvest_initial/down.sql
DROP TABLE IF EXISTS harvest_dead_letters;
DROP TABLE IF EXISTS harvest_timers;
DROP TABLE IF EXISTS harvest_signals;
DROP TABLE IF EXISTS harvest_schedules;
DROP TABLE IF EXISTS harvest_dag_runs;
DROP TABLE IF EXISTS harvest_task_queue;
DROP TABLE IF EXISTS harvest_events;
DROP TABLE IF EXISTS harvest_workflow_executions;
