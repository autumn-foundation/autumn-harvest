-- Operator-mutable triage note for live runs (issue #759).
--
-- `owner` and `severity` already exist on `harvest_workflow_executions`
-- (issue #372, start-time-only ownership metadata). This migration adds the
-- one additive column the operator-mutable triage feature needs beyond
-- those two: a free-text note an operator can set/update/clear at any point
-- in an execution's life via `PATCH /workflows/{id}/triage`.
--
-- Additive nullable column only -- no new WorkflowEvent variant, no replay
-- impact, shard-local (the triage endpoint routes to the execution's own
-- shard via ExecutionId::shard()). Never read by the workflow function --
-- this is purely operator-facing metadata, not event-sourced workflow
-- state, distinct from author-controlled `search_attrs`.
--
-- Deliberately no index: unlike `owner`/`severity` (which back
-- `GET /workflows?owner=...&severity=...`, issue #372), the triage note is
-- free text with no filtering requirement in this issue's scope -- it is
-- read and written only by exact-row (primary key / describe) operations.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS triage_note TEXT NULL;
