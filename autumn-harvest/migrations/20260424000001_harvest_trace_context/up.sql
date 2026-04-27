-- Carry W3C trace context alongside each enqueued task so workers can
-- continue the trace of whoever produced the work.
ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS trace_context JSONB;
