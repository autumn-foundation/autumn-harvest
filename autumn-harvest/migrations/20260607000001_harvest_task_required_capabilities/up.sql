-- Add required_capabilities column to harvest_task_queue for rolling deploy capability-gated activity routing (issue #382).
ALTER TABLE harvest_task_queue
    ADD COLUMN required_capabilities JSONB NULL;
