-- Remove required_capabilities column from harvest_task_queue (issue #382).
ALTER TABLE harvest_task_queue
    DROP COLUMN required_capabilities;
