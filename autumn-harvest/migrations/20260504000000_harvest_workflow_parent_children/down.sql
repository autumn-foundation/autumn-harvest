DROP INDEX IF EXISTS idx_harvest_we_parent_id;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES harvest_workflow_executions(id) NOT VALID;
