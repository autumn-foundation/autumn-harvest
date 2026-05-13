-- Step 5 of issue #256: retire harvest_dag_runs now that all DAG runs are
-- workflow executions. The table was write-only during the soak window
-- (Steps 3–4); all reads already go through harvest_workflow_executions.

DROP TABLE IF EXISTS harvest_dag_runs;
