-- Step 5 of issue #256: retire harvest_dag_runs now that all DAG runs are
-- workflow executions. Copy legacy classic DAG rows first so historical run
-- listings and backfill de-dupe continue to see them after the table is gone.
INSERT INTO harvest_workflow_executions (
    id,
    workflow_name,
    workflow_id,
    run_id,
    shard_id,
    state,
    input,
    output,
    error,
    parent_id,
    sticky_worker_id,
    queue_name,
    started_at,
    completed_at,
    execution_timeout,
    memo,
    search_attrs,
    created_at,
    assigned_build_id
)
SELECT
    COALESCE(dr.workflow_exec_id, dr.id) AS id,
    dr.dag_name AS workflow_name,
    'sched:' || dr.dag_name || ':' || (FLOOR(EXTRACT(EPOCH FROM dr.logical_date))::BIGINT)::TEXT AS workflow_id,
    dr.id AS run_id,
    0 AS shard_id,
    CASE dr.state
        WHEN 'SUCCESS' THEN 'COMPLETED'
        WHEN 'FAILED' THEN 'FAILED'
        ELSE 'RUNNING'
    END AS state,
    jsonb_build_object(
        '_harvest_migrated_legacy_dag_run', true,
        'dag_run_id', dr.id::TEXT,
        'conf', dr.conf,
        'logical_date', to_jsonb(dr.logical_date),
        'data_interval_start', to_jsonb(dr.data_interval_start),
        'data_interval_end', to_jsonb(dr.data_interval_end)
    ) AS input,
    NULL AS output,
    CASE
        WHEN dr.state = 'FAILED' THEN 'migrated legacy DAG run failed before unified execution'
        ELSE NULL
    END AS error,
    NULL AS parent_id,
    NULL AS sticky_worker_id,
    COALESCE(s.queue_name, 'default') AS queue_name,
    COALESCE(dr.started_at, dr.created_at, dr.logical_date, NOW()) AS started_at,
    CASE
        WHEN dr.state IN ('SUCCESS', 'FAILED') THEN COALESCE(dr.completed_at, dr.started_at, dr.created_at, dr.logical_date, NOW())
        ELSE dr.completed_at
    END AS completed_at,
    NULL AS execution_timeout,
    NULL AS memo,
    jsonb_build_object(
        'legacy_dag_run_id', dr.id::TEXT,
        'dag_logical_date', to_jsonb(dr.logical_date)
    ) AS search_attrs,
    COALESCE(dr.created_at, NOW()) AS created_at,
    NULL AS assigned_build_id
FROM harvest_dag_runs dr
LEFT JOIN harvest_schedules s ON s.dag_name = dr.dag_name
ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS harvest_dag_runs;
