CREATE INDEX CONCURRENTLY idx_spike_activity_pending
    ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'))
    WHERE event_type IN ('ActivityScheduled','ActivityCompleted','ActivityFailed','ActivityTimedOut');
