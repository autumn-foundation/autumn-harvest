// autumn-harvest/src/schema.rs
// Hand-written to match migrations/20260409000000_harvest_initial/up.sql.
// Keep in sync with the migration. In production, regenerate via `diesel print-schema`.

diesel::table! {
    use diesel::sql_types::*;

    harvest_workflow_executions (id) {
        id -> Uuid,
        workflow_name -> Text,
        workflow_id -> Text,
        run_id -> Uuid,
        shard_id -> Int4,
        state -> Text,
        input -> Jsonb,
        output -> Nullable<Jsonb>,
        error -> Nullable<Text>,
        parent_id -> Nullable<Uuid>,
        sticky_worker_id -> Nullable<Text>,
        queue_name -> Text,
        started_at -> Timestamptz,
        completed_at -> Nullable<Timestamptz>,
        execution_timeout -> Nullable<Interval>,
        memo -> Nullable<Jsonb>,
        search_attrs -> Nullable<Jsonb>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_events (id) {
        id -> Int8,
        workflow_exec_id -> Uuid,
        event_id -> Int4,
        event_type -> Text,
        event_data -> Jsonb,
        timestamp -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_task_queue (id) {
        id -> Uuid,
        queue_name -> Text,
        task_type -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        activity_name -> Nullable<Text>,
        input -> Jsonb,
        state -> Text,
        priority -> Int4,
        worker_id -> Nullable<Text>,
        attempt -> Int4,
        max_attempts -> Int4,
        scheduled_at -> Timestamptz,
        started_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        last_heartbeat_at -> Nullable<Timestamptz>,
        heartbeat_timeout -> Nullable<Interval>,
        start_to_close -> Nullable<Interval>,
        schedule_to_start -> Nullable<Interval>,
        retry_policy -> Nullable<Jsonb>,
        output -> Nullable<Jsonb>,
        error -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_dag_runs (id) {
        id -> Uuid,
        dag_name -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        state -> Text,
        logical_date -> Timestamptz,
        data_interval_start -> Timestamptz,
        data_interval_end -> Timestamptz,
        conf -> Nullable<Jsonb>,
        started_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_schedules (id) {
        id -> Uuid,
        dag_name -> Text,
        schedule_expr -> Nullable<Text>,
        timezone -> Text,
        catchup -> Bool,
        max_active_runs -> Int4,
        is_paused -> Bool,
        last_run_at -> Nullable<Timestamptz>,
        next_run_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_signals (id) {
        id -> Uuid,
        workflow_exec_id -> Uuid,
        signal_name -> Text,
        payload -> Jsonb,
        received_at -> Timestamptz,
        consumed -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_timers (id) {
        id -> Uuid,
        workflow_exec_id -> Uuid,
        timer_id -> Text,
        fires_at -> Timestamptz,
        fired -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    harvest_dead_letters (id) {
        id -> Uuid,
        original_task_id -> Uuid,
        queue_name -> Text,
        task_type -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        activity_name -> Nullable<Text>,
        input -> Jsonb,
        error -> Text,
        attempts -> Int4,
        failed_at -> Timestamptz,
    }
}

diesel::joinable!(harvest_events -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_task_queue -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_dag_runs -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_signals -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_timers -> harvest_workflow_executions (workflow_exec_id));

diesel::allow_tables_to_appear_in_same_query!(
    harvest_workflow_executions,
    harvest_events,
    harvest_task_queue,
    harvest_dag_runs,
    harvest_schedules,
    harvest_signals,
    harvest_timers,
    harvest_dead_letters,
);
