async fn handle_suspended_workflow(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    context: SuspendedWorkflowContext<'_>,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    let result = {
        if should_requeue_signal_wait(commands) {
            let marker_events = marker_events_from_commands(commands);
            persist_signal_wait_park(
                conn,
                context.persistence.task.id,
                context.persistence.exec_id,
                context.persistence.next_event_id,
                &marker_events,
            )
            .await
        } else if let Some(scheduled) = extract_single_schedule_activity(commands) {
            persist_scheduled_activity(
                conn,
                registry,
                context.persistence.task.id,
                context.persistence.exec_id,
                context.persistence.next_event_id,
                commands,
                &scheduled,
            )
            .await
        } else if let Some(timer) = extract_single_started_timer(commands) {
            persist_started_timer(
                conn,
                context.persistence.exec_id,
                context.persistence.next_event_id,
                context.persistence.task.id,
                commands,
                &timer,
            )
            .await
        } else if let Some(child) = extract_single_started_child_workflow(commands) {
            persist_started_child_workflow(
                conn,
                registry,
                context.persistence.task.id,
                context.execution,
                context.persistence.next_event_id,
                commands,
                &child,
            )
            .await
        } else {
            let error = suspended_workflow_error(commands);
            persist_workflow_failure(
                conn,
                context.persistence.task.id,
                context.persistence.exec_id,
                context.persistence.next_event_id,
                context.persistence.worker_id,
                &error,
            )
            .await
        }
    };

    fail_execution_on_error(
        conn,
        context.persistence.task,
        context.persistence.worker_id,
        result,
    )
    .await
}
