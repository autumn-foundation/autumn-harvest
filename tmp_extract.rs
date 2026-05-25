                        task,
                        &prepared.execution,
                        prepared.exec_id,
                        next_event_id,
                        worker_id,
                        started_at,
                        current_history_event_count,
                        cap,
                    )
                    .await;
                }
            }
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. }))
                    && commands.iter().all(|c| {
                        matches!(
                            c,
                            WorkflowCommand::SignalExternalWorkflow { .. }
                                | WorkflowCommand::RecordMarker { .. }
                                | WorkflowCommand::RecordUpdateResult { .. }
                                | WorkflowCommand::UpsertSearchAttributes { .. }
                        )
                    }) =>
            {
                // Only enters this path when every non-bookkeeping command in the
                // batch is a SignalExternalWorkflow (or RecordMarker). Mixed batches
                // that also contain ScheduleActivity / StartTimer / etc. fall through
                // to the regular suspension path so those commands are not dropped.
                //
                // Persist bookkeeping commands (update-result events, search-attribute
                // patches) first, just as the RunLocalActivity path does.
                if let Err(e) = persist_update_result_commands(
                    conn,
                    prepared.exec_id,
                    &commands,
                    &mut next_event_id,
                )
                .await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                if let Err(e) =
                    persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                drop(execute_span);
                let items = extract_signal_external_workflow(commands);
                let items_clone = items.clone();
                let new_events = match persist_external_signal_inline(
                    conn,
                    prepared.exec_id,
                    items,
                    &mut next_event_id,
                )
                .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
                            .await;
                    }
                };
                history_events.extend(new_events.clone());
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    return fail_workflow_for_history_cap(
                        conn,
                        &telemetry,
                        task,
                        &prepared.execution,
                        prepared.exec_id,
                        next_event_id,
                        worker_id,
                        started_at,
                        current_history_event_count,
                        cap,
                    )
                    .await;
                }

                // If any signal in the batch was not resolved inline (remains pending/suspended),
                // we must break the loop and suspend the workflow task.
                let mut all_resolved = true;
                for item in &items_clone {
                    if let SignalBatchItem::Signal(run) = item {
                        let resolved = new_events.iter().any(|e| match e {
                            WorkflowEvent::ExternalSignalDelivered { signal_id }
                            | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                                *signal_id == run.signal_id
                            }
                            _ => false,
                        });
                        if !resolved {
                            all_resolved = false;
                            break;
                        }
                    }
                }

                if !all_resolved {
                    let mut reconstructed_commands = Vec::with_capacity(items_clone.len());
                    for item in items_clone {
                        match item {
                            SignalBatchItem::Marker(_) => {
                                // Already persisted via persist_external_signal_inline.
                                // Do not reconstruct or re-append to avoid duplicate marker events in history.
                            }
                            SignalBatchItem::Signal(run) => {
                                let (dummy_tx, _) = tokio::sync::oneshot::channel();
                                reconstructed_commands.push(
                                    WorkflowCommand::SignalExternalWorkflow {
                                        signal_id: run.signal_id,
                                        target: run.target,
                                        signal_name: run.signal_name,
                                        payload: run.payload,
                                        result_tx: dummy_tx,
                                        already_requested: run.already_requested,
                                    },
                                );
                            }
                        }
                    }

                    // Re-acquire a fresh execute_span so persist_workflow_outcome
                    // (via handle_suspended_workflow) gets a valid span reference.
                    let execute_span = tracing::Span::none();
                    break (
                        WorkflowOutcome::Suspended {
                            commands: reconstructed_commands,
                        },
                        pending_cmds,
                        execute_span,
                    );
                }
            }
            // Mixed batch: contains SignalExternalWorkflow AND other durable commands
            // (ScheduleActivity, StartTimer, etc.). The "all signals" guard above did
            // not match because not all commands are signals/markers. Write signal events
            // to history FIRST (so drain_early_signals stashes them on the next replay
            // pass), then break with the remaining commands for handle_suspended_workflow.
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. })) =>
            {
                if let Err(e) = persist_update_result_commands(
                    conn,
