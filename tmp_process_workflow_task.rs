async fn process_workflow_task(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    sticky_timeout: Duration,
    max_local_activity_start_to_close: Duration,
    workflow_cache: Arc<tokio::sync::Mutex<crate::cache::WorkflowCache>>,
) -> HarvestResult<()> {
    let mut prepared =
        prepare_workflow_task_with_cache(conn, task, worker_id, &workflow_cache, sticky_timeout)
            .await?;
    let Some(workflow) = registry.workflows.get(&prepared.execution.workflow_name) else {
        let error = format!(
            "no workflow handler registered for '{}'",
            prepared.execution.workflow_name
        );
        fail_task_and_execution(conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    let telemetry = registry.telemetry().clone();

    // Emit cache hit/miss metric now that we know the workflow name.
    if prepared.was_cache_hit {
        telemetry
            .metrics
            .record_workflow_cache_hit(&prepared.execution.workflow_name, &task.queue_name);
    } else {
        telemetry
            .metrics
            .record_workflow_cache_miss(&prepared.execution.workflow_name, &task.queue_name);
    }

    let trace_carrier = task
        .trace_context
        .as_ref()
        .and_then(TraceContextCarrier::from_json);

    // ADR-0001 §2.6 + §2.7: emit harvest.signal.deliver and harvest.timer.fire
    // spans here, after the trace context is restored, so they are correlated
    // with the workflow execution trace rather than being orphaned.
    // EnteredSpan is !Send; .in_scope() drops it before any subsequent .await.
    // ADR-0001 §2.7: one span per fired timer.
    for timer_id in &prepared.timers_fired {
        tracing::info_span!(
            "harvest.timer.fire",
            "otel.kind" = "internal",
            { ATTR_EXECUTION_ID } = %prepared.exec_id,
            timer.id = %timer_id,
        )
        .in_scope(|| {});
    }
    for signal_name in &prepared.signals_delivered {
        tracing::info_span!(
            "harvest.signal.deliver",
            "otel.kind" = "consumer",
            { ATTR_WORKFLOW_ID } = prepared.execution.workflow_name.as_str(),
            { ATTR_EXECUTION_ID } = %prepared.exec_id,
            signal.name = signal_name.as_str(),
        )
        .in_scope(|| {});
    }

    // Emit workflow.started exactly once per execution.  Two independent
    // conditions must both hold:
    //
    // 1. task.attempt == 1: the task queue has never dispatched this execution
    //    before (attempt starts at 0 and is incremented to 1 on first claim;
    //    signal-resume paths increment it again on re-claim).
    //
    // 2. No scheduling events in history: guards against counting replayed
    //    first-dispatch tasks that already committed scheduling work.
    //    load_workflow_replay_state prepends SignalReceived/TimerFired for
    //    pending signals and fired timers, so checking raw length alone is
    //    unreliable for brand-new workflows.
    let has_scheduling_events = prepared.history_events.iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::ActivityAwaitingExternal { .. }
                | WorkflowEvent::MarkerRecorded { .. }
        )
    });
    if task.attempt == 1 && !has_scheduling_events {
        telemetry
            .metrics
            .record_workflow_started(&prepared.execution.workflow_name, &task.queue_name);
    }
    let started_at = std::time::Instant::now();

    // Drive the workflow in a loop so that local activities can be executed
    // inline without parking the task. Each iteration runs the workflow until
    // it suspends; if it suspends on a RunLocalActivity command the handler
    // is executed here, its events are appended to history, and the workflow
    // is re-run with the extended history. Any other suspension (regular
    // activity, timer, signal wait, …) breaks out of the loop.
    let mut history_events = prepared.history_events;
    let mut next_event_id = prepared.next_event_id;

    let loop_result = loop {
        // Recompute is_replay each iteration: after local-activity events are
        // appended the workflow re-runs in replay mode (history_events.len() > 1).
        // ADR-0001 §2.1: span metadata must reflect the current replay state so
        // harvest.replay and link.traceparent are accurate on every executor call.
        let is_replay = history_events.len() > 1;

        // ADR-0001 §3 + §4: install the producer's trace context only for live
        // (non-replay) iterations so the harvest.workflow.execute span is
        // correctly parented.  For replay iterations the context must NOT be
        // installed — replay spans must be new root spans (the original trace
        // may have long since expired).  Installing per-iteration ensures that
        // when local-activity events push history_events.len() > 1 the
        // transition to is_replay=true correctly clears the live parent context.
        let _iter_parent_guard = trace_carrier
            .as_ref()
            .filter(|_| !is_replay)
            .map(|c| telemetry.install_trace_context(c));

        let span_meta = WorkflowExecuteSpanMeta {
            workflow_name: prepared.execution.workflow_name.clone(),
            shard_id: i64::from(prepared.execution.shard_id),
            queue_name: task.queue_name.clone(),
            is_replay,
            link_traceparent: trace_carrier
                .as_ref()
                .filter(|_| is_replay)
                .and_then(|c| c.link_traceparent.clone().or_else(|| c.traceparent.clone())),
        };

        // Filter declarative handlers to those that target this workflow type.
        let wf_name = prepared.execution.workflow_name.as_str();
        let dq: Vec<&crate::info::QueryHandlerInfo> = registry
            .query_handlers
            .iter()
            .filter(|h| h.workflow == wf_name)
            .collect();
        let du: Vec<&crate::info::UpdateHandlerInfo> = registry
            .update_handlers
            .iter()
            .filter(|h| h.workflow == wf_name)
            .collect();

        let (run_outcome, pending_cmds, execute_span) =
            run_workflow_with_state_history_policy_and_caps(
                prepared.exec_id,
                history_events.clone(),
                workflow.handler,
                task.input.clone(),
                registry.shared_state(),
                registry.history_policy(),
                Some(&span_meta),
                &dq,
                &du,
                wf_name,
                registry.max_activity_input_bytes,
                registry.max_signal_payload_bytes,
                workflow
                    .max_input_bytes
                    .map_or(registry.max_workflow_input_bytes, |per| {
                        per.max(registry.max_workflow_input_bytes)
                    }),
            )
            .await;

        match run_outcome {
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::RunLocalActivity { .. })) =>
            {
                // Apply any search-attribute patches before running the local
                // activity so that attributes are visible even if the worker
                // crashes during inline execution.
                persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await?;
                // Sync in-memory snapshot so a subsequent continue_as_new in the
                // same task copies the patched attrs to the successor row.
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                // Local-activity re-run: drop this iteration's execute span
                // so the OTel span closes before we start inline execution.
                drop(execute_span);
                // If the batch also contains SignalExternalWorkflow commands,
                // write their history events BEFORE the local-activity events.
                // This preserves correct replay ordering: on the next run
                // drain_early_signals stashes the signal events so
                // match_external_signal sees them before LocalActivityScheduled.
                let commands = if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. }))
                {
                    let (signal_items, remaining) = split_mixed_signal_batch(commands);
                    if !signal_items.is_empty() {
                        let new_events = match persist_external_signal_inline(
                            conn,
                            prepared.exec_id,
                            signal_items,
                            &mut next_event_id,
                        )
                        .await
                        {
                            Ok(events) => events,
                            Err(e) => {
                                return fail_execution_on_error(
                                    conn,
                                    task,
                                    worker_id,
                                    Err::<(), _>(e),
                                )
                                .await;
                            }
                        };
                        history_events.extend(new_events);
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
                    }
                    remaining
                } else {
                    commands
                };
                let (markers, local_run) = extract_run_local_activity(commands);
                let inline_outcome = run_local_activity_inline(
                    conn,
                    registry,
                    prepared.exec_id,
                    markers,
                    local_run,
                    max_local_activity_start_to_close,
                    &mut next_event_id,
                )
                .await?;
                let new_events = match inline_outcome {
                    LocalActivityInlineOutcome::Complete(events) => events,
                    LocalActivityInlineOutcome::HistoryCapReached {
                        events,
                        event_count,
                    } => {
                        history_events.extend(events);
                        return fail_workflow_for_history_cap(
                            conn,
                            &telemetry,
                            task,
                            &prepared.execution,
                            prepared.exec_id,
                            next_event_id,
                            worker_id,
                            started_at,
                            event_count,
                            registry
                                .history_policy()
                                .event_hard_cap()
                                .expect("HistoryCapReached requires a configured hard cap"),
                        )
                        .await;
                    }
                };
                history_events.extend(new_events);
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
                let (signal_items, remaining_commands) = split_mixed_signal_batch(commands);
                let new_events = match persist_external_signal_inline(
                    conn,
                    prepared.exec_id,
                    signal_items,
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
                let remaining_commands_with_unresolved = remaining_commands;
                history_events.extend(new_events);
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
                // Re-acquire a fresh execute_span so persist_workflow_outcome
                // (via handle_suspended_workflow) gets a valid span reference.
                // The original span was dropped above.
                let execute_span = tracing::Span::none();
                break (
                    WorkflowOutcome::Suspended {
                        commands: remaining_commands_with_unresolved,
                    },
                    pending_cmds,
                    execute_span,
                );
            }
            other => break (other, pending_cmds, execute_span),
        }
    };

    let (outcome, pending_cmds, execute_span) = loop_result;
    let pending_durable_event_count = match &outcome {
        WorkflowOutcome::Suspended { commands } => {
            match suspended_command_event_count(conn, task.workflow_exec_id, commands).await {
                Ok(count) => count,
                Err(error) => {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(error))
                        .await;
                }
            }
        }
        _ => pending_update_result_event_count(&pending_cmds),
    };
    let current_history_event_count = u64::try_from(history_events.len())
        .unwrap_or(u64::MAX)
        .saturating_add(pending_durable_event_count);

    if let Some(cap) = registry.history_policy().event_hard_cap()
        && current_history_event_count >= cap
        && !matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. })
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

    let status = match &outcome {
        WorkflowOutcome::Completed { .. } => WorkflowStatus::Completed,
        WorkflowOutcome::Failed { .. } => WorkflowStatus::Failed,
        WorkflowOutcome::Suspended { .. } => WorkflowStatus::Suspended,
        WorkflowOutcome::ContinuedAsNew { .. } => WorkflowStatus::ContinuedAsNew,
    };
    telemetry.metrics.record_workflow_completed(
        &prepared.execution.workflow_name,
        &task.queue_name,
        started_at.elapsed().as_secs_f64(),
        status,
    );
    if !matches!(&outcome, WorkflowOutcome::Suspended { .. }) {
        telemetry.metrics.record_workflow_history_size(
            &prepared.execution.workflow_name,
            terminal_history_event_count(next_event_id, &pending_cmds),
        );
    }
    if matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. }) {
        telemetry
            .metrics
            .record_workflow_continue_as_new(&prepared.execution.workflow_name);
    }

    // Append UpdateCompleted/UpdateFailed events and apply search-attribute
    // patches for any commands emitted during this live execution cycle before
    // the terminal event.  For Suspended outcomes these commands are inside the
    // variant and are handled inside handle_suspended_workflow; pending_cmds is
    // only non-empty for Completed/Failed/ContinuedAsNew outcomes.
    if !pending_cmds.is_empty() {
        persist_update_result_commands(conn, prepared.exec_id, &pending_cmds, &mut next_event_id)
            .await?;
        persist_search_attrs_from_commands(conn, prepared.exec_id, &pending_cmds).await?;
        // Keep the in-memory execution snapshot current so that
        // persist_workflow_continue_as_new copies the patched attrs to the
        // successor row rather than the stale pre-patch snapshot.
        prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
            prepared.execution.search_attrs.take(),
            &pending_cmds,
        );
    }

    // Pre-compute the cache action while `outcome` is still accessible (it
    // will be consumed by `persist_workflow_outcome` below).  We do NOT apply
    // the update yet: the cache must only be written AFTER persistence succeeds
    // so that a failed commit never leaves a warm cache snapshot pointing at
    // events that were never durably written.
    //
    // `Some(state)` → insert on success; `None` → evict on success.
    // Cache operations are skipped entirely when sticky routing is disabled.
    let pending_cache_update = if sticky_timeout.is_zero() {
        None
    } else if let WorkflowOutcome::Suspended { .. } = &outcome {
        Some(Some(crate::cache::CachedWorkflowState {
            events: history_events.clone(),
            next_event_id,
        }))
    } else {
        Some(None) // terminal — evict
    };

    persist_workflow_outcome(
        conn,
        registry,
        &prepared.execution,
        WorkflowTaskPersistence {
            task,
            worker_id,
            exec_id: prepared.exec_id,
            next_event_id,
            sticky_timeout,
        },
        outcome,
        &execute_span,
    )
    .await?;
    // execute_span is dropped here, closing the OTel span after all producer
    // spans have been emitted as its children.

    // Update the in-process LRU cache ONLY on successful persistence.
    // A Suspended outcome inserts the warm snapshot; terminal outcomes evict.
    // Skipped entirely when sticky routing is disabled (sticky_timeout == 0).
    if let Some(update) = pending_cache_update {
        let exec_uuid = prepared.exec_id.as_uuid();
        let mut guard = workflow_cache.lock().await;
        match update {
            Some(state) => guard.insert(exec_uuid, state),
            None => {
                guard.remove(&exec_uuid);
            }
        }
    }

    Ok(())
}
