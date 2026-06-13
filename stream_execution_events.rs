    Path(exec_id_raw): Path<String>,
    headers: axum::http::HeaderMap,
    session: Option<axum::extract::Extension<Session>>,
) -> axum::response::Response {
    use autumn_harvest::audit::{OP_EXECUTION_STREAM_OPEN, STATUS_SUCCEEDED, TARGET_WORKFLOW};
    use autumn_harvest::models::NewAuditRecord;
    use autumn_harvest::notify::{WorkflowEventListener, WorkflowEventWaitOutcome};
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::SinkExt as _;

    // Parse execution ID
    let exec_id = match parse_execution_id(&exec_id_raw) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // Auth check — rejects unauthenticated requests with 401 (issue #174)
    if !has_harvest_admin_access(&api_state, session.map(|s| s.0)).await {
        return AutumnError::unauthorized_msg("authentication required").into_response();
    }

    // Extract Last-Event-ID for resume (harvest_events.id BIGSERIAL cursor).
    // An absent header means "start from the beginning" (cursor = -1).
    // A present but non-parseable value is a client error → 400.
    let last_row_id: i64 = match headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
        None => -1,
        Some(s) => match s.parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "invalid_last_event_id",
                        "message": "Last-Event-ID must be a valid i64"
                    })),
                )
                    .into_response();
            }
        },
    };

    // Resolve the LISTEN/NOTIFY database URL for this execution's shard
    let shard = exec_id.shard();
    let notification_url = match api_state.sse_notification_url(shard) {
        Ok(url) => url,
        Err(e) => return map_error(e).into_response(),
    };

    // Establish LISTEN connection before the backfill query to avoid the
    // race where new events are committed between the query and LISTEN setup
    let listener = match WorkflowEventListener::connect(&notification_url).await {
        Ok(l) => l,
        Err(e) => return map_error(e).into_response(),
    };

    // Get a pooled connection for the initial verification and backfill
    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    // Verify the execution exists
    let execution = match load_execution(&mut conn, exec_id).await {
        Ok(e) => e,
        Err(e) => return map_error(e).into_response(),
    };

    // Load backfill events, capped at buffer_depth + 1. Fetching one extra lets
    // us distinguish "exactly buffer_depth events" from "client is too far behind"
    // without loading an unbounded history into memory.
    let buffer_depth = api_state.sse_buffer_depth();
    let backfill = match store::load_events_after_row_id(
        &mut conn,
        exec_id,
        last_row_id,
        i64::try_from(buffer_depth + 1).ok(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => return map_error(e).into_response(),
    };

    // Slow-consumer check: if reconnecting client is too far behind, return 409
    if backfill.len() > buffer_depth {
        let drop_id = backfill.last().map_or(last_row_id, |r| r.id);
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "slow_consumer",
                "drop_after_event_id": drop_id,
            })),
        )
            .into_response();
    }

    let terminal = is_terminal_state(&execution.state);
    let execution_state = execution.state.to_lowercase().replace('_', "-");

    // Audit stream open (issue #158: only stream open/close are audited, not per-event)
    // Capture audit context now so the producer task can write stream-close on exit.
    let (audit_actor, audit_source, audit_request_id) = audit_context(&headers, &api_state);
    {
        let target = exec_id.to_string();
        let ar = NewAuditRecord {
            actor: &audit_actor,
            operation: OP_EXECUTION_STREAM_OPEN,
            target_type: TARGET_WORKFLOW,
            target_id: Some(target.as_str()),
            route_or_command: "GET /executions/{exec_id}/events/stream",
            request_id: audit_request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(shard.as_i32()),
            source: &audit_source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
    }

    // Release the pooled DB connection — SSE streams must not hold connections while idle
    drop(conn);

    // Bounded channel: capacity = buffer_depth.  When the receiver (axum SSE) drops,
    // further sends fail and the producer task shuts down cleanly.
    let (mut tx, rx) =
        futures::channel::mpsc::channel::<Result<Event, std::convert::Infallible>>(buffer_depth);

    let keepalive_interval = api_state.sse_keepalive_interval();
    let api_clone = api_state.clone();

    // Producer task: runs independently of the HTTP handler after we return
    tokio::spawn(async move {
        use autumn_harvest::audit::OP_EXECUTION_STREAM_CLOSE;

        // Helper: extract the inner payload from the adjacently-tagged envelope
        // `{"type":"...","data":{...}}` — the `event:` field already carries the
        // type, so `data:` should contain only the payload object.
        let sse_data = |event_data: &serde_json::Value| -> String {
            let inner = event_data.get("data").unwrap_or(event_data);
            serde_json::to_string(inner).unwrap_or_default()
        };

        // Helper: flush a slice of DB rows into the SSE channel.
        // Returns the last `row.id` seen and the first terminal state name found,
        // or breaks early if the channel is full / dropped.
        // Using a macro-style closure here because closures can't easily `break 'notify`.
        // We use a boolean return: (last_id, terminal_state, should_break).
        let send_rows =
            |rows: &[autumn_harvest::models::HarvestEvent],
             mut cur_last_seen: i64,
             tx: &mut futures::channel::mpsc::Sender<Result<Event, std::convert::Infallible>>|
             -> (i64, Option<&'static str>, bool) {
                let mut found_terminal: Option<&'static str> = None;
                for row in rows {
                    let sse_event = Event::default()
                        .id(row.id.to_string())
                        .event(row.event_type.as_str())
                        .data(sse_data(&row.event_data));
                    if tx.try_send(Ok(sse_event)).is_err() {
                        return (cur_last_seen, None, true);
                    }
                    cur_last_seen = row.id;
                    if is_terminal_event_type(&row.event_type) {
                        found_terminal = Some(terminal_event_type_to_state(&row.event_type));
                    }
                }
                (cur_last_seen, found_terminal, false)
            };

        // ── 1. Send backfill events (events already committed before this request) ──
        // Also track whether a terminal event appears in the backfill: the execution
        // may have transitioned between load_execution and load_events_after_row_id.
        let mut backfill_terminal: Option<&'static str> = None;
        for row in &backfill {
            let sse_event = Event::default()
                .id(row.id.to_string())
                .event(row.event_type.as_str())
                .data(sse_data(&row.event_data));
            if tx.send(Ok(sse_event)).await.is_err() {
                // Client disconnected during backfill — skip straight to close audit
                if let Ok(mut conn) = db_conn_for_execution(&api_clone, exec_id).await {
                    let target = exec_id.to_string();
                    let ar = NewAuditRecord {
                        actor: &audit_actor,
                        operation: OP_EXECUTION_STREAM_CLOSE,
                        target_type: TARGET_WORKFLOW,
                        target_id: Some(target.as_str()),
                        route_or_command: "GET /executions/{exec_id}/events/stream",
                        request_id: audit_request_id.as_deref(),
                        idempotency_key: None,
                        status: STATUS_SUCCEEDED,
                        error_summary: None,
                        shard_id: Some(shard.as_i32()),
                        source: &audit_source,
                    };
                    let _ = audit::insert_audit(&mut conn, &ar).await;
                }
                return;
            }
            if is_terminal_event_type(&row.event_type) {
