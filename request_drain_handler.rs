fn parse_worker_filters_api(pairs: &[(String, String)]) -> Result<WorkerFilters, AutumnError> {
    parse_worker_filters(pairs).map_err(AutumnError::bad_request_msg)
}

// ---------------------------------------------------------------------------
// Remote drain controls (issue #170)
// ---------------------------------------------------------------------------

/// Request body for `POST /workers/{worker_id}/drain`.
#[derive(Debug, Deserialize)]
struct DrainWorkerRequest {
    /// Optional ISO 8601 deadline by which the worker must have drained.
    /// When absent the server uses its configured worker shutdown timeout.
    #[serde(default)]
    deadline_at: Option<String>,
}

#[allow(clippy::too_many_lines)]
async fn request_drain_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(worker_id): Path<String>,
    Json(request): Json<DrainWorkerRequest>,
) -> Result<Json<DrainResponse>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    // Track whether the deadline is operator-supplied so we can avoid
    // shortening an existing window when re-draining without --deadline.
    let (deadline_at, deadline_is_explicit) = if let Some(raw) = &request.deadline_at {
        let dt = chrono::DateTime::parse_from_rfc3339(raw).map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid deadline_at '{raw}'; expected RFC 3339 (e.g. 2026-05-09T12:00:00Z)"
            ))
        })?;
        (Some(dt.with_timezone(&chrono::Utc)), true)
    } else {
        // Compute a default deadline from the configured worker shutdown timeout so
        // operators always get a finite drain window even when they omit the field.
        let timeout = api_state.worker_shutdown_timeout();
        let computed = chrono::Duration::from_std(timeout)
            .ok()
            .map(|d| chrono::Utc::now() + d);
        (computed, false)
    };

    // Search every shard for the worker — workers are registered on exactly
    // one shard, so the first hit wins. Connection failures on individual shards
    // are recorded as unavailable rather than aborting the whole request (AC #8).
    let mut unavailable_shards: Vec<i32> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            unavailable_shards.push(shard_id.as_i32());
            continue;
        };

        let mut response = request_drain(
            &mut conn,
            &worker_id,
            deadline_at,
            deadline_is_explicit,
            stale_threshold,
        )
        .await
        .map_err(map_error)?;

        if response.outcome == autumn_harvest::workers::DrainOutcome::NotFound {
            continue;
        }

        response.unavailable_shards = std::mem::take(&mut unavailable_shards);

        let ar = NewAuditRecord {
            actor: &actor,
            source: &source,
            operation: OP_WORKER_DRAIN,
            target_type: TARGET_WORKER,
            target_id: Some(worker_id.as_str()),
            route_or_command: "POST /workers/{worker_id}/drain",
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(shard_id.as_i32()),
        };
        audit::insert_audit(&mut conn, &ar)
            .await
            .map_err(map_error)?;

        return Ok(Json(response));
    }

    // Worker not found on any reachable shard. If some shards were unavailable
    // the worker may live there — return a degraded 200 rather than 404.
    // Write an audit record on any reachable shard so the attempt is traceable
    // even when the owning shard is down.
    if !unavailable_shards.is_empty() {
        'audit: for (_shard_id, shard_pool) in pool.iter_shards() {
            if let Ok(mut conn) = acquire_conn(shard_pool).await {
                let ar = NewAuditRecord {
                    actor: &actor,
                    source: &source,
                    operation: OP_WORKER_DRAIN,
                    target_type: TARGET_WORKER,
                    target_id: Some(worker_id.as_str()),
                    route_or_command: "POST /workers/{worker_id}/drain",
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some(
                        "degraded: worker not found on reachable shards; may exist on unavailable shard",
                    ),
                    shard_id: None,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
                break 'audit;
            }
        }
        return Ok(Json(DrainResponse {
            worker_id: worker_id.clone(),
            outcome: autumn_harvest::workers::DrainOutcome::NotFound,
            in_flight_count: 0,
            drain_deadline_at: None,
            shard_ids: vec![],
            unavailable_shards,
        }));
    }

    // All shards reachable but the worker ID was absent on every one.
    // Write an audit record on any available shard so that
    // `harvest audit list --operation worker.drain --target-id <id>`
    // shows the attempted drain even for a 404 response.
    'audit: for (_shard_id, shard_pool) in pool.iter_shards() {
        if let Ok(mut conn) = acquire_conn(shard_pool).await {
            let ar = NewAuditRecord {
                actor: &actor,
                source: &source,
                operation: OP_WORKER_DRAIN,
                target_type: TARGET_WORKER,
                target_id: Some(worker_id.as_str()),
                route_or_command: "POST /workers/{worker_id}/drain",
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("worker not found"),
                shard_id: None,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            break 'audit;
        }
    }
    Err(AutumnError::not_found_msg(format!("worker '{worker_id}'")))
}

async fn drain_preview_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<Vec<DrainPreviewItem>>, AutumnError> {
    let filters = parse_worker_filters_api(&pairs)?;
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    let per_shard_filters = WorkerFilters {
        limit: i64::MAX,
        ..filters.clone()
    };

    let mut results: Vec<DrainPreviewItem> = Vec::new();
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut items = drain_preview(&mut conn, &per_shard_filters, stale_threshold)
            .await
            .map_err(map_error)?;
        results.append(&mut items);
    }

    results.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
