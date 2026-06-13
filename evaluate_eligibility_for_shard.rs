    clippy::cast_precision_loss,
    clippy::too_many_lines
)]
async fn evaluate_eligibility_for_shard(
    api_state: &HarvestApiState,
    shard_id: ShardId,
    queue_name: &str,
    target_task_id: Option<uuid::Uuid>,
) -> Result<ShardEligibilityResponse, AutumnError> {
    use std::collections::{HashMap, HashSet};
    let mut conn = db_conn_for_shard(api_state, shard_id).await?;

    let mut tasks = Vec::new();
    if let Some(task_id) = target_task_id {
        let task = harvest_task_queue::table
            .find(task_id)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .first::<autumn_harvest::models::TaskQueueItem>(&mut conn)
            .await
            .optional()
            .map_err(database_error)?;
        if let Some(t) = task {
            tasks.push(t);
        }
    } else {
        tasks = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .order((
                harvest_task_queue::priority.desc(),
                harvest_task_queue::scheduled_at.asc(),
            ))
            .limit(1000)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .load::<autumn_harvest::models::TaskQueueItem>(&mut conn)
            .await
            .map_err(database_error)?;
    }

    let pending_count = if target_task_id.is_some() {
        i64::from(tasks.iter().any(|t| {
            t.state == "PENDING"
                && t.scheduled_at <= chrono::Utc::now()
                && (t.schedule_to_close_at.is_none()
                    || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
        }))
    } else {
        let count: i64 = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .count()
            .get_result(&mut conn)
            .await
            .map_err(database_error)?;
        count
    };

    let oldest_pending_age_secs = if target_task_id.is_some() {
        tasks
            .as_slice()
            .first()
            .and_then(|t| {
                if t.state == "PENDING"
                    && t.scheduled_at <= chrono::Utc::now()
                    && (t.schedule_to_close_at.is_none()
                        || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
                {
                    Some(t)
                } else {
                    None
                }
            })
            .map(|t| {
                let age = chrono::Utc::now().signed_duration_since(t.scheduled_at);
                age.num_seconds()
            })
    } else {
        let oldest_scheduled: Option<chrono::DateTime<chrono::Utc>> = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .select(harvest_task_queue::scheduled_at)
            .order(harvest_task_queue::scheduled_at.asc())
            .first::<chrono::DateTime<chrono::Utc>>(&mut conn)
            .await
            .optional()
            .map_err(database_error)?;
        oldest_scheduled.map(|ts| {
            let age = chrono::Utc::now().signed_duration_since(ts);
            age.num_seconds()
        })
    };

    let mut required_build_ids = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref bid) = t.required_build_id {
            if !required_build_ids.contains(bid) {
                required_build_ids.push(bid.clone());
            }
        }
    }

    let stale_threshold = api_state.worker_stale_threshold();
    let workers = list_workers(
        &mut conn,
        &WorkerFilters {
            limit: i64::MAX,
            ..Default::default()
        },
        stale_threshold,
    )
    .await
    .map_err(map_error)?;

    let online_workers: Vec<_> = workers
        .into_iter()
        .filter(|w| w.health == autumn_harvest::workers::WorkerHealth::Healthy)
        .collect();

    let compat_set = autumn_harvest::build_routing::load_compat_set(&mut conn)
        .await
        .map_err(map_error)?;

    let mut keys_to_check = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref k) = t.concurrency_key {
            if !keys_to_check.contains(k) {
                keys_to_check.push(k.clone());
            }
        }
    }

    let mut running_map = HashMap::new();
    if !keys_to_check.is_empty() {
        #[derive(diesel::QueryableByName)]
        struct ConcurrencyRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            key: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            task_type: String,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            running_count: i64,
        }

        let rows: Vec<ConcurrencyRow> = diesel::sql_query(
            "SELECT concurrency_key AS key, task_type, COUNT(*) AS running_count \
             FROM harvest_task_queue \
             WHERE state = 'RUNNING' \
               AND concurrency_key = ANY($1) \
               AND worker_id IS NOT NULL \
             GROUP BY concurrency_key, task_type",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&keys_to_check)
        .load(&mut conn)
        .await
        .map_err(database_error)?;

        for r in rows {
            running_map.insert((r.key, r.task_type), r.running_count);
        }
    }

    let cb_activities = api_state
        .runtime()
        .ok()
        .map(|r| {
            r.registry()
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec()
        })
        .unwrap_or_default();

    let mut rate_limit_keys = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref rlk) = t.rate_limit_key {
            let has_cb = t
                .activity_name
                .as_ref()
                .is_some_and(|act_name| cb_activities.contains(act_name));
            if !has_cb && !rate_limit_keys.contains(rlk) {
                rate_limit_keys.push(rlk.clone());
            }
        }
    }

    let mut saturated_rate_limits = HashSet::new();
    if !rate_limit_keys.is_empty() {
        #[derive(diesel::QueryableByName)]
        struct RateLimitRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            key: String,
            #[diesel(sql_type = diesel::sql_types::Double)]
            tokens: f64,
            #[diesel(sql_type = diesel::sql_types::Double)]
            burst: f64,
            #[diesel(sql_type = diesel::sql_types::Double)]
            refill_rate: f64,
            #[diesel(sql_type = diesel::sql_types::Timestamptz)]
            last_refilled_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<RateLimitRow> = diesel::sql_query(
            "SELECT key, tokens, burst, refill_rate, last_refilled_at \
             FROM harvest_rate_limit_buckets \
             WHERE key = ANY($1)",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&rate_limit_keys)
        .load(&mut conn)
        .await
        .map_err(database_error)?;

        for r in rows {
            let elapsed = chrono::Utc::now()
                .signed_duration_since(r.last_refilled_at)
                .num_milliseconds() as f64
                / 1000.0;
            let current_tokens = (r.tokens + elapsed * r.refill_rate).min(r.burst);
            if current_tokens < 1.0 {
                saturated_rate_limits.insert(r.key);
            }
        }
    }

    let mut eligible_workers = Vec::new();
    let mut ineligible_workers = Vec::new();

    let registry = api_state.runtime().map(|r| r.registry().clone()).ok();

    let pending_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| {
            t.state == "PENDING"
                && t.scheduled_at <= chrono::Utc::now()
                && (t.schedule_to_close_at.is_none()
                    || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
        })
        .collect();

    for w in &online_workers {
        let w_id = w.worker.worker_id.clone();
        let build_id = w.worker.build_id.clone();
        let deployment_name = w.worker.deployment_name.clone();
        let shard_assignments: Vec<i32> = w
            .worker
            .shard_assignments
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                    .collect()
            })
            .unwrap_or_default();
        let status = w.worker.status.clone();

        let w_info = EligibleWorkerInfo {
            worker_id: w_id.clone(),
            build_id,
            deployment_name,
            shard_assignments: shard_assignments.clone(),
            status: status.clone(),
            in_flight_count: w.worker.in_flight_count,
            max_concurrency: w.worker.max_concurrency,
        };

        let mut worker_reasons = Vec::new();

        let subscribed_queues: Vec<String> = w
            .worker
            .queues
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if !subscribed_queues.contains(&queue_name.to_string()) {
            worker_reasons.push("wrong_queue_subscription".to_string());
        }

        if !shard_assignments.contains(&(shard_id.as_i32())) {
            worker_reasons.push("wrong_shard_assignment".to_string());
        }

        if status == "Draining" {
            worker_reasons.push("worker_draining".to_string());
        }

        if status == "Stopped" {
            worker_reasons.push("worker_stopped".to_string());
        }

        if !worker_reasons.is_empty() {
            ineligible_workers.push(IneligibleWorkerInfo {
                worker_id: w_id,
                reason_codes: worker_reasons,
            });
            continue;
        }

        if pending_tasks.is_empty() {
            eligible_workers.push(w_info);
        } else {
            let mut eligible_for_any = false;
            let mut task_failures = Vec::new();

            for t in &pending_tasks {
                let mut reasons = Vec::new();

                if !compat_set.is_eligible(&w.worker.build_id, t.required_build_id.as_deref()) {
                    reasons.push("build_incompatible".to_string());
                }

                if let (Some(sticky_worker), Some(sticky_until)) =
                    (&t.sticky_worker_id, &t.sticky_until)
                {
                    if *sticky_until > chrono::Utc::now() && w.worker.worker_id != *sticky_worker {
                        reasons.push("sticky_owned_by_other_worker".to_string());
                    }
                }

                if let (Some(key), Some(cap)) = (&t.concurrency_key, t.concurrency_cap) {
                    let running = running_map
                        .get(&(key.clone(), t.task_type.clone()))
                        .copied()
                        .unwrap_or(0);
                    if running >= i64::from(cap) {
                        reasons.push("concurrency_saturated".to_string());
                    }
                }

                if let Some(ref rlk) = t.rate_limit_key {
                    let has_cb = t
                        .activity_name
                        .as_ref()
                        .is_some_and(|act_name| cb_activities.contains(act_name));
                    if !has_cb && saturated_rate_limits.contains(rlk) {
                        reasons.push("rate_limit_saturated".to_string());
                    }
                }

                let parsed_reqs = if t.task_type == "activity" {
                    t.required_capabilities.as_ref().map_or_else(
                        || {
                            if let Some(ref act_name) = t.activity_name
                                && let Some(ref reg) = registry
                                && let Some(activity) = reg.activities.get(act_name)
                                && let Some(req_str) = activity.requires
                            {
                                autumn_harvest::eligibility::parse_requirements(req_str).ok()
                            } else {
                                None
                            }
                        },
                        |req_val| {
                            serde_json::from_value::<Vec<autumn_harvest::eligibility::Requirement>>(
                                req_val.clone(),
                            )
                            .ok()
                        },
                    )
                } else {
                    None
                };

                if let Some(reqs) = parsed_reqs {
                    let worker_labels: std::collections::HashMap<String, String> =
                        serde_json::from_value(w.worker.labels.clone()).unwrap_or_default();
                    for req in &reqs {
                        let satisfied = match req {
                            autumn_harvest::eligibility::Requirement::Exact { key, value } => {
                                worker_labels.get(key) == Some(value)
                            }
                            autumn_harvest::eligibility::Requirement::In { key, values } => {
                                worker_labels
                                    .get(key)
                                    .is_some_and(|val| values.contains(val))
                            }
                        };
                        if !satisfied {
                            match req {
                                autumn_harvest::eligibility::Requirement::Exact { key, value } => {
                                    reasons.push(format!("unsatisfied_requirement:{key}={value}"));
                                }
                                autumn_harvest::eligibility::Requirement::In { key, values } => {
                                    reasons.push(format!(
                                        "unsatisfied_requirement:{key} in [{}]",
                                        values.join(", ")
                                    ));
                                }
                            }
                        }
                    }
                }

                if reasons.is_empty() {
                    eligible_for_any = true;
                    break;
                }
                task_failures.push(reasons);
            }

            if eligible_for_any {
                eligible_workers.push(w_info);
            } else {
                let mut merged_reasons = HashSet::new();
                for tf in task_failures {
                    for r in tf {
                        merged_reasons.insert(r);
                    }
                }
                let mut reason_codes: Vec<String> = merged_reasons.into_iter().collect();
                reason_codes.sort();
                if reason_codes.is_empty() {
                    reason_codes.push("unknown".to_string());
                }
                ineligible_workers.push(IneligibleWorkerInfo {
                    worker_id: w_id,
                    reason_codes,
                });
            }
        }
    }

    let num_online = eligible_workers.len() + ineligible_workers.len();
    let diagnosis = if num_online == 0 {
        "no_online_workers".to_string()
    } else {
        let all_draining = eligible_workers.is_empty()
            && !ineligible_workers.is_empty()
            && ineligible_workers
                .iter()
                .all(|w| w.reason_codes == vec!["worker_draining".to_string()]);
        if all_draining {
            "all_draining".to_string()
        } else {
            let eligible_non_draining: Vec<_> = eligible_workers
                .iter()
                .filter(|w| w.status != "Draining")
                .collect();

            if eligible_workers.is_empty() {
                "no_eligible_workers".to_string()
            } else if !eligible_non_draining.is_empty() {
                let mut all_full = true;
                for w_info in &eligible_non_draining {
                    if w_info.in_flight_count < w_info.max_concurrency {
                        all_full = false;
                        break;
                    }
                }
                if all_full {
                    "all_capacity_full".to_string()
                } else {
                    "healthy".to_string()
                }
            } else {
                "healthy".to_string()
            }
        }
    };

    let summary = EligibilitySummary { diagnosis };

    Ok(ShardEligibilityResponse {
