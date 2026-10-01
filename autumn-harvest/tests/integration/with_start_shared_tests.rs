#![cfg(feature = "db")]
#![allow(clippy::too_many_lines, clippy::await_holding_lock)]
//! Characterization tests for the shared start path of signal-with-start and
//! update-with-start (issue #1440).
//!
//! Both routes run the same admission steps. These tests pin each step on
//! both routes, so a shared implementation cannot change behavior.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to run against a local Postgres. Without
//! it, a testcontainers Postgres starts.

use std::sync::Mutex;

use autumn_harvest::concurrency::ConcurrencyOnConflict;
use autumn_harvest::error::{HarvestError, PayloadKind};
use autumn_harvest::execution::{
    SignalWithStartOutcome, SignalWithStartParams, UpdateWithStartOutcome, UpdateWithStartParams,
    signal_with_start_workflow_execution, update_with_start_workflow_execution,
};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions as wf;
use autumn_harvest::types::{ExecutionId, StartSource, UpdateId, WorkflowIdReusePolicy};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Each test scrubs the shared database, so the tests run one at a time.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

async fn setup() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for stmt in [
        "DELETE FROM harvest_signals",
        "DELETE FROM harvest_task_queue",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .expect("scrub");
    }
    (conn, container)
}

fn sws<'a>(id: &'a str, policy: WorkflowIdReusePolicy) -> SignalWithStartParams<'a> {
    SignalWithStartParams {
        workflow_name: "ws_wf",
        workflow_id: id,
        exec_id: ExecutionId::new(),
        input: json!({"hello": "world"}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: policy,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: ConcurrencyOnConflict::Defer,
        signal_name: "go",
        signal_payload: json!({}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        reject_fresh_if_debounced: false,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        workflow_info: None,
        start_source_override: None,
        start_source_ref_override: None,
    }
}

fn uws<'a>(id: &'a str, policy: WorkflowIdReusePolicy) -> UpdateWithStartParams<'a> {
    UpdateWithStartParams {
        workflow_name: "ws_wf",
        workflow_id: id,
        exec_id: ExecutionId::new(),
        input: json!({"hello": "world"}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: policy,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: ConcurrencyOnConflict::Defer,
        update_id: UpdateId::new(),
        update_name: "add".to_string(),
        update_args: json!({}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
    }
}

const ALLOW: WorkflowIdReusePolicy = WorkflowIdReusePolicy::AllowDuplicate;

async fn run_sws(
    conn: &mut AsyncPgConnection,
    p: SignalWithStartParams<'_>,
) -> Result<SignalWithStartOutcome, HarvestError> {
    signal_with_start_workflow_execution(conn, p).await
}

async fn run_uws(
    conn: &mut AsyncPgConnection,
    p: UpdateWithStartParams<'_>,
) -> Result<UpdateWithStartOutcome, HarvestError> {
    update_with_start_workflow_execution(conn, p).await
}

async fn row(conn: &mut AsyncPgConnection, id: ExecutionId) -> WorkflowExecution {
    wf::table
        .find(id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("row")
}

async fn set_state(conn: &mut AsyncPgConnection, id: ExecutionId, state: &str) {
    diesel::update(wf::table.find(id.as_uuid()))
        .set((
            wf::state.eq(state),
            wf::completed_at.eq((state != "PAUSED").then(chrono::Utc::now)),
        ))
        .execute(conn)
        .await
        .expect("set state");
}

async fn count_rows(conn: &mut AsyncPgConnection, id: &str) -> i64 {
    wf::table
        .filter(wf::workflow_id.eq(id))
        .count()
        .get_result(conn)
        .await
        .expect("count")
}

/// Seed one RUNNING run for `id`.
async fn seed(conn: &mut AsyncPgConnection, id: &str) -> ExecutionId {
    run_sws(conn, sws(id, ALLOW)).await.expect("seed").exec_id
}

// ── Debounce gate ───────────────────────────────────────────────────────────

#[tokio::test]
async fn debounced_fresh_start_is_rejected_and_rolled_back_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let mut p = sws("deb-sws", ALLOW);
    p.reject_fresh_if_debounced = true;
    let err = run_sws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(err, HarvestError::DebounceFreshStart { .. }),
        "{err:?}"
    );
    assert_eq!(count_rows(&mut conn, "deb-sws").await, 0);

    let mut p = uws("deb-uws", ALLOW);
    p.reject_fresh_if_debounced = true;
    let err = run_uws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(err, HarvestError::DebounceFreshStart { .. }),
        "{err:?}"
    );
    assert_eq!(count_rows(&mut conn, "deb-uws").await, 0);
}

#[tokio::test]
async fn debounced_call_attaches_to_a_live_run_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let prior = seed(&mut conn, "deb-live-sws").await;
    let mut p = sws("deb-live-sws", ALLOW);
    p.reject_fresh_if_debounced = true;
    let out = run_sws(&mut conn, p).await.expect("attach");
    assert_eq!(out.exec_id, prior);
    assert!(!out.started_fresh && out.signal_delivered);

    let prior = seed(&mut conn, "deb-live-uws").await;
    let mut p = uws("deb-live-uws", ALLOW);
    p.reject_fresh_if_debounced = true;
    let out = run_uws(&mut conn, p).await.expect("attach");
    assert_eq!(out.exec_id, prior);
    assert!(!out.started_fresh && out.update_admitted);
}

#[tokio::test]
async fn debounced_call_never_escalates_a_terminal_prior_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    for id in ["deb-term-sws", "deb-term-uws"] {
        let prior = seed(&mut conn, id).await;
        set_state(&mut conn, prior, "COMPLETED").await;
        let err = if id.ends_with("sws") {
            let mut p = sws(id, ALLOW);
            p.reject_fresh_if_debounced = true;
            run_sws(&mut conn, p).await.unwrap_err()
        } else {
            let mut p = uws(id, ALLOW);
            p.reject_fresh_if_debounced = true;
            run_uws(&mut conn, p).await.unwrap_err()
        };
        assert!(
            matches!(err, HarvestError::DebounceFreshStart { .. }),
            "{err:?}"
        );
        assert_eq!(count_rows(&mut conn, id).await, 1, "no fresh run for {id}");
        assert_eq!(row(&mut conn, prior).await.state, "COMPLETED");
    }
}

// ── Input cap ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn start_input_cap_applies_to_a_fresh_start_only_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let mut p = sws("cap-sws", ALLOW);
    p.max_workflow_input_bytes = 4;
    let err = run_sws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(
            err,
            HarvestError::PayloadTooLarge {
                kind: PayloadKind::WorkflowInput,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(
        count_rows(&mut conn, "cap-sws").await,
        0,
        "fresh run rolled back"
    );

    let mut p = uws("cap-uws", ALLOW);
    p.max_workflow_input_bytes = 4;
    let err = run_uws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(
            err,
            HarvestError::PayloadTooLarge {
                kind: PayloadKind::WorkflowInput,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(
        count_rows(&mut conn, "cap-uws").await,
        0,
        "fresh run rolled back"
    );

    // An attach writes no start input, so the cap does not apply.
    seed(&mut conn, "cap-live-sws").await;
    let mut p = sws("cap-live-sws", ALLOW);
    p.max_workflow_input_bytes = 4;
    assert!(!run_sws(&mut conn, p).await.expect("attach").started_fresh);

    seed(&mut conn, "cap-live-uws").await;
    let mut p = uws("cap-live-uws", ALLOW);
    p.max_workflow_input_bytes = 4;
    assert!(!run_uws(&mut conn, p).await.expect("attach").started_fresh);
}

#[tokio::test]
async fn start_input_cap_applies_to_the_escalated_fresh_start_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let prior = seed(&mut conn, "cap-esc-sws").await;
    set_state(&mut conn, prior, "COMPLETED").await;
    let mut p = sws("cap-esc-sws", ALLOW);
    p.max_workflow_input_bytes = 4;
    let err = run_sws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(err, HarvestError::PayloadTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(count_rows(&mut conn, "cap-esc-sws").await, 1);

    let prior = seed(&mut conn, "cap-esc-uws").await;
    set_state(&mut conn, prior, "COMPLETED").await;
    let mut p = uws("cap-esc-uws", ALLOW);
    p.max_workflow_input_bytes = 4;
    let err = run_uws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(err, HarvestError::PayloadTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(count_rows(&mut conn, "cap-esc-uws").await, 1);
}

#[tokio::test]
async fn signal_payload_cap_yields_to_already_exists_under_reject_duplicate() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    seed(&mut conn, "sigcap").await;
    let mut p = sws("sigcap", WorkflowIdReusePolicy::RejectDuplicate);
    p.max_signal_payload_bytes = 2;
    p.signal_payload = json!({"big": "payload"});
    let err = run_sws(&mut conn, p).await.unwrap_err();
    assert!(matches!(err, HarvestError::AlreadyExists { .. }), "{err:?}");

    let mut p = sws("sigcap", ALLOW);
    p.max_signal_payload_bytes = 2;
    p.signal_payload = json!({"big": "payload"});
    let err = run_sws(&mut conn, p).await.unwrap_err();
    assert!(
        matches!(
            err,
            HarvestError::PayloadTooLarge {
                kind: PayloadKind::SignalPayload,
                ..
            }
        ),
        "{err:?}"
    );
}

// ── Escalation, terminate, pause ────────────────────────────────────────────

#[tokio::test]
async fn terminal_prior_escalates_to_a_fresh_run_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let prior = seed(&mut conn, "esc-sws").await;
    set_state(&mut conn, prior, "COMPLETED").await;
    let out = run_sws(&mut conn, sws("esc-sws", ALLOW))
        .await
        .expect("sws");
    assert!(out.started_fresh && out.signal_delivered);
    assert_ne!(out.exec_id, prior);

    let prior = seed(&mut conn, "esc-uws").await;
    set_state(&mut conn, prior, "FAILED").await;
    let out = run_uws(&mut conn, uws("esc-uws", ALLOW))
        .await
        .expect("uws");
    assert!(out.started_fresh && out.update_admitted);
    assert_ne!(out.exec_id, prior);
    assert_eq!(row(&mut conn, prior).await.state, "FAILED");
}

#[tokio::test]
async fn terminate_if_running_cancels_the_prior_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;
    let policy = WorkflowIdReusePolicy::TerminateIfRunning;

    let prior = seed(&mut conn, "term-sws").await;
    let out = run_sws(&mut conn, sws("term-sws", policy))
        .await
        .expect("sws");
    assert!(out.started_fresh);
    assert_eq!(row(&mut conn, prior).await.state, "CANCELLED");
    assert_eq!(row(&mut conn, out.exec_id).await.state, "RUNNING");

    let prior = seed(&mut conn, "term-uws").await;
    let out = run_uws(&mut conn, uws("term-uws", policy))
        .await
        .expect("uws");
    assert!(out.started_fresh);
    assert_eq!(row(&mut conn, prior).await.state, "CANCELLED");
    assert_eq!(row(&mut conn, out.exec_id).await.state, "RUNNING");
}

#[tokio::test]
async fn paused_prior_buffers_a_signal_but_rejects_an_update() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;

    let prior = seed(&mut conn, "pause-sws").await;
    set_state(&mut conn, prior, "PAUSED").await;
    let out = run_sws(&mut conn, sws("pause-sws", ALLOW))
        .await
        .expect("sws");
    assert_eq!(out.exec_id, prior);
    assert!(!out.started_fresh && out.signal_delivered);

    let prior = seed(&mut conn, "pause-uws").await;
    set_state(&mut conn, prior, "PAUSED").await;
    let err = run_uws(&mut conn, uws("pause-uws", ALLOW))
        .await
        .unwrap_err();
    assert!(matches!(err, HarvestError::WorkflowPaused(_)), "{err:?}");
}

// ── Field forwarding and provenance ─────────────────────────────────────────

#[tokio::test]
async fn common_start_fields_reach_the_row_identically_on_both_routes() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;
    let hour = chrono::Duration::hours(1);

    let mut a = sws("fields-sws", ALLOW);
    a.queue_name = "q-fields";
    a.execution_timeout = Some(hour);
    a.chain_execution_timeout = Some(hour * 2);
    a.memo = Some(json!({"m": 1}));
    a.search_attrs = Some(json!({"s": 2}));
    a.owner = Some("team-a");
    a.runbook_url = Some("https://example.test/rb");
    a.severity = Some("high");
    a.context_headers = Some([("h".to_string(), "v".to_string())].into());
    a.sla = Some(hour / 2);
    let a = row(&mut conn, run_sws(&mut conn, a).await.expect("sws").exec_id).await;

    let mut b = uws("fields-uws", ALLOW);
    b.queue_name = "q-fields";
    b.execution_timeout = Some(hour);
    b.chain_execution_timeout = Some(hour * 2);
    b.memo = Some(json!({"m": 1}));
    b.search_attrs = Some(json!({"s": 2}));
    b.owner = Some("team-a");
    b.runbook_url = Some("https://example.test/rb");
    b.severity = Some("high");
    b.context_headers = Some([("h".to_string(), "v".to_string())].into());
    b.sla = Some(hour / 2);
    let b = row(&mut conn, run_uws(&mut conn, b).await.expect("uws").exec_id).await;

    assert_eq!(a.queue_name, "q-fields");
    assert_eq!(a.execution_timeout, Some(hour));
    assert_eq!(a.chain_execution_timeout, Some(hour * 2));
    assert!(a.chain_deadline_at.is_some());
    assert_eq!(a.memo, Some(json!({"m": 1})));
    assert_eq!(a.search_attrs, Some(json!({"s": 2})));
    assert_eq!(a.owner.as_deref(), Some("team-a"));
    assert_eq!(a.severity.as_deref(), Some("high"));
    assert_eq!(a.sla, Some(hour / 2));
    assert_eq!(a.workflow_attempt, 1);
    assert_eq!(a.schedule_id, None);
    assert_eq!(a.retry_of_exec_id, None);
    assert_eq!(a.parent_id, None);

    assert_eq!(a.queue_name, b.queue_name);
    assert_eq!(a.execution_timeout, b.execution_timeout);
    assert_eq!(a.chain_execution_timeout, b.chain_execution_timeout);
    assert_eq!(a.chain_deadline_at.is_some(), b.chain_deadline_at.is_some());
    assert_eq!(a.memo, b.memo);
    assert_eq!(a.search_attrs, b.search_attrs);
    assert_eq!(a.owner, b.owner);
    assert_eq!(a.runbook_url, b.runbook_url);
    assert_eq!(a.severity, b.severity);
    assert_eq!(a.context_headers, b.context_headers);
    assert_eq!(a.sla, b.sla);
    assert_eq!(a.workflow_attempt, b.workflow_attempt);
    assert_eq!(a.input, b.input);
}

#[tokio::test]
async fn start_provenance_is_data_the_routes_set_independently() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (mut conn, _c) = setup().await;
    let src = |r: &WorkflowExecution| {
        (
            r.start_source.clone().unwrap_or_default(),
            r.start_source_ref.clone(),
        )
    };

    // Signal-with-start: ref falls back to the idempotency key, then the id.
    let mut p = sws("prov-sws-key", ALLOW);
    p.idempotency_key = Some("idem-1".to_string());
    let r = row(&mut conn, run_sws(&mut conn, p).await.unwrap().exec_id).await;
    assert_eq!(src(&r), ("signal_with_start".into(), Some("idem-1".into())));

    let r = row(
        &mut conn,
        run_sws(&mut conn, sws("prov-sws-id", ALLOW))
            .await
            .unwrap()
            .exec_id,
    )
    .await;
    assert_eq!(
        src(&r),
        ("signal_with_start".into(), Some("prov-sws-id".into()))
    );

    // Signal-with-start only: both overrides win over the defaults.
    let mut p = sws("prov-sws-ovr", ALLOW);
    p.idempotency_key = Some("idem-2".to_string());
    p.start_source_override = Some(StartSource::Webhook);
    p.start_source_ref_override = Some("hook-ref".to_string());
    let r = row(&mut conn, run_sws(&mut conn, p).await.unwrap().exec_id).await;
    assert_eq!(src(&r), ("webhook".into(), Some("hook-ref".into())));

    // Update-with-start: no override exists.
    let mut p = uws("prov-uws-key", ALLOW);
    p.idempotency_key = Some("idem-3".to_string());
    let r = row(&mut conn, run_uws(&mut conn, p).await.unwrap().exec_id).await;
    assert_eq!(src(&r), ("update_with_start".into(), Some("idem-3".into())));

    let r = row(
        &mut conn,
        run_uws(&mut conn, uws("prov-uws-id", ALLOW))
            .await
            .unwrap()
            .exec_id,
    )
    .await;
    assert_eq!(
        src(&r),
        ("update_with_start".into(), Some("prov-uws-id".into()))
    );
}
