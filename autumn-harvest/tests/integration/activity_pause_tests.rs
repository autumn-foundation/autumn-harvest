#![cfg(feature = "db")]
// Test-code style lints (consistent with the other integration test files).
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::unused_async
)]
//! Per-activity-type pause/resume integration tests — issue #807.
//!
//! An activity pause **holds** dispatch for a named activity *type*: no worker
//! claims new tasks of that activity, held tasks stay `PENDING` (never
//! `FAILED`, never DLQ'd), in-flight instances run to completion, the relative
//! `schedule_to_start` timer is suspended, and a resume makes the held backlog
//! immediately claimable again.
//!
//! These tests prove the behaviour the SQL-shape unit tests in
//! `activity_pause.rs` / `queue.rs` cannot: that the gate actually holds and
//! releases real rows, that a *sibling activity on the same queue is untouched*
//! (the whole reason this exists alongside the coarser queue pause, #619), and
//! that a workflow task is never collaterally held.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle.

use autumn_harvest::activity_pause;
use autumn_harvest::queue::{self, EnqueueParams, TaskType, claim_task};
use autumn_harvest::timeout::{self, TimeoutReason};
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── DB setup ──────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// A migrated Postgres 16 — the env URL when set, else a fresh testcontainer.
async fn setup_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("map postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migrations");
    (url, Some(container))
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = setup_db_url().await;
    let mut conn = connect(&url).await;
    // Each test uses uniquely-named activities, but the pause table is keyed by
    // activity name alone, so scrub it when reusing an env-provided database.
    conn.batch_execute("DELETE FROM harvest_activity_pauses")
        .await
        .expect("scrub activity pauses");
    (conn, container)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    use diesel_async::RunQueryDsl;
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'ap', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue an activity task named `activity` on `queue`.
async fn enqueue_activity(
    conn: &mut AsyncPgConnection,
    queue: &str,
    activity: &str,
    schedule_to_start: Option<chrono::Duration>,
) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some(activity.to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.schedule_to_start = schedule_to_start;
    queue::enqueue(conn, &params).await.expect("enqueue")
}

/// Enqueue a *workflow* task, optionally carrying the engine's
/// `mixed_signal_suspension` sentinel in `activity_name`.
async fn enqueue_workflow(
    conn: &mut AsyncPgConnection,
    queue: &str,
    sentinel: Option<&str>,
) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = sentinel.map(ToString::to_string);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow")
}

async fn task_state(conn: &mut AsyncPgConnection, id: Uuid) -> String {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<S>(conn)
        .await
        .expect("state")
        .state
}

async fn attempt_of(conn: &mut AsyncPgConnection, id: Uuid) -> i32 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct A {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    diesel::sql_query("SELECT attempt FROM harvest_task_queue WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<A>(conn)
        .await
        .expect("attempt")
        .attempt
}

async fn dead_letter_count(conn: &mut AsyncPgConnection) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_dead_letters")
        .get_result::<C>(conn)
        .await
        .expect("dlq count")
        .count
}

/// Backdate a task's `scheduled_at` (and `created_at`, which cannot be later)
/// so it looks like it has been waiting.
async fn backdate_scheduled_at(conn: &mut AsyncPgConnection, id: Uuid, secs: i64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = NOW() - ($2 || ' seconds')::interval, \
             created_at = CASE WHEN created_at IS NULL THEN NULL \
                               ELSE NOW() - ($2 || ' seconds')::interval END \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(secs.to_string())
    .execute(conn)
    .await
    .expect("backdate");
}

/// Backdate a pause's `paused_at` so a resume computes a non-trivial held span.
async fn backdate_paused_at(conn: &mut AsyncPgConnection, activity: &str, secs: i64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_activity_pauses SET paused_at = NOW() - ($2 || ' seconds')::interval \
         WHERE activity_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(activity)
    .bind::<diesel::sql_types::Text, _>(secs.to_string())
    .execute(conn)
    .await
    .expect("backdate pause");
}

async fn claim_one(conn: &mut AsyncPgConnection, queue: &str) -> Option<Uuid> {
    claim_task(conn, &[queue.to_string()], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

/// Drain every claimable task on `queue`, returning the ids in claim order.
async fn claim_all(conn: &mut AsyncPgConnection, queue: &str) -> Vec<Uuid> {
    let mut out = Vec::new();
    while let Some(id) = claim_one(conn, queue).await {
        out.push(id);
    }
    out
}

async fn pause(conn: &mut AsyncPgConnection, activity: &str) -> bool {
    activity_pause::pause_activity(conn, activity, "downstream outage", "oncall", None)
        .await
        .expect("pause")
        .newly_paused
}

// ── AC3 / AC4 — the hold, and what it deliberately does not touch ─────────────

/// The headline behaviour: a paused activity's queued tasks are skipped by
/// claim and stay `PENDING`; a resume makes them claimable again.
#[tokio::test]
async fn pause_holds_dispatch_and_resume_releases_it() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("charge");

    let held = enqueue_activity(&mut conn, &q, &act, None).await;

    assert!(pause(&mut conn, &act).await, "first pause is a fresh hold");

    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "AC3 — a paused activity's task must be skipped by claim"
    );
    assert_eq!(
        task_state(&mut conn, held).await,
        "PENDING",
        "AC3 — a held task stays PENDING; it is never failed"
    );
    assert_eq!(
        dead_letter_count(&mut conn).await,
        0,
        "AC3 / success metric — a hold must produce zero DLQ entries"
    );

    let outcome = activity_pause::resume_activity(&mut conn, &act)
        .await
        .expect("resume");
    assert!(outcome.newly_resumed, "the hold was actually released");

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(held),
        "AC3 — on resume the held task is claimable again"
    );
}

/// **The reason this exists alongside the queue pause (#619).** Two activity
/// types share one queue; pausing one must not delay a single task of the
/// other. This is the issue's own success metric: "zero impact on other
/// activity types sharing the same queue".
#[tokio::test]
async fn a_sibling_activity_on_the_same_queue_is_completely_untouched() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("shared");
    let broken = unique("charge_card");
    let healthy = unique("send_email");

    let broken_task = enqueue_activity(&mut conn, &q, &broken, None).await;
    let healthy_a = enqueue_activity(&mut conn, &q, &healthy, None).await;
    let healthy_b = enqueue_activity(&mut conn, &q, &healthy, None).await;

    pause(&mut conn, &broken).await;

    let mut claimed = claim_all(&mut conn, &q).await;
    claimed.sort();
    let mut expected = vec![healthy_a, healthy_b];
    expected.sort();

    assert_eq!(
        claimed, expected,
        "the sibling activity's tasks must all dispatch normally while its \
         queue-mate is held — pausing one activity must never become a queue pause"
    );
    assert_eq!(
        task_state(&mut conn, broken_task).await,
        "PENDING",
        "and the paused activity's own task is still held"
    );
}

/// AC4 — pause blocks only *new* dispatch. A task already claimed (RUNNING) is
/// untouched by a pause that lands afterwards, and can still be completed.
#[tokio::test]
async fn pause_does_not_abort_in_flight_instances() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("transcode");

    let in_flight = enqueue_activity(&mut conn, &q, &act, None).await;
    let claimed = claim_one(&mut conn, &q).await;
    assert_eq!(
        claimed,
        Some(in_flight),
        "precondition: the task is claimed"
    );
    assert_eq!(task_state(&mut conn, in_flight).await, "RUNNING");

    pause(&mut conn, &act).await;

    assert_eq!(
        task_state(&mut conn, in_flight).await,
        "RUNNING",
        "AC4 — a pause must not abort an in-flight instance"
    );

    queue::complete_task(&mut conn, in_flight, serde_json::json!({"ok": true}))
        .await
        .expect("in-flight task must still be completable while its type is paused");
    assert_eq!(
        task_state(&mut conn, in_flight).await,
        "COMPLETED",
        "AC4 — an in-flight instance runs to completion"
    );
}

/// The claim-path pre-filter is an array membership test, which is **not**
/// NULL-safe — and a *workflow* row can carry the engine's
/// `mixed_signal_suspension` sentinel in `activity_name`. Neither shape may be
/// collaterally held: an activity pause that stopped workflow dispatch would
/// halt the fleet.
#[tokio::test]
async fn a_pause_never_holds_a_workflow_task_even_with_the_sentinel_name() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");

    let plain_wf = enqueue_workflow(&mut conn, &q, None).await;
    let sentinel_wf = enqueue_workflow(&mut conn, &q, Some("mixed_signal_suspension")).await;

    // Pause an ordinary activity, and then the sentinel's own name — the
    // adversarial case, where an operator pauses an activity that happens to
    // share the engine's internal sentinel string.
    pause(&mut conn, &unique("unrelated")).await;
    pause(&mut conn, "mixed_signal_suspension").await;

    let mut claimed = claim_all(&mut conn, &q).await;
    claimed.sort();
    let mut expected = vec![plain_wf, sentinel_wf];
    expected.sort();

    assert_eq!(
        claimed, expected,
        "workflow tasks must dispatch normally regardless of any activity pause \
         — including one whose name collides with the mixed_signal_suspension sentinel"
    );
}

// ── AC3 — the schedule_to_start carve-out ────────────────────────────────────

/// A held task must not be terminally failed merely because it waited while an
/// operator held its activity type. The scan spares it during the hold, and the
/// resume credits the held time back so the thaw does not immediately kill the
/// released backlog instead.
///
/// The credit is **held time only** — the span from `paused_at` to the resume,
/// not the task's whole wait. Both halves are asserted here: a task whose
/// *pre-pause* wait was within its budget survives the thaw, while one that had
/// already blown its budget before the operator ever touched anything is still
/// failed. Crediting the full wait would silently extend deadlines the pause had
/// nothing to do with.
#[tokio::test]
async fn resume_credits_the_held_time_only_not_the_whole_wait() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("slow");

    // Both tasks waited the same 600s wall-clock; 595s of that was under the
    // hold, so each accrued only ~5s of wait the operator is not responsible
    // for. They differ only in whether that 5s fits their budget.
    let within_budget =
        enqueue_activity(&mut conn, &q, &act, Some(chrono::Duration::seconds(60))).await;
    let already_blown =
        enqueue_activity(&mut conn, &q, &act, Some(chrono::Duration::seconds(2))).await;

    pause(&mut conn, &act).await;
    backdate_scheduled_at(&mut conn, within_budget, 600).await;
    backdate_scheduled_at(&mut conn, already_blown, 600).await;
    backdate_paused_at(&mut conn, &act, 595).await;

    let due = timeout::find_timed_out_tasks(&mut conn)
        .await
        .expect("scan for timed-out tasks");
    for id in [within_budget, already_blown] {
        assert!(
            !due.iter()
                .any(|(t, r)| t.id == id && matches!(r, TimeoutReason::ScheduleToStart)),
            "AC3 — while held, no task of a paused activity may be selected for \
             schedule-to-start failure"
        );
        assert_eq!(task_state(&mut conn, id).await, "PENDING");
    }

    activity_pause::resume_activity(&mut conn, &act)
        .await
        .expect("resume");

    let after = timeout::find_timed_out_tasks(&mut conn)
        .await
        .expect("scan after resume");

    assert!(
        !after
            .iter()
            .any(|(t, r)| t.id == within_budget && matches!(r, TimeoutReason::ScheduleToStart)),
        "the resume must credit the held time back into scheduled_at, or the \
         thaw instantly kills the very backlog the hold preserved"
    );
    assert!(
        after
            .iter()
            .any(|(t, r)| t.id == already_blown && matches!(r, TimeoutReason::ScheduleToStart)),
        "the credit is held-time-only: a task that had already exhausted its \
         schedule_to_start *before* the pause began must still time out, or a \
         pause becomes a way to launder expired deadlines"
    );

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(within_budget),
        "and the released, still-in-budget task is claimable"
    );
}

/// A **workflow** task's schedule-to-start stays enforced while an activity is
/// paused — the anti-join is scoped by `task_type`, so the carve-out cannot
/// leak into unrelated rows.
#[tokio::test]
async fn a_workflow_tasks_schedule_to_start_is_not_suspended_by_an_activity_pause() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");

    let wf = enqueue_workflow(&mut conn, &q, Some("mixed_signal_suspension")).await;
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET schedule_to_start = INTERVAL '5 seconds' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(wf)
    .execute(&mut conn)
    .await
    .expect("set schedule_to_start");
    backdate_scheduled_at(&mut conn, wf, 600).await;

    pause(&mut conn, "mixed_signal_suspension").await;

    let due = timeout::find_timed_out_tasks(&mut conn)
        .await
        .expect("scan");
    assert!(
        due.iter()
            .any(|(t, r)| t.id == wf && matches!(r, TimeoutReason::ScheduleToStart)),
        "an activity pause must not suspend a workflow task's schedule-to-start, \
         even when the workflow row carries the sentinel activity_name"
    );
}

// ── AC1 — idempotency and provenance ─────────────────────────────────────────

/// Pausing an already-paused type is a success, not an error (AC1), and the
/// original provenance is preserved rather than overwritten by the second call.
#[tokio::test]
async fn pause_and_resume_are_idempotent_and_preserve_provenance() {
    let (mut conn, _c) = setup_db().await;
    let act = unique("charge");

    let first = activity_pause::pause_activity(&mut conn, &act, "first reason", "alice", None)
        .await
        .expect("first pause");
    assert!(first.newly_paused);

    let second = activity_pause::pause_activity(&mut conn, &act, "second reason", "bob", None)
        .await
        .expect("re-pausing must succeed, not error");
    assert!(
        !second.newly_paused,
        "AC1 — a repeat pause reports that it was already held"
    );
    assert_eq!(
        second.reason, "first reason",
        "the original reason survives, so the audit trail records who first \
         held it and why, not whoever last retried the call"
    );
    assert_eq!(second.paused_by, "alice");

    let r1 = activity_pause::resume_activity(&mut conn, &act)
        .await
        .expect("resume");
    assert!(r1.newly_resumed);
    let r2 = activity_pause::resume_activity(&mut conn, &act)
        .await
        .expect("resuming an unpaused type must succeed, not error");
    assert!(
        !r2.newly_resumed,
        "AC1 — a repeat resume is a reported no-op"
    );
}

/// The read model backing `GET /activities`: a paused activity is listed with
/// its provenance and the size of the backlog it is holding — including `0`,
/// so a hold with nothing queued is still visible.
#[tokio::test]
async fn list_reports_provenance_and_held_backlog_size() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let busy = unique("charge");
    let idle = unique("refund");

    enqueue_activity(&mut conn, &q, &busy, None).await;
    enqueue_activity(&mut conn, &q, &busy, None).await;

    activity_pause::pause_activity(&mut conn, &busy, "vendor 503s", "oncall", None)
        .await
        .expect("pause busy");
    activity_pause::pause_activity(&mut conn, &idle, "preemptive", "oncall", None)
        .await
        .expect("pause idle");

    let listed = activity_pause::list_paused_activities(&mut conn)
        .await
        .expect("list");

    let busy_row = listed
        .iter()
        .find(|p| p.activity_name == busy)
        .expect("the busy activity is listed");
    assert_eq!(busy_row.held_task_count, 2);
    assert_eq!(busy_row.reason, "vendor 503s");
    assert_eq!(busy_row.paused_by, "oncall");

    let idle_row = listed
        .iter()
        .find(|p| p.activity_name == idle)
        .expect("a hold with an empty backlog must still be listed");
    assert_eq!(
        idle_row.held_task_count, 0,
        "an empty hold reports 0 rather than dropping out of the list"
    );
}

/// One activity type's held backlog can span several queues — a per-call
/// `queue_override` routes the same activity onto a priority queue alongside its
/// default. A resume must release **every** queue's backlog, not just the first.
#[tokio::test]
async fn resume_releases_the_backlog_on_every_queue_the_activity_spans() {
    let (mut conn, _c) = setup_db().await;
    let q_default = unique("apq-default");
    let q_priority = unique("apq-priority");
    let act = unique("charge");

    let on_default = enqueue_activity(&mut conn, &q_default, &act, None).await;
    let on_priority = enqueue_activity(&mut conn, &q_priority, &act, None).await;

    pause(&mut conn, &act).await;
    assert_eq!(claim_one(&mut conn, &q_default).await, None);
    assert_eq!(
        claim_one(&mut conn, &q_priority).await,
        None,
        "the hold is per activity type, so it spans every queue that type sits on"
    );

    let outcome = activity_pause::resume_activity(&mut conn, &act)
        .await
        .expect("resume");
    assert_eq!(
        outcome.released_task_count, 2,
        "the resume must credit the backlog on both queues"
    );

    assert_eq!(claim_one(&mut conn, &q_default).await, Some(on_default));
    assert_eq!(
        claim_one(&mut conn, &q_priority).await,
        Some(on_priority),
        "a resume must not leave a second queue's released backlog behind"
    );
}

// ── AC7 — composition with the circuit breaker (#369) ────────────────────────

/// An operator pause takes **precedence** over breaker state.
///
/// The two brakes sit at different layers, and that ordering is what makes the
/// precedence structural rather than a coin toss: a pause is enforced at
/// **claim**, while the breaker is consulted at **dispatch**
/// (`worker::process_activity_task`). A held task never reaches dispatch, so it
/// cannot be short-circuited into the breaker's non-retryable `CircuitOpen`
/// `ActivityFailed` — it just stays `PENDING`. The operator's deliberate hold
/// beats the automatic reflex, and the held work survives instead of failing.
#[tokio::test]
async fn an_operator_pause_wins_over_a_tripped_breaker_by_holding_before_dispatch() {
    use autumn_harvest::circuit_breaker::CircuitBreakerRegistry;
    use autumn_harvest::policy::CircuitBreakerPolicy;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("charge");

    let mut policies = HashMap::new();
    policies.insert(
        act.clone(),
        CircuitBreakerPolicy::new(1, Duration::from_secs(60), Duration::from_secs(60)),
    );
    let breakers = CircuitBreakerRegistry::new(policies);
    breakers.force_open(&act, Instant::now());

    let task = enqueue_activity(&mut conn, &q, &act, None).await;
    pause(&mut conn, &act).await;

    // The claim gate runs first, so the task is never handed to the dispatcher
    // that would consult the breaker at all.
    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "a held task is not claimed, so it can never be short-circuited by the breaker"
    );
    assert_eq!(
        task_state(&mut conn, task).await,
        "PENDING",
        "AC7 — the pause holds the work rather than letting the breaker fail it"
    );
}

/// Resuming a paused activity must **not** auto-close a tripped breaker.
///
/// # Why this is a *structural* test and not a behavioural one
///
/// The obvious behavioural shape — build a `CircuitBreakerRegistry`, trip it,
/// call `resume_activity`, assert it is still open — **cannot fail**. The pause
/// lives in Postgres and the breaker lives in worker process memory;
/// `resume_activity` takes a `&mut AsyncPgConnection` and no registry handle, so
/// such a test asserts only that two unrelated objects are unrelated. It would
/// pass against an empty `resume_activity` body, and — the case that matters —
/// it would still pass if someone later wired breaker-clearing into the *plugin*
/// resume handler via `HandlerRegistry::circuit_breakers()`, which is reachable
/// from that layer. Green CI, violated AC.
///
/// So assert the thing that actually guarantees the property: the pause module
/// has no way to reach a breaker at all. This test fails the moment a
/// `CircuitBreakerRegistry` reference appears in `activity_pause.rs` — which is
/// exactly the change that would break the AC — in the same spirit as the
/// module's SQL-shape tests.
#[test]
fn resuming_a_pause_cannot_auto_close_a_tripped_breaker() {
    // Compiled in, so it tracks the real file rather than a stale copy.
    let source = include_str!("../../src/activity_pause.rs");

    // Strip doc comments and line comments: the module doc legitimately
    // *discusses* the circuit breaker (the "how it composes" table), and
    // prose must not be mistaken for a code reference.
    let code_only: String = source
        .lines()
        .filter(|line| {
            let t = line.trim_start();
            !t.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n");

    for forbidden in [
        "CircuitBreakerRegistry",
        "circuit_breaker::",
        "force_close",
        "force_open",
    ] {
        assert!(
            !code_only.contains(forbidden),
            "AC7 — `activity_pause` must have NO code path to the circuit breaker, \
             so a resume cannot auto-close a tripped one. Found `{forbidden}` in \
             non-comment source. If this is a deliberate change, the AC is being \
             violated: resuming an operator hold must never discard the automatic \
             protection that tripped for its own separate reason."
        );
    }
}

// ── AC5 — durability ─────────────────────────────────────────────────────────

/// The hold lives in the database, not in a worker's memory: a brand-new
/// connection — standing in for a worker that started mid-incident — observes
/// it on its very first claim, with no boot window and no refresh interval.
#[tokio::test]
async fn a_freshly_connected_worker_observes_the_hold_on_its_first_claim() {
    let (url, _c) = setup_db_url().await;
    let mut setup = connect(&url).await;
    setup
        .batch_execute("DELETE FROM harvest_activity_pauses")
        .await
        .expect("scrub");

    let q = unique("apq");
    let act = unique("charge");
    let held = enqueue_activity(&mut setup, &q, &act, None).await;
    pause(&mut setup, &act).await;
    drop(setup);

    let mut fresh = connect(&url).await;
    assert_eq!(
        claim_one(&mut fresh, &q).await,
        None,
        "AC5 — a worker that connects after the pause must honour it immediately"
    );
    assert_eq!(task_state(&mut fresh, held).await, "PENDING");
}

/// The post-claim re-check (the authoritative layer): a pause that commits
/// while a claim is in flight releases the claim back to `PENDING` rather than
/// dispatching the task into the outage — and restores the attempt counter, so
/// a hold never burns retry budget.
#[tokio::test]
async fn a_pause_committed_after_the_claim_releases_it_and_restores_the_attempt() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("charge");

    let task = enqueue_activity(&mut conn, &q, &act, None).await;
    let before = attempt_of(&mut conn, task).await;

    // Simulate the race directly: claim, then pause, then run the same
    // re-check the claim path runs before it hands the task to a worker.
    let claimed = claim_one(&mut conn, &q).await;
    assert_eq!(claimed, Some(task));
    pause(&mut conn, &act).await;

    let released = activity_pause::release_claim_if_activity_paused(&mut conn, task, "w1")
        .await
        .expect("re-check");
    assert!(
        released,
        "a claim made just before a pause must be given back"
    );
    assert_eq!(
        task_state(&mut conn, task).await,
        "PENDING",
        "the released task returns to the held backlog"
    );
    assert_eq!(
        attempt_of(&mut conn, task).await,
        before,
        "the attempt counter is restored — a hold must never consume retry budget"
    );
}

/// The same re-check must not release a claim held by a *different* worker: it
/// is guarded on the claiming worker id, so one worker's re-check can never
/// yank another's in-flight task.
#[tokio::test]
async fn the_post_claim_recheck_only_releases_this_workers_own_claim() {
    let (mut conn, _c) = setup_db().await;
    let q = unique("apq");
    let act = unique("charge");

    let task = enqueue_activity(&mut conn, &q, &act, None).await;
    claim_one(&mut conn, &q).await;
    pause(&mut conn, &act).await;

    let released =
        activity_pause::release_claim_if_activity_paused(&mut conn, task, "someone-else")
            .await
            .expect("re-check");
    assert!(
        !released,
        "a re-check from a worker that does not own the claim must be a no-op"
    );
    assert_eq!(
        task_state(&mut conn, task).await,
        "RUNNING",
        "and the owning worker's in-flight task is untouched"
    );
}

/// A task enqueued *during* the resume transaction — after the primary shift
/// took its snapshot, before the pause DELETE commits — is genuinely held and
/// is owed a credit (issue #807 review, Codex P1).
///
/// The primary shift cannot see it: under READ COMMITTED each statement takes
/// its own snapshot, so a row committed after that statement began is invisible
/// to it. With a single pass such a row thaws with its entire held wait
/// uncredited, and the `schedule_to_start` scanner can terminally fail it the
/// instant the hold lifts — the exact failure the credit exists to prevent, and
/// worst on a large backlog where the bulk `UPDATE` and the per-queue
/// notification pass keep the transaction open longest.
///
/// This drives the two passes by hand across two connections so the window is
/// deterministic rather than raced. Mirrors
/// `queue_pause_tests::resume_credits_a_task_that_arrives_between_the_two_passes`,
/// whose two-pass design (issue #619 rounds 16/19) this fix adopts.
#[tokio::test]
async fn resume_credits_a_task_that_arrives_between_the_two_passes() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let mut resumer = connect(&url).await;

    let q = unique("q_late");
    let activity = unique("act_late");

    // A pre-existing held task: the primary pass's row.
    let early = enqueue_activity(&mut conn, &q, &activity, None).await;
    backdate_scheduled_at(&mut conn, early, 100).await;
    assert!(pause(&mut conn, &activity).await);
    backdate_paused_at(&mut conn, &activity, 100).await;

    #[derive(diesel::QueryableByName)]
    struct PausedAtRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        paused_at: chrono::DateTime<chrono::Utc>,
    }
    let paused_at = {
        use diesel_async::RunQueryDsl;
        let rows: Vec<PausedAtRow> = diesel::sql_query(
            "SELECT paused_at FROM harvest_activity_pauses WHERE activity_name = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&activity)
        .load(&mut conn)
        .await
        .expect("read paused_at");
        rows[0].paused_at
    };

    // Open the resume transaction and release the hold — uncommitted, so every
    // other session still sees the pause row and still holds.
    resumer
        .batch_execute(&format!(
            "BEGIN; DELETE FROM harvest_activity_pauses WHERE activity_name = '{activity}'"
        ))
        .await
        .expect("begin and release the hold");

    // Pass 1 — the primary shift.
    #[derive(diesel::QueryableByName)]
    struct ShiftedTaskIdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let primary_ids: Vec<ShiftedTaskIdRow> = {
        use diesel_async::RunQueryDsl;
        diesel::sql_query(activity_pause::resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&activity)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .load(&mut resumer)
            .await
            .expect("primary shift")
    };
    let primary = primary_ids.len();
    let shifted_ids: Vec<Uuid> = primary_ids.into_iter().map(|r| r.id).collect();

    // ── THE WINDOW ──────────────────────────────────────────────────────────
    let late = enqueue_activity(&mut conn, &q, &activity, None).await;
    backdate_scheduled_at(&mut conn, late, 100).await;
    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "the hold is still in force mid-resume: the DELETE has not committed, so \
         a task enqueued now is genuinely held and owed a credit"
    );
    // Prove the primary pass genuinely missed it, so the pass-2 assertion below
    // cannot be vacuous.
    let before_pass2 = (chrono::Utc::now() - scheduled_at_of(&mut conn, late).await).num_seconds();
    assert!(
        (99..=101).contains(&before_pass2),
        "the primary shift cannot see a row enqueued after its snapshot; if this \
         already read ~0s the second pass would be untested. apparent wait \
         {before_pass2}s"
    );

    // Pass 2 — the late-arrivals credit, excluding by id rather than predicate.
    let late_shifted = {
        use diesel_async::RunQueryDsl;
        diesel::sql_query(activity_pause::resume_shift_late_arrivals_query())
            .bind::<diesel::sql_types::Text, _>(&activity)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
            .execute(&mut resumer)
            .await
            .expect("late-arrivals shift")
    };
    resumer
        .batch_execute("COMMIT")
        .await
        .expect("commit resume");

    // The passes partition: exactly one row each, never both on one row.
    assert_eq!(
        primary, 1,
        "the pre-existing backlog is the primary pass's row"
    );
    assert_eq!(
        late_shifted, 1,
        "the row that arrived mid-resume is pass 2's"
    );

    // Both are credited: neither carries its held wait past the thaw.
    for (id, label) in [(early, "pre-existing"), (late, "late-arriving")] {
        let wait = (chrono::Utc::now() - scheduled_at_of(&mut conn, id).await).num_seconds();
        assert!(
            wait <= 2,
            "the {label} task must be credited for the held time, else the \
             schedule_to_start scanner can fail it the instant the hold lifts; \
             apparent wait {wait}s"
        );
    }

    // And the whole backlog is claimable again.
    assert_eq!(
        claim_all(&mut conn, &q).await.len(),
        2,
        "both tasks are claimable once the resume commits"
    );
}

/// `scheduled_at` of one task — how long the claim gate believes it has waited.
async fn scheduled_at_of(conn: &mut AsyncPgConnection, id: Uuid) -> chrono::DateTime<chrono::Utc> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        scheduled_at: chrono::DateTime<chrono::Utc>,
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT scheduled_at FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .load(conn)
            .await
            .expect("read scheduled_at");
    rows[0].scheduled_at
}

/// The credit pass must be the **last** statement of the resume transaction, so
/// a task that commits while the doorbell is being rung is still credited.
///
/// Issue #807 review, Codex P1. The resume originally ran
/// `primary shift -> late-arrivals credit -> notify SELECT -> one pg_notify per
/// queue -> COMMIT`, which left the uncredited window bounded by the *whole
/// notification pass* — one round-trip per queue holding backlog, i.e. widest
/// during exactly the incident being resumed — rather than by the single small
/// `UPDATE` that `resume_shift_late_arrivals_query` documents. A row landing in
/// that window keeps its entire held wait, and the `schedule_to_start` scanner
/// can terminally fail it the instant the hold lifts.
///
/// Hand-drives the statements on an explicit transaction (the idiom
/// `resume_credits_a_task_that_arrives_between_the_two_passes` establishes) so
/// the window is opened deterministically rather than raced: here it is opened
/// *after* the doorbell and *before* the credit, which is the arrangement the
/// fix produces. The pre-window assertion proves the row is genuinely
/// uncredited at that point, so the post-commit assertion cannot pass vacuously.
///
/// Scope, stated plainly: because the statements are hand-driven, this pins the
/// **ordering contract** — that a row arriving after the doorbell is still the
/// credit pass's to claim — rather than `resume_activity`'s own wiring. The
/// wiring is a three-statement arrangement verified by reading, exactly as the
/// arrangement it replaces was. A test that falsified the wiring would have to
/// land a concurrent commit *inside* the notification pass, and that window is
/// not blockable: `pg_notify` takes no lock, and the notify `SELECT` blocks only
/// on `ACCESS EXCLUSIVE`, which the resume's own transaction already holds
/// conflicting rights against. The available alternative — flooding inserts and
/// hoping one lands in the window — is probabilistic in the *passing*
/// direction, so it would be a flaky guard rather than a real one.
#[tokio::test]
async fn resume_credits_a_task_that_arrives_during_the_notification_pass() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let mut resumer = connect(&url).await;

    let q = unique("q_notifywin");
    let activity = unique("act_notifywin");

    let early = enqueue_activity(&mut conn, &q, &activity, None).await;
    backdate_scheduled_at(&mut conn, early, 100).await;
    assert!(pause(&mut conn, &activity).await);
    backdate_paused_at(&mut conn, &activity, 100).await;

    #[derive(diesel::QueryableByName)]
    struct PausedAtRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        paused_at: chrono::DateTime<chrono::Utc>,
    }
    let paused_at = {
        use diesel_async::RunQueryDsl;
        let rows: Vec<PausedAtRow> = diesel::sql_query(
            "SELECT paused_at FROM harvest_activity_pauses WHERE activity_name = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&activity)
        .load(&mut conn)
        .await
        .expect("read paused_at");
        rows[0].paused_at
    };

    // Release the hold, uncommitted: every other session still sees the pause
    // row, so anything enqueued from here is genuinely held and owed a credit.
    resumer
        .batch_execute(&format!(
            "BEGIN; DELETE FROM harvest_activity_pauses WHERE activity_name = '{activity}'"
        ))
        .await
        .expect("begin and release the hold");

    #[derive(diesel::QueryableByName)]
    struct ShiftedTaskIdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let shifted_ids: Vec<Uuid> = {
        use diesel_async::RunQueryDsl;
        let rows: Vec<ShiftedTaskIdRow> =
            diesel::sql_query(activity_pause::resume_shift_scheduled_at_query())
                .bind::<diesel::sql_types::Text, _>(&activity)
                .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
                .load(&mut resumer)
                .await
                .expect("primary shift");
        rows.into_iter().map(|r| r.id).collect()
    };
    assert_eq!(shifted_ids.len(), 1, "the pre-existing backlog is pass 1's");

    // The notification pass, in the position the fix gives it: after the
    // primary shift, before the credit.
    #[derive(diesel::QueryableByName)]
    struct NotifyTaskRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
    }
    let doorbells: Vec<NotifyTaskRow> = {
        use diesel_async::RunQueryDsl;
        diesel::sql_query(activity_pause::resumed_activity_notify_task_query())
            .bind::<diesel::sql_types::Text, _>(&activity)
            .load(&mut resumer)
            .await
            .expect("notify lookup")
    };
    assert_eq!(
        doorbells.len(),
        1,
        "one doorbell for the activity's one queue"
    );
    for row in doorbells {
        autumn_harvest::notify::notify_task_enqueued(&mut resumer, &row.queue_name, row.id)
            .await
            .expect("ring the doorbell");
    }

    // ── THE WINDOW: inside the notification pass ────────────────────────────
    let late = enqueue_activity(&mut conn, &q, &activity, None).await;
    backdate_scheduled_at(&mut conn, late, 100).await;
    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "the hold is still in force: the resume's DELETE has not committed, so a \
         task enqueued during the notification pass is genuinely held"
    );
    let before_credit = (chrono::Utc::now() - scheduled_at_of(&mut conn, late).await).num_seconds();
    assert!(
        (99..=101).contains(&before_credit),
        "neither the primary shift nor the doorbell credits this row, so the \
         post-commit assertion below is not vacuous; apparent wait \
         {before_credit}s"
    );

    // The credit pass, last.
    let late_shifted = {
        use diesel_async::RunQueryDsl;
        diesel::sql_query(activity_pause::resume_shift_late_arrivals_query())
            .bind::<diesel::sql_types::Text, _>(&activity)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
            .execute(&mut resumer)
            .await
            .expect("late-arrivals shift")
    };
    resumer
        .batch_execute("COMMIT")
        .await
        .expect("commit resume");

    assert_eq!(
        late_shifted, 1,
        "a task arriving during the notification pass is still the credit \
         pass's row -- with the notification pass running last it would have \
         been missed entirely"
    );
    let wait = (chrono::Utc::now() - scheduled_at_of(&mut conn, late).await).num_seconds();
    assert!(
        wait <= 2,
        "the task that arrived during the notification pass must be credited, \
         else schedule_to_start can fail it the instant the hold lifts; \
         apparent wait {wait}s"
    );
    assert_eq!(
        claim_all(&mut conn, &q).await.len(),
        2,
        "the whole backlog is claimable once the resume commits"
    );
}

/// `paused_at` must be stamped **after** the backlog count, not at transaction
/// start, or a slow count back-dates the hold and the resume credits back wait
/// the tasks spent claimable and unheld.
///
/// Issue #807 review, Codex P2. `pause_activity` originally ran
/// `upsert (paused_at = NOW()) -> held_task_count -> COMMIT`, and `NOW()` is
/// `transaction_timestamp()` — frozen at transaction start. The hold only takes
/// effect for other sessions at COMMIT, so the recorded `paused_at` preceded the
/// effective hold by the entire count, whose cost scales with the backlog. The
/// resume subtracts `paused_at`, so the gap is credited as if it were held time:
/// a `schedule_to_start` budget the task had already burned gets laundered.
///
/// Deterministic rather than raced: an `ACCESS EXCLUSIVE` lock on
/// `harvest_task_queue` held from a second session blocks the count for a known
/// duration. With the fix (count first, `clock_timestamp()` in the upsert) the
/// stamp lands after the lock releases; without it the stamp predates the whole
/// block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_stamps_paused_at_after_the_backlog_count_not_at_transaction_start() {
    /// How long the count is blocked. The assertion threshold is half this, so
    /// the pre-fix behaviour (stamp before the block) and the post-fix one
    /// (stamp after it) are separated by a 2x margin in both directions.
    const BLOCK: std::time::Duration = std::time::Duration::from_millis(1_500);

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let mut locker = connect(&url).await;

    let q = unique("q_stamp");
    let activity = unique("act_stamp");
    // Backlog for the count to walk; its size is irrelevant, the lock is what
    // makes the count slow.
    let _ = enqueue_activity(&mut conn, &q, &activity, None).await;
    let _ = enqueue_activity(&mut conn, &q, &activity, None).await;

    locker
        .batch_execute("BEGIN; LOCK TABLE harvest_task_queue IN ACCESS EXCLUSIVE MODE")
        .await
        .expect("take the table lock");

    let pause_url = url.clone();
    let pause_activity_name = activity.clone();
    let pausing = tokio::spawn(async move {
        let mut pauser = connect(&pause_url).await;
        activity_pause::pause_activity(
            &mut pauser,
            &pause_activity_name,
            "downstream outage",
            "oncall",
            None,
        )
        .await
        .expect("pause")
    });

    tokio::time::sleep(BLOCK).await;
    locker.batch_execute("COMMIT").await.expect("release lock");
    let outcome = pausing.await.expect("pause task joined");
    assert!(outcome.newly_paused);
    assert_eq!(
        outcome.held_task_count, 2,
        "the count still sees the backlog"
    );

    // Measured entirely database-side, so no app/DB clock skew enters the
    // comparison.
    #[derive(diesel::QueryableByName)]
    struct AgeRow {
        #[diesel(sql_type = diesel::sql_types::Double)]
        secs: f64,
    }
    let age = {
        use diesel_async::RunQueryDsl;
        let rows: Vec<AgeRow> = diesel::sql_query(
            "SELECT EXTRACT(EPOCH FROM (clock_timestamp() - paused_at))::float8 AS secs \
             FROM harvest_activity_pauses WHERE activity_name = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&activity)
        .load(&mut conn)
        .await
        .expect("read paused_at age");
        rows[0].secs
    };

    let threshold = BLOCK.as_secs_f64() / 2.0;
    assert!(
        age < threshold,
        "paused_at must be stamped once the count has finished, not at \
         transaction start: a back-dated hold lets the resume credit back wait \
         the tasks spent claimable and unheld. blocked the count for {}s, but \
         paused_at is already {age:.3}s old",
        BLOCK.as_secs_f64()
    );
}
