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
//! Task-queue pause/resume integration tests — issue #619.
//!
//! A queue pause **holds** dispatch on a named task queue: no worker claims new
//! tasks from it, held tasks stay `PENDING` (never `FAILED`, never DLQ'd), the
//! relative `schedule_to_start` timer is suspended, and a resume makes the held
//! backlog immediately claimable again with no replay divergence.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle.

use autumn_harvest::queue::{self, EnqueueParams, TaskType, claim_task};
use autumn_harvest::queue_pause::{self, MAX_QUEUE_NAME_LEN};
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
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migrations");
    (url, Some(container))
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = setup_db_url().await;
    let mut conn = connect(&url).await;
    // Each test uses uniquely-named queues, but the pause table is keyed by
    // queue name alone, so scrub it when reusing an env-provided database.
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");
    (conn, container)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    use diesel_async::RunQueryDsl;
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'qp', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue an activity task on `queue`, optionally with a `schedule_to_start`.
async fn enqueue_activity(
    conn: &mut AsyncPgConnection,
    queue: &str,
    schedule_to_start: Option<chrono::Duration>,
) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.schedule_to_start = schedule_to_start;
    queue::enqueue(conn, &params).await.expect("enqueue")
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

async fn scheduled_at(
    conn: &mut AsyncPgConnection,
    id: Uuid,
) -> chrono::DateTime<chrono::offset::Utc> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        scheduled_at: chrono::DateTime<chrono::offset::Utc>,
    }
    diesel::sql_query("SELECT scheduled_at FROM harvest_task_queue WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<S>(conn)
        .await
        .expect("scheduled_at")
        .scheduled_at
}

/// Backdate a task's `scheduled_at` so it looks like it has been waiting.
async fn backdate_scheduled_at(conn: &mut AsyncPgConnection, id: Uuid, secs: i64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() - ($2 || ' seconds')::interval \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(secs.to_string())
    .execute(conn)
    .await
    .expect("backdate");
}

/// Backdate a pause's `paused_at` so a resume computes a non-trivial span.
async fn backdate_paused_at(conn: &mut AsyncPgConnection, queue: &str, secs: i64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_queue_pauses SET paused_at = NOW() - ($2 || ' seconds')::interval \
         WHERE queue_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
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

// ── Pure (no-DB) unit tests ───────────────────────────────────────────────────

/// AC2 — the claim predicate carries the queue-pause anti-join, so a paused
/// queue is skipped by *every* worker with no cache and no boot-ordering race.
#[test]
fn claim_query_contains_the_queue_pause_anti_join() {
    let sql = queue::claim_task_query();
    assert!(
        sql.contains("harvest_queue_pauses"),
        "claim_task must anti-join the pause table; got:\n{sql}"
    );
    assert!(
        sql.contains(queue_pause::QUEUE_PAUSE_CLAIM_PREDICATE),
        "claim_task must embed the shared predicate verbatim so the two cannot drift"
    );
}

/// The two queries that document "mirrors every claim-time gate" must mirror
/// this one too, or a deliberately-paused backlog drives false capacity alerts.
#[test]
fn claim_mirror_queries_contain_the_queue_pause_anti_join() {
    for (name, sql) in [
        ("oldest_pending_ages", queue::oldest_pending_ages_query()),
        (
            "claimable_pending_demand_by_queue",
            &queue::claimable_pending_demand_query(),
        ),
    ] {
        assert!(
            sql.contains("harvest_queue_pauses"),
            "{name} must mirror the claim-time queue-pause gate; got:\n{sql}"
        );
    }
}

/// AC4 — a task held by a queue pause is not schedule-to-start timed out.
#[test]
fn schedule_to_start_scan_excludes_paused_queues() {
    let sql = timeout::schedule_to_start_timeout_query();
    assert!(
        sql.contains("harvest_queue_pauses"),
        "schedule_to_start scan must skip tasks on a paused queue; got:\n{sql}"
    );
}

/// The out-of-scope decision, pinned: `schedule_to_close` keeps ticking during a
/// queue pause (a pause does NOT extend an absolute SLA deadline).
#[test]
fn schedule_to_close_scan_does_not_exclude_paused_queues() {
    let sql = timeout::schedule_to_close_timeout_query();
    assert!(
        !sql.contains("harvest_queue_pauses"),
        "issue #619 keeps schedule_to_close ticking during a queue pause"
    );
}

/// AC4 truth table — only the *relative* schedule-to-start timer is suspended.
#[test]
fn queue_pause_suppresses_only_schedule_to_start() {
    assert!(queue_pause::queue_pause_suppresses_timeout(
        &TimeoutReason::ScheduleToStart,
        true
    ));
    assert!(!queue_pause::queue_pause_suppresses_timeout(
        &TimeoutReason::ScheduleToClose,
        true
    ));
    assert!(!queue_pause::queue_pause_suppresses_timeout(
        &TimeoutReason::Heartbeat,
        true
    ));
    assert!(!queue_pause::queue_pause_suppresses_timeout(
        &TimeoutReason::StartToClose,
        true
    ));
    // Not paused: never suppressed.
    for reason in [
        TimeoutReason::ScheduleToStart,
        TimeoutReason::ScheduleToClose,
        TimeoutReason::Heartbeat,
        TimeoutReason::StartToClose,
    ] {
        assert!(!queue_pause::queue_pause_suppresses_timeout(&reason, false));
    }
}

/// Queue names are free-form TEXT everywhere else, but the pause table's PK
/// must be bounded and non-blank so an operator typo cannot wedge the row.
#[test]
fn queue_name_validation_rejects_blank_and_oversized() {
    assert!(queue_pause::validate_queue_name("payments").is_ok());
    assert!(queue_pause::validate_queue_name("").is_err());
    assert!(queue_pause::validate_queue_name("   ").is_err());
    assert!(queue_pause::validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN)).is_ok());
    assert!(queue_pause::validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN + 1)).is_err());
}

/// The resume shift must never push a held task's `scheduled_at` past NOW()
/// (that would make a thawed task *un*claimable — the inverse of AC5).
#[test]
fn resume_shift_query_never_pushes_scheduled_at_past_now() {
    let sql = queue_pause::resume_shift_scheduled_at_query();
    assert!(sql.contains("GREATEST"), "expected the held-span formula");
    assert!(
        sql.contains("scheduled_at <= NOW()"),
        "only rows that are actually due may be shifted; got:\n{sql}"
    );
    assert!(
        sql.contains("'PENDING'"),
        "the shift must not touch RUNNING rows"
    );
}

// ── DB integration tests ──────────────────────────────────────────────────────

/// AC2 + AC3 — while paused, no worker claims a new task and the task stays
/// PENDING. AC5 — resuming makes it immediately claimable again.
#[tokio::test]
async fn pause_holds_dispatch_and_resume_releases_it() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("hold");
    let task = enqueue_activity(&mut conn, &q, None).await;

    // Claimable before the pause.
    assert_eq!(claim_one(&mut conn, &q).await, Some(task));
    // Put it back so the pause has something to hold.
    queue::requeue_for_retry(&mut conn, task, chrono::Duration::zero(), "reset")
        .await
        .expect("requeue");
    assert_eq!(task_state(&mut conn, task).await, "PENDING");

    let outcome = queue_pause::pause_queue(&mut conn, &q, "stripe outage", "alice", None)
        .await
        .expect("pause");
    assert!(outcome.newly_paused);
    assert_eq!(outcome.held_task_count, 1);

    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "AC2: no worker may claim a new task from a paused queue"
    );
    assert_eq!(
        task_state(&mut conn, task).await,
        "PENDING",
        "AC3: a held task is never failed"
    );

    let resumed = queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");
    assert!(resumed.newly_resumed);

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task),
        "AC5: held tasks are immediately claimable after resume"
    );
}

/// AC2 — a pause holds *dispatch*; a task already RUNNING when the pause lands
/// finishes naturally and is never aborted.
#[tokio::test]
async fn pause_does_not_abort_in_flight_tasks() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("inflight");
    let task = enqueue_activity(&mut conn, &q, None).await;
    assert_eq!(claim_one(&mut conn, &q).await, Some(task));
    assert_eq!(task_state(&mut conn, task).await, "RUNNING");

    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");

    assert_eq!(
        task_state(&mut conn, task).await,
        "RUNNING",
        "AC2: in-flight work is untouched by a pause"
    );
    // ... and it can still complete normally.
    queue::complete_task(&mut conn, task, serde_json::json!({}))
        .await
        .expect("complete");
    assert_eq!(task_state(&mut conn, task).await, "COMPLETED");
}

/// AC4 — a task held past its `schedule_to_start` is NOT timed out while its
/// queue is paused, and is NOT retroactively timed out the moment it resumes.
#[tokio::test]
async fn schedule_to_start_is_suspended_while_paused_and_after_resume() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("sts");
    let task = enqueue_activity(&mut conn, &q, Some(chrono::Duration::seconds(30))).await;

    queue_pause::pause_queue(&mut conn, &q, "warehouse down", "alice", None)
        .await
        .expect("pause");

    // Simulate a long hold: the task became due 10 minutes ago and the pause
    // has been on for 10 minutes — well past the 30s schedule_to_start.
    backdate_scheduled_at(&mut conn, task, 600).await;
    backdate_paused_at(&mut conn, &q, 600).await;

    let timed_out = timeout::find_timed_out_tasks(&mut conn)
        .await
        .expect("scan")
        .into_iter()
        .filter(|(t, r)| t.id == task && matches!(r, TimeoutReason::ScheduleToStart))
        .count();
    assert_eq!(
        timed_out, 0,
        "AC4: a task held by a queue pause must not be schedule-to-start timed out"
    );

    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");

    let timed_out_after = timeout::find_timed_out_tasks(&mut conn)
        .await
        .expect("scan")
        .into_iter()
        .filter(|(t, r)| t.id == task && matches!(r, TimeoutReason::ScheduleToStart))
        .count();
    assert_eq!(
        timed_out_after, 0,
        "AC5: resuming must not retroactively time out the whole held backlog"
    );
    assert_eq!(task_state(&mut conn, task).await, "PENDING");
    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task),
        "AC5: and the thawed task is immediately claimable"
    );
}

/// The resume shift credits held time back to `scheduled_at` without ever
/// pushing it into the future, and preserves pre-pause waiting.
#[tokio::test]
async fn resume_shifts_scheduled_at_by_held_time_only() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("shift");
    // Waited 100s before the pause, then held for 600s.
    let task = enqueue_activity(&mut conn, &q, None).await;
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_scheduled_at(&mut conn, task, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;

    let before = scheduled_at(&mut conn, task).await;
    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");
    let after = scheduled_at(&mut conn, task).await;

    let shift = (after - before).num_seconds();
    assert!(
        (595..=605).contains(&shift),
        "expected ~600s of held time credited back, got {shift}s"
    );
    let age_now = (chrono::Utc::now() - after).num_seconds();
    assert!(
        (95..=115).contains(&age_now),
        "pre-pause waiting must be preserved (~100s), got {age_now}s"
    );
    assert!(
        after <= chrono::Utc::now(),
        "AC5: the shift must never push a held task into the future"
    );
}

/// A task that became due *during* the pause accrues zero wait — its shifted
/// `scheduled_at` collapses to the resume instant.
#[tokio::test]
async fn resume_shift_collapses_mid_pause_arrivals_to_now() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("midpause");
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_paused_at(&mut conn, &q, 600).await;
    // Enqueued mid-pause: due 60s ago, but the pause started 600s ago.
    let task = enqueue_activity(&mut conn, &q, None).await;
    backdate_scheduled_at(&mut conn, task, 60).await;

    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");

    let age = (chrono::Utc::now() - scheduled_at(&mut conn, task).await).num_seconds();
    assert!(
        (0..=5).contains(&age),
        "a mid-pause arrival accrues no wait; got {age}s"
    );
}

/// A task scheduled for the *future* (a retry backoff) was never held, so its
/// backoff must not be extended by the pause.
#[tokio::test]
async fn resume_shift_leaves_future_scheduled_tasks_alone() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("future");
    let task = enqueue_activity(&mut conn, &q, None).await;
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() + INTERVAL '300 seconds' WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .execute(&mut conn)
    .await
    .expect("future");
    let before = scheduled_at(&mut conn, task).await;

    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_paused_at(&mut conn, &q, 600).await;
    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");

    let after = scheduled_at(&mut conn, task).await;
    assert_eq!(
        before, after,
        "a not-yet-due task was never held, so its backoff must not shift"
    );
}

/// Pause/resume are idempotent operator actions (mirrors #504 terminate and
/// #609 resume): repeating either is a success no-op, not an error.
#[tokio::test]
async fn pause_and_resume_are_idempotent() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("idem");

    let first = queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    assert!(first.newly_paused);
    let second = queue_pause::pause_queue(&mut conn, &q, "different reason", "bob", None)
        .await
        .expect("re-pause");
    assert!(!second.newly_paused, "re-pausing is a no-op");
    assert_eq!(
        second.reason, "outage",
        "the original pause provenance is preserved"
    );

    assert!(
        queue_pause::resume_queue(&mut conn, &q, "alice")
            .await
            .expect("resume")
            .newly_resumed
    );
    assert!(
        !queue_pause::resume_queue(&mut conn, &q, "alice")
            .await
            .expect("resume again")
            .newly_resumed,
        "resuming an unpaused queue is a success no-op"
    );
}

/// AC7 — the read surface lists every paused queue with `paused_at`, `reason`
/// and a held-task count, and a pause is scoped to exactly one queue.
#[tokio::test]
async fn list_paused_queues_reports_reason_and_held_count() {
    let (mut conn, _c) = setup_db().await;
    let paused = unique_queue("listed");
    let other = unique_queue("other");
    enqueue_activity(&mut conn, &paused, None).await;
    enqueue_activity(&mut conn, &paused, None).await;
    let untouched = enqueue_activity(&mut conn, &other, None).await;

    queue_pause::pause_queue(&mut conn, &paused, "sendgrid down", "alice", None)
        .await
        .expect("pause");

    let listed = queue_pause::list_paused_queues(&mut conn)
        .await
        .expect("list");
    let entry = listed
        .iter()
        .find(|e| e.queue_name == paused)
        .expect("paused queue is listed");
    assert_eq!(entry.reason, "sendgrid down");
    assert_eq!(entry.paused_by, "alice");
    assert_eq!(entry.held_task_count, 2);
    assert!(entry.paused_at <= chrono::Utc::now());
    assert!(
        !listed.iter().any(|e| e.queue_name == other),
        "a pause is scoped to exactly the named queue"
    );

    assert_eq!(
        claim_one(&mut conn, &other).await,
        Some(untouched),
        "an unpaused sibling queue keeps dispatching"
    );
}

/// The gauge sampler's source of truth: the set of currently-paused queues.
#[tokio::test]
async fn paused_queue_names_feeds_the_gauge() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("gauge");
    assert!(
        !queue_pause::paused_queue_names(&mut conn)
            .await
            .expect("names")
            .contains(&q)
    );
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    assert!(
        queue_pause::paused_queue_names(&mut conn)
            .await
            .expect("names")
            .contains(&q)
    );
}

// ── AC9: no `WorkflowEvent` variant, replay byte-identical ────────────────────

/// AC9, the schema half: pausing a queue must add **no** `WorkflowEvent`
/// variant. Because the whole feature is queue metadata plus a claim-path
/// anti-join, `queue_pause.rs` must never mention the event enum at all — the
/// cheapest possible proof, and one that fails loudly if a future change tries
/// to record a pause in history.
#[test]
fn queue_pause_module_never_touches_the_event_enum() {
    let src = include_str!("../../src/queue_pause.rs");
    // Strip doc comments: the module doc legitimately *discusses* the invariant
    // (`introduces no WorkflowEvent variant`), and that prose must not be
    // mistaken for a real reference.
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !code.contains("WorkflowEvent"),
        "queue_pause.rs must never construct or match a WorkflowEvent -- a pause \
         writes nothing to harvest_events (AC9)"
    );
    assert!(
        !code.contains("harvest_events"),
        "queue_pause.rs must never read or write harvest_events (AC9)"
    );
}

/// AC9, the history half: a pause/resume cycle around a task must leave the
/// owning execution's `harvest_events` **byte-identical**.
///
/// This is the assertion that makes the "replay is unaffected" claim
/// falsifiable rather than merely asserted: it snapshots the raw event rows
/// before the pause and compares them byte-for-byte after the resume, so any
/// future change that records a pause in history fails here.
#[tokio::test]
async fn pause_and_resume_leave_the_event_history_byte_identical() {
    use diesel_async::RunQueryDsl;

    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("replay");

    // Seed an execution with a real, non-empty history.
    let exec_id = insert_execution(&mut conn).await;
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) VALUES \
         ($1, 1, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{}}}'::jsonb), \
         ($1, 2, 'ActivityScheduled', '{\"type\":\"ActivityScheduled\",\"data\":{\"name\":\"noop\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("seed history");

    // A task on the queue we are about to freeze, owned by that execution.
    let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
    struct EventRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        event_id: i32,
        #[diesel(sql_type = diesel::sql_types::Text)]
        event_type: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        event_data_text: String,
    }

    async fn snapshot(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Vec<EventRow> {
        diesel::sql_query(
            "SELECT id, event_id, event_type, event_data::text AS event_data_text \
             FROM harvest_events WHERE workflow_exec_id = $1 ORDER BY event_id",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .load::<EventRow>(conn)
        .await
        .expect("snapshot history")
    }

    let before = snapshot(&mut conn, exec_id).await;
    assert_eq!(before.len(), 2, "fixture should seed exactly two events");

    queue_pause::pause_queue(&mut conn, &q, "downstream outage", "alice", None)
        .await
        .expect("pause");
    let during = snapshot(&mut conn, exec_id).await;
    assert_eq!(
        before, during,
        "a pause must append NOTHING to harvest_events (AC9)"
    );

    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");
    let after = snapshot(&mut conn, exec_id).await;
    assert_eq!(
        before, after,
        "a pause/resume cycle must leave the event history byte-identical, so a \
         replay of an execution that touched a paused queue is indistinguishable \
         from one that never paused (AC9)"
    );
}

// ── Post-review hardening (correctness + AC-compliance review) ────────────────

/// The queue name is **canonicalised** (trimmed) on the write path, not merely
/// validated as non-blank.
///
/// Validating `name.trim()` while storing the raw string would be a silent
/// no-op hold: the claim anti-join matches on exact equality, so a stray
/// trailing space from a copy-paste (`--queue "payments "`) would insert a row
/// matching no task, and the API would report a successful hold on a queue that
/// keeps dispatching. This asserts the *behaviour* an operator cares about —
/// the padded name really holds the queue — not just the stored string.
#[tokio::test]
async fn a_padded_queue_name_still_holds_the_real_queue() {
    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("trim");
    let padded = format!("  {q}\t");

    let params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let outcome = queue_pause::pause_queue(&mut conn, &padded, "outage", "alice", None)
        .await
        .expect("pause with a padded name");
    assert_eq!(
        outcome.queue_name, q,
        "the stored name must be the canonical (trimmed) one"
    );
    assert_eq!(
        outcome.held_task_count, 1,
        "the padded name must resolve to the real queue and see its held task"
    );
    assert!(
        queue_pause::is_queue_paused(&mut conn, &q)
            .await
            .expect("is_paused"),
        "the UNPADDED queue must actually be held -- a padded name must never \
         create a phantom hold that matches no task"
    );

    // And the padded form releases the same hold.
    let resumed = queue_pause::resume_queue(&mut conn, &padded, "alice")
        .await
        .expect("resume with a padded name");
    assert!(resumed.newly_resumed);
    assert!(
        !queue_pause::is_queue_paused(&mut conn, &q)
            .await
            .expect("is_paused")
    );
}

/// A pause racing a concurrent resume must not fail — and must not leave the
/// queue **unpaused** while telling the operator the pause failed.
///
/// The original `INSERT ... ON CONFLICT DO NOTHING` + fallback `SELECT` shape
/// had exactly that bug: `DO NOTHING` does not lock the conflicting row, so a
/// resume could delete it between the two statements, the fallback `SELECT`
/// found zero rows, and the whole transaction rolled back into a 500 — with the
/// queue live. This reproduces the interleaving by holding the resume's
/// transaction open across the pause attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pause_racing_a_concurrent_resume_still_holds_the_queue() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("race-upsert");

    queue_pause::pause_queue(&mut conn, &q, "first hold", "alice", None)
        .await
        .expect("seed pause");

    // Hold the row lock the way a resume transaction does, so the re-pause
    // below is forced to queue behind it rather than racing it by luck.
    let mut holder = connect(&url).await;
    holder.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT 1 FROM harvest_queue_pauses WHERE queue_name = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut holder)
        .await
        .expect("lock the pause row");

    let (url_for_task, q_for_task) = (url.clone(), q.clone());
    let repause = tokio::spawn(async move {
        let mut conn = connect(&url_for_task).await;
        queue_pause::pause_queue(&mut conn, &q_for_task, "second hold", "bob", None).await
    });

    // Let the re-pause reach the lock, then complete the resume and release.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    diesel::sql_query("DELETE FROM harvest_queue_pauses WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut holder)
        .await
        .expect("delete the pause row");
    holder.batch_execute("COMMIT").await.expect("commit");

    let outcome = repause
        .await
        .expect("join")
        .expect("a pause racing a resume must not error");
    assert_eq!(outcome.reason, "second hold");
    assert!(
        queue_pause::is_queue_paused(&mut conn, &q)
            .await
            .expect("is_paused"),
        "after a pause that raced a resume, the queue must actually be HELD -- \
         the failure mode this guards is an operator told the pause failed \
         while the queue keeps dispatching into the outage"
    );
}

/// AC4, the authoritative half: a queue pause committing *after* the timeout
/// scanner's non-locking snapshot must still suppress the `schedule_to_start`
/// timeout.
///
/// The scan predicate alone cannot guarantee this — it is a snapshot, and the
/// enforcer performs several more round trips before committing. Both sides
/// therefore take the same queue-scoped advisory lock. This reproduces the race
/// deterministically: the enforcer is started while a pause transaction holds
/// that lock, so the enforcer must block on it and observe the pause.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_pause_committed_after_the_scan_still_suppresses_the_timeout() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("race-timeout");

    // A task already past its schedule_to_start deadline, so the scanner picks
    // it up on its very next pass.
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.schedule_to_start = Some(chrono::Duration::seconds(1));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("backdate");

    // Hold the queue-pause advisory lock, simulating a pause transaction that
    // began before the enforcer ran and commits while it is mid-flight.
    let mut pauser = connect(&url).await;
    pauser.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut pauser)
        .await
        .expect("take the queue lock");
    diesel::sql_query(
        "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by) VALUES ($1, $2, $3)",
    )
    .bind::<diesel::sql_types::Text, _>(&q)
    .bind::<diesel::sql_types::Text, _>("outage")
    .bind::<diesel::sql_types::Text, _>("alice")
    .execute(&mut pauser)
    .await
    .expect("insert the pause row (uncommitted)");

    // The enforcer's scan snapshot predates the pause, so it selects the task;
    // its authoritative re-check must then block on the lock above.
    let url_for_task = url.clone();
    let enforcer = tokio::spawn(async move {
        let mut conn = connect(&url_for_task).await;
        autumn_harvest::timeout::enforce_timeouts_once(
            &mut conn,
            &autumn_harvest::telemetry::NoOpMetrics,
            std::time::Duration::from_secs(60),
            &None,
            &[],
            None,
            None,
            60,
        )
        .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    pauser.batch_execute("COMMIT").await.expect("commit pause");
    let _ = enforcer.await.expect("join");

    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let row: StateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(
        row.state, "PENDING",
        "a task must NOT be schedule-to-start-failed because its queue was \
         being paused -- a pause committing after the scan must still be \
         honoured by the enforcer's authoritative re-check (AC2/AC3/AC4)"
    );
}

/// Lock-ordering guard: the `schedule_to_start` enforcer must take the queue
/// advisory lock BEFORE it row-locks the execution and the task.
///
/// `resume_queue` takes the advisory lock and *then* row-locks every PENDING
/// task on the queue (its `scheduled_at` shift). If the enforcer took the rows
/// first and the advisory lock after, the two would form an ABBA cycle --
/// enforcement holding a task row while waiting on the advisory lock, resume
/// holding the advisory lock while waiting on that task row -- and Postgres
/// would abort one of them, failing either the timeout pass or the operator's
/// resume request.
///
/// Proven deterministically without provoking an actual deadlock (mirrors the
/// issue #779 `materializer_locks_execution_row_before_timers_no_abba` probe):
/// hold the advisory lock, start the enforcer so it blocks on it, then probe
/// both row locks with `FOR UPDATE NOWAIT` from a third connection. Both must
/// still be free -- under the inverted order the enforcer would already hold
/// them and the probe would fail with `lock_not_available`.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn enforcer_takes_the_queue_lock_before_the_row_locks_no_abba() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("abba-order");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.schedule_to_start = Some(chrono::Duration::seconds(1));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("backdate");

    // Stand in for `resume_queue`'s first lock: hold the queue advisory lock so
    // the enforcer must block on it.
    let mut holder = connect(&url).await;
    holder.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut holder)
        .await
        .expect("take the queue lock");

    let url_for_task = url.clone();
    let enforcer = tokio::spawn(async move {
        let mut conn = connect(&url_for_task).await;
        autumn_harvest::timeout::enforce_timeouts_once(
            &mut conn,
            &autumn_harvest::telemetry::NoOpMetrics,
            std::time::Duration::from_secs(60),
            &None,
            &[],
            None,
            None,
            60,
        )
        .await
    });

    // Give the enforcer time to reach (and block on) the advisory lock.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut probe = connect(&url).await;
    probe.batch_execute("BEGIN").await.expect("probe begin");
    let task_free =
        diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE NOWAIT")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .execute(&mut probe)
            .await
            .is_ok();
    assert!(
        task_free,
        "the enforcer must still be blocked on the QUEUE ADVISORY LOCK, not \
         holding the task row -- holding it here is the ABBA cycle against \
         resume_queue's scheduled_at shift"
    );
    let exec_free = diesel::sql_query(
        "SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE NOWAIT",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut probe)
    .await
    .is_ok();
    assert!(
        exec_free,
        "the enforcer must not have taken the execution row lock before the \
         queue advisory lock"
    );
    probe.batch_execute("COMMIT").await.expect("probe commit");

    holder.batch_execute("COMMIT").await.expect("release lock");
    let _ = enforcer.await.expect("join");
}

/// The release statement only ever undoes *this* worker's own claim, and it
/// restores the attempt the claim consumed.
#[test]
fn release_claim_query_is_guarded_and_restores_the_attempt() {
    let sql = queue_pause::release_claim_if_queue_paused_query();
    assert!(
        sql.contains("state = 'RUNNING'") && sql.contains("worker_id = $2"),
        "the release must be guarded on this worker's own RUNNING claim so it \
         can never disturb another worker's task: {sql}"
    );
    assert!(
        sql.contains("attempt = GREATEST(attempt - 1, 0)"),
        "a held task never ran, so the release must give back the attempt the \
         claim consumed -- otherwise a pause burns retry budget, which AC3 \
         forbids: {sql}"
    );
    assert!(
        sql.contains("EXISTS (SELECT 1 FROM harvest_queue_pauses qp"),
        "the release must be conditional on the queue actually being paused, \
         so an ordinary claim is never rolled back: {sql}"
    );
}

/// AC2/AC3 — a claim that won the race against a pause is released back to
/// `PENDING` with its attempt restored, so the worker never dispatches it.
#[tokio::test]
async fn a_claim_that_beat_the_pause_is_released_with_its_attempt_restored() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("claim-release");

    let task_id = enqueue_activity(&mut conn, &q, None).await;
    let claimed = claim_one(&mut conn, &q).await;
    assert_eq!(claimed, Some(task_id), "the unpaused claim must succeed");

    // The pause commits after the claim -- exactly the state the post-claim
    // re-check is reached with when a claim's snapshot predates the pause.
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");

    let released = queue_pause::release_claim_if_queue_paused(&mut conn, task_id, "w1")
        .await
        .expect("release");
    assert!(released, "a claim on a now-paused queue must be released");

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        worker_id: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        started_at: Option<chrono::DateTime<chrono::Utc>>,
    }
    let row: Row = diesel::sql_query(
        "SELECT state, attempt, worker_id, started_at FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result(&mut conn)
    .await
    .expect("load task");

    assert_eq!(
        row.state, "PENDING",
        "the task must be held, not dispatched"
    );
    assert_eq!(
        row.attempt, 0,
        "a hold must consume no retry budget (AC3) -- the claim's attempt \
         increment has to be given back"
    );
    assert!(row.worker_id.is_none(), "the claim must be fully undone");
    assert!(row.started_at.is_none(), "the claim must be fully undone");
}

/// The re-check must not disturb an ordinary claim: with no pause in effect it
/// is a no-op and the task stays `RUNNING`.
#[tokio::test]
async fn an_ordinary_claim_is_not_released_when_the_queue_is_not_paused() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("claim-noop");

    let task_id = enqueue_activity(&mut conn, &q, None).await;
    assert_eq!(claim_one(&mut conn, &q).await, Some(task_id));

    let released = queue_pause::release_claim_if_queue_paused(&mut conn, task_id, "w1")
        .await
        .expect("release");
    assert!(
        !released,
        "an unpaused queue's claim must never be rolled back"
    );

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    let row: Row = diesel::sql_query("SELECT state, attempt FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(row.state, "RUNNING");
    assert_eq!(row.attempt, 1, "the ordinary claim's attempt must stand");
}

/// AC2, the authoritative half of the *claim* path: a pause committing while a
/// claim statement is already in flight must still hold the task.
///
/// `claim_task` is a single autocommit statement, so under `READ COMMITTED` its
/// anti-join is evaluated against one snapshot taken at statement start. A
/// pause committing after that snapshot is invisible to it, and the claim goes
/// on to transition the task to `RUNNING` -- handing it to a worker that
/// dispatches straight into the outage the operator is riding out.
///
/// Reproduced deterministically by stalling the claim mid-statement on its
/// rate-limit debit (the one part of the CTE that can block, since the
/// candidate scan uses `SKIP LOCKED`): hold the bucket row from another
/// transaction, start the claim, commit the pause while the claim is parked,
/// then release the bucket. The claim then commits with a stale snapshot, and
/// the post-claim re-check -- a fresh statement, hence a fresh snapshot -- must
/// catch it and release the task.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_pause_committed_mid_claim_still_holds_the_task() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("mid-claim");
    let rl_key = format!("{q}-bucket");

    queue::ensure_rate_limit_bucket(&mut conn, &rl_key, 100.0, 100.0)
        .await
        .expect("bucket");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(rl_key.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    // Hold the rate-limit bucket row so the claim's debit blocks mid-statement,
    // freezing its snapshot from before the pause.
    let mut bucket_holder = connect(&url).await;
    bucket_holder.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET last_refilled_at = last_refilled_at WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&rl_key)
    .execute(&mut bucket_holder)
    .await
    .expect("hold the bucket row");

    let url_for_claim = url.clone();
    let q_for_claim = q.clone();
    let claim_in_flight = tokio::spawn(async move {
        let mut conn = connect(&url_for_claim).await;
        claim_task(&mut conn, &[q_for_claim], "w1", "", None, &[], &[])
            .await
            .expect("claim")
            .map(|t| t.id)
    });

    // Let the claim reach (and block on) the bucket row.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    queue_pause::pause_queue(&mut conn, &q, "outage started mid-claim", "alice", None)
        .await
        .expect("pause");
    bucket_holder
        .batch_execute("COMMIT")
        .await
        .expect("release the bucket row");

    let claimed = claim_in_flight.await.expect("join");
    assert!(
        claimed.is_none(),
        "a claim whose snapshot predated the pause must NOT be handed to the \
         worker -- the post-claim re-check has to release it, or the task is \
         dispatched into exactly the outage the hold exists to ride out"
    );

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    let row: Row = diesel::sql_query("SELECT state, attempt FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(row.state, "PENDING", "the task must be held (AC3)");
    assert_eq!(
        row.attempt, 0,
        "the hold must consume no retry budget (AC3)"
    );
}
