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

/// One id returned by the primary resume pass's `RETURNING id`.
///
/// The second pass excludes the rows the first one actually shifted rather than
/// re-deriving them from a predicate (issue #619 round 19), so any test that
/// drives the two passes by hand has to thread these ids through exactly the way
/// `resume_queue` does.
#[derive(diesel::QueryableByName)]
struct ShiftedTaskIdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
}

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
    // `created_at` moves with `scheduled_at`, because that is the only shape a
    // real row can have: `crate::queue::enqueue` sets
    // `created_at = NOW(), scheduled_at = NOW() + delay`, so a task that has
    // been *due* for N seconds was necessarily *created* at least N seconds
    // ago. Backdating `scheduled_at` alone would fabricate a row that claims to
    // have been waiting since before it existed — invisible while the resume
    // partition keyed on `transaction_timestamp()`, but it decides the row's
    // side now that the split is `created_at` vs `paused_at` (issue #619
    // round-16 review), and would silently move a pre-pause backlog row onto the
    // late-arrival side.
    //
    // A row whose `created_at` is already `NULL` keeps it: that is the legacy
    // pre-`20260619000000` shape one test deliberately constructs, and the
    // `IS NULL` arm of the primary pass exists precisely for it.
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

/// Read a pause's `paused_at`, so a hand-driven resume binds exactly what
/// `resume_queue` binds.
async fn paused_at_of(
    conn: &mut AsyncPgConnection,
    queue: &str,
) -> chrono::DateTime<chrono::offset::Utc> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct P {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        paused_at: chrono::DateTime<chrono::offset::Utc>,
    }
    diesel::sql_query("SELECT paused_at FROM harvest_queue_pauses WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .get_result::<P>(conn)
        .await
        .expect("paused_at")
        .paused_at
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
    // Surrounding whitespace is rejected, never trimmed -- see
    // `a_padded_queue_name_is_rejected_not_silently_retargeted`.
    assert!(queue_pause::validate_queue_name("payments ").is_err());
    assert!(queue_pause::validate_queue_name(" payments").is_err());
}

/// The resume shift must never push a held task's `scheduled_at` past the thaw
/// instant (that would make a thawed task *un*claimable — the inverse of AC5).
///
/// The clock is `clock_timestamp()`, not `NOW()`: `NOW()` is frozen at
/// transaction start, before the advisory-lock wait and the bulk update, so it
/// silently charges the resume's own runtime to every held task. See
/// `resume_credits_the_time_the_resume_itself_spends_waiting` for the behavioural
/// proof and the query's own doc comment for the full rationale.
#[test]
fn resume_shift_query_never_pushes_scheduled_at_past_now() {
    let sql = queue_pause::resume_shift_scheduled_at_query();
    assert!(sql.contains("GREATEST"), "expected the held-span formula");
    assert!(
        sql.contains("scheduled_at <= clock_timestamp()"),
        "only rows that are actually due may be shifted, measured against the \
         live clock; got:\n{sql}"
    );
    let (set_clause, _) = sql.split_once(" WHERE ").expect("UPDATE ... WHERE");
    assert!(
        !set_clause.contains("NOW()") && !set_clause.contains("transaction_timestamp()"),
        "the frozen transaction clock omits the resume's own runtime from the \
         credit; it may appear only in the created_at partition predicate; got:\n{sql}"
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

/// The resume's **own** runtime must be credited too, not just the pause span.
///
/// `NOW()` is `transaction_timestamp()`, frozen before `resume_queue` waits on
/// the advisory lock, deletes the pause row, and runs the bulk shift — yet the
/// hold stays in force until that transaction commits. With `NOW()`, every
/// second the resume itself spends is charged to the task: a backlog whose
/// `schedule_to_start` is shorter than the resume's runtime times out the
/// instant the queue thaws, which is exactly the AC3/AC4 promise ("never failed
/// *solely because* of the pause") being broken by the thaw itself.
///
/// The delay is injected the way it actually happens in production — by holding
/// the same advisory lock `resume_queue` takes — rather than by patching a
/// timestamp, so this exercises the real contended path.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resume_credits_the_time_the_resume_itself_spends_waiting() {
    const LOCK_HOLD_SECS: u64 = 3;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("thawclock");
    let task = enqueue_activity(&mut conn, &q, None).await;
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    // Genuinely waited 100s before the pause, then held 600s.
    backdate_scheduled_at(&mut conn, task, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;

    // Hold the advisory lock `resume_queue` needs, so the resume blocks on it
    // for a wall-clock span the frozen transaction clock cannot see.
    let holder_url = url.clone();
    let holder_queue = q.clone();
    let holder = tokio::spawn(async move {
        let mut holder = connect(&holder_url).await;
        let cls = autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS;
        holder
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock({cls}, hashtext('{holder_queue}'))"
            ))
            .await
            .expect("take advisory lock");
        tokio::time::sleep(std::time::Duration::from_secs(LOCK_HOLD_SECS)).await;
        holder.batch_execute("COMMIT").await.expect("release lock");
    });

    // Give the holder a moment to actually acquire the lock before resuming.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut resumer = connect(&url).await;
    let started = std::time::Instant::now();
    queue_pause::resume_queue(&mut resumer, &q, "alice")
        .await
        .expect("resume");
    let resume_wall = started.elapsed();
    holder.await.expect("holder task");

    let after = scheduled_at(&mut conn, task).await;
    let apparent_wait = (chrono::Utc::now() - after).num_seconds();

    // The task genuinely waited ~100s pre-pause. The frozen-clock bug charges
    // the resume's own runtime on top of that, so the assertion tolerance has to
    // be strictly SMALLER than the delay we injected — otherwise it could not
    // tell the fixed and broken behaviours apart. Prove that first, so a slow or
    // descheduled runner turns this into a clear failure rather than a test that
    // silently stops falsifying anything.
    const TOLERANCE_SECS: i64 = 1;
    let charged = resume_wall.as_secs_f64();
    assert!(
        charged > TOLERANCE_SECS as f64 + 0.5,
        "the resume must block long enough to exceed the {TOLERANCE_SECS}s \
         assertion tolerance, or this test cannot falsify the frozen-clock \
         behaviour; the resume took only {resume_wall:?}"
    );

    assert!(
        (100 - TOLERANCE_SECS..=100 + TOLERANCE_SECS).contains(&apparent_wait),
        "the resume's own {charged:.1}s must not be charged to the task: apparent \
         wait {apparent_wait}s, but the genuine pre-pause wait was ~100s (with a \
         frozen NOW() this reads ~{:.0}s)",
        100.0 + charged
    );
    assert!(
        after <= chrono::Utc::now(),
        "AC5: the shift must never push a held task into the future"
    );
}

/// Issue #619 review — a task enqueued *during* the resume is still held: the
/// pause row's `DELETE` is uncommitted, so a concurrent claimer's snapshot still
/// sees the hold and skips the queue. But it lands after the primary shift's
/// snapshot, so that statement can never see it, and without the late-arrivals
/// pass it thaws carrying its full uncredited wait — letting the
/// `schedule_to_start` scanner kill it for time it spent held (AC3/AC4).
///
/// Driven by hand because the window exists only *inside* `resume_queue`'s
/// transaction, between its two statements: no end-to-end call can produce it.
/// The statements, their order and their binds are exactly what `resume_queue`
/// issues, so this tests the real composition rather than a paraphrase.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resume_credits_a_task_enqueued_between_the_two_shift_passes() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("latearrival");
    let early = enqueue_activity(&mut conn, &q, None).await;
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    // Pre-existing backlog: genuinely waited 100s before the pause, held 600s.
    backdate_scheduled_at(&mut conn, early, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;
    let paused_at = paused_at_of(&mut conn, &q).await;

    // Open resume_queue's transaction by hand: advisory lock, then release the
    // hold. The DELETE stays uncommitted for the rest of this test.
    let mut resumer = connect(&url).await;
    let cls = autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS;
    resumer
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({cls}, hashtext('{q}')); \
             DELETE FROM harvest_queue_pauses WHERE queue_name = '{q}'"
        ))
        .await
        .expect("begin, lock, release the hold");

    // Pass 1 — the primary shift.
    let primary_ids: Vec<ShiftedTaskIdRow> =
        diesel::sql_query(queue_pause::resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&q)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .load(&mut resumer)
            .await
            .expect("primary shift");
    let primary = primary_ids.len();
    let shifted_ids: Vec<Uuid> = primary_ids.into_iter().map(|r| r.id).collect();

    // ── THE WINDOW ──────────────────────────────────────────────────────────
    let late = enqueue_activity(&mut conn, &q, None).await;
    backdate_scheduled_at(&mut conn, late, 100).await;
    assert_eq!(
        claim_one(&mut conn, &q).await,
        None,
        "the hold is still in force mid-resume: its DELETE has not committed, so \
         a task enqueued now is genuinely held and owed a credit"
    );
    // Prove the primary pass genuinely missed it, so the pass-2 assertion below
    // cannot be vacuous.
    let before_pass2 = (chrono::Utc::now() - scheduled_at(&mut conn, late).await).num_seconds();
    assert!(
        (99..=101).contains(&before_pass2),
        "the primary shift cannot see a row enqueued after its snapshot; if this \
         already read ~0s the second pass would be untested. apparent wait \
         {before_pass2}s"
    );

    // Pass 2 — the late-arrivals credit. It covers every due row the primary
    // pass did *not* shift, excluded by id rather than by predicate.
    let late_shifted = diesel::sql_query(queue_pause::resume_shift_late_arrivals_query())
        .bind::<diesel::sql_types::Text, _>(&q)
        .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
        .execute(&mut resumer)
        .await
        .expect("late-arrivals shift");
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
        "the task enqueued mid-resume must be credited by the second pass"
    );

    let late_wait = (chrono::Utc::now() - scheduled_at(&mut conn, late).await).num_seconds();
    assert!(
        (0..=1).contains(&late_wait),
        "a task enqueued during the thaw was held from the moment it became due, \
         so it accrues no wait; apparent wait {late_wait}s (uncredited it reads \
         ~{before_pass2}s)"
    );

    // ...and the pre-pause wait the primary pass preserved is untouched by it.
    let early_wait = (chrono::Utc::now() - scheduled_at(&mut conn, early).await).num_seconds();
    assert!(
        (99..=101).contains(&early_wait),
        "the second pass must not re-shift the pre-existing backlog (its relative \
         += credit is not idempotent): apparent wait {early_wait}s, expected the \
         genuine ~100s pre-pause wait"
    );

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(early),
        "AC5: after the thaw the longest-waiting task is claimable again"
    );
}

/// Issue #619 round-16 review — the partition between the two resume passes must
/// be decided by values that are **fixed on the row**, not by when a statement
/// happens to *see* it.
///
/// An earlier revision split on `created_at < transaction_timestamp()`
/// ("existed when this resume began"). But `created_at` records when the
/// enqueuing `INSERT` *executed*, while visibility is decided by when that
/// enqueuing transaction **committed** — and the two can straddle the resume.
/// A row inserted *before* the resume began but committed *after* the primary
/// pass took its snapshot therefore satisfied neither predicate: invisible to
/// pass 1, and excluded from pass 2 by its own `created_at` test. It thawed with
/// its entire held wait uncredited and could be schedule-to-start-failed the
/// instant the queue came back — the exact AC3/AC4 failure the shift exists to
/// prevent.
///
/// Constructed deterministically by holding the enqueuing transaction open
/// across pass 1, which is the only way to separate insert time from commit
/// time. Note this row is *not* the same as the one in
/// `resume_credits_a_task_enqueued_between_the_two_shift_passes`: that one is
/// inserted **and** committed inside the window, so its `created_at` is after
/// the resume's transaction start and the old predicate caught it too. Only an
/// insert-before / commit-after row distinguishes the two designs.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resume_credits_a_task_committed_late_but_inserted_before_the_resume() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("latecommit");
    let early = enqueue_activity(&mut conn, &q, None).await;
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_scheduled_at(&mut conn, early, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;
    let paused_at = paused_at_of(&mut conn, &q).await;

    // Enqueue on a connection whose transaction stays open: the row's INSERT
    // (and therefore its `created_at`) happens now, but nothing else can see it
    // until this transaction commits, several statements from now.
    let mut inserter = connect(&url).await;
    inserter.batch_execute("BEGIN").await.expect("begin insert");
    let straddler = enqueue_activity(&mut inserter, &q, None).await;
    // Give it a visible pre-credit wait so an uncredited outcome is detectable.
    backdate_scheduled_at(&mut inserter, straddler, 100).await;

    // Now open the resume's transaction — strictly after the straddler's INSERT,
    // so `transaction_timestamp() > straddler.created_at` and the old predicate
    // would place it on the primary side, where pass 1 cannot see it.
    let mut resumer = connect(&url).await;
    let cls = autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS;
    resumer
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({cls}, hashtext('{q}')); \
             DELETE FROM harvest_queue_pauses WHERE queue_name = '{q}'"
        ))
        .await
        .expect("begin, lock, release the hold");

    let primary_ids: Vec<ShiftedTaskIdRow> =
        diesel::sql_query(queue_pause::resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&q)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .load(&mut resumer)
            .await
            .expect("primary shift");
    let shifted_ids: Vec<Uuid> = primary_ids.into_iter().map(|r| r.id).collect();
    assert_eq!(
        shifted_ids,
        vec![early],
        "pass 1 sees only the committed backlog; the straddler is still \
         uncommitted and invisible to it"
    );

    // The straddler becomes visible only now — after pass 1's snapshot.
    inserter
        .batch_execute("COMMIT")
        .await
        .expect("commit the straddling enqueue");

    let late_shifted = diesel::sql_query(queue_pause::resume_shift_late_arrivals_query())
        .bind::<diesel::sql_types::Text, _>(&q)
        .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
        .execute(&mut resumer)
        .await
        .expect("late-arrivals shift");
    resumer
        .batch_execute("COMMIT")
        .await
        .expect("commit resume");

    assert_eq!(
        late_shifted, 1,
        "a row inserted before the resume but committed after pass 1 is still \
         genuinely held, so pass 2 must own it; partitioning on \
         transaction_timestamp() drops it from both passes"
    );

    let wait = (chrono::Utc::now() - scheduled_at(&mut conn, straddler).await).num_seconds();
    assert!(
        (0..=2).contains(&wait),
        "the straddling row must be credited for the time it spent held; \
         apparent wait {wait}s (uncredited it reads ~100s)"
    );

    // The committed backlog keeps the wait it genuinely accrued before the hold.
    let early_wait = (chrono::Utc::now() - scheduled_at(&mut conn, early).await).num_seconds();
    assert!(
        (99..=101).contains(&early_wait),
        "the pre-pause wait must survive both passes: apparent wait {early_wait}s"
    );
}

/// The `created_at` partition between the two resume passes must be **total**.
/// `harvest_task_queue.created_at` is nullable — a `PENDING` row enqueued before
/// migration `20260619000000` has none — and in SQL a comparison against `NULL`
/// yields `NULL`, not `false`. A bare `<` / `>=` split would therefore leave such
/// a legacy row matching *neither* pass and credited by *neither*, silently
/// regressing against the pre-partition query (which had no `created_at`
/// predicate and shifted it). It must land on the primary side: no `created_at`
/// means it was certainly enqueued before this resume began.
#[tokio::test]
async fn resume_credits_a_legacy_task_with_no_created_at() {
    use diesel_async::RunQueryDsl;

    let (mut conn, _c) = setup_db().await;
    let q = unique_queue("legacycreated");
    let task = enqueue_activity(&mut conn, &q, None).await;
    // Simulate a row that predates the created_at column.
    diesel::sql_query("UPDATE harvest_task_queue SET created_at = NULL WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task)
        .execute(&mut conn)
        .await
        .expect("null out created_at");

    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    // Genuinely waited 100s before the pause, then held 600s.
    backdate_scheduled_at(&mut conn, task, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;

    let resumed = queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");
    assert_eq!(
        resumed.released_task_count, 1,
        "a legacy NULL-created_at row must be counted as released, not skipped by \
         both passes"
    );

    let wait = (chrono::Utc::now() - scheduled_at(&mut conn, task).await).num_seconds();
    assert!(
        (99..=101).contains(&wait),
        "the legacy row must be credited the 600s it was held and keep its genuine \
         ~100s pre-pause wait; apparent wait {wait}s (uncredited it reads ~700s)"
    );
    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task),
        "AC5: it is claimable again after the thaw"
    );
}

/// Issue #619 round-17 review — a resume must **wake parked workers**, not just
/// make the backlog claimable and wait for the next poll tick.
///
/// A worker configured with `notification_database_url` parks in
/// `wait_for_notification(poll_interval)` and only re-polls on a queue
/// notification or the timeout. The resume flips held rows to claimable but
/// emitted nothing, so on a fleet tuned for a long `poll_interval` (the whole
/// point of running with LISTEN/NOTIFY) the thawed backlog sat idle for up to a
/// full interval after the operator declared the outage over — with the
/// dashboard showing the queue live and nothing moving.
///
/// The notify is emitted **inside the resume transaction**: Postgres queues
/// `NOTIFY` and delivers at `COMMIT`, so listeners wake exactly when the hold
/// lifts — never earlier (an early wake would still see the pause row and skip
/// the queue) and never at all if the resume rolls back.
///
/// Asserted through a real [`QueueListener`], the same type the worker's poll
/// loop uses. The payload assertion is load-bearing beyond "something arrived":
/// the listener *parses* `NotifyPayload { task_id }` and surfaces a parse
/// failure as an error the poll loop handles by sleeping a full interval — so a
/// synthetic or nil id would be both a lie and strictly worse than sending
/// nothing. Requiring the real claimable task id pins that.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resume_notifies_parked_listeners_so_they_re_poll_immediately() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("resumenotify");

    let task = enqueue_activity(&mut conn, &q, None).await;
    queue_pause::pause_queue(&mut conn, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_scheduled_at(&mut conn, task, 700).await;
    backdate_paused_at(&mut conn, &q, 600).await;

    let mut listener =
        autumn_harvest::notify::QueueListener::connect(&url, std::slice::from_ref(&q))
            .await
            .expect("listen on the paused queue");

    // Baseline: nothing is ringing the doorbell while the hold is in place, so a
    // wake after the resume cannot be attributed to unrelated traffic.
    let quiet = listener
        .wait_for_notification(std::time::Duration::from_millis(300))
        .await
        .expect("baseline wait");
    assert!(
        quiet.is_none(),
        "the paused queue must be silent before the resume, or this test cannot \
         attribute the wake below to the resume itself"
    );

    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");

    let woken = listener
        .wait_for_notification(std::time::Duration::from_secs(5))
        .await
        .expect("notification payload must parse")
        .expect(
            "a resume that releases held tasks must ring the queue doorbell; \
             without it a parked worker sleeps out its whole poll_interval \
             before noticing the thaw",
        );
    assert_eq!(
        woken.task_id, task,
        "the payload must name a genuinely claimable task: the listener parses \
         this field, and a synthetic or nil id would be a lie in a field typed \
         as a real task"
    );

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task),
        "AC5: the woken worker finds the task claimable"
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

/// A queue name carrying surrounding whitespace is **rejected**, never
/// silently retargeted at a different queue.
///
/// `EnqueueParams` stores `queue_name` verbatim (it does not normalise) and the
/// claim anti-join matches on exact equality, so `"payments "` and `"payments"`
/// are two genuinely distinct, independently reachable queues. That makes both
/// of the quieter options a lie:
///
/// - Validating `name.trim()` while storing the **raw** string inserts a row
///   matching no task -- a reported hold on a queue that keeps dispatching.
/// - **Trimming** inserts a row for `"payments"`, so pausing `"payments "`
///   would hold a *different, valid* queue while the real one kept dispatching
///   -- and the API would still report success.
///
/// This test proves the two names really are distinct queues (so trimming would
/// have conflated them), and that the padded form is refused with a
/// `Config`-shaped error (a `400`) rather than quietly holding the wrong one.
#[tokio::test]
async fn a_padded_queue_name_is_rejected_not_silently_retargeted() {
    let (mut conn, _c) = setup_db().await;
    let plain = unique_queue("trim");
    let padded_name = format!("{plain} ");

    // Two genuinely distinct queues -- one task each. `enqueue` stores the name
    // verbatim, so the trailing-space queue is real and independently claimable.
    for name in [plain.as_str(), padded_name.as_str()] {
        let params = EnqueueParams::new(name, TaskType::Activity, serde_json::json!({}));
        queue::enqueue(&mut conn, &params).await.expect("enqueue");
    }

    // Pausing the exact padded name is refused, not applied to `plain`.
    let err = queue_pause::pause_queue(&mut conn, &padded_name, "outage", "alice", None)
        .await
        .expect_err("a queue name with surrounding whitespace must be rejected");
    assert!(
        matches!(err, autumn_harvest::HarvestError::Config(_)),
        "must surface as a Config error (a 400), not a 500; got {err:?}"
    );
    assert!(
        err.to_string().contains("whitespace"),
        "the message must name the actual problem so an operator can fix the \
         typo mid-incident; got {err}"
    );

    // Crucially: the refusal did not silently hold the *other* queue.
    assert!(
        !queue_pause::is_queue_paused(&mut conn, &plain)
            .await
            .expect("is_paused"),
        "rejecting the padded name must never hold the trimmed queue -- that \
         would be the silent-retarget bug this rejection exists to prevent"
    );
    assert!(
        !queue_pause::is_queue_paused(&mut conn, &padded_name)
            .await
            .expect("is_paused"),
        "a rejected pause must leave no row at all"
    );

    // And the two names really are separate queues: holding `plain` leaves the
    // whitespace-named queue dispatching. If the write path trimmed, pausing
    // the padded name would have held THIS queue instead.
    queue_pause::pause_queue(&mut conn, &plain, "outage", "alice", None)
        .await
        .expect("pause the plain queue");
    assert!(
        claim_one(&mut conn, &plain).await.is_none(),
        "the held queue must not dispatch"
    );
    assert!(
        claim_one(&mut conn, &padded_name).await.is_some(),
        "the whitespace-named queue is a DIFFERENT queue and must keep \
         dispatching -- proving a trim-based pause would have hit the wrong one"
    );

    // Resume is symmetric: the padded form is refused there too.
    let err = queue_pause::resume_queue(&mut conn, &padded_name, "alice")
        .await
        .expect_err("resume must reject a padded name too");
    assert!(matches!(err, autumn_harvest::HarvestError::Config(_)));
    assert!(
        queue_pause::is_queue_paused(&mut conn, &plain)
            .await
            .expect("is_paused"),
        "a rejected resume must not release the trimmed queue's hold"
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
    diesel::sql_query(autumn_harvest::queue_pause::lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS)
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
    diesel::sql_query(autumn_harvest::queue_pause::lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS)
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

/// Issue #619 round-17 review — the post-claim pause re-check must be **atomic
/// with the claim**, so a re-check that fails cannot strand the task.
///
/// The re-check has to run in its own statement (a fresh `READ COMMITTED`
/// snapshot is the entire mechanism that lets it see a pause the claim's own
/// snapshot predated). But two *autocommit* statements are not atomic: if the
/// re-check errors, the claim is already durable, so the row is left `RUNNING`
/// with its attempt consumed while `claim_task` returns `Err` and the worker
/// never receives the task. Nothing recovers it in bounded time:
///
/// - the poison-pill reclaimer (#367) keys on a **missing worker heartbeat**,
///   and this worker is alive and polling, so it never touches the row;
/// - `start_to_close` / `heartbeat_timeout` would eventually reclaim it, but
///   both are optional — an activity declaring neither strands indefinitely.
///
/// So the claim and the re-check run inside one `READ COMMITTED` transaction:
/// each statement still gets its own snapshot (the re-check keeps working),
/// and a re-check failure rolls the claim back. The isolation level is
/// **pinned on the `BEGIN`** rather than inherited, because under `REPEATABLE
/// READ` both statements would share one snapshot and the re-check would go
/// blind to the pause — silently reinstating the round-2 P1 this guard exists
/// to close.
///
/// Injection: the same mid-claim harness as above (hold the rate-limit bucket
/// row so the pause commits while the claim is blocked, which is what makes the
/// re-check fire at all), plus a `BEFORE UPDATE` trigger that raises only on the
/// release-shaped update — `RUNNING -> PENDING` with `worker_id` cleared, which
/// the claim's own `PENDING -> RUNNING` update cannot match. `claim_task`
/// returns `Err` under both designs, so the assertion is purely on the row: the
/// question is whether the claim survived the failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_failed_pause_recheck_rolls_the_claim_back_instead_of_stranding_it() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("recheck-fail");
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

    // Fail exactly the release update, and only for this test's queue: the
    // claim moves PENDING -> RUNNING and sets a worker, the release moves
    // RUNNING -> PENDING and clears it, so the shapes cannot be confused.
    let trigger_fn = format!("fail_release_{}", q.replace('-', "_"));
    conn.batch_execute(&format!(
        "CREATE FUNCTION {trigger_fn}() RETURNS trigger AS $$
         BEGIN
           IF OLD.queue_name = '{q}'
              AND OLD.state = 'RUNNING'
              AND NEW.state = 'PENDING'
              AND NEW.worker_id IS NULL THEN
             RAISE EXCEPTION 'injected re-check failure';
           END IF;
           RETURN NEW;
         END; $$ LANGUAGE plpgsql;
         CREATE TRIGGER {trigger_fn}_trg BEFORE UPDATE ON harvest_task_queue
           FOR EACH ROW EXECUTE FUNCTION {trigger_fn}()"
    ))
    .await
    .expect("install release-failure trigger");

    // Hold the rate-limit bucket row so the claim blocks mid-statement and the
    // pause lands underneath it -- otherwise the claim's own anti-join skips the
    // queue and there is no claim to roll back.
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
        claim_task(&mut conn, &[q_for_claim], "w1", "", None, &[], &[]).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    queue_pause::pause_queue(&mut conn, &q, "outage started mid-claim", "alice", None)
        .await
        .expect("pause");
    bucket_holder
        .batch_execute("COMMIT")
        .await
        .expect("release the bucket row");

    let outcome = claim_in_flight.await.expect("join");
    assert!(
        outcome.is_err(),
        "the injected trigger must surface as a claim error under both designs; \
         if it does not, the injection missed the release statement and this \
         test is no longer falsifying anything"
    );

    // Drop the trigger before asserting so a failure leaves a usable database.
    conn.batch_execute(&format!(
        "DROP TRIGGER {trigger_fn}_trg ON harvest_task_queue; DROP FUNCTION {trigger_fn}()"
    ))
    .await
    .expect("drop trigger");

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        worker_id: Option<String>,
    }
    let row: Row =
        diesel::sql_query("SELECT state, attempt, worker_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result(&mut conn)
            .await
            .expect("load task");

    assert_eq!(
        row.state, "PENDING",
        "a failed re-check must roll the claim back, not leave the task RUNNING \
         under a worker that never received it (nothing reclaims it: the \
         poison-pill sweep keys on a dead worker, and this one is alive)"
    );
    assert_eq!(
        row.attempt, 0,
        "the rolled-back claim must not consume a retry attempt (AC3)"
    );
    assert_eq!(
        row.worker_id, None,
        "the rolled-back claim must leave no worker ownership behind"
    );
}

/// `paused_at` must be stamped when the hold becomes effective, not at
/// transaction start (round 11 P2).
///
/// The column defaults to `NOW()`, which Postgres freezes at transaction
/// start — before `lock_queue_for_pause` waits. A pause that queues behind a
/// large concurrent resume therefore records a `paused_at` that predates the
/// effective hold by the whole lock wait, so:
///
/// - `GET /admin/queues/paused`, the Vantage banner, the resume response's
///   `paused_duration_secs`, and the `harvest_queue_paused_too_long` alert all
///   report a hold that started earlier (and has lasted longer) than it has;
/// - the resume shift credits `clock_timestamp() - GREATEST(scheduled_at,
///   paused_at)`, so it credits an interval the task was never held, eroding the
///   pre-pause wait the shift exists to preserve.
///
/// The delay is injected the way production produces it — by holding the very
/// advisory lock `pause_queue` takes — and the tolerance is strictly smaller
/// than the injected delay, so a slow runner fails loudly rather than silently
/// stopping falsifying anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pause_stamps_paused_at_after_the_lock_wait_not_at_transaction_start() {
    const LOCK_HOLD_SECS: u64 = 3;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("pauseclock");

    // Hold the advisory lock `pause_queue` needs, so the pause blocks on it for
    // a wall-clock span the frozen transaction clock cannot see.
    let holder_url = url.clone();
    let holder_queue = q.clone();
    let holder = tokio::spawn(async move {
        let mut holder = connect(&holder_url).await;
        let cls = autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS;
        holder
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock({cls}, hashtext('{holder_queue}'))"
            ))
            .await
            .expect("take advisory lock");
        tokio::time::sleep(std::time::Duration::from_secs(LOCK_HOLD_SECS)).await;
        holder.batch_execute("COMMIT").await.expect("release lock");
    });

    // Let the holder actually acquire the lock before the pause starts.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Anchor BEFORE the pause transaction opens: this is the instant a frozen
    // `NOW()` would (approximately) record.
    let before_pause = chrono::Utc::now();

    let mut pauser = connect(&url).await;
    let started = std::time::Instant::now();
    queue_pause::pause_queue(&mut pauser, &q, "outage", "alice", None)
        .await
        .expect("pause");
    let pause_wall = started.elapsed();
    holder.await.expect("holder task");

    // The pause genuinely blocked for the injected span, so the test is
    // exercising the window at all.
    assert!(
        pause_wall.as_secs_f64() >= f64::from(u32::try_from(LOCK_HOLD_SECS).unwrap()) - 1.0,
        "the pause must have actually blocked on the lock for ~{LOCK_HOLD_SECS}s, \
         otherwise this test proves nothing; blocked {pause_wall:?}"
    );

    let stored = paused_at_of(&mut conn, &q).await;
    let lag = (stored - before_pause).num_milliseconds();

    // Tolerance is strictly SMALLER than the injected delay so it can actually
    // falsify: the frozen clock records ~0ms of lag, the live clock ~3000ms.
    assert!(
        lag >= 2_000,
        "paused_at must be stamped AFTER the lock wait (expected >= ~{LOCK_HOLD_SECS}s \
         of lag past the pre-pause anchor, got {lag}ms) — a frozen transaction \
         clock records the hold as starting before it was effective"
    );
    // And it must not be in the future.
    assert!(
        stored <= chrono::Utc::now(),
        "paused_at must not be stamped in the future; got {stored}"
    );
}

/// An idempotent re-pause must preserve the ORIGINAL `paused_at`.
///
/// The runbook explicitly tells an operator to re-issue a pause to repair a
/// `partial_fleet` hold, so re-stamping would erase how long the queue had
/// really been held — and the resume shift would then credit only from the
/// re-pause, under-crediting the very backlog the hold was protecting.
#[tokio::test]
async fn a_repause_preserves_the_original_paused_at() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("repausestamp");
    queue_pause::pause_queue(&mut conn, &q, "stripe outage", "alice", None)
        .await
        .expect("first pause");
    // Backdate so a re-stamp would be unmistakable.
    backdate_paused_at(&mut conn, &q, 600).await;
    let original = paused_at_of(&mut conn, &q).await;

    let second = queue_pause::pause_queue(&mut conn, &q, "fleet freeze", "bob", None)
        .await
        .expect("re-pause");

    assert!(!second.newly_paused, "a re-pause is not a fresh hold");
    let after = paused_at_of(&mut conn, &q).await;
    assert_eq!(
        after, original,
        "a re-pause must preserve the original hold's start time"
    );
    assert_eq!(
        second.paused_at, original,
        "the echoed paused_at must be the preserved value, not the re-pause instant"
    );
    // Provenance is preserved too (the round-1 idempotency rule).
    assert_eq!(second.reason, "stripe outage");
    assert_eq!(second.paused_by, "alice");
}

/// A queue pause committing after the scan must also suppress the **workflow**
/// schedule-to-start timeout, not just the activity one (issue #619 round-15
/// review).
///
/// `find_timed_out_tasks` does not filter on `task_type`, so a PENDING workflow
/// task carrying `schedule_to_start` is routed to `enforce_workflow_timeout`
/// exactly as an activity task is routed to `enforce_activity_timeout` — but
/// only the latter had the authoritative queue-pause re-check. The blast radius
/// on this path is strictly larger: it appends `WorkflowFailed` and seals the
/// whole execution `TIMED_OUT`, where the activity path fails one task.
///
/// No supported product path sets `schedule_to_start` on a workflow task (every
/// workflow enqueue site leaves it `None`; the one site that sets it is
/// hard-coded `TaskType::Activity`), so the row is crafted here with a raw
/// UPDATE — the same technique `child_policy_tests::
/// workflow_task_timeout_cascades_detached_children` already uses to exercise
/// this enforcement path. The state is nonetheless reachable by an embedder,
/// since `queue::enqueue` and `EnqueueParams::schedule_to_start` are both public.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_committed_after_the_scan_suppresses_the_workflow_timeout_too() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("race-wf-timeout");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    // Craft the state no product path produces: a workflow task that is already
    // past a schedule_to_start deadline, so the scanner selects it next pass.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = NOW() - INTERVAL '1 hour', \
             schedule_to_start = INTERVAL '1 second' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("make the workflow task timeout-eligible");

    // Hold the queue-pause advisory lock, simulating a pause transaction that
    // began before the enforcer ran and commits while it is mid-flight.
    let mut pauser = connect(&url).await;
    pauser.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(autumn_harvest::queue_pause::lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS)
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
    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }

    let task: StateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(
        task.state, "PENDING",
        "a held WORKFLOW task must NOT be schedule-to-start-failed because its \
         queue was being paused (AC3/AC4)"
    );

    let execution: StateRow =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id)
            .get_result(&mut conn)
            .await
            .expect("load execution");
    assert_ne!(
        execution.state, "TIMED_OUT",
        "the pause must not let the enforcer seal the whole execution -- this is \
         the blast radius that makes the workflow path worse than the activity one"
    );

    let events: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = 'WorkflowFailed'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result(&mut conn)
    .await
    .expect("count WorkflowFailed events");
    assert_eq!(
        events.count, 0,
        "no WorkflowFailed may be appended for a task held by a queue pause"
    );
}

/// The companion to the test above: with **no** pause in force, a workflow task
/// past its `schedule_to_start` deadline must still be timed out.
///
/// Without this, the queue-pause guard added to `enforce_workflow_timeout` could
/// silently suppress *every* workflow schedule-to-start timeout (e.g. if the
/// `is_queue_paused` sense were inverted, or the `matches!` reason scope
/// widened) and the paused-side test above would still pass. This is also the
/// local-Postgres stand-in for `child_policy_tests::
/// workflow_task_timeout_cascades_detached_children`, which exercises the same
/// enforcement path but is testcontainers-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpaused_workflow_task_is_still_schedule_to_start_timed_out() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("wf-to-fires");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = NOW() - INTERVAL '1 hour', \
             schedule_to_start = INTERVAL '1 second' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("make the workflow task timeout-eligible");

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
    .expect("enforce");

    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }

    let task: StateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(
        task.state, "FAILED",
        "with no pause in force the workflow schedule-to-start timeout must \
         still fire -- the guard must suppress a HELD task, not every task"
    );

    let execution: StateRow =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id)
            .get_result(&mut conn)
            .await
            .expect("load execution");
    assert_eq!(
        execution.state, "TIMED_OUT",
        "the execution must still be sealed TIMED_OUT when nothing is holding it"
    );
}

/// Round-18 P1 — a claim must never commit *after* a pause has been
/// acknowledged.
///
/// Round 17 wrapped the claim and its authoritative pause re-check in one
/// transaction so a re-check error could no longer strand a `RUNNING` row. That
/// moved the claim's commit *after* the re-check's snapshot, opening a window:
/// a pause committing in between is invisible to the re-check, the pause API
/// returns success, and the claim commits afterwards anyway — a worker
/// dispatching into an outage the operator has already been told is contained.
///
/// The barrier is `try_lock_queue_for_claim`, the **shared** mode of the key
/// `pause_queue` takes exclusively. This test holds that exclusive lock from a
/// second connection (an in-flight, uncommitted pause transaction) and asserts
/// the claim declines rather than dispatching.
///
/// Note what makes this a genuine regression test rather than a restatement of
/// the round-2 guard: **no pause row is committed**, so the anti-join and the
/// re-check both legitimately see an unpaused queue. Before the barrier the
/// task is claimed; after it, the claim is given back untouched — same state,
/// same attempt, immediately re-claimable once the pause transaction settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_declines_while_a_pause_transaction_is_committing() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("claim-barrier");

    let params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    // An in-flight pause: it holds the exclusive queue lock but has NOT
    // committed, so its pause row is invisible to every other transaction.
    let mut pauser = connect(&url).await;
    pauser.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(autumn_harvest::queue_pause::lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS)
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut pauser)
        .await
        .expect("take the exclusive queue lock");

    let claimed = claim_one(&mut conn, &q).await;
    assert!(
        claimed.is_none(),
        "a claim must not be handed out while a pause transaction is committing: \
         its own re-check cannot see the uncommitted pause, so without the shared-lock \
         barrier the claim commits after the pause is acknowledged"
    );

    #[derive(diesel::QueryableByName)]
    struct TaskRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        worker_id: Option<String>,
    }

    let row: TaskRow =
        diesel::sql_query("SELECT state, attempt, worker_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result(&mut conn)
            .await
            .expect("load task");
    assert_eq!(
        row.state, "PENDING",
        "the declined claim must be given back"
    );
    assert_eq!(row.attempt, 0, "declining must not consume retry budget");
    assert_eq!(
        row.worker_id, None,
        "the declined claim must clear worker_id"
    );

    // Once the pause transaction settles (here: rolls back, so the queue was
    // never actually held), the task is immediately claimable again — the
    // barrier costs at most one poll interval.
    pauser.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task_id),
        "with no pause in force the task must be claimable again immediately"
    );
}

/// Round-18 P2 — a stale scan result must not time out a task whose deadline a
/// completed resume has already credited forward.
///
/// `find_timed_out_tasks` produces a snapshot and a large batch takes time, so
/// a whole pause/resume cycle can complete before a given row is reached.
/// Resume shifts `scheduled_at` forward to credit the held time and then
/// **deletes** the pause row — so the pause re-check finds nothing to suppress
/// on, and enforcement proceeds on the scan-time deadline, timing the task out
/// the instant its deadline was extended. On this (workflow) path that seals
/// the entire execution `TIMED_OUT`, which is exactly what AC3/AC4 forbid.
///
/// The advisory lock sequences it deterministically: the enforcer scans, then
/// blocks on the queue lock, while the held transaction performs the very
/// `scheduled_at` shift `resume_queue` performs and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_shift_after_the_scan_suppresses_the_stale_timeout() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("stale-scan");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = NOW() - INTERVAL '1 hour', \
             schedule_to_start = INTERVAL '1 second' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("make the workflow task timeout-eligible");

    // Hold the queue lock so the enforcer's scan lands first and its
    // per-row enforcement blocks, standing in for the batch delay.
    let mut shifter = connect(&url).await;
    shifter.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(autumn_harvest::queue_pause::lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS)
        .bind::<diesel::sql_types::Text, _>(&q)
        .execute(&mut shifter)
        .await
        .expect("take the queue lock");

    let url_for_enforcer = url.clone();
    let enforcer = tokio::spawn(async move {
        let mut conn = connect(&url_for_enforcer).await;
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

    // Exactly the row mutation `resume_queue` performs when it credits held
    // time back, committed while the enforcer is blocked on the queue lock and
    // leaving NO pause row behind for the re-check to find.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET scheduled_at = NOW() + INTERVAL '1 hour' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut shifter)
    .await
    .expect("credit the held time back");
    shifter.batch_execute("COMMIT").await.expect("commit shift");

    let _ = enforcer.await.expect("join");

    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }

    let task: StateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("load task");
    assert_eq!(
        task.state, "PENDING",
        "a task whose deadline was credited forward before enforcement reached it \
         must not be failed on the stale scan result"
    );

    let execution: StateRow =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id)
            .get_result(&mut conn)
            .await
            .expect("load execution");
    assert_ne!(
        execution.state, "TIMED_OUT",
        "sealing the whole execution on a credited-forward deadline is the AC3/AC4 violation"
    );
}

/// Round-19 P2 (issue #619): a row whose enqueue *executed* before the pause but
/// *committed* after the primary resume shift's snapshot must still be credited.
///
/// This is the case the round-16 semantic partition could not reach. The
/// partition is total and visibility-independent — every due `PENDING` row
/// belongs to exactly one side whenever it becomes visible — but totality of the
/// *predicates* is not coverage by the *passes*. Such a row is on pass 1's side
/// (`created_at < paused_at`) yet invisible to pass 1's snapshot, and the old
/// pass 2 excluded it by re-testing that very predicate. Credited by neither, it
/// thawed carrying its whole held wait and was immediately
/// `schedule_to_start`-failable — the AC3/AC4 failure the shift exists to prevent.
///
/// The race is sequenced deterministically rather than hoped for: a second
/// connection holds the enqueue open across pass 1 and commits before pass 2, so
/// the two statements are driven with exactly the binds `resume_queue` uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_pause_enqueue_committing_after_the_first_pass_is_still_credited() {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct PausedAtRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        paused_at: chrono::DateTime<chrono::offset::Utc>,
    }

    let (url, _c) = setup_db_url().await;
    let mut driver = connect(&url).await;
    let mut slow = connect(&url).await;
    let q = unique_queue("late-commit");

    // An ordinary pre-pause task the primary pass WILL see. Its presence is what
    // makes the exclusion set non-empty, so this also proves the id-exclusion
    // does not double-shift a row pass 1 already credited.
    let visible = enqueue_activity(&mut driver, &q, None).await;
    backdate_scheduled_at(&mut driver, visible, 7200).await;

    // The slow enqueue: INSERT executes now (so `created_at` will predate the
    // pause once we backdate it), but the transaction stays open across pass 1.
    diesel::sql_query("BEGIN")
        .execute(&mut slow)
        .await
        .expect("open the slow enqueue transaction");
    let late_commit = enqueue_activity(&mut slow, &q, None).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET created_at = NOW() - INTERVAL '2 hours', \
             scheduled_at = NOW() - INTERVAL '2 hours' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(late_commit)
    .execute(&mut slow)
    .await
    .expect("age the uncommitted row onto the primary pass's side");

    // Hold for an hour: created_at (2h ago) < paused_at (1h ago), so this row is
    // squarely on the PRIMARY pass's side of the partition.
    queue_pause::pause_queue(&mut driver, &q, "outage", "alice", None)
        .await
        .expect("pause");
    backdate_paused_at(&mut driver, &q, 3600).await;
    let paused_at =
        diesel::sql_query("SELECT paused_at FROM harvest_queue_pauses WHERE queue_name = $1")
            .bind::<diesel::sql_types::Text, _>(&q)
            .load::<PausedAtRow>(&mut driver)
            .await
            .expect("read paused_at")
            .into_iter()
            .next()
            .expect("the hold exists")
            .paused_at;

    // Pass 1. The slow enqueue is still uncommitted, so this cannot see it.
    let shifted: Vec<ShiftedTaskIdRow> =
        diesel::sql_query(queue_pause::resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&q)
            .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
            .load(&mut driver)
            .await
            .expect("primary shift");
    let shifted_ids: Vec<Uuid> = shifted.into_iter().map(|r| r.id).collect();
    assert_eq!(
        shifted_ids,
        vec![visible],
        "pass 1 must shift exactly the row it can see"
    );

    // The enqueue lands between the two snapshots — the whole point.
    diesel::sql_query("COMMIT")
        .execute(&mut slow)
        .await
        .expect("commit the slow enqueue");

    // Pass 2, excluding exactly what pass 1 reported.
    diesel::sql_query(queue_pause::resume_shift_late_arrivals_query())
        .bind::<diesel::sql_types::Text, _>(&q)
        .bind::<diesel::sql_types::Timestamptz, _>(paused_at)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
        .execute(&mut driver)
        .await
        .expect("second pass");

    // Both rows were due 2h ago and held for 1h, so both keep the 1h of wait
    // they had accrued *before* the hold and none of the hold itself.
    let late_age =
        (chrono::Utc::now() - scheduled_at(&mut driver, late_commit).await).num_seconds();
    assert!(
        (3540..=3660).contains(&late_age),
        "a pre-pause enqueue that committed after the primary pass must still be \
         credited the held hour; got {late_age}s (uncredited would be ~7200s)"
    );

    let visible_age = (chrono::Utc::now() - scheduled_at(&mut driver, visible).await).num_seconds();
    assert!(
        (3540..=3660).contains(&visible_age),
        "the row pass 1 already shifted must be excluded from pass 2, not shifted \
         a second time; got {visible_age}s"
    );
}

/// Round-20 P2: the backlog scan's own duration must be credited back on resume.
///
/// The queue becomes effectively held the instant `lock_queue_for_pause` is
/// granted — round 18's `try_lock_queue_for_claim` takes the **shared** mode of
/// that very key, so from then on every claim fails its `try` and gives the task
/// back. Round 14 nonetheless stamped `paused_at` *after* the
/// `held_task_count_query` backlog `COUNT(*)`, on the (then-true, now-false)
/// premise that claimers share neither the lock nor visibility of the uncommitted
/// pause row. That scan is unbounded in principle on a large `PENDING` backlog,
/// so its whole duration was held-but-uncredited: a task with a short
/// `schedule_to_start` could be timed out immediately after the thaw for time the
/// pause transaction itself blocked it.
///
/// Made deterministic rather than hoped for: a second connection holds an
/// `ACCESS EXCLUSIVE` lock on `harvest_task_queue`, which blocks the `COUNT(*)`
/// (a plain `SELECT` does wait on that lock) while leaving the advisory lock and
/// the `harvest_queue_pauses` insert untouched. The pause therefore stalls
/// *exactly* in the window under test.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pause_credits_the_backlog_scan_interval() {
    use diesel_async::RunQueryDsl;

    const BLOCK_SECS: u64 = 3;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    let q = unique_queue("scanstamp");
    let task = enqueue_activity(&mut conn, &q, None).await;
    backdate_scheduled_at(&mut conn, task, 100).await;

    // Block the backlog COUNT(*) — and nothing else the pause touches.
    let mut blocker = connect(&url).await;
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_task_queue IN ACCESS EXCLUSIVE MODE")
        .await
        .expect("hold the task table");

    let pause_url = url.clone();
    let pause_queue_name = q.clone();
    let pausing = tokio::spawn(async move {
        let mut c = connect(&pause_url).await;
        queue_pause::pause_queue(&mut c, &pause_queue_name, "scan stall", "alice", None)
            .await
            .expect("pause")
    });

    // Let the pause take the advisory lock, read the clock, and park on the scan.
    tokio::time::sleep(std::time::Duration::from_secs(BLOCK_SECS)).await;

    // The instant just before the scan can proceed. A correctly-stamped
    // `paused_at` was taken ~BLOCK_SECS ago and so sits strictly before this.
    #[derive(diesel::QueryableByName)]
    struct NowRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        now: chrono::DateTime<chrono::Utc>,
    }
    let unblock_at: NowRow = diesel::sql_query("SELECT clock_timestamp() AS now")
        .get_result(&mut conn)
        .await
        .expect("read the unblock instant");

    blocker.batch_execute("COMMIT").await.expect("release");
    let outcome = pausing.await.expect("the pause task must not panic");

    let lead = (unblock_at.now - outcome.paused_at).num_milliseconds();
    assert!(
        lead >= (BLOCK_SECS as i64 - 1) * 1000,
        "paused_at must be stamped when the queue actually became held (at lock \
         acquisition, ~{BLOCK_SECS}s before the scan could run), not after the \
         scan — otherwise the whole held-but-blocked interval is credited to \
         nobody and a short schedule_to_start fails the task right after the \
         thaw. paused_at leads the unblock instant by only {lead}ms"
    );

    // And the credit is real: resuming restores the pre-pause wait without
    // charging the task for the scan stall.
    queue_pause::resume_queue(&mut conn, &q, "alice")
        .await
        .expect("resume");
    let wait = (chrono::Utc::now() - scheduled_at(&mut conn, task).await).num_seconds();
    assert!(
        (99..=103).contains(&wait),
        "the task's 100s pre-pause wait must survive the thaw, with the held scan \
         interval credited back; apparent wait {wait}s"
    );
}

/// Round-21 P2: a timeout enforcement must not stall dispatch on an unpaused queue.
///
/// The `schedule_to_start` enforcer takes the queue advisory lock so a pause
/// committing after its scan still suppresses the timeout. It originally took the
/// **exclusive** mode — the same one `pause_queue`/`resume_queue` take. From round
/// 18 onward that is a dispatch outage: `try_lock_queue_for_claim` takes the
/// **shared** mode of that key on every claim, and exclusive blocks shared, so for
/// the whole enforcement transaction every claim on the queue fails its `try` and
/// releases its task — on a queue with no pause at all. The workflow sibling holds
/// the lock across `append_events`, the parent-close cascade and trigger
/// evaluation, so a backlog of expired tasks can halt unrelated work repeatedly.
///
/// Shared mode is exactly enough: it still mutually excludes the exclusive
/// pause/resume (the ordering the re-check depends on) while being compatible with
/// the claim barrier.
///
/// Both halves are asserted, because dropping the lock entirely would also make
/// the first half pass while silently destroying the round-2/round-15 guarantee.
///
/// Parking the enforcer *inside* the window is the whole difficulty. A table lock
/// on `harvest_queue_pauses` does not work: `find_timed_out_tasks` consults it for
/// the scan's own carve-out, so the enforcer blocks in the scan, *before* taking
/// the advisory lock — the probes then measure nothing (and the second half
/// passes off the probe connection's own shared lock). It is instead parked on the
/// **execution row**: the enforcer takes the advisory lock, clears both re-checks,
/// and only then calls `lock_workflow_execution_row_and_load_history`, so holding
/// that row `FOR UPDATE` elsewhere stalls it with the queue lock held. The scan
/// takes no execution-row locks, so it still runs to completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn timeout_enforcement_does_not_block_claims_on_an_unpaused_queue() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    conn.batch_execute("DELETE FROM harvest_queue_pauses")
        .await
        .expect("scrub pauses");

    // An expired schedule_to_start task on a queue that is NOT paused.
    let q = unique_queue("sharedlock");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.schedule_to_start = Some(chrono::Duration::seconds(1));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    backdate_scheduled_at(&mut conn, task_id, 3600).await;

    // Park the enforcer with the advisory lock held: it clears both re-checks and
    // then row-locks the execution, which this holder owns for the duration.
    let mut blocker = connect(&url).await;
    blocker.batch_execute("BEGIN").await.expect("blocker begin");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .execute(&mut blocker)
        .await
        .expect("hold the execution row");

    let enforcer_url = url.clone();
    let enforcer = tokio::spawn(async move {
        let mut c = connect(&enforcer_url).await;
        autumn_harvest::timeout::enforce_timeouts_once(
            &mut c,
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

    // Let it reach (and park inside) the locked window.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    #[derive(diesel::QueryableByName)]
    struct AcquiredRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        acquired: bool,
    }

    // Half 1 — a claim's barrier must still succeed. This is the shared/shared
    // pair; under the exclusive lock it returns false and the claim is released.
    let mut claimer = connect(&url).await;
    claimer.batch_execute("BEGIN").await.expect("claimer begin");
    let claim_barrier: AcquiredRow =
        diesel::sql_query(autumn_harvest::queue_pause::try_lock_queue_for_claim_query())
            .bind::<diesel::sql_types::Integer, _>(
                autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS,
            )
            .bind::<diesel::sql_types::Text, _>(&q)
            .get_result(&mut claimer)
            .await
            .expect("probe the claim barrier");
    assert!(
        claim_barrier.acquired,
        "a timeout enforcement must not block claims on an unpaused queue: the \
         claim barrier takes the SHARED mode of the queue key, so an enforcement \
         holding it EXCLUSIVELY silently stops dispatch for its whole transaction"
    );
    // Drop it before probing the exclusive side, or half 2 would be satisfied by
    // this probe's own shared lock rather than by the enforcer's.
    claimer
        .batch_execute("ROLLBACK")
        .await
        .expect("claimer end");

    // Half 2 — pause/resume must still be excluded. Probed with the non-blocking
    // TRY exclusive so the assertion is deterministic rather than a timeout.
    let mut pauser = connect(&url).await;
    pauser.batch_execute("BEGIN").await.expect("pauser begin");
    let pause_lock: AcquiredRow =
        diesel::sql_query("SELECT pg_try_advisory_xact_lock($1, hashtext($2)) AS acquired")
            .bind::<diesel::sql_types::Integer, _>(
                autumn_harvest::queue_pause::QUEUE_PAUSE_LOCK_CLASS,
            )
            .bind::<diesel::sql_types::Text, _>(&q)
            .get_result(&mut pauser)
            .await
            .expect("probe the exclusive pause lock");
    assert!(
        !pause_lock.acquired,
        "the shared re-check lock must still exclude the exclusive pause/resume, \
         or a pause could commit between the enforcer's re-check and its own \
         commit and the task would be timed out because its queue was paused"
    );

    pauser.batch_execute("ROLLBACK").await.expect("pauser end");
    blocker.batch_execute("COMMIT").await.expect("release");
    enforcer
        .await
        .expect("the enforcer task must not panic")
        .expect("enforcement must succeed");
}
