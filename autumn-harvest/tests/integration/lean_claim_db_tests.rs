//! The lean form of the default claim (issue #2680).
//!
//! The lean form is a smaller statement in front of the window form. It is
//! exact for a window of plain rows. When its window holds a row and it
//! locks none, it sets its flag. It then claimed no row and changed no row,
//! and the window form runs in the same transaction. A window with a row
//! that is not plain locks no row. After a peer commit on a window row, the
//! claim keeps its lock on the row that it skipped, as the window form does.
//!
//! The form tests send one form alone through the cached claim query, with
//! the binds of the window form. The claim tests go through
//! `queue::claim_task_with_lean`, as a worker claim loop does.
//!
//! # How a test counts the forms
//!
//! A claim keeps one prepared statement per form on its connection.
//! `pg_prepared_statements` counts the plans that each one ran, and each
//! run takes one plan. So a test reads the forms that its own connection
//! sent, with no extension and no hook in the claim. The count reads the
//! `generic_plans` and `custom_plans` columns, so these tests need Postgres
//! 14 or newer.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a fresh testcontainers Postgres boots with the migrations. The
//! differential tests each create a database of their own, because they
//! turn owner mode on and write rows that every claim reads. The worker
//! test does the same, so its worker finds no DR marker and pins no shard
//! generation.
//!
//! The fence registry is one per process. A test here that needs it clear
//! takes the registry guard of `cross_region_dr_tests` first.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, EnableOwnerMode, OwnerHolder, OwnerLeaseConfig,
};
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::queue::{
    self, ClaimFairness, ClaimFormBinds, ClaimFormProbe, ClaimFormStep, EnqueueParams, LeanSkips,
    OwnedClaim, TaskType,
};
use autumn_harvest::replication::FenceRegistry;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::cross_region_dr_tests::{RegistryGuard, registry_guard};
use crate::decision_owner_claim_tests::OwnDb;

const WORKER: &str = "lean-worker";

/// The fallback CTE of the lean text. No other claim text holds the name.
const LEAN_MARKER: &str = "claim_lean_fallback";

/// The fallback CTE of the window text.
const WINDOW_MARKER: &str = "claim_window_fallback";

/// A CTE that only the full statement holds.
const FULL_MARKER: &str = "legacy_candidate";

/// The longest run of claims that skip the lean form.
const SKIP_CAP: usize = 64;

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    connect(&url)
        .await
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (url, Some(container))
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

/// One of the twelve windowed claim texts.
#[derive(Debug, Clone, Copy)]
struct Variant {
    kind: Option<TaskType>,
    fenced: bool,
    owned: bool,
}

impl Variant {
    const BASE: Self = Self {
        kind: None,
        fenced: false,
        owned: false,
    };

    /// The file name stem of the variant.
    fn name(self) -> String {
        let kind = match self.kind {
            None => "any",
            Some(TaskType::Workflow) => "workflow",
            Some(TaskType::Activity) => "activity",
        };
        let fenced = if self.fenced { "-fenced" } else { "" };
        let owned = if self.owned { "-owned" } else { "" };
        format!("{kind}{fenced}{owned}")
    }
}

fn variants() -> Vec<Variant> {
    let mut all = Vec::with_capacity(12);
    for kind in [None, Some(TaskType::Workflow), Some(TaskType::Activity)] {
        for fenced in [false, true] {
            for owned in [false, true] {
                all.push(Variant {
                    kind,
                    fenced,
                    owned,
                });
            }
        }
    }
    all
}

/// What one form returned, in a form that a test can compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Claimed(Uuid),
    Empty,
    Fallback,
}

async fn open(conn: &mut AsyncPgConnection) {
    queue::begin_claim_transaction_for_test(conn, queue::CLAIM_BEGIN_WITH_SETTINGS_SQL)
        .await
        .expect("open a claim transaction");
}

async fn close(conn: &mut AsyncPgConnection, keep: bool) {
    use diesel_async::TransactionManager as _;
    if keep {
        diesel_async::AnsiTransactionManager::commit_transaction(conn)
            .await
            .expect("commit");
    } else {
        diesel_async::AnsiTransactionManager::rollback_transaction(conn)
            .await
            .expect("roll back");
    }
}

/// Sends one form of `variant` in the open claim transaction.
///
/// A fenced text binds one pin that no generation row matches. So its
/// fence is closed in every database, and the result does not depend on the
/// rows that other tests leave in `harvest_shard_generation`.
async fn send(
    conn: &mut AsyncPgConnection,
    form: ClaimFormProbe,
    variant: Variant,
    queue: &str,
) -> ClaimFormStep {
    send_to(conn, form, variant, &[queue.to_owned()]).await
}

/// [`send`] over `queues`.
async fn send_to(
    conn: &mut AsyncPgConnection,
    form: ClaimFormProbe,
    variant: Variant,
    queues: &[String],
) -> ClaimFormStep {
    let owner = OwnedClaim {
        holder: "lean-holder".to_owned(),
        partitions: Vec::new(),
    };
    let binds = ClaimFormBinds {
        worker_id: WORKER,
        queues,
        worker_build_id: "",
        circuit_breaker_activities: &[],
        ineligible_activities: &[],
        kind: variant.kind,
        fence: variant.fenced.then_some((&[i32::MAX][..], &[-1_i64][..])),
        owned: variant.owned.then_some(&owner),
    };
    queue::claim_form_step_for_test(conn, form, binds)
        .await
        .unwrap_or_else(|e| panic!("{form:?} of {variant:?} runs: {e:?}"))
}

fn outcome(step: &ClaimFormStep) -> Outcome {
    match step {
        ClaimFormStep::Claimed(task) => Outcome::Claimed(task.id),
        ClaimFormStep::Empty => Outcome::Empty,
        ClaimFormStep::Fallback => Outcome::Fallback,
    }
}

/// One form of `variant` in a claim transaction of its own, rolled back.
async fn probe(
    conn: &mut AsyncPgConnection,
    form: ClaimFormProbe,
    variant: Variant,
    queue: &str,
) -> Outcome {
    open(conn).await;
    let step = send(conn, form, variant, queue).await;
    close(conn, false).await;
    outcome(&step)
}

async fn enqueue_activity(conn: &mut AsyncPgConnection, queue: &str, priority: i32) -> Uuid {
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.activity_name = Some("noop".to_owned());
    params.activity_id = Some(Uuid::new_v4());
    params.priority = priority;
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn enqueue_keyed_activity(conn: &mut AsyncPgConnection, queue: &str, priority: i32) -> Uuid {
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.activity_name = Some("noop".to_owned());
    params.activity_id = Some(Uuid::new_v4());
    params.priority = priority;
    params.concurrency_key = Some(format!("{queue}-key"));
    params.max_concurrent = Some(1);
    queue::enqueue(conn, &params).await.expect("enqueue")
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// The rows of `queue` in `state`.
async fn rows_in_state(conn: &mut AsyncPgConnection, queue: &str, state: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue WHERE queue_name = $1 AND state = $2",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .bind::<diesel::sql_types::Text, _>(state)
    .get_result::<Count>(conn)
    .await
    .expect("count the rows")
    .n
}

/// The rows of `queue` that no transaction holds a row lock on.
async fn free_rows(conn: &mut AsyncPgConnection, queue: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM ( \
             SELECT 1 FROM harvest_task_queue WHERE queue_name = $1 FOR UPDATE SKIP LOCKED \
         ) free",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .get_result::<Count>(conn)
    .await
    .expect("count the free rows")
    .n
}

/// Deletes the task rows of `queues` and the bucket rows of `buckets`.
///
/// The other suites share this database, and some of them run workers. A
/// reclaimer of such a worker takes each `RUNNING` row that has no live
/// worker. So a test here leaves no row behind.
async fn forget(conn: &mut AsyncPgConnection, queues: &[String], buckets: &[String]) {
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = ANY($1)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
        .execute(conn)
        .await
        .expect("delete the task rows");
    diesel::sql_query("DELETE FROM harvest_rate_limit_buckets WHERE key = ANY($1)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(buckets)
        .execute(conn)
        .await
        .expect("delete the bucket rows");
}

/// Postgres prepares and runs each of the twelve lean texts with the binds
/// of the window form. The lean text reads no `$3` and no `$5`, and the
/// cached claim query still binds both.
#[tokio::test]
async fn every_lean_text_runs_with_the_binds_of_the_window_form() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("lean-empty");
    for variant in variants() {
        for form in [ClaimFormProbe::Lean, ClaimFormProbe::Window] {
            assert_eq!(
                probe(&mut conn, form, variant, &queue).await,
                Outcome::Empty,
                "{form:?} of {variant:?}: an empty window gives no row and no flag"
            );
            // The second run sends Bind and Execute on the cached statement.
            assert_eq!(
                probe(&mut conn, form, variant, &queue).await,
                Outcome::Empty,
                "{form:?} of {variant:?}: the cached statement runs again"
            );
        }
    }
}

/// With one plain due row, the lean form takes the row that the window form
/// takes. A text of another kind sees an empty window. A closed fence lets
/// no row pass the lean candidate scan, so the lean form defers.
#[tokio::test]
async fn the_lean_form_takes_the_plain_row_that_the_window_form_takes() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("lean-plain");
    let task_id = enqueue_activity(&mut conn, &queue, 0).await;

    for variant in variants() {
        let lean = probe(&mut conn, ClaimFormProbe::Lean, variant, &queue).await;
        let window = probe(&mut conn, ClaimFormProbe::Window, variant, &queue).await;
        let (want_lean, want_window) = match (variant.kind, variant.fenced) {
            (Some(TaskType::Workflow), _) => (Outcome::Empty, Outcome::Empty),
            (_, false) => (Outcome::Claimed(task_id), Outcome::Claimed(task_id)),
            // The window holds the row, and the closed fence stops it.
            (_, true) => (Outcome::Fallback, Outcome::Empty),
        };
        assert_eq!(lean, want_lean, "the lean form of {variant:?}");
        assert_eq!(window, want_window, "the window form of {variant:?}");
    }
    assert_eq!(rows_in_state(&mut conn, &queue, "PENDING").await, 1);

    open(&mut conn).await;
    let step = send(&mut conn, ClaimFormProbe::Lean, Variant::BASE, &queue).await;
    close(&mut conn, true).await;
    let ClaimFormStep::Claimed(task) = step else {
        panic!("the lean form claims the plain row: {step:?}");
    };
    assert_eq!(task.id, task_id);
    assert_eq!(task.state, "RUNNING");
    assert_eq!(task.worker_id.as_deref(), Some(WORKER));
    assert_eq!(task.attempt, 1);
    assert_eq!(rows_in_state(&mut conn, &queue, "RUNNING").await, 1);
    assert_eq!(
        probe(&mut conn, ClaimFormProbe::Lean, Variant::BASE, &queue).await,
        Outcome::Empty,
        "no due row is left"
    );
    forget(&mut conn, &[queue], &[]).await;
}

/// A keyed row in the window makes the lean form defer. It locks no row and
/// claims no row. The window form then runs in the same transaction and
/// takes its row.
#[tokio::test]
async fn a_keyed_row_in_the_window_makes_the_lean_form_defer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let mut peer = connect(&url).await;
    let queue = unique_queue("lean-keyed");
    let plain_id = enqueue_activity(&mut conn, &queue, 5).await;
    let _keyed_id = enqueue_keyed_activity(&mut conn, &queue, 1).await;

    open(&mut conn).await;
    let lean = send(&mut conn, ClaimFormProbe::Lean, Variant::BASE, &queue).await;
    assert_eq!(outcome(&lean), Outcome::Fallback);
    assert_eq!(
        free_rows(&mut peer, &queue).await,
        2,
        "the lean form holds no row lock after a fallback"
    );
    let window = send(&mut conn, ClaimFormProbe::Window, Variant::BASE, &queue).await;
    assert_eq!(outcome(&window), Outcome::Claimed(plain_id));
    close(&mut conn, true).await;
    assert_eq!(rows_in_state(&mut conn, &queue, "RUNNING").await, 1);
    assert_eq!(rows_in_state(&mut conn, &queue, "PENDING").await, 1);
    forget(&mut conn, &[queue], &[]).await;
}

// ── The fence registry ──────────────────────────────────────────────────────

/// Takes the fence registry of the process, with no pin in it.
///
/// A worker on a database with a DR marker pins a shard generation for the
/// whole process. Such a pin fences each claim and each owner acquisition
/// of a database with another generation. The guard keeps the tests that
/// pin a generation out until it drops, and it clears the registry then.
async fn clear_fence_registry() -> RegistryGuard {
    let guard = registry_guard().await;
    FenceRegistry::clear();
    guard
}

// ── The forms that a connection sent ────────────────────────────────────────

/// The claim statements that one connection ran, by form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Forms {
    lean: i64,
    window: i64,
    full: i64,
}

/// The runs of the prepared statements of `conn` whose text holds `marker`.
async fn runs_of(conn: &mut AsyncPgConnection, marker: &str) -> i64 {
    diesel::sql_query(
        "SELECT COALESCE(SUM(generic_plans + custom_plans), 0)::bigint AS n \
         FROM pg_prepared_statements WHERE statement LIKE '%' || $1 || '%'",
    )
    .bind::<diesel::sql_types::Text, _>(marker)
    .get_result::<Count>(conn)
    .await
    .expect("read the prepared statements (needs Postgres 14 or newer)")
    .n
}

/// The claim forms that `conn` sent since it connected.
async fn forms(conn: &mut AsyncPgConnection) -> Forms {
    Forms {
        lean: runs_of(conn, LEAN_MARKER).await,
        window: runs_of(conn, WINDOW_MARKER).await,
        full: runs_of(conn, FULL_MARKER).await,
    }
}

// ── Claims through the public API ───────────────────────────────────────────

/// One claim of `kind` over `queues`, as a worker claim loop sends it.
///
/// `lean` is the state of the loop. `None` is a caller with no loop.
async fn claim(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    kind: Option<TaskType>,
    lean: Option<&mut LeanSkips>,
) -> Option<TaskQueueItem> {
    queue::claim_task_with_lean(
        conn,
        queues,
        WORKER,
        "",
        None,
        &[],
        &[],
        None,
        kind,
        ClaimFairness::Off,
        None,
        None,
        lean,
    )
    .await
    .expect("claim")
    .into_task()
}

/// The number that the seed wrote into the input of `task`.
fn ordinal(task: &TaskQueueItem) -> i64 {
    task.input["n"]
        .as_i64()
        .unwrap_or_else(|| panic!("row {} has no ordinal: {}", task.id, task.input))
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    conn.batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}

/// `n` plain due activity rows in `queue`. Row 1 is the oldest, so the rows
/// claim in the order of their ordinals.
async fn seed_plain(conn: &mut AsyncPgConnection, queue: &str, n: i64) {
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at) \
             SELECT '{queue}', 'activity', 'noop', jsonb_build_object('n', g), 'PENDING', 0, 3, \
                    NOW() - make_interval(secs => 100000 - g) \
             FROM generate_series(1, {n}) g"
        ),
    )
    .await;
}

/// A plain queue never needs the window form. Each claim of the loop sends
/// the lean form alone, the empty poll at the end too. A caller with no
/// loop state still sends the window form first.
#[tokio::test]
async fn a_plain_queue_sends_the_lean_form_only() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queues = [unique_queue("lean-census")];
    seed_plain(&mut conn, &queues[0], 12).await;

    let mut skip = LeanSkips::new();
    let mut claimed = Vec::new();
    while let Some(task) = claim(&mut conn, &queues, None, Some(&mut skip)).await {
        assert_eq!(task.state, "RUNNING");
        assert_eq!(task.attempt, 1);
        claimed.push(ordinal(&task));
    }
    assert_eq!(claimed, (1..=12).collect::<Vec<_>>());
    assert_eq!(
        forms(&mut conn).await,
        Forms {
            lean: 13,
            window: 0,
            full: 0
        },
        "twelve claims and one empty poll, all in the lean form"
    );
    assert!(skip.sends_lean(None), "no fallback, so no skip");

    assert!(claim(&mut conn, &queues, None, None).await.is_none());
    assert_eq!(
        forms(&mut conn).await,
        Forms {
            lean: 13,
            window: 1,
            full: 0
        },
        "a caller with no loop state sends the window form first"
    );
    forget(&mut conn, &queues, &[]).await;
}

/// A keyed row that stays at the head makes each lean attempt fall back.
/// The loop then sends the window form first, and tries the lean form
/// again after 4, 8, 12 and so on up to 64 claims. Each claim still takes
/// the next plain row. When the keyed row goes, the next lean attempt
/// takes a row, and the loop sends the lean form first again.
#[tokio::test]
async fn a_keyed_row_at_the_head_moves_the_loop_to_the_window_form() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("lean-converge");
    let queues = [queue.clone()];
    seed_plain(&mut conn, &queue, 241).await;
    // One running row holds the only slot of the key. The pending row of
    // the key has the top priority, so it stays in the window.
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, attempt, max_attempts, \
                priority, worker_id, started_at, concurrency_key, concurrency_cap) \
             VALUES ('{queue}', 'activity', 'noop', '{{}}'::jsonb, 'RUNNING', 1, 3, 0, 'holder', \
                     NOW(), '{queue}-key', 1), \
                    ('{queue}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 0, 3, 9, NULL, \
                     NULL, '{queue}-key', 1)"
        ),
    )
    .await;

    let mut skip = LeanSkips::new();
    let mut lean_claims: Vec<usize> = Vec::new();
    let mut sent = Forms::default();
    for n in 1..=230_usize {
        let task = claim(&mut conn, &queues, None, Some(&mut skip))
            .await
            .unwrap_or_else(|| panic!("claim {n} takes a plain row"));
        assert_eq!(usize::try_from(ordinal(&task)).ok(), Some(n));
        let now = forms(&mut conn).await;
        if now.lean > sent.lean {
            lean_claims.push(n);
        }
        assert_eq!(
            now.window,
            sent.window + 1,
            "claim {n} takes its row through the window form"
        );
        sent = now;
    }
    assert_eq!(
        lean_claims,
        [1, 6, 15, 28, 45, 66, 91, 120, 153, 190],
        "each fallback adds four claims to the skip"
    );
    assert!(
        lean_claims.windows(2).all(|w| w[1] - w[0] <= SKIP_CAP + 1),
        "the loop tries the lean form again within the cap: {lean_claims:?}"
    );
    assert_eq!(sent.full, 0, "the window form finds each row");
    assert!(
        skip.sends_lean(None),
        "the skip of 40 claims is over, so claim 231 is the next lean attempt"
    );
    assert!(
        skip.sends_lean(Some(TaskType::Workflow)) && skip.sends_lean(Some(TaskType::Activity)),
        "the fallbacks of one claim kind leave the other kinds alone"
    );

    exec(
        &mut conn,
        &format!(
            "DELETE FROM harvest_task_queue WHERE queue_name = '{queue}' AND state = 'PENDING' \
             AND concurrency_key IS NOT NULL"
        ),
    )
    .await;
    for n in 231..=241_usize {
        let task = claim(&mut conn, &queues, None, Some(&mut skip))
            .await
            .unwrap_or_else(|| panic!("claim {n} takes a plain row"));
        assert_eq!(usize::try_from(ordinal(&task)).ok(), Some(n));
    }
    let end = forms(&mut conn).await;
    assert_eq!(
        (end.lean, end.window, end.full),
        (sent.lean + 11, sent.window, 0),
        "claim 231 is the next lean attempt; each lean claim sends the lean form first again"
    );
    assert!(skip.sends_lean(None));
    forget(&mut conn, &queues, &[]).await;
}

/// `n` plain due workflow rows in `queue`, with no run. Row 1 is the oldest.
async fn seed_plain_workflows(conn: &mut AsyncPgConnection, queue: &str, n: i64) {
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, input, state, priority, max_attempts, scheduled_at) \
             SELECT '{queue}', 'workflow', jsonb_build_object('n', g), 'PENDING', 0, 3, \
                    NOW() - make_interval(secs => 100000 - g) \
             FROM generate_series(1, {n}) g"
        ),
    )
    .await;
}

/// A loop holds one skip for each claim kind. A keyed activity row stays at
/// the head, so each lean claim of any kind falls back. A workflow claim
/// reads no activity row, so its lean form takes a row each time. The lean
/// claims of the workflow kind leave the skip of the other kind alone. So
/// the claims of any kind move to the window form first.
#[tokio::test]
async fn a_loop_of_two_claim_kinds_keeps_one_skip_for_each_kind() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("lean-kinds");
    let queues = [queue.clone()];
    seed_plain(&mut conn, &queue, 600).await;
    seed_plain_workflows(&mut conn, &queue, 600).await;
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, attempt, max_attempts, \
                priority, worker_id, started_at, concurrency_key, concurrency_cap) \
             VALUES ('{queue}', 'activity', 'noop', '{{}}'::jsonb, 'RUNNING', 1, 3, 0, 'holder', \
                     NOW(), '{queue}-key', 1), \
                    ('{queue}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 0, 3, 9, NULL, \
                     NULL, '{queue}-key', 1)"
        ),
    )
    .await;

    let mut skip = LeanSkips::new();
    let mut any_lean: Vec<usize> = Vec::new();
    let mut sent = Forms::default();
    for n in 1..=600_usize {
        let kind = n.is_multiple_of(2).then_some(TaskType::Workflow);
        let task = claim(&mut conn, &queues, kind, Some(&mut skip))
            .await
            .unwrap_or_else(|| panic!("claim {n} takes a plain row"));
        assert!(
            task.concurrency_key.is_none(),
            "claim {n} takes a plain row"
        );
        let now = forms(&mut conn).await;
        if kind.is_some() {
            assert_eq!(
                (now.lean, now.window),
                (sent.lean + 1, sent.window),
                "workflow claim {n} sends the lean form alone"
            );
        } else if now.lean > sent.lean {
            any_lean.push(n.div_ceil(2));
        }
        sent = now;
    }
    assert_eq!(
        any_lean,
        [1, 6, 15, 28, 45, 66, 91, 120, 153, 190, 231, 276],
        "the claims of any kind keep their own skip"
    );
    assert_eq!(
        sent,
        Forms {
            lean: 312,
            window: 300,
            full: 0
        },
        "300 lean workflow claims, 12 lean attempts of any kind, 300 window claims"
    );
    forget(&mut conn, &queues, &[]).await;
}

/// One due workflow task in `queue`, of a new paused run. The row is plain.
/// Returns the id of the run.
async fn seed_parked_row(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let run = Uuid::new_v4();
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_workflow_executions \
               (id, workflow_name, workflow_id, shard_id, input, state) \
             VALUES ('{run}', 'lean-parked', '{run}', 0, 'null'::jsonb, 'PAUSED'); \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, input, state, priority, max_attempts, workflow_exec_id) \
             VALUES ('{queue}', 'workflow', '{{}}'::jsonb, 'PENDING', 0, 3, '{run}')"
        ),
    )
    .await;
    run
}

/// A window of plain rows can still set the lean flag. The only due row is
/// the workflow task of a paused run. Its window is not empty, and no row
/// passes the run gate. So the lean form sets its flag, where the window
/// form returns no row and a clear flag.
#[tokio::test]
async fn a_parked_plain_row_of_a_paused_run_sets_the_lean_flag() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("lean-parked");
    let run = seed_parked_row(&mut conn, &queue).await;

    assert_eq!(
        probe(&mut conn, ClaimFormProbe::Lean, Variant::BASE, &queue).await,
        Outcome::Fallback,
        "the window holds a row, and the lean form locks none"
    );
    assert_eq!(
        probe(&mut conn, ClaimFormProbe::Window, Variant::BASE, &queue).await,
        Outcome::Empty
    );
    assert_eq!(rows_in_state(&mut conn, &queue, "PENDING").await, 1);

    forget(&mut conn, std::slice::from_ref(&queue), &[]).await;
    exec(
        &mut conn,
        &format!("DELETE FROM harvest_workflow_executions WHERE id = '{run}'"),
    )
    .await;
}

/// The committed census scripts count the lean form. Each one filters
/// `pg_stat_statements` by text, and the lean text does not start with the
/// `worker_info` CTE. So each script names the lean marker.
#[test]
fn the_committed_census_scripts_name_the_lean_marker() {
    let root = crate::ci_run_coverage::repo_root();
    for script in [
        "docs/perf-artifacts/claim-window-split/claim_forms.sql",
        "docs/perf-artifacts/claim-batch/claim_forms.sql",
        "docs/perf-artifacts/notify-cuts-settle/run_cell.sh",
    ] {
        let text = std::fs::read_to_string(root.join(script))
            .unwrap_or_else(|e| panic!("read {script}: {e}"));
        assert!(
            text.contains(LEAN_MARKER),
            "{script} must count the statements that hold {LEAN_MARKER}"
        );
    }
}

// ── Fallback exactness ──────────────────────────────────────────────────────

/// The rows of `queues` that a claim changed, and the tokens of `buckets`.
///
/// Each seeded row holds its ordinal in `input`. A copy of a state under
/// other names gives the same text.
async fn digest(conn: &mut AsyncPgConnection, queues: &[String], buckets: &[String]) -> String {
    #[derive(diesel::QueryableByName)]
    struct Digest {
        #[diesel(sql_type = diesel::sql_types::Text)]
        digest: String,
    }
    diesel::sql_query(
        "SELECT COALESCE(( \
                    SELECT string_agg((input->>'n') || ':' || state || ':' || attempt || ':' \
                                          || COALESCE(worker_id, '-'), \
                                      ' ' ORDER BY (input->>'n')::int) \
                    FROM harvest_task_queue \
                    WHERE queue_name = ANY($1) AND input ? 'n' \
                      AND (state <> 'PENDING' OR attempt <> 0)), '') \
                || ' | ' || COALESCE(( \
                    SELECT string_agg(split_part(key, '-rate-', 2) || '=' || tokens, \
                                      ' ' ORDER BY key) \
                    FROM harvest_rate_limit_buckets WHERE key = ANY($2)), '') AS digest",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(buckets)
    .get_result::<Digest>(conn)
    .await
    .expect("read the state")
    .digest
}

/// The bucket rows of `keys` that no transaction holds a row lock on.
async fn free_buckets(conn: &mut AsyncPgConnection, keys: &[String]) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM ( \
             SELECT 1 FROM harvest_rate_limit_buckets WHERE key = ANY($1) \
             FOR UPDATE SKIP LOCKED \
         ) free",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(keys)
    .get_result::<Count>(conn)
    .await
    .expect("count the free buckets")
    .n
}

/// A feature that makes a row not plain.
#[derive(Debug, Clone, Copy)]
enum Feature {
    /// A concurrency key with a free slot.
    Key,
    /// A rate-limit key whose bucket holds tokens.
    Rate,
    /// A build pin that no build of this test meets.
    Build,
    /// A capability that the worker does not have.
    Capability,
}

/// One row with `feature` at the top priority and two plain rows under it.
/// Returns the bucket keys of the queue.
async fn seed_one_feature(
    conn: &mut AsyncPgConnection,
    queue: &str,
    feature: Feature,
) -> Vec<String> {
    let bucket = format!("{queue}-rate-0");
    let (column, value) = match feature {
        Feature::Key => (
            "concurrency_key, concurrency_cap",
            format!("'{queue}-key', 1"),
        ),
        Feature::Rate => ("rate_limit_key", format!("'{bucket}'")),
        Feature::Build => ("required_build_id", "'lean-no-such-build'".to_owned()),
        Feature::Capability => (
            "required_capabilities",
            r#"'[{"Exact":{"key":"lean-gpu","value":"yes"}}]'::jsonb"#.to_owned(),
        ),
    };
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
             VALUES ('{bucket}', 0, 100, 2, NOW()); \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at, {column}) \
             VALUES ('{queue}', 'activity', 'noop', jsonb_build_object('n', 1), 'PENDING', 5, 3, \
                     NOW() - INTERVAL '3 seconds', {value}); \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at) \
             VALUES ('{queue}', 'activity', 'noop', jsonb_build_object('n', 2), 'PENDING', 1, 3, \
                     NOW() - INTERVAL '2 seconds'), \
                    ('{queue}', 'activity', 'noop', jsonb_build_object('n', 3), 'PENDING', 1, 3, \
                     NOW() - INTERVAL '1 second')"
        ),
    )
    .await;
    vec![bucket]
}

/// A row with a key, a rate limit, a build pin or a capability makes the
/// lean form defer. The lean statement locks no row and no bucket, and it
/// changes no row and no token. The window form then runs in the same
/// transaction. It claims the row that a claim with no lean form claims,
/// and a rate-limited claim pays one token, not two.
#[tokio::test]
async fn a_lean_fallback_changes_nothing_and_the_window_form_claims_as_before() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let mut peer = connect(&url).await;
    for feature in [
        Feature::Key,
        Feature::Rate,
        Feature::Build,
        Feature::Capability,
    ] {
        let queue = unique_queue("lean-exact");
        let queues = [queue.clone()];
        let buckets = seed_one_feature(&mut conn, &queue, feature).await;
        let before = digest(&mut conn, &queues, &buckets).await;
        assert_eq!(before, " | 0=2", "{feature:?}: no row is claimed yet");

        // What a claim with no lean form does, rolled back.
        open(&mut conn).await;
        let window_only = send(&mut conn, ClaimFormProbe::Window, Variant::BASE, &queue).await;
        let want_state = digest(&mut conn, &queues, &buckets).await;
        close(&mut conn, false).await;
        let ClaimFormStep::Claimed(want) = window_only else {
            panic!("{feature:?}: the window form claims a row: {window_only:?}");
        };
        let want_ordinal = match feature {
            Feature::Key | Feature::Rate => 1,
            Feature::Build | Feature::Capability => 2,
        };
        assert_eq!(ordinal(&want), want_ordinal, "{feature:?}");

        open(&mut conn).await;
        let lean = send(&mut conn, ClaimFormProbe::Lean, Variant::BASE, &queue).await;
        assert_eq!(outcome(&lean), Outcome::Fallback, "{feature:?}");
        assert_eq!(
            free_rows(&mut peer, &queue).await,
            3,
            "{feature:?}: the lean form holds no row lock after a fallback"
        );
        assert_eq!(
            free_buckets(&mut peer, &buckets).await,
            1,
            "{feature:?}: the lean form holds no bucket lock"
        );
        assert_eq!(
            digest(&mut conn, &queues, &buckets).await,
            before,
            "{feature:?}: the lean form changed no row and no token"
        );
        let window = send(&mut conn, ClaimFormProbe::Window, Variant::BASE, &queue).await;
        assert_eq!(outcome(&window), Outcome::Claimed(want.id), "{feature:?}");
        assert_eq!(
            digest(&mut conn, &queues, &buckets).await,
            want_state,
            "{feature:?}: the same rows and tokens as a claim with no lean form"
        );
        close(&mut conn, true).await;
        let tokens = if matches!(feature, Feature::Rate) {
            1
        } else {
            2
        };
        assert_eq!(
            digest(&mut peer, &queues, &buckets).await,
            format!("{want_ordinal}:RUNNING:1:{WORKER} | 0={tokens}"),
            "{feature:?}: one claimed row, and one token for a rate-limited row"
        );
        forget(&mut conn, &queues, &buckets).await;
    }
}

/// The same four states through the claim itself. A loop with lean state
/// and a caller with none claim the same row. The lean loop sends the lean
/// form and then the window form, in one claim.
#[tokio::test]
async fn a_claim_that_falls_back_takes_the_row_of_a_claim_with_no_lean_form() {
    let (url, _container) = setup_db().await;
    let mut lean_conn = connect(&url).await;
    let mut window_conn = connect(&url).await;
    let mut sent = Forms::default();
    for feature in [
        Feature::Key,
        Feature::Rate,
        Feature::Build,
        Feature::Capability,
    ] {
        let lean_queues = [unique_queue("lean-exact-a")];
        let window_queues = [unique_queue("lean-exact-b")];
        let lean_buckets = seed_one_feature(&mut lean_conn, &lean_queues[0], feature).await;
        let window_buckets = seed_one_feature(&mut window_conn, &window_queues[0], feature).await;

        let mut skip = LeanSkips::new();
        let with_lean = claim(&mut lean_conn, &lean_queues, None, Some(&mut skip)).await;
        let without = claim(&mut window_conn, &window_queues, None, None).await;
        assert_eq!(
            with_lean.as_ref().map(ordinal),
            without.as_ref().map(ordinal),
            "{feature:?}"
        );
        assert!(with_lean.is_some(), "{feature:?}: a row is due");
        assert_eq!(
            digest(&mut lean_conn, &lean_queues, &lean_buckets).await,
            digest(&mut window_conn, &window_queues, &window_buckets).await,
            "{feature:?}"
        );
        assert!(
            !skip.sends_lean(None),
            "{feature:?}: the first fallback skips"
        );

        let now = forms(&mut lean_conn).await;
        assert_eq!(
            (now.lean, now.window, now.full),
            (sent.lean + 1, sent.window + 1, sent.full),
            "{feature:?}: one lean statement, then one window statement"
        );
        sent = now;
        forget(&mut lean_conn, &lean_queues, &lean_buckets).await;
        forget(&mut window_conn, &window_queues, &window_buckets).await;
    }
    assert_eq!(forms(&mut window_conn).await.lean, 0);
}

// ── A peer commit between the snapshot and the row lock ─────────────────────

const PEER: &str = "lean-peer";

/// The start of the lean text of the base variant. `pg_stat_activity`
/// keeps the start of a statement only.
const LEAN_BASE_PREFIX: &str = "WITH paused_queues AS";

/// `n` queue names that hold no row.
///
/// Each name costs the lean statement four empty head scans. So a long
/// list keeps the statement between its snapshot and its row lock for a
/// time that the test can measure.
fn filler_queues(n: usize) -> Vec<String> {
    let tag = Uuid::new_v4().simple().to_string();
    (0..n).map(|i| format!("{}-{i}", &tag[..8])).collect()
}

/// A queue list that ends with `queue` and makes the lean statement of
/// `conn` run for `floor` or longer, and that run time.
async fn slow_queue_list(
    conn: &mut AsyncPgConnection,
    queue: &str,
    floor: Duration,
) -> (Vec<String>, Duration) {
    let mut fillers = 100_000_usize;
    loop {
        let mut queues = filler_queues(fillers);
        queues.push(queue.to_owned());
        let mut took = Duration::ZERO;
        // The first run plans the statement. The second run gives the time.
        for _ in 0..2 {
            open(conn).await;
            let started = Instant::now();
            let step = send_to(conn, ClaimFormProbe::Lean, Variant::BASE, &queues).await;
            took = started.elapsed();
            close(conn, false).await;
            assert_eq!(outcome(&step), Outcome::Empty, "the queue holds no row yet");
        }
        if took >= floor {
            return (queues, took);
        }
        assert!(
            fillers < 3_200_000,
            "{fillers} empty queues keep the lean statement for {took:?} only"
        );
        fillers *= 2;
    }
}

async fn backend_pid(conn: &mut AsyncPgConnection) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }
    diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(conn)
        .await
        .expect("read the backend pid")
        .pid
}

/// Waits until backend `pid` runs the lean statement.
async fn wait_for_the_lean_statement(observer: &mut AsyncPgConnection, pid: i32) {
    for _ in 0..5_000 {
        let running = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM pg_stat_activity \
             WHERE pid = $1 AND state = 'active' AND query LIKE $2 || '%'",
        )
        .bind::<diesel::sql_types::Integer, _>(pid)
        .bind::<diesel::sql_types::Text, _>(LEAN_BASE_PREFIX)
        .get_result::<Count>(observer)
        .await
        .expect("read the activity")
        .n;
        if running == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("backend {pid} never ran the lean statement");
}

/// The state, the attempt and the worker of row `id`.
async fn row_state(conn: &mut AsyncPgConnection, id: Uuid) -> (String, i32, Option<String>) {
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
            .bind::<diesel::sql_types::Uuid, _>(id)
            .get_result(conn)
            .await
            .expect("read the row");
    (row.state, row.attempt, row.worker_id)
}

/// Runs the lean form over `queues` on `slow`, and commits a peer claim of
/// the head row of `queue` while that statement runs.
///
/// The lean statement takes its snapshot first and locks its row last. The
/// long queue list puts `took` between the two. The peer claims and commits
/// a quarter of `took` after the statement starts. The slow statement must
/// still run after that commit, or the test did not place the commit.
///
/// Returns `slow` with its open claim transaction, the lean step, and the
/// row that the peer claimed.
async fn lean_step_around_a_peer_commit(
    mut slow: AsyncPgConnection,
    url: &str,
    queues: Vec<String>,
    queue: &str,
    took: Duration,
) -> (AsyncPgConnection, ClaimFormStep, Uuid) {
    let mut observer = connect(url).await;
    let mut peer = connect(url).await;
    // A first claim plans the statement of the peer, so its real claim is short.
    let warm = queue::claim_task(
        &mut peer,
        &[unique_queue("lean-warm")],
        PEER,
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("warm the peer");
    assert!(warm.is_none());

    let pid = backend_pid(&mut slow).await;
    let mut running = tokio::spawn(async move {
        open(&mut slow).await;
        let step = send_to(&mut slow, ClaimFormProbe::Lean, Variant::BASE, &queues).await;
        (slow, step)
    });
    wait_for_the_lean_statement(&mut observer, pid).await;
    tokio::time::sleep(took / 4).await;

    let taken = queue::claim_task(&mut peer, &[queue.to_owned()], PEER, "", None, &[], &[])
        .await
        .expect("the peer claims")
        .expect("the peer takes the head row");
    assert!(
        tokio::time::timeout(took / 10, &mut running).await.is_err(),
        "the lean statement ended before the peer commit could precede its row lock"
    );
    let (slow, step) = running.await.expect("the slow claim joins");
    (slow, step, taken.id)
}

/// A peer claim commits the head row after the snapshot of a lean statement
/// and before its row lock. The lean statement then finds a new version of
/// that row. Postgres checks the row again, and the row is no longer
/// `PENDING`. So the lean statement takes the next row, and no row is
/// claimed twice.
#[tokio::test]
async fn a_lean_claim_takes_the_next_row_after_a_peer_commits_the_head_row() {
    let (url, _container) = setup_db().await;
    let mut slow = connect(&url).await;
    let mut check = connect(&url).await;
    let queue = unique_queue("lean-epq-next");
    let (queues, took) = slow_queue_list(&mut slow, &queue, Duration::from_millis(800)).await;
    let head = enqueue_activity(&mut check, &queue, 5).await;
    let next = enqueue_activity(&mut check, &queue, 1).await;

    let (mut slow, step, taken) =
        lean_step_around_a_peer_commit(slow, &url, queues, &queue, took).await;
    assert_eq!(taken, head, "the peer takes the head row");
    assert_eq!(
        outcome(&step),
        Outcome::Claimed(next),
        "the lean form skips the row that the peer took, and takes the next row"
    );
    // The row lock found the head row first and checked its new version.
    // So the claim holds the lock of that row too. A statement that started
    // after the peer commit never locks the head row.
    assert_eq!(
        free_rows(&mut check, &queue).await,
        0,
        "the claim holds the row that it took and the row that it skipped"
    );
    close(&mut slow, true).await;

    assert_eq!(
        row_state(&mut check, head).await,
        ("RUNNING".to_owned(), 1, Some(PEER.to_owned())),
        "the head row has one claim, by the peer"
    );
    assert_eq!(
        row_state(&mut check, next).await,
        ("RUNNING".to_owned(), 1, Some(WORKER.to_owned())),
        "the next row has one claim, by the lean statement"
    );
    forget(&mut check, &[queue], &[]).await;
}

/// The same race with one due row. The snapshot of the lean statement
/// holds the row, so its window is not empty. The peer commit takes the
/// row away, so the lean statement claims nothing and sets its flag. A
/// statement that started after the peer commit would see an empty window
/// and leave the flag clear. So the flag proves the order of the race. The
/// window form then runs in the same transaction and finds no row.
#[tokio::test]
async fn a_lean_claim_defers_when_a_peer_commits_its_only_row() {
    let (url, _container) = setup_db().await;
    let mut slow = connect(&url).await;
    let mut check = connect(&url).await;
    let queue = unique_queue("lean-epq-only");
    let (queues, took) = slow_queue_list(&mut slow, &queue, Duration::from_millis(800)).await;
    let only = enqueue_activity(&mut check, &queue, 0).await;

    let (mut slow, step, taken) =
        lean_step_around_a_peer_commit(slow, &url, queues, &queue, took).await;
    assert_eq!(taken, only);
    assert_eq!(
        outcome(&step),
        Outcome::Fallback,
        "the snapshot of the lean statement held the row, and the peer took it"
    );
    let window = send(&mut slow, ClaimFormProbe::Window, Variant::BASE, &queue).await;
    assert_eq!(outcome(&window), Outcome::Empty);
    assert_eq!(
        free_rows(&mut check, &queue).await,
        0,
        "the claim keeps its lock on the row that it skipped, as the window form does"
    );
    close(&mut slow, true).await;
    assert_eq!(
        free_rows(&mut check, &queue).await,
        1,
        "the end of the claim transaction frees the row"
    );

    assert_eq!(
        row_state(&mut check, only).await,
        ("RUNNING".to_owned(), 1, Some(PEER.to_owned())),
        "the row has one claim, by the peer"
    );
    forget(&mut check, &[queue], &[]).await;
}

// ── The worker claim loop ───────────────────────────────────────────────────

fn small_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build the pool")
}

/// The claim forms that the idle connections of `pool` sent.
async fn pool_forms(pool: &DbPool) -> Forms {
    let mut held = Vec::new();
    for _ in 0..pool.status().size {
        held.push(pool.get().await.expect("a pool connection"));
    }
    let mut total = Forms::default();
    for conn in &mut held {
        let sent = forms(conn).await;
        total.lean += sent.lean;
        total.window += sent.window;
        total.full += sent.full;
    }
    total
}

/// Runs a worker with no handler over `queues` until its pool has sent
/// `enough` windowed claim statements. No row of `queues` may be claimable.
/// Returns the claim forms that its pool sent.
async fn forms_of_a_worker_with_no_claim(
    url: &str,
    config: WorkerConfig,
    queues: Vec<String>,
    enough: i64,
) -> Forms {
    let pool = small_pool(url);
    let mut config: WorkerRuntimeConfig = config.into();
    config.worker_id = unique_queue("lean-loop-worker");
    config.queues = queues;
    config.poll_interval = Duration::from_millis(20);
    config.shutdown_timeout = Duration::from_secs(5);
    config.shard_assignments = vec![ShardId::new(0)];
    let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
    let worker = Arc::new(Worker::new(config, registry).expect("the worker builds"));
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&run_pool).await });

    // A census of a running pool can miss the connection that a claim
    // holds. It only decides when to stop. The census after the join is
    // exact.
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let seen = pool_forms(&pool).await;
        if seen.lean + seen.window >= enough {
            break;
        }
    }
    worker.shutdown();
    handle.await.expect("the worker joins");
    pool_forms(&pool).await
}

/// Runs a worker with no handler on an empty queue until its claim loop
/// has polled a few times. Returns the claim forms that its pool sent.
async fn forms_of_an_idle_worker(url: &str, claim_lean: bool) -> Forms {
    let config = WorkerConfig::default().with_claim_lean(claim_lean);
    forms_of_a_worker_with_no_claim(url, config, vec![unique_queue("lean-loop")], 5).await
}

/// Each claim loop of a worker holds lean state, so an idle worker polls
/// with the lean form alone. With the switch off, the worker sends the
/// window form first and never the lean form.
#[tokio::test]
async fn a_worker_claim_loop_sends_the_lean_form_unless_the_switch_is_off() {
    let _registry = clear_fence_registry().await;
    let db = OwnDb::new().await;
    let url = db.url.clone();

    let on = forms_of_an_idle_worker(&url, true).await;
    assert!(
        on.lean >= 5,
        "the claim loop polls in the lean form: {on:?}"
    );
    assert_eq!(
        (on.window, on.full),
        (0, 0),
        "an empty queue never needs the window form: {on:?}"
    );

    let off = forms_of_an_idle_worker(&url, false).await;
    assert!(
        off.window >= 5,
        "the claim loop polls in the window form: {off:?}"
    );
    assert_eq!(
        (off.lean, off.full),
        (0, 0),
        "the switch keeps the lean form off: {off:?}"
    );
    db.drop_db().await;
}

/// A worker with queue weights claims queue by queue, and its loop holds
/// one lean state for each queue. One queue is empty. The other holds the
/// parked row of a paused run, so each lean claim of that queue sets its
/// flag. Each poll claims from both queues, and neither claim takes a row.
///
/// The loop sends the window form once in each poll, for the parked queue.
/// It sends the lean form alone for the empty queue in each poll. So the
/// pool sent the lean form at least as often as the window form. With one
/// state for both queues, the flags of the parked queue would also move
/// the claims of the empty queue to the window form.
#[tokio::test]
async fn a_weighted_worker_keeps_the_lean_form_on_its_plain_queue() {
    let _registry = clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let empty = unique_queue("lean-weighted-empty");
    let parked = unique_queue("lean-weighted-parked");
    seed_parked_row(&mut conn, &parked).await;

    let config =
        WorkerConfig::default().with_queue_weights([(empty.as_str(), 1), (parked.as_str(), 1)]);
    let sent =
        forms_of_a_worker_with_no_claim(&db.url, config, vec![empty, parked.clone()], 40).await;
    assert!(sent.window >= 5, "the worker polled: {sent:?}");
    assert!(
        sent.lean >= sent.window,
        "the empty queue keeps the lean form in each poll: {sent:?}"
    );
    assert_eq!(sent.full, 0, "{sent:?}");
    assert_eq!(
        rows_in_state(&mut conn, &parked, "PENDING").await,
        1,
        "no claim takes the parked row"
    );
    drop(conn);
    db.drop_db().await;
}

// ── Random states ───────────────────────────────────────────────────────────

/// The build id of the worker of the differential tests.
const BUILD: &str = "lean-diff-b1";
/// A build that `BUILD` is declared compatible with.
const COMPATIBLE_BUILD: &str = "lean-diff-b3";
const PAUSED_ACTIVITY: &str = "lean-diff-paused";
/// An activity that the worker cannot run. It goes into `$6` as it is.
const UNMARKED_ACTIVITY: &str = "lean-diff-unmarked";
/// An activity at its adaptive limit. It goes into `$6` behind `chr(1)`.
const MARKED_ACTIVITY: &str = "lean-diff-marked";
/// An activity with a circuit breaker. It goes into `$5`.
const BREAKER_ACTIVITY: &str = "lean-diff-breaker";
/// A partition that the worker owns.
const OWN_PARTITION: i16 = 11;
/// A partition in the list of the worker that another holder owns.
const STALE_PARTITION: i16 = 12;
/// A partition that is not in the list of the worker.
const OTHER_PARTITION: i16 = 13;
const FENCE_SHARD: i32 = 7;
const FENCE_GENERATION: i64 = 3;

/// The seeds of the differential tests. The list is fixed, so each run
/// builds the same states.
const SEEDS: [u64; 14] = [3, 7, 11, 19, 23, 42, 57, 64, 101, 128, 256, 777, 2680, 4096];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Activity {
    Noop,
    Paused,
    Unmarked,
    Marked,
    Breaker,
}

impl Activity {
    const fn name(self) -> &'static str {
        match self {
            Self::Noop => "lean-diff-noop",
            Self::Paused => PAUSED_ACTIVITY,
            Self::Unmarked => UNMARKED_ACTIVITY,
            Self::Marked => MARKED_ACTIVITY,
            Self::Breaker => BREAKER_ACTIVITY,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pin {
    None,
    /// A live pin to the worker of the test.
    Own,
    /// A live pin to another worker.
    Foreign,
    /// An expired pin to another worker.
    ForeignExpired,
}

/// The run of a workflow row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Run {
    None,
    Running,
    Paused,
    /// A running run past its deadline.
    Expired,
    /// A running run past the deadline of its chain.
    ChainExpired,
}

/// What makes a row not plain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Extra {
    Plain,
    /// The index of a concurrency key of the state.
    Key(usize),
    /// The index of a rate-limit bucket of the state.
    Rate(usize),
    Build(&'static str),
    /// The value of the label that the row requires.
    Capability(&'static str),
}

// struct_excessive_bools: each flag is an independent trait of one row.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct RowSpec {
    queue: usize,
    workflow: bool,
    activity: Activity,
    priority: i32,
    /// The age of the row. Each row of a state has its own age, so no two
    /// rows tie on the whole claim order.
    age_ms: u32,
    /// The row is due in one hour.
    future: bool,
    new_start: bool,
    pin: Pin,
    session: bool,
    extra: Extra,
    run: Run,
    partition: Option<i16>,
    /// The schedule-to-close deadline of the row has passed.
    closed: bool,
}

#[derive(Debug)]
struct KeySpec {
    cap: u32,
    /// The running rows that hold the key from the start.
    running: u32,
}

/// One random queue state, with no names.
#[derive(Debug)]
struct StateSpec {
    queues: usize,
    paused_queue: Option<usize>,
    keys: Vec<KeySpec>,
    /// The tokens of each rate-limit bucket. No bucket refills.
    buckets: Vec<u32>,
    rows: Vec<RowSpec>,
}

/// True for `percent` of the calls.
fn roll(rng: &mut StdRng, percent: u32) -> bool {
    rng.random_range(0..100) < percent
}

fn row_spec(
    rng: &mut StdRng,
    queues: usize,
    special: u32,
    deep: bool,
    gates: bool,
    age_ms: u32,
) -> RowSpec {
    let workflow = roll(rng, 40);
    let extra = if roll(rng, special) {
        // A workflow row gets a key or a build pin only.
        match rng.random_range(0..if workflow { 2 } else { 4 }) {
            0 => Extra::Key(rng.random_range(0..3)),
            1 => Extra::Build([BUILD, "lean-diff-b2", COMPATIBLE_BUILD][rng.random_range(0..3)]),
            2 => Extra::Rate(rng.random_range(0..2)),
            _ => Extra::Capability(["true", "false"][rng.random_range(0..2)]),
        }
    } else {
        Extra::Plain
    };
    // A deep state puts each row that is not plain under every plain row.
    let priority = if deep && extra != Extra::Plain {
        -5
    } else {
        [0, 0, 0, 1, 2][rng.random_range(0..5)]
    };
    let activity = if workflow {
        Activity::Noop
    } else {
        match rng.random_range(0..100) {
            0..8 if gates => Activity::Paused,
            8..14 if gates => Activity::Unmarked,
            14..20 if gates => Activity::Marked,
            20..26 => Activity::Breaker,
            _ => Activity::Noop,
        }
    };
    let pin = match rng.random_range(0..100) {
        0..5 if gates => Pin::Own,
        5..11 if gates => Pin::Foreign,
        11..15 if gates => Pin::ForeignExpired,
        _ => Pin::None,
    };
    let session = matches!(pin, Pin::Own | Pin::Foreign) && roll(rng, 30);
    let run = if workflow {
        match rng.random_range(0..100) {
            0..40 => Run::None,
            75..85 if gates => Run::Paused,
            85..93 if gates => Run::Expired,
            93..100 if gates => Run::ChainExpired,
            _ => Run::Running,
        }
    } else {
        Run::None
    };
    let partition = if workflow {
        [
            None,
            None,
            Some(OWN_PARTITION),
            Some(OWN_PARTITION),
            Some(STALE_PARTITION),
            Some(OTHER_PARTITION),
        ][rng.random_range(0..6)]
    } else {
        None
    };
    RowSpec {
        queue: rng.random_range(0..queues),
        workflow,
        activity,
        priority,
        age_ms,
        future: gates && roll(rng, 3),
        new_start: workflow && roll(rng, 30),
        pin,
        session,
        extra,
        run,
        partition,
        closed: gates && roll(rng, 3),
    }
}

/// Three held activity rows in front of each other row of a state.
///
/// One row has the paused activity, one the marked name and one the
/// unmarked name.
///
/// A random row of such an activity is seldom first in the claim order. So
/// with random rows only, a seed list can leave a gate with no claim to
/// decide. These rows have the top priority and the greatest age. Each
/// claim of an activity row in their state then passes all three. They
/// draw no random number, so the other rows of a seed do not change.
fn held_head_rows(queue: usize) -> Vec<RowSpec> {
    [Activity::Paused, Activity::Marked, Activity::Unmarked]
        .into_iter()
        .zip(0_u32..)
        .map(|(activity, n)| RowSpec {
            queue,
            workflow: false,
            activity,
            priority: 3,
            age_ms: 9_000 + n,
            future: false,
            new_start: false,
            pin: Pin::None,
            session: false,
            extra: Extra::Plain,
            run: Run::None,
            partition: None,
            closed: false,
        })
        .collect()
}

/// The state of `seed`.
///
/// A seed picks how many rows are not plain, and where they sit. No such
/// row gives the lean form every answer. A few of them under the plain
/// rows of a full head stay outside the window. Many of them at any
/// priority put one in most windows. A state with gates also holds the rows
/// of [`held_head_rows`].
fn state_spec(seed: u64) -> StateSpec {
    let mut rng = StdRng::seed_from_u64(seed);
    let queues = rng.random_range(1..=3_usize);
    let paused_queue = (queues > 1 && roll(&mut rng, 25)).then(|| rng.random_range(0..queues));
    let special = [0_u32, 0, 4, 30][rng.random_range(0..4)];
    let deep = roll(&mut rng, 50);
    let gates = roll(&mut rng, 70);
    let count = [5_u32, 18, 60, 160][rng.random_range(0..4)];
    let keys = (0..3)
        .map(|_| {
            let cap = rng.random_range(1..=2_u32);
            KeySpec {
                cap,
                running: if roll(&mut rng, 50) { cap } else { 0 },
            }
        })
        .collect();
    let buckets = (0..2)
        .map(|_| if roll(&mut rng, 50) { 2 } else { 0 })
        .collect();
    let mut ages: Vec<u32> = (0..count).map(|i| 1_000 + 17 * i).collect();
    ages.shuffle(&mut rng);
    let mut rows: Vec<RowSpec> = ages
        .into_iter()
        .map(|age_ms| row_spec(&mut rng, queues, special, deep, gates, age_ms))
        .collect();
    if gates {
        let live_queue = (0..queues)
            .find(|queue| Some(*queue) != paused_queue)
            .unwrap_or(0);
        rows.extend(held_head_rows(live_queue));
    }
    StateSpec {
        queues,
        paused_queue,
        keys,
        buckets,
        rows,
    }
}

/// The names of one written copy of a state.
#[derive(Debug)]
struct Written {
    queues: Vec<String>,
    buckets: Vec<String>,
}

/// The `VALUES` tuple of row `n` of a copy with the names of `tag`.
fn row_values(spec: &StateSpec, n: usize, tag: &str, run: Option<Uuid>) -> String {
    let row = &spec.rows[n];
    let (task_type, activity) = if row.workflow {
        ("workflow", "NULL".to_owned())
    } else {
        ("activity", format!("'{}'", row.activity.name()))
    };
    let due = if row.future {
        format!(
            "NOW() + INTERVAL '1 hour' + make_interval(secs => {}::float8 / 1000)",
            row.age_ms
        )
    } else {
        format!(
            "NOW() - make_interval(secs => {}::float8 / 1000)",
            row.age_ms
        )
    };
    let (pinned_to, pinned_until) = match row.pin {
        Pin::None => ("NULL".to_owned(), "NULL"),
        Pin::Own => (format!("'{WORKER}'"), "NOW() + INTERVAL '1 hour'"),
        Pin::Foreign => (
            "'lean-someone-else'".to_owned(),
            "NOW() + INTERVAL '1 hour'",
        ),
        Pin::ForeignExpired => (
            "'lean-someone-else'".to_owned(),
            "NOW() - INTERVAL '1 minute'",
        ),
    };
    let session = if row.session {
        "gen_random_uuid()"
    } else {
        "NULL"
    };
    let mut key = ("NULL".to_owned(), "NULL".to_owned());
    let mut rate = "NULL".to_owned();
    let mut build = "NULL".to_owned();
    let mut capability = "NULL".to_owned();
    match row.extra {
        Extra::Plain => {}
        Extra::Key(k) => key = (format!("'{tag}-key-{k}'"), spec.keys[k].cap.to_string()),
        Extra::Rate(b) => rate = format!("'{tag}-rate-{b}'"),
        Extra::Build(id) => build = format!("'{id}'"),
        Extra::Capability(value) => {
            capability =
                format!(r#"'[{{"Exact":{{"key":"lean-gpu","value":"{value}"}}}}]'::jsonb"#);
        }
    }
    let run = run.map_or_else(|| "NULL".to_owned(), |id| format!("'{id}'::uuid"));
    let partition = row
        .partition
        .map_or_else(|| "NULL".to_owned(), |p| format!("{p}::smallint"));
    let close = if row.closed {
        "NOW() - INTERVAL '1 hour'"
    } else {
        "NULL"
    };
    format!(
        "('{tag}-q{queue}', '{task_type}', {activity}, jsonb_build_object('n', {n}), 'PENDING', \
          {priority}, 3, {due}, {new_start}, {pinned_to}, {pinned_until}, {session}, {key_name}, \
          {key_cap}, {rate}, {build}, {capability}, {run}, {partition}, {close})",
        queue = row.queue,
        priority = row.priority,
        new_start = row.new_start,
        key_name = key.0,
        key_cap = key.1,
    )
}

/// Writes one copy of `spec` under the names of `tag`.
///
/// Two copies of a state differ in their names only: queues, keys, buckets
/// and run ids. Row `n` of each copy holds `n` in its input.
async fn write_state(conn: &mut AsyncPgConnection, spec: &StateSpec, tag: &str) -> Written {
    let queues: Vec<String> = (0..spec.queues).map(|q| format!("{tag}-q{q}")).collect();
    let buckets: Vec<String> = (0..spec.buckets.len())
        .map(|b| format!("{tag}-rate-{b}"))
        .collect();
    let mut statements: Vec<String> = Vec::new();
    if let Some(paused) = spec.paused_queue {
        statements.push(format!(
            "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by) \
             VALUES ('{}', 'lean differential', 'lean-test')",
            queues[paused]
        ));
    }
    for (bucket, tokens) in buckets.iter().zip(&spec.buckets) {
        statements.push(format!(
            "INSERT INTO harvest_rate_limit_buckets \
               (key, refill_rate, burst, tokens, last_refilled_at) \
             VALUES ('{bucket}', 0, 100, {tokens}, NOW())"
        ));
    }
    for (k, key) in spec.keys.iter().enumerate() {
        for _ in 0..key.running {
            statements.push(format!(
                "INSERT INTO harvest_task_queue \
                   (queue_name, task_type, activity_name, input, state, attempt, max_attempts, \
                    worker_id, started_at, concurrency_key, concurrency_cap) \
                 VALUES ('{tag}-q0', 'activity', 'lean-diff-noop', '{{}}'::jsonb, 'RUNNING', 1, \
                         3, 'lean-holder', NOW(), '{tag}-key-{k}', {cap})",
                cap = key.cap
            ));
        }
    }
    let mut tuples = Vec::with_capacity(spec.rows.len());
    for (n, row) in spec.rows.iter().enumerate() {
        let run = (row.run != Run::None).then(Uuid::new_v4);
        if let Some(id) = run {
            let (state, deadline, chain_deadline) = match row.run {
                Run::Paused => ("PAUSED", "NULL", "NULL"),
                Run::Expired => ("RUNNING", "NOW() - INTERVAL '1 hour'", "NULL"),
                Run::ChainExpired => ("RUNNING", "NULL", "NOW() - INTERVAL '1 hour'"),
                Run::None | Run::Running => ("RUNNING", "NULL", "NULL"),
            };
            statements.push(format!(
                "INSERT INTO harvest_workflow_executions \
                   (id, workflow_name, workflow_id, shard_id, input, state, deadline_at, \
                    chain_deadline_at) \
                 VALUES ('{id}', 'lean-diff', '{id}', 0, 'null'::jsonb, '{state}', {deadline}, \
                         {chain_deadline})"
            ));
        }
        tuples.push(row_values(spec, n, tag, run));
    }
    statements.push(format!(
        "INSERT INTO harvest_task_queue \
           (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
            scheduled_at, new_start, sticky_worker_id, sticky_until, session_id, \
            concurrency_key, concurrency_cap, rate_limit_key, required_build_id, \
            required_capabilities, workflow_exec_id, decision_partition, schedule_to_close_at) \
         VALUES {}",
        tuples.join(", ")
    ));
    exec(conn, &statements.join("; ")).await;
    Written { queues, buckets }
}

/// The rows that every state of a differential database reads. They are
/// the paused activity, the labels and the compatible build of the worker,
/// and one shard generation for the fence.
async fn write_shared_rows(conn: &mut AsyncPgConnection) {
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_activity_pauses (activity_name, reason, paused_by) \
             VALUES ('{PAUSED_ACTIVITY}', 'lean differential', 'lean-test'); \
             INSERT INTO harvest_workers \
               (worker_id, queues, shard_assignments, max_concurrency, host, build_id, labels) \
             VALUES ('{WORKER}', '[]'::jsonb, '[]'::jsonb, 1, 'test-host', '{BUILD}', \
                     '{{\"lean-gpu\": \"true\"}}'::jsonb); \
             INSERT INTO harvest_build_compat (build_id, compatible_with) \
             VALUES ('{BUILD}', '{COMPATIBLE_BUILD}'); \
             DELETE FROM harvest_shard_generation; \
             INSERT INTO harvest_shard_generation (shard_id, generation) \
             VALUES ({FENCE_SHARD}, {FENCE_GENERATION})"
        ),
    )
    .await;
}

/// Turns owner mode on. The worker of the test takes `OWN_PARTITION`, and
/// another holder takes `STALE_PARTITION`. The claim of the worker lists
/// both, so its list is stale for the second one.
async fn turn_owner_mode_on(conn: &mut AsyncPgConnection) -> OwnedClaim {
    decision_owner::enable_owner_mode(
        conn,
        &EnableOwnerMode {
            force: true,
            ..EnableOwnerMode::default()
        },
    )
    .await
    .expect("enable owner mode");
    let mut holders = Vec::new();
    for (worker, partition) in [
        (WORKER, OWN_PARTITION),
        ("lean-other-owner", STALE_PARTITION),
    ] {
        let holder = OwnerHolder::for_worker(worker);
        let partition = u16::try_from(partition)
            .ok()
            .and_then(DecisionPartition::new)
            .expect("a partition in range");
        let acquired = decision_owner::acquire(
            conn,
            ShardId::UNENCODED,
            &holder,
            partition,
            &OwnerLeaseConfig::default(),
        )
        .await
        .expect("acquire");
        assert!(
            matches!(acquired, Acquisition::Acquired(_)),
            "{worker} takes {partition}: {acquired:?}"
        );
        holders.push(holder.as_str().to_owned());
    }
    OwnedClaim {
        holder: holders.swap_remove(0),
        partitions: vec![OWN_PARTITION, STALE_PARTITION],
    }
}

/// The `$5` and `$6` binds of the differential tests.
fn activity_binds() -> (Vec<String>, Vec<String>) {
    (
        vec![BREAKER_ACTIVITY.to_owned()],
        vec![
            UNMARKED_ACTIVITY.to_owned(),
            format!("\u{1}{MARKED_ACTIVITY}"),
        ],
    )
}

/// Sends one form of `variant` over a written state.
async fn send_state(
    conn: &mut AsyncPgConnection,
    form: ClaimFormProbe,
    variant: Variant,
    state: &Written,
    fence_open: bool,
    owner: &OwnedClaim,
) -> ClaimFormStep {
    let (breakers, ineligible) = activity_binds();
    let generations = [FENCE_GENERATION + i64::from(!fence_open)];
    let binds = ClaimFormBinds {
        worker_id: WORKER,
        queues: &state.queues,
        worker_build_id: BUILD,
        circuit_breaker_activities: &breakers,
        ineligible_activities: &ineligible,
        kind: variant.kind,
        fence: variant
            .fenced
            .then_some((&[FENCE_SHARD][..], &generations[..])),
        owned: variant.owned.then_some(owner),
    };
    queue::claim_form_step_for_test(conn, form, binds)
        .await
        .unwrap_or_else(|e| panic!("{form:?} of {variant:?} runs: {e:?}"))
}

/// What one claim transaction did.
#[derive(Debug, PartialEq, Eq)]
struct Chain {
    /// The ordinal of the claimed row.
    claimed: Option<i64>,
    /// The rows and the tokens after the claim, read in the transaction.
    state: String,
}

/// One claim transaction over a written state. It sends the forms as the
/// claim does. The lean form goes first when `lean_first`. The window form
/// follows a lean fallback, and the full statement a window fallback.
///
/// Returns what the transaction did, and what the lean form answered.
async fn chain(
    conn: &mut AsyncPgConnection,
    variant: Variant,
    state: &Written,
    fence_open: bool,
    owner: &OwnedClaim,
    lean_first: bool,
    keep: bool,
) -> (Chain, Option<Outcome>) {
    open(conn).await;
    let (mut step, lean) = if lean_first {
        let lean_form = ClaimFormProbe::Lean;
        let step = send_state(conn, lean_form, variant, state, fence_open, owner).await;
        let answer = outcome(&step);
        (step, Some(answer))
    } else {
        (ClaimFormStep::Fallback, None)
    };
    for form in [ClaimFormProbe::Window, ClaimFormProbe::Full] {
        if matches!(step, ClaimFormStep::Fallback) {
            step = send_state(conn, form, variant, state, fence_open, owner).await;
        }
    }
    let after = digest(conn, &state.queues, &state.buckets).await;
    close(conn, keep).await;
    let claimed = match step {
        ClaimFormStep::Claimed(task) => Some(ordinal(&task)),
        ClaimFormStep::Empty => None,
        ClaimFormStep::Fallback => panic!("the full statement of {variant:?} set a flag"),
    };
    (
        Chain {
            claimed,
            state: after,
        },
        lean,
    )
}

/// The fewest lean claims that each gate of [`GateCover`] must decide.
const GATE_COVER_FLOOR: u32 = 20;

/// The answers of the lean form over a test.
#[derive(Debug, Default)]
struct Tally {
    claimed: u32,
    empty: u32,
    fallback: u32,
    /// The fallbacks after which a later form claimed a row. A lean form
    /// that read its own fallback as an empty poll would lose these rows.
    fallback_then_claimed: u32,
}

/// The lean claims of a test that a pause gate or a `$6` gate decided.
///
/// A gate decides a claim when a held row is ahead of the claimed row in
/// the claim order. Without the gate, the claim takes the held row. The
/// window form behind a lean fallback does not count, so each count is an
/// answer of the lean text alone.
#[derive(Debug, Default)]
struct GateCover {
    /// A row of a paused queue is ahead.
    paused_queue: u32,
    /// A row of a paused activity is ahead.
    paused_activity: u32,
    /// A row of an activity that `$6` marks with `chr(1)` is ahead.
    marked: u32,
    /// A row of an activity that `$6` names with no mark is ahead.
    unmarked: u32,
    /// The claimed row is a workflow row, which has no activity name. The
    /// paused activity and both `$6` names are in the arrays of each claim.
    workflow_row: u32,
}

/// True when only its pause gate or its `$6` gate keeps `row` from a claim
/// of `kind`.
fn held_by_a_gate_only(row: &RowSpec, kind: Option<TaskType>) -> bool {
    let kind_reads_it = match kind {
        None => true,
        Some(TaskType::Workflow) => row.workflow,
        Some(TaskType::Activity) => !row.workflow,
    };
    kind_reads_it
        && row.extra == Extra::Plain
        && row.pin == Pin::None
        && !row.future
        && !row.closed
        && matches!(row.run, Run::None | Run::Running)
        && row.partition.is_none()
}

/// The claim order of two rows with no live pin: true when `a` is first.
fn claims_before(a: &RowSpec, b: &RowSpec) -> bool {
    // A new start is due 30 seconds after its schedule time.
    let due = |row: &RowSpec| i64::from(row.new_start) * 30_000 - i64::from(row.age_ms);
    (b.priority, due(a)) < (a.priority, due(b))
}

impl GateCover {
    /// Counts the gates that decided the lean claim of row `claimed`.
    fn count(&mut self, spec: &StateSpec, kind: Option<TaskType>, claimed: usize) {
        let taken = &spec.rows[claimed];
        self.workflow_row += u32::from(taken.workflow);
        // A row with a live pin to the worker is first in each order.
        if taken.pin == Pin::Own {
            return;
        }
        let ahead = |held: &dyn Fn(&RowSpec) -> bool| {
            u32::from(spec.rows.iter().any(|row| {
                held(row) && held_by_a_gate_only(row, kind) && claims_before(row, taken)
            }))
        };
        self.paused_queue += ahead(&|row| Some(row.queue) == spec.paused_queue);
        let in_live_queue = |row: &RowSpec| Some(row.queue) != spec.paused_queue && !row.workflow;
        self.paused_activity +=
            ahead(&|row| in_live_queue(row) && row.activity == Activity::Paused);
        self.marked += ahead(&|row| in_live_queue(row) && row.activity == Activity::Marked);
        self.unmarked += ahead(&|row| in_live_queue(row) && row.activity == Activity::Unmarked);
    }
}

/// The lean form in front changes no claim, over random states.
///
/// Each state mixes plain rows with keyed, rate-limited, build-pinned and
/// capability rows. It also holds paused queues and activities, ineligible
/// and saturated activities, paused and expired runs, pins, sessions and
/// full heads.
///
/// For each of the twelve variants, one claim transaction runs twice on the
/// same state, with the lean form first and without it. Both must claim the
/// same row and leave the same rows and tokens. Each is rolled back. Then
/// one claim is committed, and the comparison runs again on the new state.
///
/// The second pass turns owner mode on, so the owned gate and the holder
/// check pass rows. A quarter of the seeds close the fence.
#[tokio::test]
async fn random_states_claim_the_same_row_through_either_first_form() {
    const STEPS: usize = 3;
    let _registry = clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    write_shared_rows(&mut conn).await;
    let mut owner = OwnedClaim {
        holder: "lean-no-holder".to_owned(),
        partitions: vec![OWN_PARTITION, STALE_PARTITION],
    };
    let mut tally = Tally::default();
    let mut cover = GateCover::default();
    for owner_mode in [false, true] {
        if owner_mode {
            owner = turn_owner_mode_on(&mut conn).await;
        }
        for (i, seed) in SEEDS.into_iter().enumerate() {
            let spec = state_spec(seed);
            let tag = format!("f{}-{seed}", u8::from(owner_mode));
            let state = write_state(&mut conn, &spec, &tag).await;
            let fence_open = i % 4 != 3;
            for step in 0..STEPS {
                for variant in variants() {
                    let (window_first, _) =
                        chain(&mut conn, variant, &state, fence_open, &owner, false, false).await;
                    let (lean_first, lean) =
                        chain(&mut conn, variant, &state, fence_open, &owner, true, false).await;
                    assert_eq!(
                        lean_first, window_first,
                        "seed {seed}, owner mode {owner_mode}, step {step}, {variant:?}: the lean \
                         form answered {lean:?}"
                    );
                    match lean {
                        Some(Outcome::Claimed(_)) => {
                            tally.claimed += 1;
                            let row = lean_first
                                .claimed
                                .and_then(|n| usize::try_from(n).ok())
                                .expect("a lean claim returns the ordinal of its row");
                            cover.count(&spec, variant.kind, row);
                        }
                        Some(Outcome::Empty) => tally.empty += 1,
                        Some(Outcome::Fallback) => {
                            tally.fallback += 1;
                            tally.fallback_then_claimed += u32::from(lean_first.claimed.is_some());
                        }
                        None => {}
                    }
                }
                let advance = Variant {
                    kind: None,
                    fenced: false,
                    owned: owner_mode,
                };
                chain(
                    &mut conn,
                    advance,
                    &state,
                    true,
                    &owner,
                    step % 2 == 0,
                    true,
                )
                .await;
            }
        }
    }
    // The seeds must reach each answer of the lean form, or the comparison
    // proves little.
    println!("lean answers: {tally:?}");
    assert!(
        tally.claimed >= 100
            && tally.empty >= 20
            && tally.fallback >= 100
            && tally.fallback_then_claimed >= 50,
        "the seeds give too few of one answer: {tally:?}"
    );
    // Each pause gate and each `$6` gate must decide lean claims. A change
    // of the generator that stops one of them fails here.
    println!("gates that decided a lean claim: {cover:?}");
    assert!(
        cover.paused_queue >= GATE_COVER_FLOOR
            && cover.paused_activity >= GATE_COVER_FLOOR
            && cover.marked >= GATE_COVER_FLOOR
            && cover.unmarked >= GATE_COVER_FLOOR
            && cover.workflow_row >= GATE_COVER_FLOOR,
        "the seeds give too few lean claims that one gate decided: {cover:?}"
    );
    db.drop_db().await;
}

/// One claim of the worker of the differential tests over a written state.
async fn state_claim(
    conn: &mut AsyncPgConnection,
    state: &Written,
    kind: Option<TaskType>,
    owner: Option<&OwnedClaim>,
    lean: Option<&mut LeanSkips>,
) -> Option<i64> {
    let (breakers, ineligible) = activity_binds();
    queue::claim_task_with_lean(
        conn,
        &state.queues,
        WORKER,
        BUILD,
        None,
        &breakers,
        &ineligible,
        None,
        kind,
        ClaimFairness::Off,
        owner,
        None,
        lean,
    )
    .await
    .expect("claim")
    .into_task()
    .as_ref()
    .map(ordinal)
}

/// A claim loop with lean state drains a queue as a caller with none does.
///
/// Each seed writes two copies of its state. One connection claims from
/// the first copy with the lean state of one loop. Another connection
/// claims from the second copy with no state, so it sends the window form
/// first. The claims change kind as the permits of a worker do. Each pair
/// of claims must take the same row, and both copies must end in the same
/// state. The second pass claims in owner mode.
#[tokio::test]
async fn random_drains_take_the_same_rows_with_and_without_the_lean_form() {
    let _registry = clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut lean_conn = db.conn().await;
    let mut window_conn = db.conn().await;
    write_shared_rows(&mut lean_conn).await;
    let mut claims = 0_i64;
    let mut owner = None;
    for owner_mode in [false, true] {
        if owner_mode {
            owner = Some(turn_owner_mode_on(&mut lean_conn).await);
        }
        for seed in SEEDS {
            let spec = state_spec(seed);
            let mode = u8::from(owner_mode);
            let lean_copy = write_state(&mut lean_conn, &spec, &format!("a{mode}-{seed}")).await;
            let window_copy =
                write_state(&mut window_conn, &spec, &format!("b{mode}-{seed}")).await;
            let mut skip = LeanSkips::new();
            for step in 0..spec.rows.len().min(36) + 4 {
                let kind = [
                    None,
                    None,
                    Some(TaskType::Workflow),
                    Some(TaskType::Activity),
                ][step % 4];
                let with_lean = state_claim(
                    &mut lean_conn,
                    &lean_copy,
                    kind,
                    owner.as_ref(),
                    Some(&mut skip),
                )
                .await;
                let without =
                    state_claim(&mut window_conn, &window_copy, kind, owner.as_ref(), None).await;
                assert_eq!(
                    with_lean, without,
                    "seed {seed}, owner mode {owner_mode}, step {step}, kind {kind:?}"
                );
                claims += 1;
            }
            assert_eq!(
                digest(&mut lean_conn, &lean_copy.queues, &lean_copy.buckets).await,
                digest(&mut window_conn, &window_copy.queues, &window_copy.buckets).await,
                "seed {seed}, owner mode {owner_mode}: the copies end in the same state"
            );
        }
    }
    let sent = forms(&mut lean_conn).await;
    let without = forms(&mut window_conn).await;
    assert_eq!(
        without.lean, 0,
        "a caller with no state never sends the lean form"
    );
    assert_eq!(
        without.window, claims,
        "it sends the window form on each claim"
    );
    // A claim that sent no window form got its answer from the lean form.
    let lean_alone = claims - sent.window;
    println!("{claims} claims, {lean_alone} by the lean form alone, lean loop sent {sent:?}");
    assert!(
        lean_alone >= 100 && sent.lean - lean_alone >= 20,
        "the seeds must give lean answers and lean fallbacks: {sent:?} over {claims} claims"
    );
    db.drop_db().await;
}

// ── The plan on a deep backlog ──────────────────────────────────────────────

/// The pending rows of the deep backlog, all on one queue.
const DEEP_ROWS: i64 = 40_000;
/// The terminal rows of the deep backlog.
const DEEP_TERMINAL_ROWS: i64 = 20_000;
/// The rows of the deep backlog with a live pin to the worker.
const DEEP_PINNED_ROWS: i64 = 3;
/// The rows that one queue head reads: the `LIMIT` of each head scan.
const HEAD_LIMIT: i64 = 32;

/// `INSERT` of `rows` plain rows in `state` on `queue`.
///
/// Three of each four rows are workflow rows, and one of them is a new
/// start. No activity row is a new start, so one of the four heads of the
/// queue is empty. An empty head on a deep queue reads the whole queue when
/// its scan has no ordered index.
fn deep_rows_sql(queue: &str, rows: i64, state: &str) -> String {
    format!(
        "INSERT INTO harvest_task_queue \
           (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
            scheduled_at, new_start) \
         SELECT '{queue}', CASE WHEN i % 4 = 3 THEN 'activity' ELSE 'workflow' END, \
                CASE WHEN i % 4 = 3 THEN 'noop' END, '{{}}'::jsonb, '{state}', i % 3, 3, \
                NOW() - make_interval(secs => 1 + (i % 3600)), i % 4 = 0 \
         FROM generate_series(1, {rows}) AS i"
    )
}

/// The child nodes of one plan node.
fn plan_children(node: &serde_json::Value) -> &[serde_json::Value] {
    node["Plans"].as_array().map_or(&[], Vec::as_slice)
}

/// Each node of `plan`, with the CTE that it runs in.
///
/// With `main_tree`, the walk leaves out each init plan and each subplan of
/// a node, so it reads the row source of a CTE only.
fn plan_nodes<'a>(
    plan: &'a serde_json::Value,
    cte: Option<&'a str>,
    main_tree: bool,
    found: &mut Vec<(Option<&'a str>, &'a serde_json::Value)>,
) {
    let named = plan["Subplan Name"]
        .as_str()
        .and_then(|name| name.strip_prefix("CTE "));
    let cte = named.or(cte);
    found.push((cte, plan));
    for child in plan_children(plan) {
        let side = matches!(
            child["Parent Relationship"].as_str(),
            Some("InitPlan" | "SubPlan")
        );
        let starts_a_cte = child["Subplan Name"]
            .as_str()
            .is_some_and(|name| name.starts_with("CTE "));
        if main_tree && side && !starts_a_cte {
            continue;
        }
        plan_nodes(child, cte, main_tree, found);
    }
}

/// A count that a plan node reports for each of its loops.
fn per_loop(node: &serde_json::Value, key: &str) -> i64 {
    node[key].as_i64().unwrap_or(0)
}

/// The `EXPLAIN (ANALYZE, BUFFERS)` plan of one lean claim, rolled back.
///
/// The statement is prepared under the planner settings of a claim
/// transaction, so the plan is the generic plan that a real claim reuses.
/// The fence binds match the generation row of `write_shared_rows`.
async fn lean_plan(
    conn: &mut AsyncPgConnection,
    variant: Variant,
    queue: &str,
    owner: &OwnedClaim,
) -> serde_json::Value {
    #[derive(diesel::QueryableByName)]
    struct Plan {
        #[diesel(sql_type = diesel::sql_types::Json, column_name = "QUERY PLAN")]
        plan: serde_json::Value,
    }
    let mut binds = vec![format!(
        "'{WORKER}', ARRAY['{queue}'], '', NULL, ARRAY[]::text[], ARRAY[]::text[]"
    )];
    if variant.fenced {
        binds.push(format!(
            "ARRAY[{FENCE_SHARD}]::int4[], ARRAY[{FENCE_GENERATION}]::int8[]"
        ));
    }
    if variant.owned {
        let mask = owner.mask().iter().fold(String::new(), |mut hex, byte| {
            // A write to a string cannot fail.
            let _ = write!(hex, "{byte:02x}");
            hex
        });
        binds.push(format!("'{}', '\\x{mask}'::bytea", owner.holder));
    }
    let binds = binds.join(", ");
    exec(
        conn,
        &format!(
            "BEGIN; {}; \
             PREPARE lean_plan_probe(text, text[], text, bigint, text[], text[]) AS {}",
            queue::CLAIM_PLAN_SETTINGS_SQL,
            queue::lean_claim_query_for(variant.kind, variant.fenced, variant.owned)
        ),
    )
    .await;
    let rows: Vec<Plan> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE lean_plan_probe({binds})"
    ))
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("explain the lean claim of {variant:?}: {e}"));
    exec(conn, "ROLLBACK; DEALLOCATE lean_plan_probe;").await;
    let plan = rows.into_iter().next().expect("one plan row").plan;
    plan[0]["Plan"].clone()
}

/// Asserts the plan shape of one lean claim over the deep backlog.
fn assert_the_lean_plan_is_bounded(plan: &serde_json::Value, case: &str, fenced: bool) {
    let mut all = Vec::new();
    plan_nodes(plan, None, false, &mut all);
    let mut rows = Vec::new();
    plan_nodes(plan, None, true, &mut rows);
    let on_the_queue_table =
        |node: &serde_json::Value| node["Relation Name"].as_str() == Some("harvest_task_queue");

    let claimed = all
        .iter()
        .find(|(_, node)| node["Subplan Name"].as_str() == Some("CTE claimed"))
        .map(|(_, node)| per_loop(node, "Actual Rows"));
    assert_eq!(claimed, Some(1), "{case}: the lean form claims a row");

    // The candidate scan reads the window rows by their row ids.
    let candidate: Vec<&str> = rows
        .iter()
        .filter(|(cte, node)| *cte == Some("seek_candidate") && on_the_queue_table(node))
        .map(|(_, node)| node["Node Type"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(candidate, ["Tid Scan"], "{case}: the candidate scan");
    let joins = rows
        .iter()
        .filter(|(cte, node)| {
            *cte == Some("seek_candidate")
                && node["Node Type"]
                    .as_str()
                    .is_some_and(|kind| kind.contains("Join") || kind == "Nested Loop")
        })
        .count();
    assert_eq!(
        joins,
        usize::from(fenced),
        "{case}: the candidate scan joins the fence only"
    );

    // Each head reads its ordered index, and reads one window of it.
    let heads: Vec<&serde_json::Value> = all
        .iter()
        .filter(|(cte, node)| *cte == Some("seek_heads") && on_the_queue_table(node))
        .map(|(_, node)| *node)
        .collect();
    let mut queue_heads = 0;
    for head in &heads {
        let index = head["Index Name"].as_str().unwrap_or("none");
        assert!(
            head["Node Type"].as_str() == Some("Index Scan")
                && (index == "idx_harvest_tq_claim_seek" || index == "idx_harvest_tq_sticky_poll"),
            "{case}: a head scan reads {index}, not its ordered index: {head}"
        );
        if index == "idx_harvest_tq_claim_seek" {
            queue_heads += 1;
            let read = per_loop(head, "Actual Rows") + per_loop(head, "Rows Removed by Filter");
            assert!(
                per_loop(head, "Actual Loops") > 0 && read <= HEAD_LIMIT,
                "{case}: a queue head reads {read} rows for each loop: {head}"
            );
        }
    }
    assert_eq!(queue_heads, 1, "{case}: one scan serves the queue heads");
    // Four queue heads and the pin head make the window.
    let window = 4 * HEAD_LIMIT + DEEP_PINNED_ROWS;
    assert!(
        !all.iter().any(|(cte, node)| *cte == Some("seek_heads")
            && node["Node Type"].as_str() == Some("Sort")
            && per_loop(node, "Actual Rows") > window),
        "{case}: no sort in the heads reads more than the window"
    );

    // Each reader takes a pause array once, as a parameter of its scan.
    let pause_reads: Vec<&serde_json::Value> = all
        .iter()
        .filter(|(_, node)| {
            node["Node Type"].as_str() == Some("CTE Scan")
                && matches!(
                    node["CTE Name"].as_str(),
                    Some("paused_queues" | "paused_activities")
                )
        })
        .map(|(_, node)| *node)
        .collect();
    assert!(!pause_reads.is_empty(), "{case}: the plan reads the pauses");
    for read in pause_reads {
        assert!(
            read["Parent Relationship"].as_str() == Some("InitPlan")
                && per_loop(read, "Actual Loops") <= 1,
            "{case}: a scan joins a pause array: {read}"
        );
    }

    let buffers = per_loop(plan, "Shared Hit Blocks") + per_loop(plan, "Shared Read Blocks");
    assert!(
        buffers <= DEEP_ROWS / 20,
        "{case}: the claim reads {buffers} buffers of a backlog of {DEEP_ROWS} rows"
    );
}

/// The lean form keeps its plan on a deep backlog of one queue (issue
/// #2680).
///
/// The lean text reads each pause array through a scalar subquery. So the
/// pause tests are tests of each scan, not of a join. The planner then
/// estimates the queue test of a table with one queue name at no row. An
/// estimate that low once moved a head scan off its ordered index (issue
/// #1971). This test pins the plan of each of the twelve lean texts:
///
/// - The candidate scan is a `Tid Scan`, and it joins the fence only.
/// - Each head scan reads `idx_harvest_tq_claim_seek`, or the pin index,
///   and a queue head reads one window of rows.
/// - No scan joins a pause array.
///
/// It does so with fresh statistics, and with the statistics of an idle
/// system. There an `ANALYZE` sees terminal rows only, and then a backlog
/// arrives. Each plan is the generic plan under the claim settings.
#[tokio::test]
async fn the_lean_form_keeps_its_plan_on_a_deep_backlog_of_one_queue() {
    #[derive(diesel::QueryableByName)]
    struct Mcv {
        #[diesel(sql_type = diesel::sql_types::Text)]
        vals: String,
    }
    let _registry = clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    write_shared_rows(&mut conn).await;
    let queue = "lean-deep";
    let terminal = deep_rows_sql(queue, DEEP_TERMINAL_ROWS, "COMPLETED");
    let backlog = format!(
        "{}; UPDATE harvest_task_queue SET sticky_worker_id = '{WORKER}', \
           sticky_until = NOW() + INTERVAL '1 hour' \
         WHERE id IN (SELECT id FROM harvest_task_queue WHERE state = 'PENDING' \
                      LIMIT {DEEP_PINNED_ROWS})",
        deep_rows_sql(queue, DEEP_ROWS, "PENDING")
    );
    exec(
        &mut conn,
        "ALTER TABLE harvest_task_queue SET (autovacuum_enabled = off)",
    )
    .await;
    let mut owner = OwnedClaim {
        holder: "lean-no-holder".to_owned(),
        partitions: Vec::new(),
    };
    // With owner mode on, the claim guard refuses a workflow row to a claim
    // with no owner. So the owned texts run in owner mode, and the other
    // texts run before it.
    for owner_mode in [false, true] {
        if owner_mode {
            owner = turn_owner_mode_on(&mut conn).await;
        }
        for statistics in ["fresh", "idle"] {
            exec(&mut conn, "TRUNCATE harvest_task_queue CASCADE").await;
            exec(&mut conn, &terminal).await;
            if statistics == "fresh" {
                exec(&mut conn, &backlog).await;
                exec(&mut conn, "VACUUM ANALYZE harvest_task_queue").await;
            } else {
                exec(&mut conn, "VACUUM ANALYZE harvest_task_queue").await;
                exec(&mut conn, &backlog).await;
            }
            let mcv: Mcv = diesel::sql_query(
                "SELECT COALESCE(most_common_vals::text, '') AS vals FROM pg_stats \
                 WHERE tablename = 'harvest_task_queue' AND attname = 'state'",
            )
            .get_result(&mut conn)
            .await
            .expect("read the state statistics");
            assert_eq!(
                mcv.vals.contains("PENDING"),
                statistics == "fresh",
                "{statistics} statistics show a PENDING row: {}",
                mcv.vals
            );
            for variant in variants() {
                if variant.owned != owner_mode {
                    continue;
                }
                let case = format!("{statistics} statistics, {}", variant.name());
                let plan = lean_plan(&mut conn, variant, queue, &owner).await;
                assert_the_lean_plan_is_bounded(&plan, &case, variant.fenced);
            }
        }
    }
    db.drop_db().await;
}

// ── The text dump ───────────────────────────────────────────────────────────

/// Writes each windowed claim text to the directory that
/// `HARVEST_CLAIM_TEXT_DIR` names: `<variant>-lean.sql` and
/// `<variant>-window.sql`.
///
/// A review or a plan measurement reads the texts without a bench run:
///
/// ```sh
/// HARVEST_CLAIM_TEXT_DIR=/tmp/claim-texts cargo test -p autumn-harvest \
///   --features db,testing --test integration \
///   lean_claim_db_tests::write_every_windowed_claim_text -- --ignored --exact
/// ```
#[test]
#[ignore = "writes files; set HARVEST_CLAIM_TEXT_DIR to the output directory"]
fn write_every_windowed_claim_text() {
    let dir = PathBuf::from(
        std::env::var("HARVEST_CLAIM_TEXT_DIR")
            .expect("HARVEST_CLAIM_TEXT_DIR names the output directory"),
    );
    std::fs::create_dir_all(&dir).expect("create the output directory");
    for variant in variants() {
        let Variant {
            kind,
            fenced,
            owned,
        } = variant;
        let texts = [
            ("lean", queue::lean_claim_query_for(kind, fenced, owned)),
            ("window", queue::window_claim_query_for(kind, fenced, owned)),
        ];
        for (form, text) in texts {
            let path = dir.join(format!("{}-{form}.sql", variant.name()));
            std::fs::write(&path, text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        }
    }
}
