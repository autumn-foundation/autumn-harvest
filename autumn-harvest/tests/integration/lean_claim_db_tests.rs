//! The lean form of the default claim (issue #2680).
//!
//! The lean form is a smaller statement in front of the window form. It is
//! exact for a window of plain rows. In every other case it locks no row and
//! sets its flag, and the window form runs in the same transaction.
//!
//! Each test sends one form alone through the cached claim query, with the
//! binds of the window form.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a fresh testcontainers Postgres boots with the migrations.

use std::path::PathBuf;

use autumn_harvest::queue::{
    self, ClaimFormBinds, ClaimFormProbe, ClaimFormStep, EnqueueParams, OwnedClaim, TaskType,
};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const WORKER: &str = "lean-worker";

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
    let queues = [queue.to_owned()];
    let owner = OwnedClaim {
        holder: "lean-holder".to_owned(),
        partitions: Vec::new(),
    };
    let binds = ClaimFormBinds {
        worker_id: WORKER,
        queues: &queues,
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
}

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
