#![cfg(feature = "db")]

//! Weighted fairness keys within a queue (issue #1976).
//!
//! # Stated bound
//!
//! Tenant A floods one queue. Tenant B then enqueues one task in the same
//! queue. The schedule-to-start of B, counted in claims that the queue serves
//! after B is due, must be at most one claim per other active key. With one
//! other key that is one claim. At a fixed service rate, one claim is one task
//! duration.
//!
//! Without fairness keys, B waits for the whole flood.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::queue::{self, ClaimFairness, EnqueueParams, TaskType};

use chrono::{Duration, Utc};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use uuid::Uuid;

use crate::integration_e2e::setup_test_database_url_or_env;

/// Rows in tenant A's flood.
const FLOOD: usize = 200;

/// The stated bound: claims that B may wait per other active key.
const CLAIMS_PER_OTHER_KEY: usize = 1;

async fn connect() -> (
    AsyncPgConnection,
    Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
) {
    let (url, container) = setup_test_database_url_or_env().await;
    let conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    (conn, container)
}

/// A queue name no other test uses.
fn fresh_queue(tag: &str) -> String {
    format!("fair-{tag}-{}", Uuid::new_v4().simple())
}

/// Enqueue one activity task for `key` that became due `age` ago.
async fn enqueue_keyed(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    key: Option<&str>,
    age: Duration,
) -> Uuid {
    let mut params = EnqueueParams::new(queue_name, TaskType::Activity, serde_json::json!({}));
    params.activity_name = Some("fair_noop".to_owned());
    params.scheduled_at = Utc::now() - age;
    params.fairness_key = key.map(str::to_owned);
    queue::enqueue(conn, &params).await.expect("enqueue")
}

/// Claim one task from `queue_name` in the given fairness mode.
async fn claim(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    fairness: ClaimFairness,
) -> Option<TaskQueueItem> {
    queue::claim_task_with_fairness(
        conn,
        &[queue_name.to_owned()],
        "fair-test-worker",
        "",
        None,
        &[],
        &[],
        None,
        None,
        fairness,
    )
    .await
    .expect("claim")
}

/// Flood the queue from tenant A, serve one claim, then enqueue tenant B.
/// Returns the number of claims served after B is due and before B.
async fn claims_before_tenant_b(fairness: ClaimFairness) -> usize {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("flood");

    // The flood is older than B, so it wins every tie on due time.
    for i in 0..FLOOD {
        let age = Duration::seconds(600) - Duration::milliseconds(i64::try_from(i).unwrap());
        enqueue_keyed(&mut conn, &queue_name, Some("tenant-a"), age).await;
    }
    claim(&mut conn, &queue_name, fairness)
        .await
        .expect("the flood has work");

    let b = enqueue_keyed(
        &mut conn,
        &queue_name,
        Some("tenant-b"),
        Duration::seconds(1),
    )
    .await;

    let mut waited = 0usize;
    loop {
        let task = claim(&mut conn, &queue_name, fairness)
            .await
            .expect("tenant B's task is still pending");
        if task.id == b {
            return waited;
        }
        waited += 1;
    }
}

/// The issue's red test: tenant A's flood must not hold tenant B past the
/// stated bound. It fails on the claim path without fairness keys.
#[tokio::test]
async fn tenant_flood_holds_tenant_b_within_the_bound_with_fairness_keys() {
    let waited = claims_before_tenant_b(ClaimFairness::Keys).await;
    assert!(
        waited <= CLAIMS_PER_OTHER_KEY,
        "tenant B waited {waited} claims behind tenant A's flood; bound {CLAIMS_PER_OTHER_KEY}"
    );
}

/// Control: without fairness keys the same flood holds B past the bound.
/// This is the defect that issue #1976 reports.
#[tokio::test]
async fn without_fairness_keys_a_flood_holds_tenant_b_past_the_bound() {
    let waited = claims_before_tenant_b(ClaimFairness::Off).await;
    assert!(
        waited > CLAIMS_PER_OTHER_KEY,
        "without fairness, B should wait behind the flood; waited {waited}"
    );
    assert_eq!(waited, FLOOD - 1, "B waits for the rest of the flood");
}

// ---------------------------------------------------------------------------
// Green-phase guards (issue #1976).
// ---------------------------------------------------------------------------

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::fairness_keys::{
    clear_fairness_weight, list_fairness_state, list_fairness_weights, prune_fairness_state,
    set_fairness_weight,
};
use autumn_harvest::queue_fairness::{FairClock, MAX_FAIRNESS_OVERRIDES_PER_QUEUE};
use diesel_async::RunQueryDsl;

/// Claim every row of `queue_name` and return the key of each claim, in order.
async fn drain_keys(conn: &mut AsyncPgConnection, queue_name: &str) -> Vec<String> {
    let mut keys = Vec::new();
    while let Some(task) = claim(conn, queue_name, ClaimFairness::Keys).await {
        keys.push(task.fairness_key.unwrap_or_default());
    }
    keys
}

/// The SQL claim and the pure model serve keys in the same order.
///
/// Weights are powers of two, so every pass is exact in both. Unkeyed rows
/// use the default key `''`.
#[tokio::test]
async fn fair_claim_matches_the_model_sequence() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("model");
    let weights = [("k1", 1.0), ("k2", 2.0), ("k4", 4.0), ("", 1.0)];
    for (key, weight) in weights {
        if !key.is_empty() {
            set_fairness_weight(&mut conn, &queue_name, key, weight, "test")
                .await
                .expect("set weight");
        }
    }

    // Interleave due times so the tie-break matters. Row i of key j became
    // due 10,000 - 10 * (4 * i + j) ms ago.
    let mut backlogs: BTreeMap<String, VecDeque<i64>> = BTreeMap::new();
    for i in 0..15i64 {
        for (j, (key, _)) in weights.iter().enumerate() {
            let age_ms = 10_000 - (4 * i + i64::try_from(j).unwrap()) * 10;
            let k = (!key.is_empty()).then_some(*key);
            enqueue_keyed(&mut conn, &queue_name, k, Duration::milliseconds(age_ms)).await;
            backlogs
                .entry((*key).to_owned())
                .or_default()
                .push_back(-age_ms);
        }
    }

    let sql_order = drain_keys(&mut conn, &queue_name).await;

    let w: BTreeMap<&str, f64> = weights.into_iter().collect();
    let mut clock = FairClock::default();
    let mut model_order = Vec::new();
    loop {
        let heads: Vec<(&str, i64)> = backlogs
            .iter()
            .filter_map(|(k, d)| d.front().map(|due| (k.as_str(), *due)))
            .collect();
        let Some(next) = clock.pick(heads).map(str::to_owned) else {
            break;
        };
        backlogs.get_mut(&next).unwrap().pop_front();
        clock.charge(&next, w[next.as_str()]);
        model_order.push(next);
    }
    assert_eq!(sql_order, model_order, "SQL and model claim orders differ");

    // The stored state matches the model too.
    let state = list_fairness_state(&mut conn, &queue_name).await.unwrap();
    assert_eq!(state.len(), weights.len(), "one state row per key");
    for row in state {
        let model = clock.state(&row.fairness_key).expect("model has the key");
        assert!(
            (row.pass - model.pass).abs() < 1e-9,
            "pass of {:?}",
            row.fairness_key
        );
        assert!((row.last_start - model.last_start).abs() < 1e-9);
    }
}

/// A weight override changes the share at the next claim. No restart.
#[tokio::test]
async fn weight_override_changes_share_at_runtime() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("runtime");
    for i in 0..400i64 {
        let age = Duration::seconds(1_000) - Duration::milliseconds(i);
        enqueue_keyed(&mut conn, &queue_name, Some("a"), age).await;
        enqueue_keyed(&mut conn, &queue_name, Some("b"), age).await;
    }

    let count =
        |keys: &[Option<String>], k: &str| keys.iter().filter(|x| x.as_deref() == Some(k)).count();
    let mut first = Vec::new();
    for _ in 0..100 {
        first.push(
            claim(&mut conn, &queue_name, ClaimFairness::Keys)
                .await
                .unwrap()
                .fairness_key,
        );
    }
    let b_first = count(&first, "b");
    assert!(
        (48..=52).contains(&b_first),
        "1:1 share gave b {b_first} of 100"
    );

    set_fairness_weight(&mut conn, &queue_name, "b", 3.0, "operator")
        .await
        .expect("set weight");
    let mut second = Vec::new();
    for _ in 0..200 {
        second.push(
            claim(&mut conn, &queue_name, ClaimFairness::Keys)
                .await
                .unwrap()
                .fairness_key,
        );
    }
    let b_second = count(&second, "b");
    assert!(
        (148..=152).contains(&b_second),
        "1:3 share gave b {b_second} of 200"
    );

    assert!(
        clear_fairness_weight(&mut conn, &queue_name, "b")
            .await
            .unwrap()
    );
    assert_eq!(
        list_fairness_weights(&mut conn, &queue_name)
            .await
            .unwrap()
            .len(),
        0
    );
}

/// Overrides are validated, and a queue holds at most 1,000 of them.
#[tokio::test]
async fn override_cap_is_1000_per_queue() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("cap");

    for bad in [0.0, -1.0, 1000.5, f64::NAN, f64::INFINITY] {
        assert!(
            set_fairness_weight(&mut conn, &queue_name, "k", bad, "t")
                .await
                .is_err()
        );
    }
    for bad_key in ["", " k", "k ", &"x".repeat(257), ".", ".."] {
        assert!(
            set_fairness_weight(&mut conn, &queue_name, bad_key, 1.0, "t")
                .await
                .is_err()
        );
    }

    let rows: Vec<String> = (0..MAX_FAIRNESS_OVERRIDES_PER_QUEUE)
        .map(|i| format!("('{queue_name}', 'k{i}', 1.0)"))
        .collect();
    diesel::sql_query(format!(
        "INSERT INTO harvest_fairness_weights (queue_name, fairness_key, weight) VALUES {}",
        rows.join(", ")
    ))
    .execute(&mut conn)
    .await
    .expect("seed 1000 overrides");

    let err = set_fairness_weight(&mut conn, &queue_name, "one-more", 2.0, "t")
        .await
        .expect_err("the 1001st override must fail");
    assert!(err.to_string().contains("1000"), "{err}");

    let updated = set_fairness_weight(&mut conn, &queue_name, "k7", 5.0, "ops")
        .await
        .expect("a change to an existing override succeeds at the cap");
    assert!((updated.weight - 5.0).abs() < f64::EPSILON);
    assert_eq!(updated.updated_by, "ops");

    assert!(
        clear_fairness_weight(&mut conn, &queue_name, "k7")
            .await
            .unwrap()
    );
    set_fairness_weight(&mut conn, &queue_name, "one-more", 2.0, "t")
        .await
        .expect("a freed slot takes a new key");
    assert_eq!(
        list_fairness_weights(&mut conn, &queue_name)
            .await
            .unwrap()
            .len(),
        MAX_FAIRNESS_OVERRIDES_PER_QUEUE
    );
}

/// Prune deletes only idle rows without debt that do not set `V`.
#[tokio::test]
async fn prune_deletes_only_rows_the_claim_cannot_tell_apart() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("prune");
    diesel::sql_query(format!(
        "INSERT INTO harvest_fairness_state (queue_name, fairness_key, pass, last_start, updated_at) VALUES \
         ('{queue_name}', 'clock', 10, 9, NOW() - INTERVAL '30 minutes'), \
         ('{queue_name}', 'idle', 5, 4, NOW() - INTERVAL '30 minutes'), \
         ('{queue_name}', 'debt', 12, 8, NOW() - INTERVAL '30 minutes'), \
         ('{queue_name}', 'busy', 3, 2, NOW() - INTERVAL '30 minutes'), \
         ('{queue_name}', 'fresh', 3, 2, NOW())"
    ))
    .execute(&mut conn)
    .await
    .expect("seed state");
    enqueue_keyed(&mut conn, &queue_name, Some("busy"), Duration::seconds(1)).await;

    // The janitor's idle window is at least 1 h. Rows 30 min old thus stay
    // out of its reach while this test runs on a shared database.
    let cutoff = Utc::now() - Duration::minutes(10);
    let preview = prune_fairness_state(&mut conn, Some(&queue_name), cutoff, 100, true)
        .await
        .unwrap();
    assert_eq!(preview, 1, "preview counts only the idle row");
    let deleted = prune_fairness_state(&mut conn, Some(&queue_name), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let mut left: Vec<String> = list_fairness_state(&mut conn, &queue_name)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.fairness_key)
        .collect();
    left.sort();
    assert_eq!(left, ["busy", "clock", "debt", "fresh"]);
}

/// A queue with no pending task and no recent charge forgets its state.
///
/// Keys that claim once never move `V`, so their rows keep a debt and the
/// plain prune rule never deletes them. Start-time fair queuing forgives
/// every debt when the queue is idle. Prune follows that rule. It deletes in
/// `last_start` order, so `V` holds until the last row goes.
#[tokio::test]
async fn prune_resets_a_queue_with_no_pending_task() {
    let (mut conn, _container) = connect().await;
    let idle = fresh_queue("idle");
    let recent = fresh_queue("recent");
    let rows: Vec<String> = (0..15)
        .map(|i| {
            format!(
                "('{idle}', 'k{i}', {pass}, {start}, NOW() - INTERVAL '30 minutes')",
                pass = f64::from(i) / 10.0 + 1.0,
                start = f64::from(i) / 10.0
            )
        })
        .chain(std::iter::once(format!(
            "('{recent}', 'one-shot', 1, 0, NOW())"
        )))
        .collect();
    diesel::sql_query(format!(
        "INSERT INTO harvest_fairness_state (queue_name, fairness_key, pass, last_start, updated_at) \
         VALUES {}",
        rows.join(", ")
    ))
    .execute(&mut conn)
    .await
    .expect("seed state");
    let cutoff = Utc::now() - Duration::minutes(10);
    let clock = |rows: &[autumn_harvest::fairness_keys::FairnessKeyState]| {
        rows.iter().map(|r| r.last_start).fold(f64::MIN, f64::max)
    };

    // One row per batch: one call deletes MAX_PRUNE_BATCHES rows, lowest
    // last_start first, so the clock row stays.
    let deleted = prune_fairness_state(&mut conn, Some(&idle), cutoff, 1, false)
        .await
        .unwrap();
    assert_eq!(deleted, 10);
    let left = list_fairness_state(&mut conn, &idle).await.unwrap();
    assert_eq!(left.len(), 5);
    assert!((clock(&left) - 1.4).abs() < 1e-9, "V holds: {left:?}");

    // The rows below `V` go first. The clock row waits for a later pass,
    // so a concurrent or rolled-back pruner never lowers `V`.
    let deleted = prune_fairness_state(&mut conn, Some(&idle), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(deleted, 4);
    let left = list_fairness_state(&mut conn, &idle).await.unwrap();
    assert_eq!(left.len(), 1);
    assert!((clock(&left) - 1.4).abs() < 1e-9, "V holds: {left:?}");
    let deleted = prune_fairness_state(&mut conn, Some(&idle), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "the idle queue resets");
    let left = list_fairness_state(&mut conn, &idle).await.unwrap();
    assert_eq!(left.len(), 0, "{left:?}");

    // A charge inside the idle window keeps the queue's state.
    let deleted = prune_fairness_state(&mut conn, Some(&recent), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(deleted, 0);
}

/// A pruner that holds a lower row blocks the reset of the clock row.
///
/// The second connection plays a concurrent pruner that locked the lowest
/// row and then rolls back. `V` must hold through both.
#[tokio::test]
async fn a_held_lower_row_keeps_the_clock_row_in_an_idle_reset() {
    use diesel_async::RunQueryDsl;
    let (mut conn, _container) = connect().await;
    let (mut other, _other_container) = connect().await;
    let idle = fresh_queue("held");
    diesel::sql_query(format!(
        "INSERT INTO harvest_fairness_state (queue_name, fairness_key, pass, last_start, updated_at) VALUES \
         ('{idle}', 'low', 1, 0, NOW() - INTERVAL '30 minutes'), \
         ('{idle}', 'mid', 1.5, 0.5, NOW() - INTERVAL '30 minutes'), \
         ('{idle}', 'clock', 2, 1, NOW() - INTERVAL '30 minutes')"
    ))
    .execute(&mut conn)
    .await
    .expect("seed state");
    diesel::sql_query("BEGIN")
        .execute(&mut other)
        .await
        .unwrap();
    diesel::sql_query(format!(
        "SELECT 1 FROM harvest_fairness_state \
         WHERE queue_name = '{idle}' AND fairness_key = 'low' FOR UPDATE"
    ))
    .execute(&mut other)
    .await
    .expect("hold the low row");

    let cutoff = Utc::now() - Duration::minutes(10);
    let deleted = prune_fairness_state(&mut conn, Some(&idle), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "only the free lower row goes");
    diesel::sql_query("ROLLBACK")
        .execute(&mut other)
        .await
        .unwrap();

    let mut left: Vec<String> = list_fairness_state(&mut conn, &idle)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.fairness_key)
        .collect();
    left.sort();
    assert_eq!(left, ["clock", "low"], "the clock row and V hold");
}

/// A preview counts what a live call deletes, across its batches.
///
/// The clock row of an idle queue qualifies only after the rows below it
/// go. One pass would miss it, so the preview must run the batches too.
#[tokio::test]
async fn prune_preview_matches_the_live_count_across_batches() {
    let (mut conn, _container) = connect().await;
    let idle = fresh_queue("preview");
    let rows: Vec<String> = (0..101)
        .map(|i| {
            format!(
                "('{idle}', 'k{i}', {pass}, {start}, NOW() - INTERVAL '30 minutes')",
                pass = f64::from(i) + 1.0,
                start = f64::from(i)
            )
        })
        .collect();
    diesel::sql_query(format!(
        "INSERT INTO harvest_fairness_state (queue_name, fairness_key, pass, last_start, updated_at) \
         VALUES {}",
        rows.join(", ")
    ))
    .execute(&mut conn)
    .await
    .expect("seed state");
    let cutoff = Utc::now() - Duration::minutes(10);
    let preview = prune_fairness_state(&mut conn, Some(&idle), cutoff, 100, true)
        .await
        .unwrap();
    assert_eq!(
        list_fairness_state(&mut conn, &idle).await.unwrap().len(),
        101,
        "a preview deletes nothing"
    );
    let live = prune_fairness_state(&mut conn, Some(&idle), cutoff, 100, false)
        .await
        .unwrap();
    assert_eq!(live, 101);
    assert_eq!(preview, live);
}

/// A claim without fairness keys writes no state.
#[tokio::test]
async fn fairness_off_writes_no_state() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("off");
    enqueue_keyed(&mut conn, &queue_name, Some("a"), Duration::seconds(1)).await;
    claim(&mut conn, &queue_name, ClaimFairness::Off)
        .await
        .unwrap();
    assert_eq!(
        list_fairness_state(&mut conn, &queue_name)
            .await
            .unwrap()
            .len(),
        0
    );
}

/// Unkeyed rows share the default key `''`, and a by-id claim charges its key.
#[tokio::test]
async fn by_id_claim_and_unkeyed_rows_charge_their_keys() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("byid");
    let keyed = enqueue_keyed(&mut conn, &queue_name, Some("k"), Duration::seconds(2)).await;
    enqueue_keyed(&mut conn, &queue_name, None, Duration::seconds(1)).await;

    let task = queue::claim_task_by_id_with_fairness(
        &mut conn,
        keyed,
        std::slice::from_ref(&queue_name),
        "fair-test-worker",
        "",
        None,
        &[],
        &[],
        None,
        ClaimFairness::Keys,
    )
    .await
    .unwrap()
    .expect("the named row is claimable");
    assert_eq!(task.id, keyed);
    claim(&mut conn, &queue_name, ClaimFairness::Keys)
        .await
        .unwrap();

    let state: BTreeMap<String, f64> = list_fairness_state(&mut conn, &queue_name)
        .await
        .unwrap()
        .into_iter()
        .map(|s| (s.fairness_key, s.pass))
        .collect();
    assert_eq!(state.len(), 2, "{state:?}");
    assert!((state["k"] - 1.0).abs() < 1e-9);
    assert!(state.contains_key(""), "unkeyed rows use the default key");
}

/// Claim once from `queues` with fairness keys on.
async fn claim_from(conn: &mut AsyncPgConnection, queues: &[String]) -> Option<TaskQueueItem> {
    queue::claim_task_with_fairness(
        conn,
        queues,
        "fair-test-worker",
        "",
        None,
        &[],
        &[],
        None,
        None,
        ClaimFairness::Keys,
    )
    .await
    .expect("claim")
}

/// A multi-queue fair claim keeps one clock per queue and starves no queue.
///
/// Queue 1 has two keys, so one of them is always at lag 0. Queue 2 has one
/// key, which keeps lag 1 after each claim. One statement over both queues
/// would never serve queue 2. The claim therefore tries one queue at a time.
#[tokio::test]
async fn multi_queue_fair_claim_starves_no_queue() {
    let (mut conn, _container) = connect().await;
    let q1 = fresh_queue("mq1");
    let q2 = fresh_queue("mq2");
    set_fairness_weight(&mut conn, &q1, "b", 2.0, "test")
        .await
        .unwrap();
    for i in 0..60 {
        let age = Duration::seconds(1_000 - i);
        enqueue_keyed(&mut conn, &q1, Some("a"), age).await;
        enqueue_keyed(&mut conn, &q1, Some("b"), age).await;
        enqueue_keyed(&mut conn, &q2, Some("k"), age).await;
    }
    let queues = [q1.clone(), q2.clone()];
    let mut from_q2 = 0;
    for _ in 0..40 {
        let task = claim_from(&mut conn, &queues).await.expect("work remains");
        if task.queue_name == q2 {
            from_q2 += 1;
        }
    }
    // Each claim tries the queues in a random order, so q2 gets about half.
    assert!(from_q2 >= 8, "q2 got {from_q2} of 40 claims");

    let q2_state = list_fairness_state(&mut conn, &q2).await.unwrap();
    assert_eq!(q2_state.len(), 1, "each queue keeps its own key state");
}

/// Priority sorts before the fairness lag.
#[tokio::test]
async fn priority_sorts_before_the_fairness_lag() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("prio");
    for i in 0..5 {
        let mut params = EnqueueParams::new(
            queue_name.as_str(),
            TaskType::Activity,
            serde_json::json!({}),
        );
        params.activity_name = Some("fair_noop".to_owned());
        params.scheduled_at = Utc::now() - Duration::seconds(100 - i);
        params.fairness_key = Some("high".to_owned());
        params.priority = 10;
        queue::enqueue(&mut conn, &params).await.unwrap();
    }
    enqueue_keyed(&mut conn, &queue_name, Some("low"), Duration::seconds(500)).await;
    let first: Vec<String> = drain_keys(&mut conn, &queue_name).await;
    assert_eq!(first, ["high", "high", "high", "high", "high", "low"]);
}

/// A weight below 1 gives a key less than an equal share.
#[tokio::test]
async fn a_weight_below_one_gives_a_smaller_share() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("half");
    set_fairness_weight(&mut conn, &queue_name, "slow", 0.5, "test")
        .await
        .unwrap();
    for i in 0..100i64 {
        let age = Duration::seconds(1_000) - Duration::milliseconds(i);
        enqueue_keyed(&mut conn, &queue_name, Some("slow"), age).await;
        enqueue_keyed(&mut conn, &queue_name, Some("fast"), age).await;
    }
    let mut slow = 0;
    for _ in 0..90 {
        if claim(&mut conn, &queue_name, ClaimFairness::Keys)
            .await
            .unwrap()
            .fairness_key
            .as_deref()
            == Some("slow")
        {
            slow += 1;
        }
    }
    // Weights 0.5 and 1 give a 1:2 split: 30 of 90, within the pair bound.
    assert!((28..=32).contains(&slow), "slow got {slow} of 90");
}

/// Every fair statement variant is valid SQL that Postgres can plan.
#[tokio::test]
async fn every_fair_statement_variant_prepares() {
    use queue::{claim_by_id_query_for, claim_query_for};
    let (mut conn, _container) = connect().await;
    let mut variants = Vec::new();
    for kind in [None, Some(TaskType::Workflow), Some(TaskType::Activity)] {
        for fenced in [false, true] {
            variants.push(claim_query_for(kind, fenced, ClaimFairness::Keys));
        }
    }
    for fenced in [false, true] {
        variants.push(claim_by_id_query_for(fenced, ClaimFairness::Keys));
    }
    variants.push(queue::FAIR_CHARGE_SQL);
    for (i, sql) in variants.into_iter().enumerate() {
        let name = format!("fair_variant_{i}_{}", Uuid::new_v4().simple());
        diesel::sql_query(format!("PREPARE {name} AS {sql}"))
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("variant {i} does not prepare: {e}"));
        diesel::sql_query(format!("DEALLOCATE {name}"))
            .execute(&mut conn)
            .await
            .unwrap();
    }
}

/// A kind-filtered fair claim takes only rows of that kind and charges them.
#[tokio::test]
async fn kind_filtered_fair_claim_charges_its_key() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("kind");
    enqueue_keyed(&mut conn, &queue_name, Some("k"), Duration::seconds(5)).await;
    let task = queue::claim_task_with_fairness(
        &mut conn,
        std::slice::from_ref(&queue_name),
        "fair-test-worker",
        "",
        None,
        &[],
        &[],
        None,
        Some(TaskType::Activity),
        ClaimFairness::Keys,
    )
    .await
    .unwrap()
    .expect("the activity row is claimable");
    assert_eq!(task.task_type, "activity");
    let state = list_fairness_state(&mut conn, &queue_name).await.unwrap();
    assert_eq!(state.len(), 1);
}

/// Concurrent fair claimers neither deadlock nor lose a charge, and tenant B
/// still gets through the flood early.
#[tokio::test]
async fn concurrent_fair_claims_keep_every_charge() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let queue_name = fresh_queue("conc");
    for i in 0..300i64 {
        let age = Duration::seconds(600) - Duration::milliseconds(i);
        enqueue_keyed(&mut conn, &queue_name, Some("a"), age).await;
    }
    for _ in 0..5 {
        enqueue_keyed(&mut conn, &queue_name, Some("b"), Duration::seconds(1)).await;
    }

    let order = Arc::new(AtomicUsize::new(0));
    let b_positions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let url = url.clone();
        let queue_name = queue_name.clone();
        let order = Arc::clone(&order);
        let b_positions = Arc::clone(&b_positions);
        handles.push(tokio::spawn(async move {
            let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
            while let Some(task) = claim(&mut conn, &queue_name, ClaimFairness::Keys).await {
                let at = order.fetch_add(1, Ordering::SeqCst);
                if task.fairness_key.as_deref() == Some("b") {
                    b_positions.lock().unwrap().push(at);
                }
            }
        }));
    }
    for h in handles {
        h.await.expect("a claimer panicked");
    }
    assert_eq!(
        AtomicUsize::load(&order, Ordering::SeqCst),
        305,
        "every row claimed once"
    );
    let last_b = b_positions.lock().unwrap().iter().copied().max().unwrap();
    assert!(
        last_b < 40,
        "B's five rows must clear early; last at claim {last_b}"
    );

    let state: BTreeMap<String, f64> = list_fairness_state(&mut conn, &queue_name)
        .await
        .unwrap()
        .into_iter()
        .map(|s| (s.fairness_key, s.pass))
        .collect();
    // Weight 1: each charge adds 1, and a key enters at V, so the pass is at
    // least the claim count.
    // Weight 1: each charge adds exactly 1. A key that falls behind the
    // clock restarts at V, which can only skip ahead. Concurrent claimers
    // can serve B back to back, and each B claim moves V by at most 1. So
    // A's pass is at least its 300 claims and at most 300 plus B's 5.
    assert!(state["a"] >= 300.0 - 1e-9, "a lost a charge: {state:?}");
    assert!(state["a"] <= 305.0 + 1e-9, "a was charged twice: {state:?}");
    assert!(state["b"] >= 5.0 - 1e-9, "b lost a charge: {state:?}");
}

/// The public enqueue paths reject a key that the weight API rejects. Such a
/// key would make the claim's charge fail, so no row may store it.
#[tokio::test]
async fn enqueue_rejects_an_invalid_fairness_key() {
    use autumn_harvest::schema::harvest_task_queue::dsl;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;
    let (mut conn, _container) = connect().await;
    let q = fresh_queue("badkey");
    let too_long = "k".repeat(autumn_harvest::queue_fairness::MAX_FAIRNESS_KEY_LEN + 1);
    for bad in ["", " a", "..", too_long.as_str()] {
        let mut params = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
        params.fairness_key = Some(bad.to_owned());
        let one = queue::enqueue(&mut conn, &params).await;
        assert!(one.is_err(), "enqueue accepted key {bad:?}");
        let good = EnqueueParams::new(&q, TaskType::Activity, serde_json::json!({}));
        let batch = queue::enqueue_batch(&mut conn, &[good, params]).await;
        assert!(batch.is_err(), "enqueue_batch accepted key {bad:?}");
    }
    let rows: i64 = dsl::harvest_task_queue
        .filter(dsl::queue_name.eq(&q))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count rows");
    assert_eq!(rows, 0, "a rejected batch inserts no row");
    enqueue_keyed(&mut conn, &q, Some("tenant-a"), Duration::zero()).await;
}
