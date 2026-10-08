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

    // Interleave due times so the tie-break matters. Row i of key j is due
    // at 1000 - (4 * i + j) ms ago.
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

    let mut count =
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
    assert!(
        list_fairness_weights(&mut conn, &queue_name)
            .await
            .unwrap()
            .is_empty()
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
    for bad_key in ["", " k", "k ", &"x".repeat(256)] {
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
         ('{queue_name}', 'clock', 10, 9, NOW() - INTERVAL '1 day'), \
         ('{queue_name}', 'idle', 5, 4, NOW() - INTERVAL '1 day'), \
         ('{queue_name}', 'debt', 12, 8, NOW() - INTERVAL '1 day'), \
         ('{queue_name}', 'busy', 3, 2, NOW() - INTERVAL '1 day'), \
         ('{queue_name}', 'fresh', 3, 2, NOW())"
    ))
    .execute(&mut conn)
    .await
    .expect("seed state");
    enqueue_keyed(&mut conn, &queue_name, Some("busy"), Duration::seconds(1)).await;

    let cutoff = Utc::now() - Duration::hours(1);
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

/// A claim without fairness keys writes no state.
#[tokio::test]
async fn fairness_off_writes_no_state() {
    let (mut conn, _container) = connect().await;
    let queue_name = fresh_queue("off");
    enqueue_keyed(&mut conn, &queue_name, Some("a"), Duration::seconds(1)).await;
    claim(&mut conn, &queue_name, ClaimFairness::Off)
        .await
        .unwrap();
    assert!(
        list_fairness_state(&mut conn, &queue_name)
            .await
            .unwrap()
            .is_empty()
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
        &[queue_name.clone()],
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

/// A multi-queue claim keeps one clock per queue and drains both queues.
#[tokio::test]
async fn multi_queue_claim_keeps_a_clock_per_queue() {
    let (mut conn, _container) = connect().await;
    let q1 = fresh_queue("mq1");
    let q2 = fresh_queue("mq2");
    for i in 0..10 {
        enqueue_keyed(&mut conn, &q1, Some("a"), Duration::seconds(100 - i)).await;
        enqueue_keyed(&mut conn, &q2, Some("a"), Duration::seconds(100 - i)).await;
    }
    let queues = [q1.clone(), q2.clone()];
    let mut claimed = 0;
    while queue::claim_task_with_fairness(
        &mut conn,
        &queues,
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
    .unwrap()
    .is_some()
    {
        claimed += 1;
    }
    assert_eq!(claimed, 20);
    for q in [&q1, &q2] {
        let state = list_fairness_state(&mut conn, q).await.unwrap();
        assert_eq!(state.len(), 1);
        assert!(
            (state[0].pass - 10.0).abs() < 1e-9,
            "each queue charged 10 claims"
        );
    }
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
    assert!(state["a"] >= 300.0 - 1e-9, "a lost a charge: {state:?}");
    assert!(state["b"] >= 5.0 - 1e-9, "b lost a charge: {state:?}");
}
