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
