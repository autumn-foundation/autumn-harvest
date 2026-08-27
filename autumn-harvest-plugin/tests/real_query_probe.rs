//! Measurement probe for `docs/performance-history-bloat-filter.md`'s
//! "Verification against the real production entry point" section (PR #1173
//! review finding, [discussion_r3786205954][pr-1173-r3786205954]).
//!
//! Captures the REAL query cost of `GET /workflows?min_history_events=N` and
//! `GET /workflows?history_bloat_min_events=N` through the actual axum
//! router (`harvest_api_router`, via `tower::ServiceExt::oneshot` -- a real
//! `Request` through the real handlers, no network port bound) -- a real
//! public entry point, not the standalone scalar-subquery approximation
//! (`InitPlan`) the rest of that doc's measurements use -- against a
//! persistent, pre-seeded fixture database matching the one
//! `history_bloat_perf_repro.sh` builds.
//!
//! **Runs `VACUUM ANALYZE` on both touched tables before measuring.** This
//! is load-bearing, not decorative: an un-matched Postgres visibility-map
//! state between a "before" and "after" comparison run against the same
//! physical data can change each side's `Heap Fetches` count by enough to
//! reverse the apparent direction of the result, even though
//! `pg_stat_statements.total_buffers` is itself a deterministic,
//! caching-order-invariant count of buffer accesses (see the doc section
//! above for the full story -- this was caught and fixed during this PR's
//! review, not a hypothetical).
//!
//! Run manually against a persistent, pre-seeded fixture database (create it
//! with the same seeding SQL `history_bloat_perf_repro.sh` uses, but against
//! a durable database rather than a script-managed ephemeral one):
//!
//! ```text
//! REAL_QUERY_PERF_DB_URL=postgres://postgres:postgres@127.0.0.1:5432/harvest_realquery_perf \
//!   cargo test -p autumn-harvest-plugin --test real_query_probe -- --ignored --nocapture
//! ```
//!
//! [pr-1173-r3786205954]: https://github.com/autumn-foundation/autumn-harvest/pull/1173#discussion_r3786205954

use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

fn build_pool(database_url: &str) -> autumn_harvest::worker::DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_app(database_url: &str) -> HarvestApiApp {
    let pool = build_pool(database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(test_app_state())
}

#[derive(diesel::QueryableByName, Debug)]
struct StatStatementsRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
}

async fn get(app: &HarvestApiApp, uri: &str) -> StatusCode {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read body");
    if !status.is_success() {
        eprintln!(
            "  !! non-success response for {uri}: {status} body={}",
            String::from_utf8_lossy(&body)
        );
    }
    status
}

#[tokio::test]
#[ignore = "manual measurement probe, not a CI test"]
async fn probe_real_endpoint_query_cost() {
    let db_url = std::env::var("REAL_QUERY_PERF_DB_URL")
        .expect("set REAL_QUERY_PERF_DB_URL to the persistent fixture database");

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&db_url)
        .await
        .expect("failed to connect");
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();
    // Match Postgres visibility-map state before measuring -- see the module
    // doc comment. Without this, a "before" and "after" run against the same
    // physical data can differ purely by how much time autovacuum has had to
    // run since the last state change, independent of which query form ran.
    diesel::sql_query("VACUUM ANALYZE harvest_workflow_executions")
        .execute(&mut conn)
        .await
        .expect("failed to VACUUM ANALYZE harvest_workflow_executions");
    diesel::sql_query("VACUUM ANALYZE harvest_events")
        .execute(&mut conn)
        .await
        .expect("failed to VACUUM ANALYZE harvest_events");
    diesel::sql_query("SELECT pg_stat_statements_reset()")
        .execute(&mut conn)
        .await
        .expect("failed to reset pg_stat_statements");

    let app = build_app(&db_url);

    let started = std::time::Instant::now();
    let status1 = get(&app, "/workflows?min_history_events=10000").await;
    let elapsed1 = started.elapsed();
    println!("GET /workflows?min_history_events=10000 -> {status1} in {elapsed1:?}");

    let started = std::time::Instant::now();
    let status2 = get(&app, "/workflows?history_bloat_min_events=10000").await;
    let elapsed2 = started.elapsed();
    println!("GET /workflows?history_bloat_min_events=10000 -> {status2} in {elapsed2:?}");

    let rows: Vec<StatStatementsRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit + shared_blks_read AS total_buffers, shared_blks_read \
         FROM pg_stat_statements \
         WHERE query ILIKE '%harvest_workflow_executions%' OR query ILIKE '%harvest_events%' \
         ORDER BY total_buffers DESC",
    )
    .load(&mut conn)
    .await
    .expect("failed to query pg_stat_statements");

    println!("\n=== pg_stat_statements snapshot (real endpoint queries) ===");
    for row in &rows {
        println!(
            "calls={:<3} total_buffers={:<10} shared_blks_read={:<10} query={}",
            row.calls,
            row.total_buffers,
            row.shared_blks_read,
            row.query.chars().take(300).collect::<String>()
        );
    }
    println!("=== end snapshot ===");
}
