#![cfg(feature = "db")]
//! The bounded queue depth (issue #2356).
//!
//! `queue::bounded_queue_depths` feeds the `harvest.queue.depth` gauge. It
//! counts each queue up to a cap, so its cost does not grow with the backlog.
//! `queue::queue_depths` is the exact count and the oracle of these tests.
//!
//! - [`bounded_depths_match_the_exact_count_up_to_the_cap`] compares the two
//!   functions at several caps, inside one transaction so that `NOW()` is the
//!   same for both.
//! - [`bounded_depth_scan_stops_at_the_cap`] reads the plan and checks that
//!   no queue scan returns more than the cap.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use autumn_harvest::queue;
use diesel::sql_types::Json;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use crate::queue_age_gauge_perf::setup_server;
use crate::throwaway_db::ThrowawayDb;

const fn cap(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("a test cap is not zero")
}

/// Runs both functions in one transaction and checks the bounded result.
async fn check(conn: &mut AsyncPgConnection, queues: &[String], limit: NonZeroU32) {
    let queues = queues.to_vec();
    conn.transaction::<(), diesel::result::Error, _>(async |conn| {
        let exact: BTreeMap<String, i64> = queue::queue_depths(conn, &queues)
            .await
            .expect("exact depths")
            .into_iter()
            .collect();
        let bounded = queue::bounded_queue_depths(conn, &queues, limit)
            .await
            .expect("bounded depths");
        let mut got = bounded;
        got.sort();
        let want: Vec<(String, i64)> = exact
            .into_iter()
            .map(|(q, n)| (q, n.min(i64::from(limit.get()))))
            .collect();
        // `want` has one entry per queue, so equality also rules out a repeat.
        assert_eq!(got, want, "cap {limit}: bounded depths differ");
        Ok(())
    })
    .await
    .expect("transaction");
}

/// The bounded depth, case by case, against the exact count.
///
/// Queue `a` holds three eligible rows. Queue `b` mixes eligible, future,
/// running and completed rows. Queue `c` is paused, and depth ignores the
/// pause. Queue `d` holds only future rows, and queue `e` holds only
/// completed rows. Queue `big` holds 250 eligible rows. Queue `x` is not in
/// the list. The list repeats `a`.
#[tokio::test]
async fn bounded_depths_match_the_exact_count_up_to_the_cap() {
    let (admin, _guard) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, "queue_depth_bound").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    let queues: Vec<String> = ["a", "b", "c", "d", "e", "big", "empty", "a"]
        .map(String::from)
        .to_vec();
    check(&mut conn, &queues, cap(1)).await;
    let empty: Vec<(String, i64)> = queue::bounded_queue_depths(&mut conn, &[], cap(5))
        .await
        .expect("an empty list");
    assert!(empty.is_empty(), "an empty list has no depth");

    conn.batch_execute(
        "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('c', 'depth'); \
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at) \
         SELECT q, 'activity', '{}', s, now() + d \
         FROM (VALUES \
             ('a', 'PENDING', interval '-1 minute', 3), \
             ('b', 'PENDING', interval '-1 minute', 5), \
             ('b', 'PENDING', interval '1 hour', 2), \
             ('b', 'RUNNING', interval '-1 minute', 3), \
             ('b', 'COMPLETED', interval '-1 minute', 2), \
             ('c', 'PENDING', interval '-1 minute', 1), \
             ('d', 'PENDING', interval '1 hour', 2), \
             ('e', 'COMPLETED', interval '-1 minute', 2), \
             ('big', 'PENDING', interval '-1 second', 250), \
             ('x', 'PENDING', interval '-1 minute', 4) \
         ) AS f(q, s, d, n) \
         CROSS JOIN LATERAL generate_series(1, n);",
    )
    .await
    .expect("fixture");

    for limit in [1, 2, 3, 4, 5, 6, 249, 250, 251, 10_000] {
        check(&mut conn, &queues, cap(limit)).await;
    }
    let depths = queue::bounded_queue_depths(
        &mut conn,
        &queues,
        autumn_harvest::telemetry::QUEUE_DEPTH_CAP,
    )
    .await
    .expect("depths at the gauge cap");
    let mut depths: Vec<(&str, i64)> = depths.iter().map(|(q, n)| (q.as_str(), *n)).collect();
    depths.sort_unstable();
    assert_eq!(
        depths,
        [("a", 3), ("b", 5), ("big", 250), ("c", 1)],
        "below the gauge cap, the depth is exact"
    );
}

#[derive(diesel::QueryableByName)]
struct Plan {
    #[diesel(sql_type = Json, column_name = "QUERY PLAN")]
    plan: serde_json::Value,
}

/// One scan node of a plan: its type, its index and the rows that it read.
///
/// A read row is a returned row or a row that a filter removed. The count
/// covers every loop.
struct Scan {
    node: String,
    index: Option<String>,
    rows: f64,
}

/// Every scan of `harvest_task_queue` in a plan tree.
///
/// A bitmap index scan names no table, so every one counts.
fn scans(node: &serde_json::Value, out: &mut Vec<Scan>) {
    let field = |name: &str| node[name].as_f64().unwrap_or(0.0);
    if let Some(kind) = node["Node Type"].as_str()
        && (node["Relation Name"] == "harvest_task_queue" || kind == "Bitmap Index Scan")
    {
        out.push(Scan {
            node: kind.to_owned(),
            index: node["Index Name"].as_str().map(str::to_owned),
            rows: (field("Actual Rows") + field("Rows Removed by Filter")) * field("Actual Loops"),
        });
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans(child, out);
    }
}

/// The scan of each queue walks the index and stops at the cap.
///
/// Queue `deep` holds 2,000 eligible rows. Queue `shallow` holds 10 eligible
/// rows and 5,000 future rows. Terminal rows make the heap large. The fixture
/// is analyzed but not vacuumed, so the visibility map is empty, as after an
/// enqueue burst. With two queue names and no visibility map, the planner
/// prefers a sequential scan unless the query orders by the index. That scan
/// reads the whole table for `shallow`. An index whose leading columns do not
/// bound `scheduled_at` reads the future rows instead.
#[tokio::test]
async fn bounded_depth_scan_stops_at_the_cap() {
    let (admin, _guard) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, "queue_depth_scan").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at) \
         SELECT CASE WHEN g BETWEEN 2001 AND 7010 THEN 'shallow' ELSE 'deep' END, \
                'activity', '{}', \
                CASE WHEN g <= 7010 THEN 'PENDING' ELSE 'COMPLETED' END, \
                CASE WHEN g BETWEEN 2011 AND 7010 THEN now() + interval '1 hour' \
                     ELSE now() - interval '1 minute' END \
         FROM generate_series(1, 27010) g;",
    )
    .await
    .expect("fixture");
    // ANALYZE without VACUUM leaves the visibility map empty.
    conn.batch_execute("ANALYZE harvest_task_queue")
        .await
        .expect("analyze");
    let queues = vec!["deep".to_owned(), "shallow".to_owned()];
    let mut depths = queue::bounded_queue_depths(&mut conn, &queues, cap(100))
        .await
        .expect("depths");
    depths.sort();
    assert_eq!(
        depths,
        [("deep".to_owned(), 100), ("shallow".to_owned(), 10)],
        "the deep queue reads the cap"
    );

    conn.batch_execute(&format!(
        "PREPARE depth_scan(text[], bigint) AS {}",
        queue::bounded_queue_depths_query()
    ))
    .await
    .expect("prepare");
    let plan: Plan = diesel::sql_query(
        "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE depth_scan('{deep,shallow}', 100)",
    )
    .get_result(&mut conn)
    .await
    .expect("explain");
    let mut found = Vec::new();
    scans(&plan.plan[0]["Plan"], &mut found);
    assert!(!found.is_empty(), "the plan has no scan: {}", plan.plan);
    for scan in &found {
        assert!(
            scan.node.starts_with("Index")
                && scan.index.as_deref() == Some("idx_harvest_tq_coverage_sample"),
            "each queue scan must walk idx_harvest_tq_coverage_sample, not a {} on {:?}: {}",
            scan.node,
            scan.index,
            plan.plan
        );
    }
    let rows: f64 = found.iter().map(|scan| scan.rows).sum();
    assert!(
        (rows - 110.0).abs() < f64::EPSILON,
        "the scans read {rows} rows, not the cap of the deep queue plus the shallow queue: {}",
        plan.plan
    );
}
