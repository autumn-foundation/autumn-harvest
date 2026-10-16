//! The workflow simulation's ingest model against Postgres (issue #2033).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, or have Docker.

#![cfg(all(feature = "db", feature = "testing"))]

use autumn_harvest::types::ExecutionId;
use diesel_async::RunQueryDsl;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A connection to `HARVEST_TEST_DATABASE_URL`, or to a new container.
async fn connect() -> (
    diesel_async::AsyncPgConnection,
    Option<ContainerAsync<Postgres>>,
) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };
    let conn = <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("connect to the test database");
    (conn, container)
}

/// Inserts one `RUNNING` execution with an empty history.
async fn insert_running_execution(conn: &mut diesel_async::AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
            (id, workflow_name, workflow_id, shard_id, state, input, started_at) \
         VALUES ($1, 'sim_ingest', $2, 0, 'RUNNING', '{}'::jsonb, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    exec_id
}

/// The workflow simulation predicts each ingest with its own model. This test
/// ties that model to the real `ingest_due_timers_and_signals` on Postgres.
///
/// Each case inserts timers and signals at offsets from one anchor. The model
/// gets the same offsets and the clock of the ingest. The appended events must
/// equal the model's batch, so the SQL filter and sort match it too.
#[tokio::test]
async fn sim_wake_order_matches_the_real_ingest() {
    use autumn_harvest::types::TimerId;
    use diesel::sql_types::{Integer, Text, Timestamptz, Uuid};

    let (mut conn, _container) = connect().await;
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();

    // The clock is 100 s after the anchor. A timer is due at offset 99 or
    // less. It waits at offset 150 or more, so a slow ingest stays exact.
    let now: chrono::DateTime<chrono::Utc> = diesel::select(diesel::dsl::now)
        .get_result::<std::time::SystemTime>(&mut conn)
        .await
        .unwrap()
        .into();
    let anchor = now - chrono::Duration::seconds(100);
    let at = |secs: u64| anchor + chrono::Duration::seconds(i64::try_from(secs).unwrap());

    let mut cases: Vec<(Vec<(&str, u64)>, Vec<u64>)> = vec![
        (vec![("t", 60)], vec![59]),
        (vec![("t", 60)], vec![60]),
        (vec![("b", 30), ("a", 30), ("late", 150)], vec![10, 30, 70]),
        (vec![], vec![5, 5, 6]),
    ];
    let mut rng = autumn_harvest::dst::SplitMix64::new(2033);
    let ids = ["a", "b", "c", "d", "e"];
    for _ in 0..16 {
        let timers = (0..rng.below(4))
            .map(|i| {
                let id = ids[usize::try_from(i).unwrap()];
                let offset = if rng.chance(25) { 150 + rng.below(50) } else { rng.below(4) * 25 };
                (id, offset)
            })
            .collect();
        let mut signals: Vec<u64> = (0..rng.below(4)).map(|_| rng.below(4) * 25).collect();
        signals.sort_unstable();
        cases.push((timers, signals));
    }

    for (timers, signals) in cases {
        let exec_id = insert_running_execution(&mut conn).await;
        for (id, offset) in &timers {
            diesel::sql_query(
                "INSERT INTO harvest_timers (workflow_exec_id, timer_id, fires_at) VALUES ($1, $2, $3)",
            )
            .bind::<Uuid, _>(exec_id.as_uuid())
            .bind::<Text, _>(*id)
            .bind::<Timestamptz, _>(at(*offset))
            .execute(&mut conn)
            .await
            .unwrap();
        }
        let mut model_signals = Vec::new();
        for (n, offset) in signals.iter().enumerate() {
            let name = format!("s{n}");
            let n = i32::try_from(n).unwrap();
            // A microsecond step keeps equal offsets in insert order.
            let received = at(*offset) + chrono::Duration::microseconds(i64::from(n));
            diesel::sql_query(
                "INSERT INTO harvest_signals (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
                 VALUES (gen_random_uuid(), $1, $2, jsonb_build_object('n', $3), $4, false)",
            )
            .bind::<Uuid, _>(exec_id.as_uuid())
            .bind::<Text, _>(&name)
            .bind::<Integer, _>(n)
            .bind::<Timestamptz, _>(received)
            .execute(&mut conn)
            .await
            .unwrap();
            model_signals.push((name, serde_json::json!({ "n": n }), *offset));
        }

        let before = autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .unwrap();
        autumn_harvest::worker::ingest_due_timers_and_signals(
            &mut conn,
            exec_id,
            before.next_event_id,
            &codecs,
        )
        .await
        .unwrap();
        let after = autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .unwrap();
        let ingested = &after.events[before.events.len()..];

        let model_timers: Vec<(TimerId, u64)> = timers
            .iter()
            .map(|(id, offset)| (TimerId::new(*id), *offset))
            .collect();
        let model = autumn_harvest::testing::sim::wake_order(100, &model_timers, &model_signals);
        assert_eq!(
            serde_json::to_value(ingested).unwrap(),
            serde_json::to_value(&model).unwrap(),
            "timers {timers:?}, signals {signals:?}"
        );
    }
}
