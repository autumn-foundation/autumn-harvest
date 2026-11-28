//! The swarm store suite on Postgres (issue #2814).
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod swarm_store_suite;

use std::sync::Arc;

use autumn_harvest::migrate;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::swarm::{NewPost, PostgresSwarmStore, SwarmStore};
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A migrated database, and the container that serves it, if any.
async fn database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    (url, container)
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_postgres_store_passes_the_suite() {
    let (url, _container) = database().await;
    let store: swarm_store_suite::Store = Arc::new(PostgresSwarmStore::new(pool(&url)));
    swarm_store_suite::run_all(&store).await;
}

/// Concurrent posts of one swarm take distinct `seq` values with no gap
/// that a cursor reader could skip past.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_posts_take_distinct_seq_values() {
    let (url, _container) = database().await;
    let store = Arc::new(PostgresSwarmStore::new(pool(&url)));
    let swarm = swarm_store_suite::fresh("race");
    let keys: Vec<String> = (0..24).map(|n| format!("k{n}")).collect();
    let writes = keys.iter().map(|key| {
        let store = store.clone();
        let swarm = swarm.clone();
        let key = key.clone();
        tokio::spawn(async move {
            let post = NewPost {
                post_key: &key,
                channel: "general",
                author: "coder/c1",
                body: "hi",
                at_ms: 1,
            };
            store.append_post(&swarm, post).await.unwrap().seq
        })
    });
    let mut seqs = Vec::new();
    for write in writes {
        seqs.push(write.await.unwrap());
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=24).collect::<Vec<u64>>());
}

/// Postgres refuses each change of a post or a verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_a_rewrite_of_a_post_or_a_verdict() {
    use autumn_harvest_agent::swarm::MergeRow;

    let (url, _container) = database().await;
    let store = PostgresSwarmStore::new(pool(&url));
    let swarm = swarm_store_suite::fresh("guard");
    let post = NewPost {
        post_key: "k1",
        channel: "general",
        author: "coder/c1",
        body: "hello",
        at_ms: 1,
    };
    store.append_post(&swarm, post).await.unwrap();
    let record = MergeRow {
        seq: 0,
        submit_key: "s1".to_owned(),
        item: "a".to_owned(),
        holder: "coder/c1".to_owned(),
        epoch: 1,
        base_version: 1,
        main_version: 1,
        verdict: "rejected".to_owned(),
        candidate_score: None,
        main_score: None,
        merged_version: None,
        snapshot: None,
        detail: String::new(),
        created_at_ms: 1,
    };
    store.record_verdict(&swarm, &record).await.unwrap();
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for statement in [
        "UPDATE harvest_swarm_posts SET body = 'edited' WHERE swarm = $1",
        "UPDATE harvest_swarm_merges SET verdict = 'merged' WHERE swarm = $1",
    ] {
        let err = diesel::sql_query(statement)
            .bind::<Text, _>(&swarm)
            .execute(&mut conn)
            .await
            .expect_err("the guard refuses the update");
        assert!(
            err.to_string().contains("is append-only"),
            "{statement}: {err}"
        );
    }
    assert_eq!(
        store.posts(&swarm, None, 0, 10).await.unwrap()[0].body,
        "hello"
    );
}
