//! The swarm store suite on the in-memory and `SQLite` stores (issue #2814).
//!
//! `swarm_store_postgres.rs` runs the same suite on Postgres.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod swarm_store_suite;

use std::sync::Arc;

use autumn_harvest_agent::swarm::InMemorySwarmStore;

#[tokio::test]
async fn the_in_memory_store_passes_the_suite() {
    let store: swarm_store_suite::Store = Arc::new(InMemorySwarmStore::new());
    swarm_store_suite::run_all(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_store_passes_the_suite() {
    use autumn_harvest_agent::swarm::SqliteSwarmStore;

    let store: swarm_store_suite::Store = Arc::new(SqliteSwarmStore::open_in_memory().unwrap());
    swarm_store_suite::run_all(&store).await;
}

/// A store on a file keeps its board after a reopen, as after a deploy.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_store_keeps_its_board_across_a_reopen() {
    use autumn_harvest_agent::swarm::{NewPost, SqliteSwarmStore, SwarmStore};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.db");
    let post = NewPost {
        post_key: "k1",
        channel: "general",
        author: "coder/c1",
        body: "hello",
        at_ms: 1,
    };
    let first = SqliteSwarmStore::open(&path)
        .unwrap()
        .append_post("s", post)
        .await
        .unwrap();
    let store = SqliteSwarmStore::open(&path).unwrap();
    assert_eq!(store.posts("s", None, 0, 10).await.unwrap(), vec![first]);
}

/// `SQLite` refuses each change of a post or a verdict, and an `INSERT OR
/// REPLACE` over a stored one.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_refuses_a_rewrite_of_a_post_or_a_verdict() {
    use autumn_harvest_agent::swarm::{MergeRow, NewPost, SqliteSwarmStore, SwarmStore};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.db");
    let store = SqliteSwarmStore::open(&path).unwrap();
    let post = NewPost {
        post_key: "k1",
        channel: "general",
        author: "coder/c1",
        body: "hello",
        at_ms: 1,
    };
    store.append_post("s", post).await.unwrap();
    let record = MergeRow {
        seq: 0,
        submit_key: "s1".to_owned(),
        item: "a".to_owned(),
        holder: "coder/c1".to_owned(),
        epoch: 1,
        base_version: 1,
        main_version: 1,
        verdict: "rejected".to_owned(),
        candidate_score: Some(1),
        main_score: Some(2),
        merged_version: None,
        snapshot: None,
        detail: String::new(),
        created_at_ms: 1,
    };
    store.record_verdict("s", &record).await.unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    for statement in [
        "UPDATE harvest_swarm_posts SET body = 'edited'",
        "INSERT OR REPLACE INTO harvest_swarm_posts
             (swarm, seq, post_key, channel, author, body, created_at_ms)
         VALUES ('s', 1, 'k9', 'general', 'mallory', 'x', 1)",
        "UPDATE harvest_swarm_merges SET verdict = 'merged'",
        "INSERT OR REPLACE INTO harvest_swarm_merges
             (swarm, seq, submit_key, item, holder, epoch, base_version, main_version,
              verdict, detail, created_at_ms)
         VALUES ('s', 1, 's1', 'a', 'mallory', 1, 1, 1, 'rejected', '', 1)",
    ] {
        let err = conn.execute(statement, []).unwrap_err();
        assert!(
            err.to_string().contains("is append-only"),
            "{statement}: {err}"
        );
    }
    assert_eq!(
        store.posts("s", None, 0, 10).await.unwrap()[0].body,
        "hello"
    );
    assert_eq!(
        store.merge_by_key("s", "s1").await.unwrap(),
        Some(MergeRow { seq: 1, ..record })
    );
}
