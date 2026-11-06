//! The overlay store suite on Postgres (issue #2371).
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_store_suite;

use std::sync::Arc;

use autumn_harvest::migrate;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::{PostgresOverlayStore, ProfileOverlay, ProfileOverlayStore, Provenance};
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

/// A kind that no earlier test or run used, so a reused database works.
/// A plain activation by `ops`.
const fn mv(
    version: u64,
    expected: Option<u64>,
) -> autumn_harvest::overlay_log::MoveRequest<'static> {
    autumn_harvest::overlay_log::MoveRequest {
        version,
        expected,
        actor_kind: "operator",
        actor: "ops",
        after_seq: None,
        head: None,
    }
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{prefix}-{nanos}")
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
    let pool = pool(&url);
    overlay_store_suite::run_all(
        &|validator| Box::new(PostgresOverlayStore::new(pool.clone(), validator)),
        &|validator, clock| {
            Box::new(PostgresOverlayStore::new(pool.clone(), validator).with_clock(clock))
        },
    )
    .await;
}

/// Postgres refuses an `UPDATE` of a version row, as the `harvest_events`
/// guard refuses a history rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_an_update_of_a_version_row() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("guard");
    let overlay = ProfileOverlay {
        max_steps: Some(3),
        ..ProfileOverlay::default()
    };
    let v1 = store
        .put(&kind, &overlay, Provenance::operator("ops"))
        .await
        .expect("put");
    let written = store.get(&kind, v1).await.expect("get").expect("v1");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for statement in [
        "UPDATE harvest_agent_overlays SET body = '\\x7b7d'::bytea WHERE kind = $1",
        "UPDATE harvest_agent_overlays SET author = 'mallory' WHERE kind = $1",
        "UPDATE harvest_agent_overlays SET version = version WHERE kind = $1",
    ] {
        let err = diesel::sql_query(statement)
            .bind::<Text, _>(&kind)
            .execute(&mut conn)
            .await
            .expect_err("the guard refuses the update");
        assert!(
            err.to_string()
                .contains("harvest_agent_overlays is append-only"),
            "{statement}: {err}"
        );
    }
    let after = store.get(&kind, v1).await.expect("get").expect("v1");
    assert_eq!(after, written);
}

/// The foreign key on the pointer refuses the delete of the active version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_a_delete_of_the_active_version() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("pointer");
    let v1 = store
        .put(
            &kind,
            &ProfileOverlay::default(),
            Provenance::operator("ops"),
        )
        .await
        .expect("put");
    store
        .activate(&kind, v1, None, Provenance::operator("ops"))
        .await
        .expect("activate");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let err = diesel::sql_query("DELETE FROM harvest_agent_overlays WHERE kind = $1")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect_err("the foreign key refuses the delete");
    assert!(err.to_string().contains("foreign key"), "{err}");
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
}

/// The trigger refuses each `UPDATE` of an activation row, so no one can
/// change the target of a rollback (issue #2374).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_an_update_of_an_activation_row() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("moves");
    let ops = || Provenance::operator("ops");
    let v1 = store
        .put(&kind, &ProfileOverlay::default(), ops())
        .await
        .expect("v1");
    let v2 = store
        .put(&kind, &ProfileOverlay::default(), ops())
        .await
        .expect("v2");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    store
        .activate(&kind, v2, Some(v1), ops())
        .await
        .expect("v2");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for statement in [
        "UPDATE harvest_agent_overlay_activations SET from_version = 2 WHERE kind = $1",
        "UPDATE harvest_agent_overlay_activations SET actor = 'mallory' WHERE kind = $1",
    ] {
        let err = diesel::sql_query(statement)
            .bind::<Text, _>(&kind)
            .execute(&mut conn)
            .await
            .expect_err("the guard refuses it");
        assert!(
            err.to_string()
                .contains("harvest_agent_overlay_activations is append-only"),
            "{statement}: {err}"
        );
    }
    let last = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("v2");
    assert_eq!((last.from(), last.to()), (Some(v1), v2));
    assert_eq!(last.actor(), &ops());
}

/// A rollback swap needs the move that it read to be the last one (issue
/// #2374). The pointer moved v2 -> v3 -> v2 after the read, so it matches
/// again, but the swap is a conflict and the pointer stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_a_rollback_over_a_stale_move() {
    use autumn_harvest::overlay_log::{ActivateOutcome, MoveRequest, activate_overlay};

    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("aba");
    let ops = || Provenance::operator("ops");
    for _ in 0..3 {
        store
            .put(&kind, &ProfileOverlay::default(), ops())
            .await
            .expect("put");
    }
    let v = |n| autumn_harvest_agent::OverlayVersion::new(n).expect("version");
    store.activate(&kind, v(1), None, ops()).await.expect("v1");
    store
        .activate(&kind, v(2), Some(v(1)), ops())
        .await
        .expect("v2");
    let read = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("move");
    store
        .activate(&kind, v(3), Some(v(2)), ops())
        .await
        .expect("v3");
    store
        .activate(&kind, v(2), Some(v(3)), ops())
        .await
        .expect("v2");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let stale = MoveRequest {
        after_seq: Some(read.seq()),
        ..mv(1, Some(2))
    };
    assert_eq!(
        activate_overlay(&mut conn, &kind, &stale)
            .await
            .expect("swap"),
        ActivateOutcome::Conflict { actual: Some(2) }
    );
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v(2));
    let back = store
        .rollback(&kind, ops())
        .await
        .expect("a fresh rollback");
    assert_eq!(
        (back.from, back.to),
        (v(2), v(3)),
        "it reverses the last move"
    );
}

/// A rollback to a deleted version refuses, and the pointer stays. A
/// pointer that moved before the log existed has no earlier activation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rollback_refuses_a_deleted_target_and_a_legacy_pointer() {
    use autumn_harvest_agent::{OverlayStoreError, OverlayVersion};

    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let ops = || Provenance::operator("ops");
    let v = |n| OverlayVersion::new(n).expect("version");
    let kind = unique("deleted");
    for _ in 0..2 {
        store
            .put(&kind, &ProfileOverlay::default(), ops())
            .await
            .expect("put");
    }
    store.activate(&kind, v(1), None, ops()).await.expect("v1");
    store
        .activate(&kind, v(2), Some(v(1)), ops())
        .await
        .expect("v2");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    diesel::sql_query("DELETE FROM harvest_agent_overlays WHERE kind = $1 AND version = 1")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect("an old version can be deleted");
    let before = store.last_activation(&kind).await.expect("last");
    assert_eq!(
        store.rollback(&kind, ops()).await,
        Err(OverlayStoreError::UnknownVersion { version: v(1) })
    );
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v(2));
    assert_eq!(store.last_activation(&kind).await.expect("last"), before);

    // A legacy pointer: set with no log row, as before migration
    // 20261102142302.
    let legacy = unique("legacy");
    store
        .put(&legacy, &ProfileOverlay::default(), ops())
        .await
        .expect("put");
    diesel::sql_query("UPDATE harvest_agent_overlay_heads SET active = 1 WHERE kind = $1")
        .bind::<Text, _>(&legacy)
        .execute(&mut conn)
        .await
        .expect("seed a legacy pointer");
    assert_eq!(
        store.rollback(&legacy, ops()).await,
        Err(OverlayStoreError::NoEarlierActivation)
    );
    let active = store.active(&legacy).await.expect("active").expect("set");
    assert_eq!(active.version(), v(1));
}

/// The log refuses the delete of a head row, so a deleted version never
/// comes back with other bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_never_gives_a_version_number_twice() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("reuse");
    let first = ProfileOverlay {
        max_steps: Some(3),
        ..ProfileOverlay::default()
    };
    let ops = || Provenance::operator("ops");
    let v1 = store.put(&kind, &first, ops()).await.expect("v1");
    let v2 = store.put(&kind, &first, ops()).await.expect("v2");
    store
        .activate(&kind, v1, None, Provenance::operator("ops"))
        .await
        .expect("activate");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    diesel::sql_query("DELETE FROM harvest_agent_overlays WHERE kind = $1 AND version = 2")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect("a version that is not active can go");
    let err = diesel::sql_query("DELETE FROM harvest_agent_overlay_heads WHERE kind = $1")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect_err("the guard keeps the head row");
    assert!(err.to_string().contains("keeps its rows"), "{err}");

    let v3 = store
        .put(&kind, &ProfileOverlay::default(), ops())
        .await
        .expect("v3");
    assert!(v3 > v2, "{v3} > {v2}");
    assert_eq!(store.get(&kind, v2).await.expect("get"), None);
}

/// The log reads the facts of an activation under the lock, so its own
/// refusals hold without the store in front of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_postgres_log_decides_under_the_lock() {
    use autumn_harvest::overlay_log::{ActivateOutcome, activate_overlay, append_overlay};

    let (url, _container) = database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let kind = unique("log");
    assert_eq!(
        activate_overlay(&mut conn, &kind, &mv(1, None))
            .await
            .expect("no head"),
        ActivateOutcome::UnknownVersion
    );
    let row = append_overlay(&mut conn, &kind, b"{}", "operator", "ops")
        .await
        .expect("append")
        .expect("a version");
    assert_eq!(row.version, 1);
    assert_eq!(
        activate_overlay(&mut conn, &kind, &mv(2, None))
            .await
            .expect("unknown"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, &mv(u64::MAX, None))
            .await
            .expect("too large"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, &mv(1, Some(u64::MAX)))
            .await
            .expect("far expected"),
        ActivateOutcome::Conflict { actual: None }
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, &mv(1, None))
            .await
            .expect("activate"),
        ActivateOutcome::Activated
    );
}

/// A full counter inserts nothing and reports it, so the store returns
/// `Full`, not a database error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_postgres_counter_reports_full() {
    use autumn_harvest::overlay_log::append_overlay;

    let (url, _container) = database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let kind = unique("full");
    diesel::sql_query(
        "INSERT INTO harvest_agent_overlay_heads (kind, latest) VALUES ($1, 9223372036854775807)",
    )
    .bind::<Text, _>(&kind)
    .execute(&mut conn)
    .await
    .expect("seed a full counter");
    let row = append_overlay(&mut conn, &kind, b"{}", "operator", "ops")
        .await
        .expect("append");
    assert_eq!(row, None);
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    assert_eq!(
        store
            .put(
                &kind,
                &ProfileOverlay::default(),
                Provenance::operator("ops")
            )
            .await,
        Err(autumn_harvest_agent::OverlayStoreError::Full)
    );
}

/// Issue #2394: a trigger refuses each `UPDATE` of a policy row, so an
/// audit row never changes. The table refuses a writer that is not an
/// operator.
#[tokio::test]
async fn postgres_refuses_an_update_of_a_policy_row() {
    use autumn_harvest_agent::RefinePolicy;

    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("policy-guard");
    store
        .set_policy(
            &kind,
            &RefinePolicy::default(),
            None,
            Provenance::operator("ops"),
        )
        .await
        .expect("set");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for statement in [
        "UPDATE harvest_agent_refine_policies SET new_body = NULL WHERE kind = $1",
        "UPDATE harvest_agent_refine_policies SET actor = 'mallory' WHERE kind = $1",
    ] {
        let err = diesel::sql_query(statement)
            .bind::<Text, _>(&kind)
            .execute(&mut conn)
            .await
            .expect_err("the guard refuses it");
        assert!(
            err.to_string()
                .contains("harvest_agent_refine_policies is append-only"),
            "{statement}: {err}"
        );
    }
    let err = diesel::sql_query(
        "INSERT INTO harvest_agent_refine_policies (kind, seq, actor_kind, actor)
         VALUES ($1, 99, 'refine', 'run')",
    )
    .bind::<Text, _>(&kind)
    .execute(&mut conn)
    .await
    .expect_err("only an operator writes");
    assert!(err.to_string().contains("check"), "{err}");
    assert_eq!(store.policy_log(&kind).await.expect("log").len(), 1);
}

/// Issue #2394: many auto adoptions at once, under a cap of 1, adopt one
/// version. Many sets over one revision store one change.
#[tokio::test]
async fn postgres_races_pass_the_cap_and_the_revision_once() {
    use autumn_harvest::ExecutionId;
    use autumn_harvest_agent::{AutoAdoptOutcome, OverlayStoreError, RefinePolicy};

    let (url, _container) = database().await;
    let store = Arc::new(PostgresOverlayStore::new(
        pool(&url),
        Arc::new(overlay_store_suite::Gate::default()),
    ));
    let kind = unique("race");
    let one: RefinePolicy = serde_json::from_value(serde_json::json!({
        "auto_adopt": {"parts": ["system"], "max_per_day": 1}
    }))
    .expect("policy");
    store
        .set_policy(&kind, &one, None, Provenance::operator("ops"))
        .await
        .expect("set");
    // Each racer reads the pointer and tries again after a lost swap. So
    // only the cap can stop a racer, and exactly one adopts.
    let mut tasks = Vec::new();
    for n in 0..6_u32 {
        let (store, kind, one) = (Arc::clone(&store), kind.clone(), one.clone());
        tasks.push(tokio::spawn(async move {
            let overlay = ProfileOverlay {
                system: Some(format!("Racer {n}.")),
                ..ProfileOverlay::default()
            };
            loop {
                let expected = store
                    .active(&kind)
                    .await
                    .expect("active")
                    .map(|stored| stored.version());
                match store
                    .auto_adopt(&kind, &overlay, ExecutionId::new(), expected, &one)
                    .await
                {
                    Err(OverlayStoreError::Conflict { .. }) => {}
                    other => return other,
                }
            }
        }));
    }
    let (mut adopted, mut capped) = (0, 0);
    for task in tasks {
        match task.await.expect("join") {
            Ok(AutoAdoptOutcome::Adopted(_)) => adopted += 1,
            Ok(AutoAdoptOutcome::CapReached { cap: 1, used: 1 }) => capped += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(
        (adopted, capped),
        (1, 5),
        "the cap stops each racer after one"
    );
    assert_eq!(store.list(&kind).await.expect("list").len(), 1);

    let mut tasks = Vec::new();
    for n in 1..=6_u32 {
        let (store, kind) = (Arc::clone(&store), kind.clone());
        tasks.push(tokio::spawn(async move {
            let policy: RefinePolicy =
                serde_json::from_value(serde_json::json!({"lessons": {"max_entries": n}}))
                    .expect("policy");
            store
                .set_policy(&kind, &policy, Some(1), Provenance::operator("ops"))
                .await
        }));
    }
    let mut stored = 0;
    for task in tasks {
        match task.await.expect("join") {
            Ok(_) => stored += 1,
            Err(OverlayStoreError::PolicyConflict { actual: Some(2) }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(stored, 1, "one set wins the revision");
    assert_eq!(store.policy_log(&kind).await.expect("log").len(), 2);

    let mut tasks = Vec::new();
    for _ in 0..6 {
        let (store, kind) = (Arc::clone(&store), kind.clone());
        tasks.push(tokio::spawn(async move {
            store.reset_policy(&kind, Provenance::operator("ops")).await
        }));
    }
    for task in tasks {
        task.await
            .expect("join")
            .expect("a reset needs no revision");
    }
    let log = store.policy_log(&kind).await.expect("log");
    assert_eq!(log.len(), 8);
    for pair in log.windows(2) {
        assert_eq!(
            pair[1].before(),
            pair[0].after(),
            "each row keeps the body before"
        );
        assert_eq!(pair[1].revision(), pair[0].revision() + 1);
    }
}

/// Issue #2394: an auto adoption checks the policy revision that its gate
/// read, under the policy lock. A stale revision moves nothing.
#[tokio::test]
async fn postgres_refuses_an_auto_move_over_a_stale_policy() {
    use autumn_harvest::chrono::Utc;
    use autumn_harvest::overlay_log::{AutoAdoptRaw, AutoAdoptRequest, auto_adopt_overlay};
    use autumn_harvest_agent::RefinePolicy;

    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("stale-policy");
    store
        .set_policy(
            &kind,
            &RefinePolicy::default(),
            None,
            Provenance::operator("ops"),
        )
        .await
        .expect("set");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let request = |policy_revision| AutoAdoptRequest {
        body: b"{}",
        expected: None,
        actor: "00000000-0000-0000-0000-000000000001",
        cap: None,
        at: Utc::now(),
        policy_revision,
    };
    for stale in [None, Some(2)] {
        let raw = auto_adopt_overlay(&mut conn, &kind, &request(stale))
            .await
            .expect("adopt");
        assert_eq!(
            raw,
            AutoAdoptRaw::PolicyMoved { actual: Some(1) },
            "{stale:?}"
        );
    }
    assert_eq!(store.list(&kind).await.expect("list"), []);
    let raw = auto_adopt_overlay(&mut conn, &kind, &request(Some(1)))
        .await
        .expect("adopt");
    assert!(matches!(raw, AutoAdoptRaw::Adopted(_)), "{raw:?}");
}
