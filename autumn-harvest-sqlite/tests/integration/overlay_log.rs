//! The overlay log on `SQLite` (issue #2371).
//!
//! The typed store and the shared suite are in `autumn-harvest-agent`.
//! These tests cover the guard, which only raw SQL can reach.

use autumn_harvest::overlay_log::ActivateOutcome;
use autumn_harvest_sqlite::SqliteOverlayLog;

fn log_with_one_version(path: &std::path::Path) -> SqliteOverlayLog {
    let log = SqliteOverlayLog::open(path).expect("open");
    let row = log
        .append("support", b"{\"max_steps\":3}", "operator", "ops")
        .expect("append")
        .expect("a version");
    assert_eq!(row.version, 1);
    log
}

#[test]
fn sqlite_refuses_an_update_of_a_version_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    let raw = rusqlite::Connection::open(&path).expect("raw");
    for statement in [
        "UPDATE harvest_agent_overlays SET body = x'7b7d'",
        "UPDATE harvest_agent_overlays SET author = 'mallory'",
        "INSERT OR REPLACE INTO harvest_agent_overlays \
         (kind, version, body, author_kind, author, created_at_ms) \
         VALUES ('support', 1, x'7b7d', 'operator', 'mallory', 0)",
    ] {
        let err = raw
            .execute(statement, [])
            .expect_err("the guard refuses it");
        assert!(
            err.to_string()
                .contains("harvest_agent_overlays is append-only"),
            "{statement}: {err}"
        );
    }
    let row = log.get("support", 1).expect("get").expect("v1");
    assert_eq!(row.body, b"{\"max_steps\":3}");
    assert_eq!(row.author, "ops");
}

#[test]
fn sqlite_refuses_a_delete_of_the_active_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    assert_eq!(
        log.activate("support", 1, None).expect("activate"),
        ActivateOutcome::Activated
    );
    // A raw connection has no `foreign_keys` pragma. The trigger refuses the
    // delete all the same.
    let raw = rusqlite::Connection::open(&path).expect("raw");
    let err = raw
        .execute("DELETE FROM harvest_agent_overlays", [])
        .expect_err("the trigger refuses the delete");
    assert!(
        err.to_string().contains("keeps its active version"),
        "{err}"
    );
    assert_eq!(
        log.active("support").expect("active").map(|r| r.version),
        Some(1)
    );
}

#[test]
fn an_activation_is_a_compare_and_swap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log_with_one_version(&dir.path().join("harvest.db"));
    log.append("support", b"{}", "operator", "ops")
        .expect("v2")
        .expect("a version");
    assert_eq!(
        log.activate("support", 3, None).expect("activate"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        log.activate("other", 1, None).expect("activate"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        log.activate("support", 1, None).expect("activate"),
        ActivateOutcome::Activated
    );
    assert_eq!(
        log.activate("support", 2, None).expect("activate"),
        ActivateOutcome::Conflict { actual: Some(1) }
    );
    assert_eq!(
        log.activate("support", 2, Some(1)).expect("activate"),
        ActivateOutcome::Activated
    );
    let versions: Vec<u64> = log
        .list("support")
        .expect("list")
        .iter()
        .map(|row| row.version)
        .collect();
    assert_eq!(versions, [1, 2]);
}

#[test]
fn a_version_number_never_comes_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    let v2 = log
        .append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v2");
    let raw = rusqlite::Connection::open(&path).expect("raw");
    raw.execute(
        "DELETE FROM harvest_agent_overlays WHERE version = ?1",
        [i64::try_from(v2.version).expect("fits")],
    )
    .expect("a version that is not active can go");
    let err = raw
        .execute("DELETE FROM harvest_agent_overlay_heads", [])
        .expect_err("the guard keeps the head row");
    assert!(err.to_string().contains("keeps its rows"), "{err}");
    let v3 = log
        .append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v3");
    assert_eq!(v3.version, 3);
    assert_eq!(log.get("support", 2).expect("get"), None);
}

/// Each thread opens its own connection to one file, as two processes do.
fn race<T: Send>(
    path: &std::path::Path,
    threads: usize,
    work: impl Fn(&SqliteOverlayLog, usize) -> T + Sync,
) -> Vec<T> {
    let barrier = std::sync::Barrier::new(threads);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|n| {
                let (barrier, work) = (&barrier, &work);
                scope.spawn(move || {
                    let log = SqliteOverlayLog::open(path).expect("open");
                    barrier.wait();
                    work(&log, n)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect()
    })
}

#[test]
fn puts_on_two_connections_get_distinct_versions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    drop(SqliteOverlayLog::open(&path).expect("create"));
    let mut versions: Vec<u64> = race(&path, 8, |log, n| {
        log.append(
            "support",
            format!("{{\"n\":{n}}}").as_bytes(),
            "operator",
            "ops",
        )
        .expect("append")
        .expect("a version")
        .version
    });
    versions.sort_unstable();
    assert_eq!(versions, (1..=8).collect::<Vec<_>>());
}

#[test]
fn activates_on_two_connections_have_one_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    for _ in 0..4 {
        log.append("support", b"{}", "operator", "ops")
            .expect("append")
            .expect("a version");
    }
    let outcomes = race(&path, 4, |log, n| {
        let target = u64::try_from(n).expect("small") + 2;
        (
            target,
            log.activate("support", target, None).expect("activate"),
        )
    });
    let winners: Vec<u64> = outcomes
        .iter()
        .filter(|(_, outcome)| *outcome == ActivateOutcome::Activated)
        .map(|(target, _)| *target)
        .collect();
    assert_eq!(winners.len(), 1, "{outcomes:?}");
    for (target, outcome) in &outcomes {
        if *target != winners[0] {
            assert_eq!(
                *outcome,
                ActivateOutcome::Conflict {
                    actual: Some(winners[0])
                }
            );
        }
    }
    assert_eq!(
        log.active("support")
            .expect("active")
            .map(|row| row.version),
        Some(winners[0])
    );
}
