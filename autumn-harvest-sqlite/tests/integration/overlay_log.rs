//! The overlay log on `SQLite` (issue #2371).
//!
//! The typed store and the shared suite are in `autumn-harvest-agent`.
//! These tests cover the guard, which only raw SQL can reach.

use autumn_harvest::overlay_log::{ActivateOutcome, MoveRequest};

/// A plain activation by the operator `actor`.
const fn mv(version: u64, expected: Option<u64>, actor: &str) -> MoveRequest<'_> {
    MoveRequest {
        version,
        expected,
        actor_kind: "operator",
        actor,
        after_seq: None,
        head: None,
    }
}
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
        log.activate("support", &mv(1, None, "ops"))
            .expect("activate"),
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
        log.activate("support", &mv(3, None, "ops"))
            .expect("activate"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        log.activate("other", &mv(1, None, "ops"))
            .expect("activate"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        log.activate("support", &mv(1, None, "ops"))
            .expect("activate"),
        ActivateOutcome::Activated
    );
    assert_eq!(
        log.activate("support", &mv(2, None, "ops"))
            .expect("activate"),
        ActivateOutcome::Conflict { actual: Some(1) }
    );
    assert_eq!(
        log.activate("support", &mv(2, Some(1), "ops"))
            .expect("activate"),
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
            log.activate("support", &mv(target, None, "ops"))
                .expect("activate"),
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
    // The race logs exactly one move: from unset to the winner (issue #2374).
    let last = log.last_activation("support").expect("last").expect("move");
    assert_eq!((last.seq, last.from, last.to), (1, None, winners[0]));
}

/// A rollback swap needs the move that it read to be the last one (issue
/// #2374). The pointer moved v2 -> v3 -> v2 after the read, so the pointer
/// matches again, but the swap is a conflict.
#[test]
fn sqlite_refuses_a_rollback_over_a_stale_move() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    for _ in 0..2 {
        log.append("support", b"{}", "operator", "ops")
            .expect("append")
            .expect("a version");
    }
    log.activate("support", &mv(1, None, "ops")).expect("v1");
    log.activate("support", &mv(2, Some(1), "ops")).expect("v2");
    let read = log
        .last_activation("support")
        .expect("last")
        .expect("v1 -> v2");
    let stale = MoveRequest {
        after_seq: Some(read.seq),
        ..mv(1, Some(2), "ops")
    };
    log.activate("support", &mv(3, Some(2), "ops")).expect("v3");
    log.activate("support", &mv(2, Some(3), "ops"))
        .expect("v2 again");
    assert_eq!(
        log.activate("support", &stale).expect("swap"),
        ActivateOutcome::Conflict { actual: Some(2) }
    );
    let last = log.last_activation("support").expect("last").expect("move");
    assert_eq!((last.seq, last.from, last.to), (4, Some(3), 2));
    let fresh = MoveRequest {
        after_seq: Some(last.seq),
        ..mv(3, Some(2), "ops")
    };
    assert_eq!(
        log.activate("support", &fresh).expect("swap"),
        ActivateOutcome::Activated
    );

    // A guarded move needs the log head that it read. A move away and back
    // keeps the pointer but moves the head, so the guarded swap refuses it.
    let head = log.last_activation("support").expect("last").map(|m| m.seq);
    let guarded = MoveRequest {
        head: Some(head),
        ..mv(1, Some(3), "run")
    };
    log.activate("support", &mv(2, Some(3), "ops"))
        .expect("away");
    log.activate("support", &mv(3, Some(2), "ops"))
        .expect("back");
    assert_eq!(
        log.activate("support", &guarded).expect("swap"),
        ActivateOutcome::Conflict { actual: Some(3) }
    );
    let head = log.last_activation("support").expect("last").map(|m| m.seq);
    let fresh = MoveRequest {
        head: Some(head),
        ..mv(1, Some(3), "run")
    };
    assert_eq!(
        log.activate("support", &fresh).expect("swap"),
        ActivateOutcome::Activated
    );
}

/// Each move logs one row, and the guard refuses a change of a row (issue
/// #2374). An activation of the active version logs nothing.
#[test]
fn sqlite_logs_each_move_and_refuses_a_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = log_with_one_version(&path);
    log.append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v2");
    assert_eq!(log.last_activation("support").expect("last"), None);
    log.activate("support", &mv(1, None, "ops")).expect("v1");
    log.activate("support", &mv(2, Some(1), "oncall"))
        .expect("v2");
    log.activate("support", &mv(2, Some(2), "ops"))
        .expect("stay");
    let last = log.last_activation("support").expect("last").expect("move");
    assert_eq!((last.seq, last.from, last.to), (2, Some(1), 2));
    assert_eq!(last.actor, "oncall");

    let raw = rusqlite::Connection::open(&path).expect("raw");
    for statement in [
        "UPDATE harvest_agent_overlay_activations SET from_version = 2",
        "INSERT OR REPLACE INTO harvest_agent_overlay_activations \
         (kind, seq, from_version, to_version, actor_kind, actor, activated_at_ms) \
         VALUES ('support', 2, 2, 1, 'operator', 'mallory', 0)",
    ] {
        let err = raw
            .execute(statement, [])
            .expect_err("the guard refuses it");
        assert!(
            err.to_string()
                .contains("harvest_agent_overlay_activations is append-only"),
            "{statement}: {err}"
        );
    }
    let last = log.last_activation("support").expect("last").expect("move");
    assert_eq!(
        (last.from, last.to, last.actor.as_str()),
        (Some(1), 2, "oncall")
    );
}

/// Issue #2394: each policy write keeps the body before it. A stale
/// compare changes nothing. The guard refuses each rewrite, and a writer
/// that is not an operator.
#[test]
fn sqlite_logs_each_policy_change_and_refuses_a_rewrite() {
    use autumn_harvest::overlay_log::{PolicyAppend, PolicyExpect, PolicyWrite};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let log = SqliteOverlayLog::open(&path).expect("open");
    let write = |body: Option<&'static [u8]>, expected| PolicyWrite {
        body,
        expected,
        actor_kind: "operator",
        actor: "ops",
    };
    let PolicyAppend::Appended(first) = log
        .append_policy(
            "support",
            &write(Some(b"{\"a\":1}"), PolicyExpect::Revision(None)),
        )
        .expect("first")
    else {
        panic!("the first write passes");
    };
    assert_eq!((first.seq, first.old_body.as_deref()), (1, None));
    assert_eq!(
        log.append_policy(
            "support",
            &write(Some(b"{}"), PolicyExpect::Revision(Some(7)))
        )
        .expect("stale"),
        PolicyAppend::Conflict { actual: Some(1) }
    );
    let PolicyAppend::Appended(reset) = log
        .append_policy("support", &write(None, PolicyExpect::Any))
        .expect("reset")
    else {
        panic!("a reset needs no revision");
    };
    assert_eq!(reset.seq, 2);
    assert_eq!(reset.old_body.as_deref(), Some(&b"{\"a\":1}"[..]));
    assert_eq!(reset.new_body, None);
    assert_eq!(log.policy_log("support").expect("log").len(), 2);
    assert_eq!(log.last_policy("other").expect("last"), None);

    let raw = rusqlite::Connection::open(&path).expect("raw");
    for statement in [
        "UPDATE harvest_agent_refine_policies SET new_body = NULL",
        "INSERT OR REPLACE INTO harvest_agent_refine_policies \
         (kind, seq, actor_kind, actor, changed_at_ms) \
         VALUES ('support', 1, 'operator', 'mallory', 0)",
    ] {
        let err = raw
            .execute(statement, [])
            .expect_err("the guard refuses it");
        assert!(
            err.to_string()
                .contains("harvest_agent_refine_policies is append-only"),
            "{statement}: {err}"
        );
    }
    let err = raw
        .execute(
            "INSERT INTO harvest_agent_refine_policies \
             (kind, seq, actor_kind, actor, changed_at_ms) \
             VALUES ('support', 9, 'refine', 'run', 0)",
            [],
        )
        .expect_err("only an operator writes");
    assert!(err.to_string().contains("CHECK"), "{err}");
}
