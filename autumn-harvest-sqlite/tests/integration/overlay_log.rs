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
        .expect("append");
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
    let raw = rusqlite::Connection::open(&path).expect("raw");
    raw.pragma_update(None, "foreign_keys", true)
        .expect("foreign keys");
    let err = raw
        .execute("DELETE FROM harvest_agent_overlays", [])
        .expect_err("the foreign key refuses the delete");
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
    assert_eq!(
        log.active("support").expect("active").map(|r| r.version),
        Some(1)
    );
}

#[test]
fn an_activation_is_a_compare_and_swap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log_with_one_version(&dir.path().join("harvest.db"));
    log.append("support", b"{}", "operator", "ops").expect("v2");
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
