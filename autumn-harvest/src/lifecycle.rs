//! The workflow execution lifecycle, as one table.
//!
//! `harvest_workflow_executions.state` is a `TEXT` column. Its `CHECK`
//! constraint lists the allowed values but says nothing about transitions.
//! Every writer guards its own transition with a `WHERE state = …` predicate
//! or a row lock. Before this module, no single place named the legal
//! transitions, so a writer could add one and no check would notice.
//!
//! [`WorkflowState`] names all ten persisted states. [`TRANSITIONS`] names
//! every sanctioned transition and the function that performs it. The tests
//! below hold the table to the code and to the schema:
//!
//! - the state set equals the latest `CHECK` constraint in `migrations/`;
//! - the terminal set equals [`crate::erase::TERMINAL_STATES`];
//! - every named writer exists in its file and writes its target state;
//! - every state is reachable from a start, and every open state has an exit.
//!
//! The table is not enforced by a database trigger. Shard-rebalance operator
//! overrides and many test fixtures write `state` directly, by design. The
//! table is the reviewable contract: a new writer adds its row here.

/// A persisted `harvest_workflow_executions.state` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkflowState {
    /// The run is open and dispatchable.
    Running,
    /// The run is open but an operator paused its dispatch.
    Paused,
    /// The run returned a value.
    Completed,
    /// The run failed permanently.
    Failed,
    /// The run was cancelled.
    Cancelled,
    /// The run passed its execution deadline, or a workflow task timed out.
    TimedOut,
    /// The run sealed itself and started a successor, or a start replaced it.
    ContinuedAsNew,
    /// An operator terminated the run, or a reset sealed it.
    Terminated,
    /// A staged shard-rebalance copy, not yet dispatchable.
    Migrating,
    /// The sealed source of a shard migration, with a forwarding pointer.
    Migrated,
}

impl WorkflowState {
    /// Every persisted state.
    pub const ALL: [Self; 10] = [
        Self::Running,
        Self::Paused,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::TimedOut,
        Self::ContinuedAsNew,
        Self::Terminated,
        Self::Migrating,
        Self::Migrated,
    ];

    /// The column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
            Self::ContinuedAsNew => "CONTINUED_AS_NEW",
            Self::Terminated => "TERMINATED",
            Self::Migrating => "MIGRATING",
            Self::Migrated => "MIGRATED",
        }
    }

    /// Parse a column value. Returns `None` for a value the schema rejects.
    #[must_use]
    pub fn from_db(state: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_str() == state)
    }

    /// True for a state in which nothing more happens to the run on this shard.
    ///
    /// This matches [`crate::erase::TERMINAL_STATES`]. `MIGRATED` is terminal
    /// here. `MIGRATING` is not.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Paused | Self::Migrating)
    }
}

/// One sanctioned change of `state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    /// The state before the write. `None` is a row insert.
    pub from: Option<WorkflowState>,
    /// The state after the write.
    pub to: WorkflowState,
    /// The source file, relative to `src/`, that holds the writer.
    pub file: &'static str,
    /// The function that performs the write.
    pub writer: &'static str,
    /// True when the writer names `to` as a literal. The staging restore
    /// writes a state it read from another column, so it is `false`.
    pub literal: bool,
}

const fn t(
    from: Option<WorkflowState>,
    to: WorkflowState,
    file: &'static str,
    writer: &'static str,
) -> Transition {
    Transition {
        from,
        to,
        file,
        writer,
        literal: true,
    }
}

use WorkflowState::{
    Cancelled, Completed, ContinuedAsNew, Failed, Migrated, Migrating, Paused, Running, Terminated,
    TimedOut,
};

/// Every sanctioned transition of `harvest_workflow_executions.state`.
///
/// A row delete is not a transition, so retention and discard paths are not
/// listed. A write that keeps the state, such as a non-determinism block, is
/// not listed either.
pub const TRANSITIONS: &[Transition] = &[
    // ── Row inserts ─────────────────────────────────────────────────────────
    t(
        None,
        Running,
        "execution.rs",
        "start_or_load_workflow_execution_collect_with_codecs_and_quota_override",
    ),
    t(None, Running, "execution.rs", "replace_execution"),
    t(
        None,
        Running,
        "worker.rs",
        "persist_workflow_continue_as_new_with_verdict",
    ),
    t(
        None,
        Running,
        "worker.rs",
        "persist_all_started_child_workflows",
    ),
    t(None, Running, "worker.rs", "insert_awaited_child_execution"),
    t(
        None,
        Running,
        "worker.rs",
        "create_detached_child_executions",
    ),
    t(
        None,
        Running,
        "cross_shard_child.rs",
        "start_child_on_target",
    ),
    t(None, Running, "reset.rs", "insert_fork_execution"),
    t(None, Migrating, "shard_rebalance.rs", "stage_copy"),
    // ── Open runs close ─────────────────────────────────────────────────────
    t(
        Some(Running),
        Completed,
        "worker.rs",
        "update_workflow_execution_completed",
    ),
    t(
        Some(Running),
        Failed,
        "worker.rs",
        "update_workflow_execution_failed",
    ),
    t(
        Some(Running),
        Failed,
        "timeout.rs",
        "enforce_workflow_history_ceiling_with_codecs",
    ),
    t(
        Some(Paused),
        Failed,
        "worker.rs",
        "quarantine_workflow_task_timeout",
    ),
    t(
        Some(Running),
        Failed,
        "poison_pill.rs",
        "fail_owning_workflow",
    ),
    t(
        Some(Paused),
        Failed,
        "poison_pill.rs",
        "fail_owning_workflow",
    ),
    t(
        Some(Running),
        Failed,
        "execution.rs",
        "cascade_terminate_detached_child",
    ),
    t(
        Some(Paused),
        Failed,
        "execution.rs",
        "cascade_terminate_detached_child",
    ),
    t(
        Some(Running),
        Cancelled,
        "execution.rs",
        "cancel_workflow_execution_collect",
    ),
    t(
        Some(Paused),
        Cancelled,
        "execution.rs",
        "cancel_workflow_execution_collect",
    ),
    t(Some(Running), Cancelled, "execution.rs", "inline_cancel"),
    t(Some(Paused), Cancelled, "execution.rs", "inline_cancel"),
    t(
        Some(Running),
        Cancelled,
        "execution.rs",
        "cascade_cancel_detached_child",
    ),
    t(
        Some(Paused),
        Cancelled,
        "execution.rs",
        "cascade_cancel_detached_child",
    ),
    t(
        Some(Running),
        Cancelled,
        "cross_shard_child.rs",
        "start_child_on_target",
    ),
    t(
        Some(Running),
        TimedOut,
        "timeout.rs",
        "update_workflow_execution_timed_out",
    ),
    t(
        Some(Paused),
        TimedOut,
        "timeout.rs",
        "update_workflow_execution_timed_out",
    ),
    t(
        Some(Running),
        Terminated,
        "execution.rs",
        "terminate_workflow_execution_collect",
    ),
    t(
        Some(Paused),
        Terminated,
        "execution.rs",
        "terminate_workflow_execution_collect",
    ),
    // An operator may terminate a staged copy before activation (#1596).
    t(
        Some(Migrating),
        Terminated,
        "execution.rs",
        "terminate_workflow_execution_collect",
    ),
    t(
        Some(Running),
        Terminated,
        "reset.rs",
        "terminate_source_execution",
    ),
    t(
        Some(Paused),
        Terminated,
        "reset.rs",
        "terminate_source_execution",
    ),
    t(
        Some(Running),
        ContinuedAsNew,
        "worker.rs",
        "persist_workflow_continue_as_new_with_verdict",
    ),
    // ── Pause and resume ────────────────────────────────────────────────────
    t(
        Some(Running),
        Paused,
        "execution.rs",
        "pause_workflow_execution",
    ),
    // The auto-resume scanner calls this writer too.
    t(
        Some(Paused),
        Running,
        "execution.rs",
        "resume_workflow_execution",
    ),
    // ── Exits from a terminal state ─────────────────────────────────────────
    // DLQ redrive reopens a failed run (issue #510).
    t(
        Some(Failed),
        Running,
        "execution.rs",
        "reactivate_failed_execution",
    ),
    // DAG retry-from-node seals a closed source before it forks (issue #366).
    t(
        Some(Failed),
        Terminated,
        "reset.rs",
        "terminate_source_execution",
    ),
    t(
        Some(Cancelled),
        Terminated,
        "reset.rs",
        "terminate_source_execution",
    ),
    t(
        Some(TimedOut),
        Terminated,
        "reset.rs",
        "terminate_source_execution",
    ),
    // A start-replace frees the business key of a finished run.
    t(
        Some(Completed),
        ContinuedAsNew,
        "execution.rs",
        "seal_replaced_execution",
    ),
    t(
        Some(Failed),
        ContinuedAsNew,
        "execution.rs",
        "seal_replaced_execution",
    ),
    t(
        Some(Cancelled),
        ContinuedAsNew,
        "execution.rs",
        "seal_replaced_execution",
    ),
    t(
        Some(TimedOut),
        ContinuedAsNew,
        "execution.rs",
        "seal_replaced_execution",
    ),
    // A shard-staging vacate does the same on the target shard.
    t(
        Some(Completed),
        ContinuedAsNew,
        "shard_rebalance.rs",
        "stage_copy",
    ),
    t(
        Some(Failed),
        ContinuedAsNew,
        "shard_rebalance.rs",
        "stage_copy",
    ),
    t(
        Some(Cancelled),
        ContinuedAsNew,
        "shard_rebalance.rs",
        "stage_copy",
    ),
    t(
        Some(TimedOut),
        ContinuedAsNew,
        "shard_rebalance.rs",
        "stage_copy",
    ),
    // ── Shard rebalancing (issue #964) ──────────────────────────────────────
    t(
        Some(Running),
        Migrated,
        "shard_rebalance.rs",
        "commit_cutover",
    ),
    t(
        Some(Migrating),
        Running,
        "shard_rebalance.rs",
        "activate_target",
    ),
    t(
        Some(Migrating),
        Migrated,
        "shard_rebalance.rs",
        "discard_staged_copy_restoring_seal",
    ),
];

/// The restore half of a shard-staging vacate. An abort writes back the state
/// the vacate recorded in `staging_vacated_state`.
pub const STAGING_RESTORES: &[Transition] = &[
    restore(Completed),
    restore(Failed),
    restore(Cancelled),
    restore(TimedOut),
];

const fn restore(to: WorkflowState) -> Transition {
    Transition {
        from: Some(ContinuedAsNew),
        to,
        file: "shard_rebalance.rs",
        writer: "discard_staged_copy_restoring_seal",
        literal: false,
    }
}

/// Every sanctioned transition, including the staging restores.
pub fn all_transitions() -> impl Iterator<Item = &'static Transition> {
    TRANSITIONS.iter().chain(STAGING_RESTORES.iter())
}

/// True when some sanctioned writer moves a row from `from` to `to`.
#[must_use]
pub fn is_sanctioned(from: WorkflowState, to: WorkflowState) -> bool {
    all_transitions().any(|tr| tr.from == Some(from) && tr.to == to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    fn crate_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    /// The state list from the last migration that redefines the CHECK.
    fn latest_check_states() -> BTreeSet<String> {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(crate_dir().join("migrations"))
            .expect("read migrations/")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .collect();
        dirs.sort();
        let mut latest = None;
        for dir in dirs {
            let Ok(sql) = std::fs::read_to_string(dir.join("up.sql")) else {
                continue;
            };
            let Some(at) = sql.find("ADD CONSTRAINT harvest_workflow_executions_state_check")
            else {
                continue;
            };
            let rest = &sql[at..];
            let open = rest.find("IN (").expect("the CHECK has an IN list") + "IN (".len();
            let close = open + rest[open..].find(')').expect("the IN list closes");
            latest = Some(
                rest[open..close]
                    .split(',')
                    .map(|s| s.trim().trim_matches('\'').to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
        latest.expect("some migration defines harvest_workflow_executions_state_check")
    }

    /// The text of `fn writer` in `file`, up to the next top-level item.
    fn writer_body(file: &str, writer: &str) -> String {
        let path = crate_dir().join("src").join(file);
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let needle = format!("fn {writer}(");
        let start = src.find(&needle).unwrap_or_else(|| {
            panic!("{file} has no `fn {writer}`; update lifecycle::TRANSITIONS")
        });
        let end = src[start..].find("\n}\n").map_or(src.len(), |i| start + i);
        src[start..end].to_string()
    }

    #[test]
    fn the_state_set_matches_the_database_check_constraint() {
        let rust: BTreeSet<String> = WorkflowState::ALL
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        assert_eq!(rust, latest_check_states());
    }

    #[test]
    fn the_terminal_set_matches_erase() {
        let here: BTreeSet<&str> = WorkflowState::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .map(|s| s.as_str())
            .collect();
        let erase: BTreeSet<&str> = crate::erase::TERMINAL_STATES.iter().copied().collect();
        assert_eq!(here, erase);
    }

    #[test]
    fn from_db_round_trips_and_rejects_unknown_values() {
        for state in WorkflowState::ALL {
            assert_eq!(WorkflowState::from_db(state.as_str()), Some(state));
        }
        // "SUSPENDED" appears in older comments but is not a persisted state.
        assert_eq!(WorkflowState::from_db("SUSPENDED"), None);
    }

    #[test]
    fn every_writer_exists_and_writes_its_target_state() {
        for tr in all_transitions() {
            let body = writer_body(tr.file, tr.writer);
            if !tr.literal {
                continue;
            }
            let to = tr.to.as_str();
            // An insert takes RUNNING from the column default. A staged copy
            // names MIGRATING in its JSON build.
            let defaulted = tr.from.is_none() && tr.to == Running;
            assert!(
                defaulted
                    || body.contains(&format!("\"{to}\""))
                    || body.contains(&format!("'{to}'")),
                "{}::{} is listed as writing {to} but never names it",
                tr.file,
                tr.writer
            );
        }
    }

    #[test]
    fn every_state_is_reachable_from_an_insert() {
        let mut reached: BTreeSet<WorkflowState> = TRANSITIONS
            .iter()
            .filter(|tr| tr.from.is_none())
            .map(|tr| tr.to)
            .collect();
        loop {
            let before = reached.len();
            for tr in all_transitions() {
                if tr.from.is_some_and(|f| reached.contains(&f)) {
                    reached.insert(tr.to);
                }
            }
            if reached.len() == before {
                break;
            }
        }
        let all: BTreeSet<WorkflowState> = WorkflowState::ALL.into_iter().collect();
        assert_eq!(reached, all);
    }

    #[test]
    fn every_open_state_has_an_exit() {
        for state in WorkflowState::ALL.into_iter().filter(|s| !s.is_terminal()) {
            assert!(
                all_transitions().any(|tr| tr.from == Some(state)),
                "{} has no sanctioned exit",
                state.as_str()
            );
        }
    }

    #[test]
    fn completed_and_terminated_never_reopen() {
        for to in [Running, Paused] {
            assert!(!is_sanctioned(Completed, to));
            assert!(!is_sanctioned(Terminated, to));
        }
        // Only a DLQ redrive reopens a run, and only a FAILED one.
        let reopeners: Vec<&Transition> = all_transitions()
            .filter(|tr| tr.from.is_some_and(WorkflowState::is_terminal) && !tr.to.is_terminal())
            .collect();
        assert_eq!(reopeners.len(), 1, "{reopeners:?}");
        assert_eq!(reopeners[0].from, Some(Failed));
        assert_eq!(reopeners[0].writer, "reactivate_failed_execution");
    }

    /// Every pair of states, so the check is exhaustive (issue #1819).
    #[test]
    fn migration_states_are_one_way() {
        for from in WorkflowState::ALL {
            for to in WorkflowState::ALL {
                assert!(!is_sanctioned(Migrated, to), "MIGRATED -> {to:?}");
                assert!(!is_sanctioned(from, Migrating), "{from:?} -> MIGRATING");
                assert!(!is_sanctioned(from, from), "{from:?} -> itself");
            }
        }
    }
}

/// CLAUDE.md names exactly two writers of stored `harvest_events.event_data`.
/// This guard fails when a third source file writes that column in place.
#[cfg(test)]
mod append_only_guard {
    use std::path::PathBuf;

    /// The sanctioned writers: PII erasure and codec key re-encryption.
    const SANCTIONED: &[&str] = &["erase.rs", "codec_rotation.rs"];

    /// A Diesel `.set` on the column, or a raw SQL `SET event_data`.
    const WRITE_MARKERS: &[&str] = &[
        "harvest_events::event_data.eq(",
        "UPDATE harvest_events SET event_data",
    ];

    #[test]
    fn only_the_two_sanctioned_files_write_stored_event_data() {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)
                .expect("read src/")
                .filter_map(Result::ok)
            {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                if SANCTIONED.contains(&name.as_str()) || name == "lifecycle.rs" {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("read source");
                for (n, line) in text.lines().enumerate() {
                    let code = line.trim_start();
                    if code.starts_with("//") {
                        continue;
                    }
                    if WRITE_MARKERS.iter().any(|m| code.contains(m)) {
                        offenders.push(format!("{}:{}", path.display(), n + 1));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "a new in-place writer of harvest_events.event_data; CLAUDE.md allows only \
             erase.rs and codec_rotation.rs:\n  {}",
            offenders.join("\n  ")
        );
    }
}
