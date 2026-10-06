//! Safety invariants that the simulator checks after each step.
//!
//! The names and meanings follow `formal/tla/ActivityClaim.tla`. Each claim
//! gets a ghost sequence number. The store never sees it. An invariant
//! compares sequence numbers, not `(worker_id, attempt)`, so a reused pair
//! cannot hide a stale write.
//!
//! One difference: the TLA+ spec does not count a start as an owner write.
//! This harness does, so `OwnerWritesByCurrentClaim` is stricter here.

use std::fmt;

use super::store::{Claim, Row, TaskState, WriteOutcome};

/// A safety property of the activity claim protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Invariant {
    /// At most one terminal write takes effect per task.
    AtMostOneTerminal,
    /// A completed row has exactly one terminal write.
    TerminalStateHasOneEvent,
    /// A terminal write takes effect only while its claim is current.
    TerminalByCurrentClaim,
    /// Every owner write takes effect only while its claim is current.
    OwnerWritesByCurrentClaim,
    /// A heartbeat takes effect only while its claim is current.
    HeartbeatByCurrentClaim,
    /// No two live claims share `(task, worker_id, attempt)`.
    ClaimIdsAreUnique,
}

impl Invariant {
    /// Every invariant.
    pub const ALL: [Self; 6] = [
        Self::AtMostOneTerminal,
        Self::TerminalStateHasOneEvent,
        Self::TerminalByCurrentClaim,
        Self::OwnerWritesByCurrentClaim,
        Self::HeartbeatByCurrentClaim,
        Self::ClaimIdsAreUnique,
    ];

    /// The name in `formal/tla/ActivityClaim.tla`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AtMostOneTerminal => "AtMostOneTerminal",
            Self::TerminalStateHasOneEvent => "TerminalStateHasOneEvent",
            Self::TerminalByCurrentClaim => "TerminalByCurrentClaim",
            Self::OwnerWritesByCurrentClaim => "OwnerWritesByCurrentClaim",
            Self::HeartbeatByCurrentClaim => "HeartbeatByCurrentClaim",
            Self::ClaimIdsAreUnique => "ClaimIdsAreUnique",
        }
    }
}

impl Invariant {
    /// Parse a name from [`Invariant::name`].
    ///
    /// # Errors
    ///
    /// Returns a message that names the accepted values.
    pub fn parse(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|invariant| invariant.name() == name)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.iter().map(|i| i.name()).collect();
                format!(
                    "unknown invariant {name:?}: use one of {}",
                    names.join(", ")
                )
            })
    }
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A failed invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The invariant that failed.
    pub invariant: Invariant,
    /// The step index where it failed.
    pub step: usize,
    /// What happened, in one line.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at step {}: {}",
            self.invariant, self.step, self.detail
        )
    }
}

/// The kind of an owner write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    /// The start fence.
    Start,
    /// A task heartbeat.
    Heartbeat,
    /// The release of a claim that did not start.
    Release,
    /// The terminal write.
    Complete,
}

/// A claim that a worker still acts on, with its ghost sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Live<'a> {
    pub claim: &'a Claim,
    pub seq: u64,
}

/// The ghost state: which claim each row holds, and the terminal writes.
#[derive(Debug, Clone)]
pub struct Ghost {
    holders: Vec<Option<u64>>,
    terminals: Vec<Vec<u64>>,
}

impl Ghost {
    /// Ghost state for `tasks` rows.
    pub fn new(tasks: usize) -> Self {
        Self {
            holders: vec![None; tasks],
            terminals: vec![Vec::new(); tasks],
        }
    }

    /// Claim `seq` now holds `task`.
    pub fn claimed(&mut self, task: usize, seq: u64) {
        self.holders[task] = Some(seq);
    }

    /// The reclaimer moved `task` back to `PENDING`.
    pub fn requeued(&mut self, task: usize) {
        self.holders[task] = None;
    }

    /// Record an owner write by claim `seq` and return what it broke.
    pub fn wrote(
        &mut self,
        kind: WriteKind,
        task: usize,
        seq: u64,
        outcome: WriteOutcome,
    ) -> Vec<(Invariant, String)> {
        let mut found = Vec::new();
        if outcome == WriteOutcome::LeaseLost {
            return found;
        }
        let holder = self.holders[task];
        if holder != Some(seq) {
            let detail = format!(
                "{kind:?} by claim #{seq} took effect on t{task}, held by {}",
                holder.map_or_else(|| "no claim".to_string(), |h| format!("claim #{h}"))
            );
            found.push((Invariant::OwnerWritesByCurrentClaim, detail.clone()));
            match kind {
                WriteKind::Start | WriteKind::Release => {}
                WriteKind::Heartbeat => found.push((Invariant::HeartbeatByCurrentClaim, detail)),
                WriteKind::Complete => found.push((Invariant::TerminalByCurrentClaim, detail)),
            }
        }
        if kind == WriteKind::Release {
            self.holders[task] = None;
        }
        if kind == WriteKind::Complete {
            self.holders[task] = None;
            self.terminals[task].push(seq);
            if self.terminals[task].len() > 1 {
                let detail = format!("t{task} has terminal writes by {:?}", self.terminals[task]);
                found.push((Invariant::AtMostOneTerminal, detail));
            }
        }
        found
    }

    /// Check that each completed row has exactly one terminal write.
    pub fn terminal_states(&self, rows: &[Row]) -> Option<(Invariant, String)> {
        rows.iter().enumerate().find_map(|(task, row)| {
            let writes = self.terminals[task].len();
            (row.state == TaskState::Completed && writes != 1).then(|| {
                let detail = format!("completed t{task} has {writes} terminal writes");
                (Invariant::TerminalStateHasOneEvent, detail)
            })
        })
    }

    /// Check that no two live claims share a fencing token.
    pub fn unique_ids(live: &[Live<'_>]) -> Option<(Invariant, String)> {
        live.iter().enumerate().find_map(|(i, a)| {
            live[i + 1..]
                .iter()
                .find(|b| b.claim == a.claim && b.seq != a.seq)
                .map(|b| {
                    let detail = format!(
                        "claims #{} and #{} share t{} {} a{}",
                        a.seq, b.seq, a.claim.task, a.claim.worker, a.claim.attempt
                    );
                    (Invariant::ClaimIdsAreUnique, detail)
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(found: &[(Invariant, String)]) -> Vec<Invariant> {
        found.iter().map(|(invariant, _)| *invariant).collect()
    }

    #[test]
    fn a_current_owner_breaks_nothing() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 1);
        for kind in [WriteKind::Start, WriteKind::Heartbeat, WriteKind::Complete] {
            assert_eq!(ghost.wrote(kind, 0, 1, WriteOutcome::Applied), Vec::new());
        }
    }

    #[test]
    fn a_lost_lease_breaks_nothing() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::LeaseLost);
        assert_eq!(found, Vec::new());
    }

    #[test]
    fn a_stale_terminal_write_breaks_terminal_and_owner_invariants() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 1);
        ghost.requeued(0);
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::Applied);
        assert_eq!(
            names(&found),
            [
                Invariant::OwnerWritesByCurrentClaim,
                Invariant::TerminalByCurrentClaim
            ]
        );
    }

    #[test]
    fn a_stale_heartbeat_breaks_the_heartbeat_invariant() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 1);
        ghost.requeued(0);
        let found = ghost.wrote(WriteKind::Heartbeat, 0, 1, WriteOutcome::Applied);
        assert_eq!(
            names(&found),
            [
                Invariant::OwnerWritesByCurrentClaim,
                Invariant::HeartbeatByCurrentClaim
            ]
        );
    }

    #[test]
    fn a_second_terminal_write_breaks_at_most_one_terminal() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 1);
        let first = ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::Applied);
        assert_eq!(first, Vec::new());
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Complete, 0, 2, WriteOutcome::Applied);
        assert_eq!(names(&found), [Invariant::AtMostOneTerminal]);
    }

    #[test]
    fn a_completed_row_needs_one_terminal_write() {
        let mut ghost = Ghost::new(1);
        let mut row = Row::pending();
        row.state = TaskState::Completed;
        let found = ghost.terminal_states(std::slice::from_ref(&row));
        assert_eq!(
            found.map(|(i, _)| i),
            Some(Invariant::TerminalStateHasOneEvent)
        );
        ghost.claimed(0, 1);
        let _ = ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::Applied);
        assert_eq!(ghost.terminal_states(&[row]), None);
    }

    #[test]
    fn a_stale_release_breaks_the_owner_invariant_only() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 1);
        ghost.requeued(0);
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Release, 0, 1, WriteOutcome::Applied);
        assert_eq!(names(&found), [Invariant::OwnerWritesByCurrentClaim]);
    }

    #[test]
    fn names_round_trip() {
        for invariant in Invariant::ALL {
            assert_eq!(Invariant::parse(invariant.name()), Ok(invariant));
        }
        assert!(Invariant::parse("Nothing").is_err());
    }

    #[test]
    fn shared_fencing_tokens_are_found() {
        let a = Claim {
            task: 0,
            worker: "w1".to_string(),
            attempt: 1,
        };
        let b = a.clone();
        let other_task = Claim {
            task: 1,
            ..a.clone()
        };
        let ok = [
            Live { claim: &a, seq: 1 },
            Live {
                claim: &other_task,
                seq: 2,
            },
        ];
        assert_eq!(Ghost::unique_ids(&ok), None);
        let bad = [Live { claim: &a, seq: 1 }, Live { claim: &b, seq: 3 }];
        let found = Ghost::unique_ids(&bad).map(|(invariant, _)| invariant);
        assert_eq!(found, Some(Invariant::ClaimIdsAreUnique));
    }
}
