//! Safety invariants that the simulator checks after each step.
//!
//! The names and meanings follow `formal/tla/ActivityClaim.tla`. Each claim
//! gets a ghost sequence number. The store never sees it. An invariant
//! compares sequence numbers, not `(worker_id, attempt)`, so a reused pair
//! cannot hide a stale write.

use std::fmt;

use super::store::{Claim, WriteOutcome};

/// A safety property of the activity claim protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Invariant {
    /// At most one terminal write takes effect per task.
    AtMostOneTerminal,
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
    pub const ALL: [Self; 5] = [
        Self::AtMostOneTerminal,
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
            Self::TerminalByCurrentClaim => "TerminalByCurrentClaim",
            Self::OwnerWritesByCurrentClaim => "OwnerWritesByCurrentClaim",
            Self::HeartbeatByCurrentClaim => "HeartbeatByCurrentClaim",
            Self::ClaimIdsAreUnique => "ClaimIdsAreUnique",
        }
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
        write!(f, "{} at step {}: {}", self.invariant, self.step, self.detail)
    }
}

/// The kind of an owner write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteKind {
    /// The start fence.
    Start,
    /// A task heartbeat.
    Heartbeat,
    /// The terminal write.
    Complete,
}

/// A claim that a worker still acts on, with its ghost sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Live<'a> {
    pub claim: &'a Claim,
    pub seq: u64,
}

/// The ghost state: which claim each row holds, and the terminal writes.
#[derive(Debug, Clone)]
pub(crate) struct Ghost {
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
        let _ = (task, seq);
        todo!("issue #1830")
    }

    /// The reclaimer moved `task` back to `PENDING`.
    pub fn requeued(&mut self, task: usize) {
        let _ = task;
        todo!("issue #1830")
    }

    /// Record an owner write by claim `seq` and return what it broke.
    pub fn wrote(
        &mut self,
        kind: WriteKind,
        task: usize,
        seq: u64,
        outcome: WriteOutcome,
    ) -> Vec<(Invariant, String)> {
        let _ = (kind, task, seq, outcome);
        todo!("issue #1830")
    }

    /// Check that no two live claims share a fencing token.
    pub fn unique_ids(live: &[Live<'_>]) -> Option<(Invariant, String)> {
        let _ = live;
        todo!("issue #1830")
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
            assert!(ghost.wrote(kind, 0, 1, WriteOutcome::Applied).is_empty());
        }
    }

    #[test]
    fn a_lost_lease_breaks_nothing() {
        let mut ghost = Ghost::new(1);
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::LeaseLost);
        assert!(found.is_empty());
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
        assert!(ghost.wrote(WriteKind::Complete, 0, 1, WriteOutcome::Applied).is_empty());
        ghost.claimed(0, 2);
        let found = ghost.wrote(WriteKind::Complete, 0, 2, WriteOutcome::Applied);
        assert_eq!(names(&found), [Invariant::AtMostOneTerminal]);
    }

    #[test]
    fn shared_fencing_tokens_are_found() {
        let a = Claim {
            task: 0,
            worker: "w1".to_string(),
            attempt: 1,
        };
        let b = a.clone();
        let other_task = Claim { task: 1, ..a.clone() };
        let ok = [Live { claim: &a, seq: 1 }, Live { claim: &other_task, seq: 2 }];
        assert_eq!(Ghost::unique_ids(&ok), None);
        let bad = [Live { claim: &a, seq: 1 }, Live { claim: &b, seq: 3 }];
        let found = Ghost::unique_ids(&bad).map(|(invariant, _)| invariant);
        assert_eq!(found, Some(Invariant::ClaimIdsAreUnique));
    }
}
