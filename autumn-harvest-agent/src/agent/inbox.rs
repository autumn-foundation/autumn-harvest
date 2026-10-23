//! The inbox kernel of a durable agent (issue #2263).
//!
//! The kernel is pure. It decides what the agent does with one op from the
//! inbox status and the op class. `formal/verus/src/agent_inbox.rs` proves it
//! for every input. The proof also shows that a sequence of ops loses no
//! work op and runs each one once, in arrival order.

/// The status of the inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxStatus {
    /// The agent runs work as it arrives.
    Open,
    /// A person paused the agent. Work waits in the held queue.
    Paused,
    /// A spent budget parked the agent. Work waits in the held queue.
    Parked,
}

/// The class of one op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpClass {
    /// A message from a person or a peer. It must never be lost.
    Work,
    /// A control op, such as pause or resume.
    Control,
    /// A heartbeat tick. A paused agent skips it.
    Tick,
}

/// What the agent does with one op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// Run the op now.
    Run,
    /// Put the op at the end of the held queue.
    Hold,
    /// Apply the control op now.
    Apply,
    /// Drop the tick. A later tick comes on schedule.
    Skip,
}

/// Decide what the agent does with one op.
///
/// A control op always applies, so a paused agent can resume. Work runs only
/// when the inbox is open, and waits otherwise. A tick runs only when the
/// inbox is open.
#[must_use]
pub const fn admit(status: InboxStatus, class: OpClass) -> Admit {
    match class {
        OpClass::Control => Admit::Apply,
        OpClass::Work => match status {
            InboxStatus::Open => Admit::Run,
            InboxStatus::Paused | InboxStatus::Parked => Admit::Hold,
        },
        OpClass::Tick => match status {
            InboxStatus::Open => Admit::Run,
            InboxStatus::Paused | InboxStatus::Parked => Admit::Skip,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUSES: [InboxStatus; 3] =
        [InboxStatus::Open, InboxStatus::Paused, InboxStatus::Parked];

    #[test]
    fn a_control_op_always_applies() {
        for status in STATUSES {
            assert_eq!(admit(status, OpClass::Control), Admit::Apply);
        }
    }

    #[test]
    fn work_runs_when_open_and_waits_otherwise() {
        assert_eq!(admit(InboxStatus::Open, OpClass::Work), Admit::Run);
        assert_eq!(admit(InboxStatus::Paused, OpClass::Work), Admit::Hold);
        assert_eq!(admit(InboxStatus::Parked, OpClass::Work), Admit::Hold);
    }

    #[test]
    fn work_is_never_skipped() {
        for status in STATUSES {
            assert_ne!(admit(status, OpClass::Work), Admit::Skip);
        }
    }

    #[test]
    fn a_tick_runs_only_when_open() {
        assert_eq!(admit(InboxStatus::Open, OpClass::Tick), Admit::Run);
        assert_eq!(admit(InboxStatus::Paused, OpClass::Tick), Admit::Skip);
        assert_eq!(admit(InboxStatus::Parked, OpClass::Tick), Admit::Skip);
    }
}
