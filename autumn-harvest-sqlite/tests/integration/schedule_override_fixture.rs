//! The schedule-override cases that both backends run (issue #2390).
//!
//! `schedule_overrides.rs` in this crate runs each case on SQLite.
//! `sqlite_schedule_override_parity_tests.rs` in the core crate runs the same
//! file on Postgres. Each backend must reach the same effective value after
//! every step, so a drift in the override rules fails one of the two suites.
//!
//! The four cases are the acceptance criteria of the DAG slice (#2385), on a
//! code-declared workflow schedule: an override wins over code, it survives
//! a restart, a clear restores the code value, and a refused override writes
//! nothing.

use std::time::Duration;

use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::schedule_patch::WorkflowSchedulePatch;

/// The workflow that each case schedules.
pub const SCHEDULED_WORKFLOW: &str = "override_parity_flow";

/// One step of a case.
pub enum Step {
    /// Start, or restart, with this code value of the schedule.
    Code(WorkflowSchedule),
    /// Merge this patch over the stored override.
    Patch(WorkflowSchedulePatch),
    /// Delete the stored override.
    Clear,
}

/// The effective value after a step, or a refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The schedule fires every `interval_secs` with this run limit.
    Effective {
        /// The cadence in seconds.
        interval_secs: u64,
        /// The run limit.
        max_active_runs: u32,
    },
    /// The backend refuses the step and the effective value does not change.
    Refused,
}

/// One case: its name and its steps, each with the value it must leave.
pub struct Case {
    /// The test name that the case stands for.
    pub name: &'static str,
    /// The steps, in order.
    pub steps: Vec<(Step, Expect)>,
}

/// A code value that fires every `secs` seconds with a run limit of `max`.
pub fn code(secs: u64, max: u32) -> WorkflowSchedule {
    let mut schedule = WorkflowSchedule::new(
        SCHEDULED_WORKFLOW,
        Schedule::Interval(Duration::from_secs(secs)),
    );
    schedule.max_active_runs = max;
    schedule
}

fn cadence(secs: u64) -> WorkflowSchedulePatch {
    WorkflowSchedulePatch {
        schedule: Some(Schedule::Interval(Duration::from_secs(secs))),
        ..WorkflowSchedulePatch::default()
    }
}

fn run_limit(max: u32) -> WorkflowSchedulePatch {
    WorkflowSchedulePatch {
        max_active_runs: Some(max),
        ..WorkflowSchedulePatch::default()
    }
}

const fn effective(interval_secs: u64, max_active_runs: u32) -> Expect {
    Expect::Effective {
        interval_secs,
        max_active_runs,
    }
}

/// Every case, in a fixed order.
pub fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "an_override_wins_over_code",
            steps: vec![
                (Step::Code(code(3600, 1)), effective(3600, 1)),
                (Step::Patch(cadence(60)), effective(60, 1)),
                (Step::Patch(run_limit(3)), effective(60, 3)),
                // The app declares the same code again, as it does on every start.
                (Step::Code(code(3600, 1)), effective(60, 3)),
            ],
        },
        Case {
            name: "an_override_survives_a_restart_with_new_code",
            steps: vec![
                (Step::Code(code(3600, 1)), effective(3600, 1)),
                (Step::Patch(cadence(60)), effective(60, 1)),
                // New code: the cadence keeps the override, the run limit follows code.
                (Step::Code(code(7200, 4)), effective(60, 4)),
            ],
        },
        Case {
            name: "a_clear_restores_the_code_value",
            steps: vec![
                (Step::Code(code(3600, 1)), effective(3600, 1)),
                (Step::Patch(cadence(60)), effective(60, 1)),
                (Step::Code(code(7200, 2)), effective(60, 2)),
                (Step::Clear, effective(7200, 2)),
                (Step::Code(code(7200, 2)), effective(7200, 2)),
            ],
        },
        Case {
            name: "a_refused_override_writes_nothing",
            steps: vec![
                (Step::Code(code(3600, 1)), effective(3600, 1)),
                (Step::Patch(cadence(60)), effective(60, 1)),
                (Step::Patch(cadence(0)), Expect::Refused),
                (Step::Code(code(3600, 1)), effective(60, 1)),
            ],
        },
    ]
}
