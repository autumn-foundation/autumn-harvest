//! Shared facts for the standalone-activity measurement (issue #1987).
//!
//! The perf harness asserts these counts against a live database. The docs
//! guard asserts that the published page quotes the same counts. One source
//! keeps the page, the harness and the decision in step.
//!
//! Pure: no database, no async.

/// The published measurement page.
pub const PERF_DOC: &str = "docs/performance-standalone-activity-overhead.md";

/// The decision record.
pub const ADR_DOC: &str = "docs/adr/0006-standalone-activity.md";

/// The guide page that shows the one-step pattern.
pub const GUIDE_DOC: &str = "docs/getting-started/activities.md";

/// The guide heading for the pattern.
pub const GUIDE_HEADING: &str = "## Run one durable job";

/// Where the evidence capture writes its raw output.
pub const ARTIFACT_DIR: &str = "docs/perf-artifacts/standalone-activity-overhead";

/// Jobs per arm in the evidence capture.
pub const JOBS_PER_ARM: usize = 50;

/// The decision line from `DESIGN-1987.md` §0.4 and §0.6.
///
/// Arm B at or below this multiple of the floor, on both deciders, means
/// "document the pattern". Above it on either means "build". §0.4 used arm
/// C as the floor. §0.6 uses arm D, and §0.6 decides.
pub const BUILD_LINE: f64 = 2.0;

/// The ADR text for each verdict.
pub const VERDICT_DOCUMENT: &str = "Document the one-step-workflow pattern";
pub const VERDICT_BUILD: &str = "Build a standalone-activity API";

/// One measured shape and its exact per-job structure.
#[derive(Debug, Clone, Copy)]
pub struct Arm {
    /// The label in the published table.
    pub label: &'static str,
    /// `harvest_events` rows per job.
    pub events: i64,
    /// `harvest_task_queue` rows per job.
    pub task_rows: i64,
    /// Task claims per job, read as `SUM(attempt)` over its task rows.
    pub claims: i64,
}

/// One-step workflow that runs a regular activity.
pub const ARM_A: Arm = Arm {
    label: "A",
    events: 5,
    task_rows: 2,
    claims: 3,
};

/// One-step workflow that runs a local activity.
pub const ARM_B: Arm = Arm {
    label: "B",
    events: 4,
    task_rows: 1,
    claims: 1,
};

/// The bare floor: one task row with no workflow.
pub const ARM_C: Arm = Arm {
    label: "C",
    events: 0,
    task_rows: 1,
    claims: 1,
};

/// The realistic floor: a task row, a job record and the handler-start
/// marker, with no events.
pub const ARM_D: Arm = Arm {
    label: "D",
    events: 0,
    task_rows: 1,
    claims: 1,
};

/// Every arm, in table order.
pub const ARMS: [Arm; 4] = [ARM_A, ARM_B, ARM_C, ARM_D];
