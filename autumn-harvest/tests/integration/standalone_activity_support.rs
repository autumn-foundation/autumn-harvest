//! Shared facts for the standalone-activity measurement (issue #1987).
//!
//! The perf harness asserts these counts against a live database. The docs
//! guard asserts that the published page quotes the same counts. One source
//! keeps the page, the harness and the decision in step.
//!
//! Pure: no database, no async.

// Only the `db`-gated harness reads some of these.
#![cfg_attr(not(feature = "db"), allow(dead_code))]

/// The published measurement page.
pub(crate) const PERF_DOC: &str = "docs/performance-standalone-activity-overhead.md";

/// The decision record.
pub(crate) const ADR_DOC: &str = "docs/adr/0006-standalone-activity.md";

/// The guide page that shows the one-step pattern.
pub(crate) const GUIDE_DOC: &str = "docs/getting-started/activities.md";

/// The guide heading for the pattern.
pub(crate) const GUIDE_HEADING: &str = "## Run one durable job";

/// Where the evidence capture writes its raw output.
pub(crate) const ARTIFACT_DIR: &str = "docs/perf-artifacts/standalone-activity-overhead";

/// Jobs per arm in the evidence capture.
pub(crate) const JOBS_PER_ARM: usize = 50;

/// The decision line from `DESIGN-1987.md` §0.4.
///
/// Arm B at or below this multiple of arm C, on both deciders, means
/// "document the pattern". Above it on either means "build".
pub(crate) const BUILD_LINE: f64 = 2.0;

/// The ADR text for each verdict.
pub(crate) const VERDICT_DOCUMENT: &str = "Document the one-step-workflow pattern";
pub(crate) const VERDICT_BUILD: &str = "Build a standalone-activity API";

/// One measured shape and its exact per-job structure.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Arm {
    /// The label in the published table.
    pub(crate) label: &'static str,
    /// `harvest_events` rows per job.
    pub(crate) events: i64,
    /// `harvest_task_queue` rows per job.
    pub(crate) task_rows: i64,
    /// Task claims per job, read as `SUM(attempt)` over its task rows.
    pub(crate) claims: i64,
}

/// One-step workflow that runs a regular activity.
pub(crate) const ARM_A: Arm = Arm {
    label: "A",
    events: 5,
    task_rows: 2,
    claims: 3,
};

/// One-step workflow that runs a local activity.
pub(crate) const ARM_B: Arm = Arm {
    label: "B",
    events: 4,
    task_rows: 1,
    claims: 1,
};

/// The bare floor: one task row with no workflow.
pub(crate) const ARM_C: Arm = Arm {
    label: "C",
    events: 0,
    task_rows: 1,
    claims: 1,
};

/// Every arm, in table order.
pub(crate) const ARMS: [Arm; 3] = [ARM_A, ARM_B, ARM_C];
