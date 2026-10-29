//! Issue #2390: operator overrides of schedules on the `SQLite` backend.
//!
//! The shared cases in `schedule_override_fixture.rs` run here on `SQLite`,
//! and in the core crate on Postgres. Each `Step::Code` reopens the file and
//! upserts the code value, as an app does on every start. So a value that
//! survives the step survives a restart.
//!
//! The other tests cover the fields that this backend cannot honor: each is
//! refused by name, and nothing is written.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::path::Path;
use std::time::Duration;

use autumn_harvest::policy::{OverlapPolicy, Schedule};
use autumn_harvest::prelude::*;
use autumn_harvest::schedule_patch::WorkflowSchedulePatch;
use autumn_harvest_sqlite::{SqliteError, SqliteRuntime};

use super::schedule_override_fixture::{Expect, SCHEDULED_WORKFLOW, Step, cases, code};

#[workflow]
async fn override_parity_flow(_ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    Ok(())
}

fn open(path: &Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).expect("open");
    rt.register_workflow(&override_parity_flow_info());
    rt
}

/// The effective value of the schedule, as the fire path reads it.
fn observed(rt: &SqliteRuntime) -> Expect {
    let schedules = rt.list_schedules().expect("list");
    let summary = schedules
        .iter()
        .find(|s| s.workflow_name == SCHEDULED_WORKFLOW)
        .expect("the schedule is stored");
    let Schedule::Interval(period) = summary.schedule else {
        panic!("the cases use interval cadences: {:?}", summary.schedule);
    };
    Expect::Effective {
        interval_secs: period.as_secs(),
        max_active_runs: summary.max_active_runs,
    }
}

#[test]
fn the_shared_override_cases_hold_on_sqlite() {
    for case in cases() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("harvest.db");
        let mut rt = open(&path);
        let mut last = None;
        for (index, (step, expect)) in case.steps.into_iter().enumerate() {
            let result = match step {
                Step::Code(code) => {
                    drop(rt);
                    rt = open(&path);
                    rt.upsert_schedule(&code).map(drop)
                }
                Step::Patch(fields) => rt
                    .set_schedule_override(SCHEDULED_WORKFLOW, &fields, "ops", Some("incident"))
                    .map(drop),
                Step::Clear => rt.clear_schedule_override(SCHEDULED_WORKFLOW).map(drop),
            };
            match expect {
                Expect::Refused => {
                    assert!(
                        result.is_err(),
                        "{} step {index}: must be refused",
                        case.name
                    );
                    assert_eq!(
                        Some(observed(&rt)),
                        last,
                        "{} step {index}: a refused step writes nothing",
                        case.name
                    );
                }
                Expect::Effective { .. } => {
                    result.unwrap_or_else(|e| panic!("{} step {index}: {e}", case.name));
                    assert_eq!(observed(&rt), expect, "{} step {index}", case.name);
                    last = Some(expect);
                }
            }
        }
    }
}

#[test]
fn an_override_records_its_actor_and_reason() {
    let mut rt = SqliteRuntime::open_in_memory().expect("open");
    rt.register_workflow(&override_parity_flow_info());
    rt.upsert_schedule(&code(3600, 1)).expect("upsert");
    assert!(
        rt.schedule_override(SCHEDULED_WORKFLOW)
            .expect("read")
            .is_none()
    );

    let patch = WorkflowSchedulePatch {
        max_active_runs: Some(2),
        overlap_policy: Some(OverlapPolicy::AllowAll),
        ..WorkflowSchedulePatch::default()
    };
    rt.set_schedule_override(SCHEDULED_WORKFLOW, &patch, "oncall", Some("backfill"))
        .expect("set");
    let stored = rt
        .schedule_override(SCHEDULED_WORKFLOW)
        .expect("read")
        .expect("an override");
    assert_eq!(stored.updated_by, "oncall");
    assert_eq!(stored.reason.as_deref(), Some("backfill"));
    assert_eq!(stored.fields.max_active_runs, Some(2));
    assert_eq!(stored.fields.overlap_policy, Some(OverlapPolicy::AllowAll));

    assert!(
        rt.clear_schedule_override(SCHEDULED_WORKFLOW)
            .expect("clear")
    );
    assert!(
        rt.schedule_override(SCHEDULED_WORKFLOW)
            .expect("read")
            .is_none()
    );
    assert!(
        !rt.clear_schedule_override(SCHEDULED_WORKFLOW)
            .expect("clear"),
        "a second clear finds no override"
    );
}

/// Issue #2390 AC3: each field that this backend cannot honor is refused by
/// name, and the refusal writes nothing.
#[test]
fn an_unsupported_override_field_is_refused_by_name() {
    let mut rt = SqliteRuntime::open_in_memory().expect("open");
    rt.register_workflow(&override_parity_flow_info());
    rt.upsert_schedule(&code(3600, 1)).expect("upsert");

    let refused = [
        (
            "jitter",
            WorkflowSchedulePatch {
                jitter: Some(Duration::from_secs(10)),
                ..WorkflowSchedulePatch::default()
            },
        ),
        (
            "catchup",
            WorkflowSchedulePatch {
                catchup: Some(true),
                ..WorkflowSchedulePatch::default()
            },
        ),
        (
            "overlap_policy",
            WorkflowSchedulePatch {
                overlap_policy: Some(OverlapPolicy::BufferAll),
                ..WorkflowSchedulePatch::default()
            },
        ),
        (
            "queue_name",
            WorkflowSchedulePatch {
                queue_name: Some("other".to_string()),
                ..WorkflowSchedulePatch::default()
            },
        ),
        (
            "input",
            WorkflowSchedulePatch {
                input: Some(serde_json::json!(1)),
                ..WorkflowSchedulePatch::default()
            },
        ),
        (
            "max_runs",
            WorkflowSchedulePatch {
                max_runs: Some(Some(3)),
                ..WorkflowSchedulePatch::default()
            },
        ),
    ];
    for (field, patch) in refused {
        let error = rt
            .set_schedule_override(SCHEDULED_WORKFLOW, &patch, "ops", None)
            .expect_err(field);
        let SqliteError::Unsupported(message) = &error else {
            panic!("{field}: expected Unsupported, got {error:?}");
        };
        assert!(
            message.starts_with(&format!("WorkflowSchedulePatch.{field} ")),
            "{field}: {message}"
        );
        assert!(
            rt.schedule_override(SCHEDULED_WORKFLOW)
                .expect("read")
                .is_none()
        );
    }
}

#[test]
fn an_override_of_an_unknown_schedule_is_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().expect("open");
    let patch = WorkflowSchedulePatch {
        max_active_runs: Some(2),
        ..WorkflowSchedulePatch::default()
    };
    let error = rt
        .set_schedule_override("absent", &patch, "ops", None)
        .expect_err("no schedule");
    assert!(
        matches!(error, SqliteError::ScheduleNotFound(_)),
        "{error:?}"
    );
}

#[test]
fn deleting_a_schedule_drops_its_override() {
    let mut rt = SqliteRuntime::open_in_memory().expect("open");
    rt.register_workflow(&override_parity_flow_info());
    rt.upsert_schedule(&code(3600, 1)).expect("upsert");
    let patch = WorkflowSchedulePatch {
        max_active_runs: Some(2),
        ..WorkflowSchedulePatch::default()
    };
    rt.set_schedule_override(SCHEDULED_WORKFLOW, &patch, "ops", None)
        .expect("set");

    rt.delete_schedule(SCHEDULED_WORKFLOW).expect("delete");
    rt.upsert_schedule(&code(3600, 1)).expect("upsert");
    assert!(
        rt.schedule_override(SCHEDULED_WORKFLOW)
            .expect("read")
            .is_none()
    );
    assert_eq!(
        observed(&rt),
        Expect::Effective {
            interval_secs: 3600,
            max_active_runs: 1
        }
    );
}
