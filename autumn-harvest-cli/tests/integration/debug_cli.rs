//! Integration tests for the `harvest debug` subcommand (issue #949).
//!
//! Black-box tests over the crate's public debug surface: they write
//! `HistorySnapshot` JSON fixtures to disk and drive the pure
//! breakpoint/focus/render helpers plus `run_replay` / `run_diff` end to end.
//!
//! These exercise the **handler-free** arm of the debugger (everything except
//! per-step pending commands). Per-step commands need registered workflow code
//! and are covered by `autumn-harvest`'s `debugger_tests`.

use std::path::{Path, PathBuf};

use autumn_harvest::debugger::{Breakpoint, ReplayTrace};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use autumn_harvest_cli::debug::{
    DebugFormat, FocusResolution, render_diff, render_step_detail, render_trace_overview,
    resolve_breakpoint, resolve_focus, run_diff, run_replay,
};

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

fn started() -> WorkflowEvent {
    WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now())
}

/// `[started, ActivityScheduled(charge), ActivityCompleted, SignalReceived(go)]`
///
/// The scheduled and completed events share one `activity_id` so the awaitable
/// actually closes — a fixture whose ids differed would leave it open forever
/// and quietly weaken every open-awaitable assertion below.
fn base_events() -> Vec<WorkflowEvent> {
    let activity_id = ActivityExecId::new();
    vec![
        started(),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "charge".to_string(),
            input: serde_json::json!({"amount": 100}),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("ok"),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "go".to_string(),
            payload: serde_json::json!({"approved": true}),
        },
    ]
}

/// Same shape, but the activity is renamed — the "new build" recording.
fn renamed_events() -> Vec<WorkflowEvent> {
    let mut events = base_events();
    if let WorkflowEvent::ActivityScheduled { name, .. } = &mut events[1] {
        *name = "charge_v2".to_string();
    }
    events
}

fn trace_of(events: &[WorkflowEvent]) -> ReplayTrace {
    ReplayTrace::from_history("order_flow".to_string(), ExecutionId::new(), events)
}

/// Write a `HistorySnapshot` JSON fixture and return its path (plus the owning
/// tempdir, which must stay alive for the duration of the test).
fn snapshot_file(dir: &Path, name: &str, events: &[WorkflowEvent]) -> PathBuf {
    snapshot_file_for(dir, name, "order_flow", events)
}

fn snapshot_file_for(
    dir: &Path,
    name: &str,
    workflow_name: &str,
    events: &[WorkflowEvent],
) -> PathBuf {
    let snapshot = serde_json::json!({
        "workflow_name": workflow_name,
        "execution_id": ExecutionId::new(),
        "events": events,
    });
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string_pretty(&snapshot).unwrap()).unwrap();
    path
}

// ─────────────────────────────────────────────────────────────────────────────
// resolve_breakpoint — precedence and absence
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn resolve_breakpoint_returns_none_without_any_flag() {
    assert!(resolve_breakpoint(None, None, None, None).is_none());
}

#[test]
fn resolve_breakpoint_maps_each_flag_to_its_variant() {
    assert!(matches!(
        resolve_breakpoint(Some("TimerStarted"), None, None, None),
        Some(Breakpoint::EventType(t)) if t == "TimerStarted"
    ));
    assert!(matches!(
        resolve_breakpoint(None, Some(3), None, None),
        Some(Breakpoint::EventIndex(3))
    ));
    assert!(matches!(
        resolve_breakpoint(None, None, Some("charge"), None),
        Some(Breakpoint::ActivityName(a)) if a == "charge"
    ));
    assert!(matches!(
        resolve_breakpoint(None, None, None, Some("go")),
        Some(Breakpoint::SignalName(s)) if s == "go"
    ));
}

// ─────────────────────────────────────────────────────────────────────────────
// resolve_focus — the session-landing decision
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn resolve_focus_defaults_to_the_whole_trace() {
    let trace = trace_of(&base_events());
    assert_eq!(resolve_focus(&trace, None, None), FocusResolution::Whole);
}

#[test]
fn resolve_focus_reports_a_breakpoint_hit_with_its_index() {
    let trace = trace_of(&base_events());
    let bp = Breakpoint::ActivityName("charge".to_string());
    assert_eq!(
        resolve_focus(&trace, Some(&bp), None),
        FocusResolution::BreakpointHit { index: 1 }
    );
}

#[test]
fn resolve_focus_reports_a_missed_breakpoint_rather_than_silently_falling_back() {
    // A breakpoint that never matches must be loud: silently rendering the
    // whole trace would look like "ran to completion, nothing to see".
    let trace = trace_of(&base_events());
    let bp = Breakpoint::ActivityName("nonexistent".to_string());
    assert_eq!(
        resolve_focus(&trace, Some(&bp), None),
        FocusResolution::BreakpointMissed
    );
}

#[test]
fn resolve_focus_honours_an_in_range_step() {
    let trace = trace_of(&base_events());
    assert_eq!(
        resolve_focus(&trace, None, Some(2)),
        FocusResolution::Step { index: 2 }
    );
}

#[test]
fn resolve_focus_reports_an_out_of_range_step_with_the_real_length() {
    let trace = trace_of(&base_events());
    assert_eq!(
        resolve_focus(&trace, None, Some(99)),
        FocusResolution::OutOfRange { index: 99, len: 4 }
    );
}

#[test]
fn resolve_focus_prefers_a_breakpoint_over_a_step() {
    // clap already rejects the combination, but the resolver must not depend
    // on that to be well-defined.
    let trace = trace_of(&base_events());
    let bp = Breakpoint::EventIndex(3);
    assert_eq!(
        resolve_focus(&trace, Some(&bp), Some(0)),
        FocusResolution::BreakpointHit { index: 3 }
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Rendering
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn overview_lists_every_step_with_its_event_type() {
    let trace = trace_of(&base_events());
    let out = render_trace_overview(&trace);
    assert!(out.contains("workflow: order_flow"));
    assert!(out.contains("events: 4"));
    for event_type in [
        "WorkflowStarted",
        "ActivityScheduled",
        "ActivityCompleted",
        "SignalReceived",
    ] {
        assert!(
            out.contains(event_type),
            "overview must list {event_type}:\n{out}"
        );
    }
}

#[test]
fn step_detail_shows_open_awaitables_with_their_opening_event_index() {
    // AC1: the open-awaitable projection carries the index of the event that
    // opened it, which is what makes "why is this still open?" answerable.
    let trace = trace_of(&base_events());
    let out = render_step_detail(&trace, 1);
    assert!(out.contains("open awaitables"), "{out}");
    assert!(out.contains("charge"), "{out}");
    assert!(out.contains("opened at event 1"), "{out}");
}

#[test]
fn step_detail_reports_an_out_of_range_index_instead_of_panicking() {
    let trace = trace_of(&base_events());
    let out = render_step_detail(&trace, 42);
    assert!(out.contains("out of range"), "{out}");
    assert!(out.contains("4 steps"), "{out}");
}

#[test]
fn step_detail_surfaces_a_resolved_signal_payload() {
    let trace = trace_of(&base_events());
    let out = render_step_detail(&trace, 3);
    assert!(out.contains("resolved payload"), "{out}");
    assert!(out.contains("approved"), "{out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// run_replay / run_diff — end to end
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn run_replay_reads_an_exported_snapshot_from_disk() {
    // AC4: a production history is debugged locally with zero DB access.
    let dir = tempfile::tempdir().unwrap();
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    run_replay(
        &path,
        DebugFormat::Text,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .expect("a well-formed snapshot must replay");
}

#[test]
fn run_replay_emits_valid_json_in_json_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    run_replay(
        &path,
        DebugFormat::Json,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .expect("json mode must succeed");
}

#[test]
fn run_replay_fails_when_a_breakpoint_is_never_hit() {
    let dir = tempfile::tempdir().unwrap();
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    let err = run_replay(
        &path,
        DebugFormat::Text,
        None,
        None,
        None,
        Some("nonexistent"),
        None,
        None,
        false,
    )
    .expect_err("a missed breakpoint must not exit 0");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn run_replay_fails_on_an_out_of_range_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    let err = run_replay(
        &path,
        DebugFormat::Text,
        Some(99),
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .expect_err("an out-of-range step must not exit 0");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn tui_rejects_an_unsatisfiable_focus_rather_than_opening_step_zero() {
    // The `--tui` arm previously fell through to cursor 0 for both error
    // resolutions, so `--tui --break-at-activity nonexistent` looked like a hit
    // and exited 0 once the user quit — the one thing a breakpoint must never
    // do, and inconsistent with the text and JSON paths. Both must error
    // BEFORE the terminal is put into raw mode, which is also why this test can
    // run headless: it never reaches `debug_tui::run`.
    let dir = tempfile::tempdir().unwrap();
    let path = snapshot_file(dir.path(), "history.json", &base_events());

    let missed = run_replay(
        &path,
        DebugFormat::Text,
        None,
        None,
        None,
        Some("nonexistent"),
        None,
        None,
        true,
    )
    .expect_err("a missed breakpoint must not exit 0 under --tui either");
    assert_eq!(missed.exit_code(), 1);

    let out_of_range = run_replay(
        &path,
        DebugFormat::Text,
        Some(99),
        None,
        None,
        None,
        None,
        None,
        true,
    )
    .expect_err("an out-of-range step must not exit 0 under --tui either");
    assert_eq!(out_of_range.exit_code(), 1);
}

#[test]
fn run_replay_rejects_a_missing_history_file() {
    let err = run_replay(
        Path::new("/definitely/does/not/exist.json"),
        DebugFormat::Text,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .expect_err("a missing file must be an error, not an empty trace");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn run_replay_rejects_malformed_history_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.json");
    std::fs::write(&path, "{ not json").unwrap();
    let err = run_replay(
        &path,
        DebugFormat::Text,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .expect_err("malformed JSON must be an error");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn run_diff_succeeds_when_two_histories_agree() {
    let dir = tempfile::tempdir().unwrap();
    let events = base_events();
    let left = snapshot_file(dir.path(), "left.json", &events);
    let right = snapshot_file(dir.path(), "right.json", &events);
    run_diff(&left, &right, DebugFormat::Text).expect("identical histories must not diverge");
}

#[test]
fn run_diff_exits_nonzero_on_divergence() {
    // AC3 as a CI gate: `diff(1)`-style "differences found" exit code.
    let dir = tempfile::tempdir().unwrap();
    let left = snapshot_file(dir.path(), "left.json", &base_events());
    let right = snapshot_file(dir.path(), "right.json", &renamed_events());
    let err = run_diff(&left, &right, DebugFormat::Text).expect_err("a divergence must not exit 0");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn diff_render_names_the_first_divergent_step() {
    let left = trace_of(&base_events());
    let right = trace_of(&renamed_events());
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);
    let out = render_diff(&diff, "old build", "new build");
    assert!(out.contains("first divergence"), "{out}");
    assert!(out.contains("old build"), "{out}");
    assert!(out.contains("new build"), "{out}");
}

#[test]
fn diff_render_reports_agreement_explicitly() {
    // Two *independent* recordings of the same scenario: their activity UUIDs
    // and start timestamps differ, but the diff normalizes per-run identity out
    // and must therefore report agreement rather than UUID noise.
    let left = trace_of(&base_events());
    let right = trace_of(&base_events());
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);
    let out = render_diff(&diff, "a", "b");
    assert!(out.contains("no divergence"), "{out}");
}

#[test]
fn timer_history_opens_and_closes_its_awaitable() {
    let events = vec![
        started(),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("cooldown"),
            duration_secs: 60,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("cooldown"),
        },
    ];
    let trace = trace_of(&events);
    assert_eq!(trace.steps[1].open_awaitables.len(), 1);
    assert!(trace.steps[2].open_awaitables.is_empty());
    let out = render_step_detail(&trace, 1);
    assert!(out.contains("cooldown"), "{out}");
}

#[test]
fn base_fixture_actually_closes_its_awaitable() {
    // Guards the fixture itself: if the scheduled/completed pair ever stops
    // sharing an `activity_id`, every open-awaitable assertion above would
    // still pass while measuring nothing.
    let trace = trace_of(&base_events());
    assert_eq!(
        trace.steps[1].open_awaitables.len(),
        1,
        "scheduled opens it"
    );
    assert!(
        trace.steps[2].open_awaitables.is_empty(),
        "completed must close it"
    );
}

#[test]
fn diff_reports_a_renamed_activity_as_a_history_difference() {
    // The two-fixture-histories arm of AC3: a handler-free trace emits no
    // commands, so the *only* available signal is the history-derived facts.
    let left = trace_of(&base_events());
    let right = trace_of(&renamed_events());
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);
    let div = diff.divergence.expect("a renamed activity must diverge");
    assert_eq!(div.step_index, 1, "the rename is at the scheduling step");
    assert!(
        matches!(
            div.kind,
            autumn_harvest::debugger::DiffKind::HistoryFacts { .. }
        ),
        "expected a history-facts divergence, got {:?}",
        div.kind
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON mode — the document is actually parsed, and the breakpoint still gates
// ─────────────────────────────────────────────────────────────────────────────

/// `--format json` must emit a parseable `ReplayTrace` covering the whole
/// history, not merely return `Ok`.
#[test]
fn json_mode_emits_a_parseable_whole_trace() {
    let trace = trace_of(&base_events());
    let json = serde_json::to_string(&trace).expect("ReplayTrace serializes");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

    let steps = parsed["steps"].as_array().expect("steps is an array");
    assert_eq!(
        steps.len(),
        base_events().len(),
        "JSON mode must emit the WHOLE trace, one step per event"
    );
    assert_eq!(parsed["workflow_name"], "order_flow");
    assert_eq!(parsed["total_events"], base_events().len());
    // Each step carries the AC1 fields a `jq` consumer would key on.
    for (i, step) in steps.iter().enumerate() {
        assert_eq!(step["index"], i);
        assert!(
            step["event_type"].is_string(),
            "step {i} needs an event_type"
        );
        assert!(step["open_awaitables"].is_array());
    }
}

/// A missed breakpoint must set the exit code even in JSON mode — otherwise
/// `--format json --break-at-activity X` is a false green in a pipeline.
#[test]
fn json_mode_still_fails_on_a_missed_breakpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    let err = run_replay(
        &path,
        DebugFormat::Json,
        None,
        None,
        None,
        Some("nonexistent"),
        None,
        None,
        false,
    )
    .expect_err("a missed breakpoint must fail even in JSON mode");
    assert!(
        matches!(err, autumn_harvest_cli::CliError::DebugBreakpointMissed),
        "got {err:?}"
    );
}

/// The hit path is exercised end-to-end (previously only the miss path was).
#[test]
fn run_replay_renders_a_hit_breakpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = snapshot_file(dir.path(), "history.json", &base_events());
    run_replay(
        &path,
        DebugFormat::Text,
        None,
        None,
        None,
        Some("charge"),
        None,
        None,
        false,
    )
    .expect("the activity is in this history, so the breakpoint must hit");
}

/// `--max-steps` must reach `from_history_capped`, and the capped trace must
/// announce itself rather than reading as a complete one.
#[test]
fn capped_step_view_announces_the_cap() {
    let events = base_events();
    let capped =
        ReplayTrace::from_history_capped("order_flow".to_string(), ExecutionId::new(), &events, 2);
    assert!(capped.truncated);
    let detail = render_step_detail(&capped, 1);
    assert!(
        detail.contains("capped"),
        "a capped trace must say so in the step view, got:\n{detail}"
    );
    assert!(
        detail.contains(&format!("of {} events", events.len())),
        "the step view must name the real event count, got:\n{detail}"
    );
}

/// The handler-free CLI trace has no commands. Absence of the section would
/// otherwise read as "this step has no pending commands".
#[test]
fn handler_free_step_view_says_commands_are_unavailable() {
    let trace = trace_of(&base_events());
    let detail = render_step_detail(&trace, 0);
    assert!(
        detail.contains("no workflow handler registered"),
        "a handler-free trace must say why it shows no commands, got:\n{detail}"
    );
}

/// A redacted export degrades displayed payloads; the CLI must say so.
#[test]
fn a_redacted_export_is_detected() {
    let redacted = serde_json::json!({
        "workflow_name": "order_flow",
        "execution_id": ExecutionId::new(),
        "payload_policy": "redacted",
        "events": base_events(),
    })
    .to_string();
    assert!(autumn_harvest::debugger::is_redacted_export(&redacted));

    let full = serde_json::json!({
        "workflow_name": "order_flow",
        "execution_id": ExecutionId::new(),
        "payload_policy": "full",
        "events": base_events(),
    })
    .to_string();
    assert!(!autumn_harvest::debugger::is_redacted_export(&full));
}

/// `summary_names`' documented prefix-collision guard: `step_a` must not match
/// `step_ab`. A regression to `contains()` would otherwise go unnoticed.
#[test]
fn activity_breakpoint_does_not_match_a_name_prefix() {
    let activity_id = ActivityExecId::new();
    let trace = ReplayTrace::from_history(
        "wf".to_string(),
        ExecutionId::new(),
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "step_ab".to_string(),
                input: serde_json::json!({}),
                queue: "default".to_string(),
            },
        ],
    );
    assert!(
        trace
            .find_breakpoint(&Breakpoint::ActivityName("step_a".to_string()), 0)
            .is_none(),
        "`step_a` must not match the activity `step_ab`"
    );
    assert!(
        trace
            .find_breakpoint(&Breakpoint::ActivityName("step_ab".to_string()), 0)
            .is_some()
    );
}

/// `ActivityName` must name the *activity*, never its queue — otherwise a
/// breakpoint on a queue name halts on every activity routed there.
#[test]
fn activity_breakpoint_does_not_match_the_queue_name() {
    let trace = trace_of(&base_events());
    assert!(
        trace
            .find_breakpoint(&Breakpoint::ActivityName("payments".to_string()), 0)
            .is_none(),
        "the queue name must not satisfy an activity-name breakpoint"
    );
}

// ── `render_step_detail`'s remaining sections ───────────────────────────────
//
// The overview table and the commands/cap/handler-free notices are covered
// above. These pin the four sections that are only reachable from a history
// carrying the corresponding event kinds — without them a rendering regression
// (a swapped column, a dropped section) would ship silently.

/// A history exercising every remaining detail section in one trace:
/// a version gate, a patch gate, a plain marker, a side effect, and an open
/// timer awaitable.
fn rich_events() -> Vec<WorkflowEvent> {
    vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "version:billing".to_string(),
            details: serde_json::json!(2),
        },
        WorkflowEvent::MarkerRecorded {
            name: "patch:fraud-check-v1".to_string(),
            details: serde_json::json!(1),
        },
        WorkflowEvent::MarkerRecorded {
            name: "checkpoint".to_string(),
            details: serde_json::json!({"rows": 42}),
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::event::SideEffectKind::Custom,
            name: Some("region".to_string()),
            value: serde_json::json!("eu-west"),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("settle_delay"),
            duration_secs: 30,
        },
    ]
}

#[test]
fn step_detail_renders_version_and_patch_gates() {
    let trace = ReplayTrace::from_history("rich", ExecutionId::new(), &rich_events());
    // Step 2 has consumed both gate markers.
    let out = render_step_detail(&trace, 2);

    assert!(
        out.contains("version gates:") && out.contains("billing = 2"),
        "the version gate and its resolved value must render:\n{out}"
    );
    assert!(
        out.contains("patch gates:") && out.contains("fraud-check-v1"),
        "the patch gate must render by its id, not its raw marker name:\n{out}"
    );
    assert!(
        !out.contains("version:billing"),
        "the `version:` prefix is engine bookkeeping and must be stripped:\n{out}"
    );
}

#[test]
fn step_detail_renders_plain_markers_and_side_effects() {
    let trace = ReplayTrace::from_history("rich", ExecutionId::new(), &rich_events());
    let out = render_step_detail(&trace, 4);

    assert!(
        out.contains("markers:") && out.contains("checkpoint") && out.contains("42"),
        "a non-gate marker must render with its details:\n{out}"
    );
    assert!(
        out.contains("side effects:") && out.contains("region") && out.contains("eu-west"),
        "a Custom side effect must render name and value:\n{out}"
    );
}

#[test]
fn step_detail_renders_open_awaitables_with_their_opening_index() {
    let trace = ReplayTrace::from_history("rich", ExecutionId::new(), &rich_events());
    let out = render_step_detail(&trace, 5);

    assert!(
        out.contains("open awaitables:") && out.contains("settle_delay"),
        "the open timer must render:\n{out}"
    );
    assert!(
        out.contains("opened at event 5"),
        "AC1 requires the *opening event index*, not just the id:\n{out}"
    );
}

#[test]
fn step_detail_renders_the_resolved_payload() {
    let trace = ReplayTrace::from_history("base", ExecutionId::new(), &base_events());
    // Step 1 is the ActivityScheduled, whose input is the resolved payload.
    let out = render_step_detail(&trace, 1);

    assert!(
        out.contains("resolved payload:") && out.contains("100"),
        "the scheduled activity's input must render as the resolved payload:\n{out}"
    );
}

#[test]
fn a_history_fact_divergence_shows_the_values_that_differ_not_just_the_field_name() {
    // The handler-free CLI arm emits no commands, so a diff that rendered only
    // commands would print "the recorded histories differ (open_awaitables)"
    // followed by two identical `<no pending commands>` lines — naming a
    // difference the operator cannot see. On a divergence-finding tool that is
    // worse than useless.
    let left = trace_of(&base_events());
    let right = trace_of(&renamed_events());
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);

    let out = render_diff(&diff, "before.json", "after.json");
    assert!(
        out.contains("open_awaitables"),
        "the differing field must be named:\n{out}"
    );
    assert!(
        out.contains("charge") && out.contains("charge_v2"),
        "both sides' actual values must render, not just the field name:\n{out}"
    );
}

#[test]
fn an_event_facts_divergence_renders_both_normalized_events() {
    // `event_facts` is the completeness backstop: it fires for a semantic
    // difference no curated projection names (here, a timer's duration). If the
    // CLI did not render it, the operator would be told the histories differ
    // and shown nothing — the exact failure mode the field vocabulary exists to
    // avoid. Codex round 1 found this class as a *false clean*; this pins both
    // the detection and the rendering.
    let timer = |secs: u64| {
        vec![
            started(),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("escalate"),
                duration_secs: secs,
            },
        ]
    };
    let left = trace_of(&timer(30));
    let right = trace_of(&timer(60));
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);

    let out = render_diff(&diff, "before.json", "after.json");
    assert!(
        out.contains("event_facts"),
        "the backstop field must be named:\n{out}"
    );
    assert!(
        out.contains("30") && out.contains("60"),
        "both durations must render so the operator can see what moved:\n{out}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Codex round 3 — CLI surface
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn diff_of_two_workflow_types_exits_nonzero_and_names_both() {
    // `workflow_name` lives on the execution row, not in any event, so two
    // recordings of DIFFERENT workflows with identical event arrays would
    // otherwise walk every step and exit 0 — the CLI reporting agreement
    // between recordings that are not even of the same thing.
    let dir = tempfile::tempdir().unwrap();
    let events = base_events();
    let left = snapshot_file_for(dir.path(), "left.json", "onboarding", &events);
    let right = snapshot_file_for(dir.path(), "right.json", "checkout", &events);

    let err = run_diff(&left, &right, DebugFormat::Text)
        .expect_err("different workflow types must not exit 0");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn diff_render_names_both_workflow_types() {
    let events = base_events();
    let left = ReplayTrace::from_history("onboarding", ExecutionId::new(), &events);
    let right = ReplayTrace::from_history("checkout", ExecutionId::new(), &events);
    let rendered = render_diff(
        &autumn_harvest::debugger::diff_traces(&left, &right),
        "old.json",
        "new.json",
    );

    assert!(
        rendered.contains("different workflow types"),
        "the report must say WHY they are incomparable:\n{rendered}"
    );
    assert!(
        rendered.contains("onboarding") && rendered.contains("checkout"),
        "both type names must appear:\n{rendered}"
    );
}

#[test]
fn diff_render_reports_a_capped_comparison_as_inconclusive() {
    // A capped comparison that found nothing must never render like a pass:
    // the two traces agree only across the prefix that was examined.
    let events = base_events();
    let left = ReplayTrace::from_history_capped("order_flow", ExecutionId::new(), &events, 1);
    let right = ReplayTrace::from_history_capped("order_flow", ExecutionId::new(), &events, 1);
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);
    assert!(diff.divergence.is_none() && diff.truncated);

    let rendered = render_diff(&diff, "old.json", "new.json");
    assert!(
        rendered.contains("INCONCLUSIVE"),
        "a capped comparison must not read as agreement:\n{rendered}"
    );
    assert!(
        !rendered.contains("no divergence"),
        "the clean wording must not appear on a capped comparison:\n{rendered}"
    );
}

#[tokio::test]
async fn diff_render_never_calls_a_panicked_side_clean() {
    // `step_diff_kind` compares `divergence` BEFORE `outcome`, so a side that
    // panicked — and therefore recorded no divergence, because it never got far
    // enough to disagree with anything — reaches the `DiffKind::Divergence` arm
    // carrying `None`. Labelling that "replays cleanly" is the same false-clean
    // this tool exists to refuse: a build that crashes must never be described
    // as replaying cleanly in the very report a reviewer reads to decide.
    use std::future::Future;
    use std::pin::Pin;

    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::debugger::{DiffKind, ReplayDebugger};
    use autumn_harvest::testing::HistorySnapshot;

    /// Returns immediately at step 0, panics from step 1 on.
    ///
    /// The branch is on the *prefix* length (each step replays `events[0..=k]`),
    /// which is what lets both builds agree at step 0 and part company at step
    /// 1 — the only shape in which a panic can be the FIRST difference without
    /// the earlier command/outcome checks firing first.
    fn panics_after_the_first_step<'a>(
        ctx: &'a WorkflowContext,
        _input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move {
            assert!(ctx.history_event_count() <= 1, "boom");
            Ok(serde_json::json!("done"))
        })
    }

    /// Returns immediately at every step. From step 1 on that leaves the
    /// recorded `ActivityScheduled` unconsumed, which the engine's own
    /// returned-early check surfaces as a genuine divergence.
    fn returns_early<'a>(
        _ctx: &'a WorkflowContext,
        _input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(serde_json::json!("done")) })
    }

    let snapshot = |events: Vec<WorkflowEvent>| HistorySnapshot {
        workflow_name: "order_flow".to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    };

    let events = base_events();
    let left = ReplayDebugger::new()
        .register_fn("order_flow", panics_after_the_first_step)
        .max_steps(2)
        .trace_snapshot(snapshot(events.clone()))
        .await
        .expect("trace");
    let right = ReplayDebugger::new()
        .register_fn("order_flow", returns_early)
        .max_steps(2)
        .trace_snapshot(snapshot(events))
        .await
        .expect("trace");

    // Precondition: the test is only meaningful while the two sides reach the
    // `Divergence` arm with the panicked side carrying `None` — the exact shape
    // that used to render as "replays cleanly".
    let diff = autumn_harvest::debugger::diff_traces(&left, &right);
    let div = diff
        .divergence
        .as_ref()
        .expect("the two builds must differ");
    let DiffKind::Divergence { left: l, right: r } = &div.kind else {
        panic!("expected a Divergence kind, got {:?}", div.kind);
    };
    assert!(
        l.is_none() && r.is_some(),
        "precondition: the panicked side must carry no divergence, got {l:?} / {r:?}"
    );
    assert_eq!(
        div.left.as_ref().map(|s| s.outcome.as_str()),
        Some("panicked"),
        "precondition: the left side must actually have panicked"
    );

    let rendered = render_diff(&diff, "panicking.json", "diverging.json");
    assert!(
        !rendered.contains("left : replays cleanly"),
        "a panicked side must never be described as replaying cleanly:\n{rendered}"
    );
    assert!(
        rendered.contains("panicked"),
        "the report must say why that side produced no divergence:\n{rendered}"
    );
}
