//! Structure manifest over two builds of one fixture (issue #1995).
//!
//! `tests/fixtures/upgrade_baseline/flow.rs` and
//! `tests/fixtures/upgrade_candidate/flow.rs` are two builds of the same
//! code. The candidate shifts every span and changes a few bodies. These
//! tests pin what `--emit-structure` writes for each workflow.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::analysis;
use autumn_harvest_verify::entry;
use autumn_harvest_verify::mir;
use autumn_harvest_verify::model::Model;
use autumn_harvest_verify::resolve::{Program, SourceRoots};
use autumn_harvest_verify::structure::{
    self, BodyNode, STRUCTURE_FORMAT, StructureManifest, WorkflowStructure,
};

fn fixture_dir(build: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(build)
}

fn manifest(build: &str) -> StructureManifest {
    let dir = fixture_dir(build);
    let path = dir.join("flow.mir");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let docs = vec![mir::parse("fixture", "flow.mir", &text)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots { roots: vec![dir] }).expect("build");
    let model = Model::builtin().expect("the embedded model must parse");
    let outcome = analysis::analyze_full(&program, &model, &entries);
    structure::manifest(&model.version, "rustc test", outcome.structures)
}

/// The manifest of a fixture made of several crates, as `(crate, file)`.
fn crates_manifest(build: &str, crates: &[(&str, &str)]) -> StructureManifest {
    let dir = fixture_dir(build);
    let docs = crates
        .iter()
        .map(|(name, file)| {
            let path = dir.join(file);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            mir::parse(name, file, &text)
        })
        .collect::<Vec<_>>();
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots { roots: vec![dir] }).expect("build");
    let model = Model::builtin().expect("the embedded model must parse");
    let outcome = analysis::analyze_full(&program, &model, &entries);
    structure::manifest(&model.version, "rustc test", outcome.structures)
}

/// The `upgrade_const` workflow, with one build of `limits` analyzed or none.
fn const_manifest(limits: Option<&str>) -> StructureManifest {
    let mut crates = vec![("flow", "flow.mir")];
    crates.extend(limits.map(|file| ("limits", file)));
    crates_manifest("upgrade_const", &crates)
}

fn workflow<'a>(m: &'a StructureManifest, name: &str) -> &'a WorkflowStructure {
    m.workflows
        .iter()
        .find(|w| w.name == name)
        .unwrap_or_else(|| panic!("no workflow {name}; have {:?}", names(m)))
}

fn names(m: &StructureManifest) -> Vec<&str> {
    m.workflows.iter().map(|w| w.name.as_str()).collect()
}

fn body<'a>(w: &'a WorkflowStructure, suffix: &str) -> &'a BodyNode {
    w.bodies
        .iter()
        .find(|b| b.id.ends_with(suffix))
        .unwrap_or_else(|| {
            let ids: Vec<&str> = w.bodies.iter().map(|b| b.id.as_str()).collect();
            panic!("no body ending in {suffix} in {}; have {ids:#?}", w.name)
        })
}

fn digests(w: &WorkflowStructure) -> Vec<(&str, &str)> {
    w.bodies
        .iter()
        .map(|b| (b.id.as_str(), b.digest.as_str()))
        .collect()
}

#[test]
fn the_manifest_names_its_format_and_every_workflow() {
    let m = manifest("upgrade_baseline");
    assert_eq!(m.format, STRUCTURE_FORMAT);
    assert_eq!(STRUCTURE_FORMAT, "harvest-structure/1");
    let mut have = names(&m);
    have.sort_unstable();
    assert_eq!(
        have,
        [
            "wf_activity_only",
            "wf_assoc",
            "wf_closure",
            "wf_condition",
            "wf_const",
            "wf_literal",
            "wf_loop",
            "wf_match",
            "wf_param_key",
            "wf_root_changed",
            "wf_signal",
            "wf_steps",
            "wf_twin"
        ]
    );
    for w in &m.workflows {
        assert!(
            w.bodies.iter().any(|b| b.id == w.root),
            "{}: the root body is in the graph",
            w.name
        );
    }
}

#[test]
fn the_manifest_round_trips_as_json() {
    let m = manifest("upgrade_baseline");
    let json = serde_json::to_string_pretty(&m).expect("serialize");
    let back: StructureManifest = serde_json::from_str(&json).expect("parse");
    assert_eq!(back, m);
}

#[test]
fn an_unchanged_body_keeps_its_digest_across_a_line_shift() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_signal");
    let b = workflow(&cand, "wf_signal");
    assert_eq!(digests(a), digests(b));
    assert!(
        !a.root.contains(".rs:"),
        "a body id holds no line or column: {}",
        a.root
    );
}

#[test]
fn an_activity_body_change_leaves_the_workflow_graph_unchanged() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_activity_only");
    let b = workflow(&cand, "wf_activity_only");
    assert_eq!(digests(a), digests(b));
    assert!(
        !a.bodies
            .iter()
            .any(|n| n.id.ends_with("charge::{closure#0}")),
        "the activity body is not reachable from the workflow"
    );
}

#[test]
fn a_changed_helper_changes_only_its_own_digest() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_steps");
    let b = workflow(&cand, "wf_steps");
    assert_eq!(body(a, &a.root).digest, body(b, &b.root).digest);
    assert_ne!(
        body(a, "reserve::{closure#0}").digest,
        body(b, "reserve::{closure#0}").digest
    );
    assert_ne!(
        body(a, "ship::{closure#0}").digest,
        body(b, "ship::{closure#0}").digest
    );
}

#[test]
fn a_root_change_changes_the_root_digest() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_root_changed");
    let b = workflow(&cand, "wf_root_changed");
    assert_ne!(body(a, &a.root).digest, body(b, &b.root).digest);
}

#[test]
fn step_keys_come_from_constants_and_info_calls() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_activity_only");
    let root = body(w, &w.root);
    let keys: Vec<(&str, Option<&str>)> = root
        .steps
        .iter()
        .map(|s| (s.kind.as_str(), s.key.as_deref()))
        .collect();
    assert_eq!(
        keys,
        [("activity", Some("charge")), ("timer", Some("settle"))]
    );

    let steps = workflow(&m, "wf_steps");
    let reserve = body(steps, "reserve::{closure#0}");
    assert_eq!(reserve.steps.len(), 1);
    assert_eq!(reserve.steps[0].kind, "activity");
    assert_eq!(reserve.steps[0].key.as_deref(), Some("reserve"));
    let ship = body(steps, "ship::{closure#0}");
    let keys: Vec<(&str, Option<&str>)> = ship
        .steps
        .iter()
        .map(|s| (s.kind.as_str(), s.key.as_deref()))
        .collect();
    assert_eq!(
        keys,
        [("activity", Some("ship")), ("timer", Some("ship_wait"))]
    );
}

#[test]
fn a_key_that_comes_from_a_parameter_is_unknown() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_param_key");
    let dispatch = body(w, "dispatch::{closure#0}");
    assert_eq!(dispatch.steps.len(), 1);
    assert_eq!(dispatch.steps[0].kind, "activity");
    assert_eq!(dispatch.steps[0].key, None);
}

#[test]
fn a_signal_wait_is_a_signal_step() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_signal");
    let root = body(w, &w.root);
    assert_eq!(root.steps.len(), 1);
    assert_eq!(root.steps[0].kind, "signal");
    assert_eq!(root.steps[0].key.as_deref(), Some("go"));
}

#[test]
fn a_call_in_a_loop_is_marked() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_loop");
    let root = body(w, &w.root);
    let to_poll: Vec<_> = root
        .calls
        .iter()
        .filter(|c| c.callee.contains("poll_once"))
        .collect();
    assert!(!to_poll.is_empty(), "the root calls poll_once");
    assert!(
        to_poll.iter().any(|c| c.in_loop && !c.resume),
        "the call that builds the future sits in the loop: {to_poll:#?}"
    );

    let steps = workflow(&m, "wf_steps");
    let root = body(steps, &steps.root);
    assert!(
        root.calls.iter().all(|c| !c.in_loop),
        "no call in wf_steps is in a loop: {:#?}",
        root.calls
    );
}

#[test]
fn an_async_helper_has_one_call_site_and_its_resume_sites() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_steps");
    let root = body(w, &w.root);
    let to_reserve: Vec<_> = root
        .calls
        .iter()
        .filter(|c| c.callee.ends_with("reserve::{closure#0}"))
        .collect();
    let built = to_reserve.iter().filter(|c| !c.resume).count();
    assert_eq!(built, 1, "one call site builds the future: {to_reserve:#?}");
    assert!(
        to_reserve.iter().any(|c| c.resume),
        "the root resumes the reserve coroutine: {to_reserve:#?}"
    );
}

#[test]
fn a_changed_match_value_changes_the_digest() {
    // The parsed `switchInt` keeps no case values, so the digest hashes the
    // raw MIR text.
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_match");
    let b = workflow(&cand, "wf_match");
    assert_ne!(body(a, &a.root).digest, body(b, &b.root).digest);
}

#[test]
fn a_changed_const_item_changes_the_digest_of_its_reader() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_const");
    let b = workflow(&cand, "wf_const");
    assert_ne!(
        body(a, "limited::{closure#0}").digest,
        body(b, "limited::{closure#0}").digest
    );
    assert_eq!(body(a, &a.root).digest, body(b, &b.root).digest);
}

#[test]
fn a_changed_associated_const_changes_the_digest_of_its_reader() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_assoc");
    let b = workflow(&cand, "wf_assoc");
    assert_ne!(
        body(a, "capped::{closure#0}").digest,
        body(b, "capped::{closure#0}").digest
    );
}

#[test]
fn a_changed_span_like_string_literal_changes_the_digest() {
    let base = manifest("upgrade_baseline");
    let cand = manifest("upgrade_candidate");
    let a = workflow(&base, "wf_literal");
    let b = workflow(&cand, "wf_literal");
    assert_ne!(
        body(a, "tagged::{closure#0}").digest,
        body(b, "tagged::{closure#0}").digest
    );
}

#[test]
fn two_bodies_with_one_id_are_a_boundary() {
    // The suffix follows the digest, not the body, so a swap between builds
    // would hide. The collision must force a review.
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_twin");
    assert!(
        w.boundaries
            .iter()
            .any(|b| b.starts_with("ambiguous-body-id: ") && b.contains("::run::")),
        "{:#?}",
        w.boundaries
    );
    let plain = workflow(&m, "wf_steps");
    assert!(
        !plain.boundaries.iter().any(|b| b.starts_with("ambiguous")),
        "{:#?}",
        plain.boundaries
    );
}

#[test]
fn a_closure_argument_counts_as_a_loop() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_closure");
    let root = body(w, &w.root);
    let closures: Vec<_> = root
        .calls
        .iter()
        .filter(|c| c.callee.contains("{closure#") && c.callee != w.root)
        .collect();
    assert!(
        !closures.is_empty(),
        "the root passes a closure: {:#?}",
        root.calls
    );
    assert!(closures.iter().all(|c| c.in_loop), "{closures:#?}");
}

#[test]
fn a_condition_wait_is_a_step_with_no_key() {
    let m = manifest("upgrade_baseline");
    let w = workflow(&m, "wf_condition");
    let wait = body(w, "wait_ready::{closure#0}");
    assert_eq!(wait.steps.len(), 1, "{wait:#?}");
    assert_eq!(wait.steps[0].sink, "await_condition");
    assert_eq!(wait.steps[0].kind, "other");
    assert_eq!(wait.steps[0].key, None);
}

#[test]
fn a_changed_const_of_an_analyzed_crate_changes_the_digest_of_its_reader() {
    // The workflow's own MIR is the same against both builds of `limits`.
    let old = const_manifest(Some("limits_v1.mir"));
    let new = const_manifest(Some("limits_v2.mir"));
    let old = workflow(&old, "wf_dep_const");
    let new = workflow(&new, "wf_dep_const");
    assert_ne!(digests(old), digests(new));
    assert!(
        !old.boundaries
            .iter()
            .any(|b| b.starts_with("external-const")),
        "{:?}",
        old.boundaries
    );
}

#[test]
fn a_const_of_a_crate_outside_the_analysis_is_a_boundary() {
    let m = const_manifest(None);
    let w = workflow(&m, "wf_dep_const");
    assert!(
        w.boundaries
            .iter()
            .any(|b| b == "external-const: limits::ATTEMPTS"),
        "{:?}",
        w.boundaries
    );
    // `u64::MAX` comes from `core`, so it is not a boundary.
    assert!(
        !w.boundaries.iter().any(|b| b.contains("MAX")),
        "{:?}",
        w.boundaries
    );
}

#[test]
fn an_associated_const_is_read_from_each_crate_it_names() {
    // `types` holds an unrelated `MAX`. The impl is in `limits`.
    let manifest = |limits: &str| {
        crates_manifest(
            "upgrade_assoc",
            &[
                ("flow", "flow.mir"),
                ("types", "types.mir"),
                ("limits", limits),
            ],
        )
    };
    let old = manifest("limits_v1.mir");
    let new = manifest("limits_v2.mir");
    let old = workflow(&old, "wf_assoc_const");
    let new = workflow(&new, "wf_assoc_const");
    assert_ne!(digests(old), digests(new));
    assert!(old.boundaries.is_empty(), "{:?}", old.boundaries);
}

#[test]
fn an_associated_const_of_a_crate_outside_the_analysis_is_a_boundary() {
    // Only `types` is analyzed. The impl can be in `limits`.
    let m = crates_manifest(
        "upgrade_assoc",
        &[("flow", "flow.mir"), ("types", "types.mir")],
    );
    let w = workflow(&m, "wf_assoc_const");
    assert!(
        w.boundaries
            .iter()
            .any(|b| b == "external-const: <types::Plan as limits::Limits>::MAX"),
        "{:?}",
        w.boundaries
    );
}

// ── end to end with the core check ─────────────────────────────────────────

mod end_to_end {
    use std::future::Future;
    use std::pin::Pin;

    use autumn_harvest::WorkflowContext;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::HistorySnapshot;
    use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
    use autumn_harvest::upgrade_check::{self, FindingKind, UpgradeCheck, Verdict};
    use chrono::Utc;
    use serde_json::{Value, json};

    type WfFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

    /// The core crate reads the manifest that this crate writes.
    fn core_manifest(build: &str) -> upgrade_check::StructureManifest {
        let json = serde_json::to_string(&super::manifest(build)).expect("serialize");
        upgrade_check::StructureManifest::parse(&json).expect("the core check reads it")
    }

    fn err(e: impl std::fmt::Display) -> String {
        e.to_string()
    }

    /// The `wf_steps` fixture, written against the real context.
    fn wf_steps(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
        Box::pin(async move {
            let v = ctx
                .execute_activity_raw("reserve", json!(1), "default")
                .await
                .map_err(err)?;
            ctx.timer("ship_wait", 60).await.map_err(err)?;
            ctx.execute_activity_raw("ship", v, "default")
                .await
                .map_err(err)
        })
    }

    /// The `wf_activity_only` fixture, written against the real context.
    fn wf_activity_only(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
        Box::pin(async move {
            let v = ctx
                .execute_activity_raw("charge", json!(5), "default")
                .await
                .map_err(err)?;
            ctx.timer("settle", 60).await.map_err(err)?;
            Ok(v)
        })
    }

    /// Activity `first` done, then the timer `wait` started and still open.
    fn waiting(name: &str, first: &str, input: Value, wait: &str) -> HistorySnapshot {
        let id = ActivityExecId::new();
        HistorySnapshot {
            workflow_name: name.into(),
            execution_id: ExecutionId::new(),
            events: vec![
                WorkflowEvent::WorkflowStarted {
                    input: Value::Null,
                    timestamp: Utc::now(),
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                },
                WorkflowEvent::ActivityScheduled {
                    activity_id: id,
                    name: first.into(),
                    input,
                    queue: "default".into(),
                },
                WorkflowEvent::ActivityCompleted {
                    activity_id: id,
                    output: json!(1),
                },
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new(wait),
                    duration_secs: 60,
                },
            ],
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
        }
    }

    fn check() -> UpgradeCheck {
        UpgradeCheck::new()
            .register_fn("wf_steps", wf_steps)
            .register_fn("wf_activity_only", wf_activity_only)
            .with_structure(
                core_manifest("upgrade_baseline"),
                core_manifest("upgrade_candidate"),
            )
    }

    #[tokio::test]
    async fn an_activity_only_change_migrates() {
        let run = check()
            .check_snapshot(waiting("wf_activity_only", "charge", json!(5), "settle"))
            .await;
        assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
    }

    #[tokio::test]
    async fn a_passed_step_migrates_and_an_unreached_step_needs_review() {
        // `reserve` and `ship` both changed. The run passed `reserve` and
        // waits on the timer inside `ship`.
        let run = check()
            .check_snapshot(waiting("wf_steps", "reserve", json!(1), "ship_wait"))
            .await;
        assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
        let kinds: Vec<FindingKind> = run.findings.iter().map(|f| f.kind).collect();
        assert_eq!(kinds, [FindingKind::StepNotPassed], "{run:#?}");
        assert!(
            run.findings[0].detail.contains("ship::{closure#0}"),
            "{run:#?}"
        );
    }
}
