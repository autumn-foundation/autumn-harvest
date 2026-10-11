//! The flow graph and the saga coverage check (issue #2010).
//!
//! `tests/fixtures/saga_graph/flow.rs` holds one workflow per case. A stub
//! `autumn_harvest` crate stands in for the engine, with no MIR of its own.
//! The check reads only the manifest, so each check test first round-trips
//! the manifest through JSON.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::analysis;
use autumn_harvest_verify::entry;
use autumn_harvest_verify::mir;
use autumn_harvest_verify::model::Model;
use autumn_harvest_verify::resolve::{Program, SourceRoots};
use autumn_harvest_verify::saga::{self, SagaReport, SagaVerdict};
use autumn_harvest_verify::structure::{
    self, BodyNode, EdgeLabel, ExitOutcome, FLOW_FORMAT, FlowEdge, FlowEvent, FlowGraph,
    StructureManifest, WorkflowStructure,
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("saga_graph")
}

fn manifest() -> StructureManifest {
    let dir = fixture_dir();
    let path = dir.join("flow.mir");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let docs = vec![mir::parse("flow", "flow.mir", &text)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots { roots: vec![dir] }).expect("build");
    let model = Model::builtin().expect("the embedded model must parse");
    let outcome = analysis::analyze_full(&program, &model, &entries);
    structure::manifest(&model.version, "rustc test", outcome.structures)
}

/// The manifest after a JSON round trip, as `--check-structure` reads it.
fn manifest_from_json() -> StructureManifest {
    let json = serde_json::to_string(&manifest()).expect("the manifest serializes");
    serde_json::from_str(&json).expect("the manifest parses")
}

fn workflow<'a>(m: &'a StructureManifest, name: &str) -> &'a WorkflowStructure {
    m.workflows
        .iter()
        .find(|w| w.name == name)
        .unwrap_or_else(|| panic!("no workflow {name}"))
}

fn root(w: &WorkflowStructure) -> &BodyNode {
    w.bodies
        .iter()
        .find(|b| b.id == w.root)
        .unwrap_or_else(|| panic!("{} has no root body", w.name))
}

fn flow(body: &BodyNode) -> &FlowGraph {
    body.flow
        .as_ref()
        .unwrap_or_else(|| panic!("{} has no flow graph", body.id))
}

fn body<'a>(w: &'a WorkflowStructure, id: &str) -> &'a BodyNode {
    w.bodies
        .iter()
        .find(|b| b.id == id)
        .unwrap_or_else(|| panic!("no body {id} in {}", w.name))
}

/// Each step key in `start` and in every body it calls.
fn subtree_keys(w: &WorkflowStructure, start: &str) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    let mut queue = vec![start.to_string()];
    let mut keys = Vec::new();
    while let Some(id) = queue.pop() {
        if seen.contains(&id) {
            continue;
        }
        seen.push(id.clone());
        let b = body(w, &id);
        keys.extend(b.steps.iter().filter_map(|s| s.key.clone()));
        queue.extend(
            b.calls
                .iter()
                .filter(|c| !c.resume)
                .map(|c| c.callee.clone()),
        );
    }
    keys.sort_unstable();
    keys
}

fn report<'a>(reports: &'a [SagaReport], name: &str) -> &'a SagaReport {
    reports
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("no report for {name}"))
}

fn check() -> Vec<SagaReport> {
    saga::check(&manifest_from_json()).expect("the manifest has flow graphs")
}

// ── the graph ──────────────────────────────────────────────────────────────

#[test]
fn every_body_has_a_flow_graph() {
    let m = manifest();
    assert_eq!(m.flow.as_deref(), Some(FLOW_FORMAT));
    assert_eq!(FLOW_FORMAT, "harvest-flow/1");
    for w in &m.workflows {
        for b in &w.bodies {
            let graph = flow(b);
            let entries = graph
                .nodes
                .iter()
                .filter(|n| matches!(n.event, FlowEvent::Entry))
                .count();
            assert_eq!(entries, 1, "{} in {}: {graph:#?}", b.id, w.name);
        }
    }
}

#[test]
fn a_saga_future_is_not_an_unresolved_callback() {
    let m = manifest();
    for name in ["wf_covered", "wf_gap_after_step", "wf_loop"] {
        assert_eq!(
            workflow(&m, name).boundaries,
            Vec::<String>::new(),
            "{name}"
        );
    }
}

#[test]
fn the_steps_inside_saga_closures_are_in_the_graph() {
    let m = manifest();
    let w = workflow(&m, "wf_covered");
    assert_eq!(
        subtree_keys(w, &w.root),
        ["charge", "refund", "release", "reserve"]
    );
}

#[test]
fn a_saga_step_names_its_forward_and_compensation_bodies() {
    let m = manifest();
    let w = workflow(&m, "wf_covered");
    let graph = flow(root(w));
    let steps: Vec<(&Vec<String>, &Vec<String>, bool)> = graph
        .nodes
        .iter()
        .filter_map(|n| match &n.event {
            FlowEvent::SagaStep {
                forward,
                compensate,
                tracked,
            } => Some((forward, compensate, *tracked)),
            _ => None,
        })
        .collect();
    assert_eq!(steps.len(), 2, "{graph:#?}");
    let mut pairs = Vec::new();
    for (forward, compensate, tracked) in steps {
        assert!(tracked, "a `?` follows each step");
        let [forward] = forward.as_slice() else {
            panic!("one forward body: {forward:?}");
        };
        let [compensate] = compensate.as_slice() else {
            panic!("one compensation body: {compensate:?}");
        };
        pairs.push((subtree_keys(w, forward), subtree_keys(w, compensate)));
    }
    pairs.sort();
    assert_eq!(
        pairs,
        [
            (vec!["charge".to_string()], vec!["refund".to_string()]),
            (vec!["reserve".to_string()], vec!["release".to_string()]),
        ]
    );
}

#[test]
fn a_tracked_saga_step_labels_its_ok_and_err_edges() {
    let m = manifest();
    let graph = flow(root(workflow(&m, "wf_gap_after_step")));
    let step = graph
        .nodes
        .iter()
        .position(|n| matches!(n.event, FlowEvent::SagaStep { .. }))
        .expect("a saga step node");
    let labels: Vec<Option<EdgeLabel>> = graph
        .edges
        .iter()
        .filter(|e| e.from == step)
        .map(|e| e.label)
        .collect();
    assert!(labels.contains(&Some(EdgeLabel::Ok)), "{graph:#?}");
    assert!(labels.contains(&Some(EdgeLabel::Err)), "{graph:#?}");
    assert!(!labels.contains(&None), "{graph:#?}");
    // The `err` edge is the `?` on the step itself.
    for edge in graph
        .edges
        .iter()
        .filter(|e| e.from == step && e.label == Some(EdgeLabel::Err))
    {
        assert!(
            matches!(
                graph.nodes.get(edge.to).map(|n| &n.event),
                Some(FlowEvent::Exit {
                    outcome: ExitOutcome::Err
                })
            ),
            "{graph:#?}"
        );
    }
}

#[test]
fn the_exits_carry_their_outcome() {
    let m = manifest();
    let outcomes = |name: &str| -> Vec<ExitOutcome> {
        let mut out: Vec<ExitOutcome> = flow(root(workflow(&m, name)))
            .nodes
            .iter()
            .filter_map(|n| match n.event {
                FlowEvent::Exit { outcome } => Some(outcome),
                _ => None,
            })
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    };
    // `?` after the step, `return Err(..)`, and `Ok(a)`.
    assert_eq!(
        outcomes("wf_gap_explicit"),
        [ExitOutcome::Ok, ExitOutcome::Err]
    );
    // The tail activity result is neither literal.
    assert!(outcomes("wf_gap_tail").contains(&ExitOutcome::Unknown));
}

#[test]
fn a_signal_handler_and_a_signal_wait_are_in_the_graph() {
    let m = manifest();
    let w = workflow(&m, "wf_handlers");
    let [handler] = w.handlers.as_slice() else {
        panic!("one handler: {:?}", w.handlers);
    };
    assert_eq!(handler.kind, "signal");
    assert_eq!(handler.name.as_deref(), Some("cancel"));
    assert_eq!(handler.method, "register_signal_handler");
    assert_eq!(handler.bodies.len(), 1, "{handler:?}");
    let graph = flow(root(w));
    assert!(
        graph
            .nodes
            .iter()
            .any(|n| matches!(n.event, FlowEvent::Handler { handler: 0 })),
        "{graph:#?}"
    );
    let steps = &root(w).steps;
    let wait = graph
        .nodes
        .iter()
        .find_map(|n| match n.event {
            FlowEvent::Step { step } => steps.get(step),
            _ => None,
        })
        .expect("a step node");
    assert_eq!(
        (wait.kind.as_str(), wait.key.as_deref()),
        ("signal", Some("go"))
    );
}

#[test]
fn an_update_handler_and_its_validator_are_in_the_graph() {
    let m = manifest();
    let w = workflow(&m, "wf_update_handler");
    let [handler] = w.handlers.as_slice() else {
        panic!("one handler: {:?}", w.handlers);
    };
    assert_eq!(handler.kind, "update");
    assert_eq!(handler.name.as_deref(), Some("set_limit"));
    assert_eq!(
        handler.bodies.len(),
        2,
        "a validator and a handler: {handler:?}"
    );
}

#[test]
fn the_manifest_is_deterministic() {
    let m = manifest();
    let again = manifest();
    assert_eq!(m, again, "two runs over one dump give one manifest");
}

#[test]
fn every_resume_target_rejoins_code_that_state_zero_reaches() {
    use autumn_harvest_verify::mir::ast::Terminator;
    use std::collections::{BTreeSet, HashMap};

    let text = std::fs::read_to_string(fixture_dir().join("flow.mir")).expect("read");
    let doc = mir::parse("flow", "flow.mir", &text);
    let mut checked = 0;
    for body in &doc.bodies {
        let coroutine = body
            .params
            .first()
            .is_some_and(|(_, ty)| ty.contains("{async"));
        let Some(Terminator::SwitchInt {
            targets, values, ..
        }) = body.blocks.first().map(|b| &b.terminator)
        else {
            continue;
        };
        if !coroutine {
            continue;
        }
        let blocks: HashMap<&str, _> = body.blocks.iter().map(|b| (b.label.as_str(), b)).collect();
        let reach = |start: &str| {
            let mut seen: BTreeSet<String> = BTreeSet::new();
            let mut queue = vec![start.to_string()];
            while let Some(label) = queue.pop() {
                if !seen.insert(label.clone()) {
                    continue;
                }
                if let Some(block) = blocks.get(label.as_str()) {
                    queue.extend(
                        block
                            .terminator
                            .successors()
                            .into_iter()
                            .map(str::to_string),
                    );
                }
            }
            seen
        };
        let entry = values
            .iter()
            .position(|v| v == "0")
            .and_then(|i| targets.get(i))
            .expect("a state 0 target");
        let from_entry = reach(entry);
        for (value, target) in values.iter().zip(targets) {
            let Some(block) = blocks.get(target.as_str()) else {
                continue;
            };
            // `1` is returned, `2` is panicked, and `otherwise` is
            // unreachable. Each of them asserts or stops.
            let stops = matches!(
                block.terminator,
                Terminator::Assert { .. } | Terminator::Unreachable
            );
            if value == "0" || stops {
                continue;
            }
            assert!(
                block
                    .terminator
                    .successors()
                    .iter()
                    .all(|next| from_entry.contains(*next)),
                "{}: resume state {value} at {target} leaves the state-0 graph",
                body.path
            );
            checked += 1;
        }
    }
    assert!(
        checked > 20,
        "the fixture has many suspend points: {checked}"
    );
}

// ── the check ──────────────────────────────────────────────────────────────

#[test]
fn a_workflow_with_no_saga_is_no_saga() {
    let reports = check();
    for name in ["wf_no_saga", "wf_handlers"] {
        assert_eq!(
            report(&reports, name).verdict,
            SagaVerdict::NoSaga,
            "{name}"
        );
    }
}

/// The root of `name` has an error exit, so a `covered` verdict is not
/// vacuous.
fn has_an_error_exit(name: &str) -> bool {
    let m = manifest();
    flow(root(workflow(&m, name))).nodes.iter().any(|n| {
        matches!(
            n.event,
            FlowEvent::Exit {
                outcome: ExitOutcome::Err
            }
        )
    })
}

#[test]
fn two_saga_steps_are_covered() {
    assert!(has_an_error_exit("wf_covered"));
    let reports = check();
    let r = report(&reports, "wf_covered");
    assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
    assert!(r.gaps.is_empty() && r.unknown.is_empty() && r.notes.is_empty());
}

#[test]
fn an_unwind_before_each_exit_is_covered() {
    assert!(has_an_error_exit("wf_compensated"));
    let m = manifest();
    assert!(
        flow(root(workflow(&m, "wf_compensated")))
            .nodes
            .iter()
            .any(|n| matches!(n.event, FlowEvent::SagaCompensate { tracked: true })),
        "an awaited unwind node"
    );
    let reports = check();
    let r = report(&reports, "wf_compensated");
    assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
}

#[test]
fn a_saga_step_in_a_loop_is_covered() {
    let reports = check();
    let r = report(&reports, "wf_loop");
    assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
}

#[test]
fn a_plain_step_after_the_saga_is_a_gap() {
    let reports = check();
    let r = report(&reports, "wf_gap_after_step");
    assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
    assert_eq!(r.gaps.len(), 1, "one `?` after the step: {r:#?}");
    let gap = r.gaps.first().expect("one gap");
    assert_eq!(gap.outcome, ExitOutcome::Err);
    assert_eq!(gap.body, workflow(&manifest(), "wf_gap_after_step").root);
}

#[test]
fn an_explicit_error_after_the_saga_is_a_gap() {
    let reports = check();
    let r = report(&reports, "wf_gap_explicit");
    assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
}

#[test]
fn a_tail_result_after_the_saga_is_a_gap() {
    let reports = check();
    let r = report(&reports, "wf_gap_tail");
    assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
    assert!(
        r.gaps.iter().any(|g| g.outcome == ExitOutcome::Unknown),
        "{r:#?}"
    );
}

#[test]
fn a_helper_error_after_the_saga_is_a_gap() {
    let reports = check();
    let r = report(&reports, "wf_gap_helper");
    assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
}

#[test]
fn a_compensation_with_no_command_is_a_note() {
    let reports = check();
    let r = report(&reports, "wf_noop_compensation");
    assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
    assert_eq!(r.notes.len(), 1, "{r:#?}");
    assert!(
        r.notes.iter().all(|n| n.starts_with("noop-compensation: ")),
        "{r:#?}"
    );
}

#[test]
fn a_saga_passed_to_a_helper_is_unknown() {
    let reports = check();
    let r = report(&reports, "wf_escapes");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown.iter().any(|u| u.starts_with("saga-escapes: ")),
        "{r:#?}"
    );
}

#[test]
fn a_matched_step_result_is_unknown() {
    let reports = check();
    let r = report(&reports, "wf_untracked");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown
            .iter()
            .any(|u| u.starts_with("saga-result-untracked: ")),
        "{r:#?}"
    );
}

#[test]
fn every_fixture_workflow_gets_its_expected_verdict() {
    use SagaVerdict::{Covered, Gap, NoSaga, Unknown};
    let expected = [
        ("wf_compensated", Covered),
        ("wf_covered", Covered),
        ("wf_escapes", Unknown),
        ("wf_gap_after_step", Gap),
        ("wf_gap_explicit", Gap),
        ("wf_gap_helper", Gap),
        ("wf_gap_tail", Gap),
        ("wf_handlers", NoSaga),
        ("wf_helper_owns", Unknown),
        ("wf_loop", Covered),
        ("wf_loop_new", Unknown),
        ("wf_map_err", Unknown),
        ("wf_no_saga", NoSaga),
        ("wf_noop_compensation", Covered),
        ("wf_not_awaited", Gap),
        ("wf_pre_err", Unknown),
        ("wf_prebuilt", Unknown),
        ("wf_reassign", Unknown),
        ("wf_rebind", Covered),
        ("wf_saga_in_block", Unknown),
        ("wf_unit", Covered),
        ("wf_untracked", Unknown),
        ("wf_unwind_then_err", Covered),
        ("wf_update_handler", NoSaga),
    ];
    let reports = check();
    let mut have: Vec<(&str, SagaVerdict)> = reports
        .iter()
        .map(|r| (r.name.as_str(), r.verdict))
        .collect();
    have.sort_unstable_by_key(|(name, _)| *name);
    assert_eq!(have, expected, "{reports:#?}");
}

#[test]
fn a_local_type_named_saga_is_not_the_engine_saga() {
    let dir = fixture_dir().with_file_name("saga_graph_own");
    let text = std::fs::read_to_string(dir.join("flow.mir")).expect("read");
    let docs = vec![mir::parse("flow", "flow.mir", &text)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots { roots: vec![dir] }).expect("build");
    let model = Model::builtin().expect("the embedded model must parse");
    let outcome = analysis::analyze_full(&program, &model, &entries);
    let m = structure::manifest(&model.version, "rustc test", outcome.structures);
    let w = workflow(&m, "wf_own_saga_type");
    for b in &w.bodies {
        assert!(
            flow(b).nodes.iter().all(|n| !matches!(
                n.event,
                FlowEvent::SagaNew | FlowEvent::SagaStep { .. } | FlowEvent::SagaCompensate { .. }
            )),
            "{}: {:#?}",
            b.id,
            flow(b)
        );
    }
    let reports = saga::check(&m).expect("check");
    assert_eq!(
        report(&reports, "wf_own_saga_type").verdict,
        SagaVerdict::NoSaga
    );
    // An engine saga handed to a method of a local type escapes. A local
    // method named `compensate_all` is not the engine unwind, so it never
    // clears a pending step.
    for name in [
        "wf_own_type_takes_engine_saga",
        "wf_module_lookalike_compensate",
        "wf_root_lookalike_compensate",
    ] {
        let r = report(&reports, name);
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{name}: {r:#?}");
        assert!(
            r.unknown.iter().any(|u| u.starts_with("saga-escapes: ")),
            "{name}: {r:#?}"
        );
    }
}

#[test]
fn a_unit_result_is_an_ok_exit() {
    let reports = check();
    let r = report(&reports, "wf_unit");
    assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
}

#[test]
fn a_moved_saga_binding_stays_in_view() {
    let reports = check();
    for name in ["wf_rebind", "wf_unwind_then_err"] {
        let r = report(&reports, name);
        assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
    }
}

#[test]
fn an_unwind_that_is_never_awaited_is_a_gap() {
    let reports = check();
    let r = report(&reports, "wf_not_awaited");
    assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
}

#[test]
fn a_new_saga_over_a_pending_step_is_unknown() {
    let reports = check();
    for name in ["wf_loop_new", "wf_reassign"] {
        let r = report(&reports, name);
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
        assert!(
            r.unknown.iter().any(|u| u.starts_with("saga-recreated: ")),
            "{r:#?}"
        );
    }
}

#[test]
fn a_helper_that_drops_a_pending_saga_is_unknown() {
    let reports = check();
    let r = report(&reports, "wf_helper_owns");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown
            .iter()
            .any(|u| u.starts_with("saga-dropped-pending: ")),
        "{r:#?}"
    );
}

#[test]
fn a_step_result_that_is_not_the_question_mark_operand_is_untracked() {
    let reports = check();
    for name in ["wf_prebuilt", "wf_pre_err", "wf_map_err"] {
        let r = report(&reports, name);
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{name}: {r:#?}");
        assert!(
            r.unknown
                .iter()
                .any(|u| u.starts_with("saga-result-untracked: ")),
            "{name}: {r:#?}"
        );
    }
}

#[test]
fn a_saga_moved_into_an_async_block_escapes() {
    let reports = check();
    let r = report(&reports, "wf_saga_in_block");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown.iter().any(|u| u.starts_with("saga-escapes: ")),
        "{r:#?}"
    );
}

#[test]
fn a_manifest_without_flow_graphs_is_refused() {
    let mut m = manifest_from_json();
    m.flow = None;
    let err = saga::check(&m).expect_err("no flow format");
    assert!(err.to_string().contains(FLOW_FORMAT), "{err}");
}

#[test]
fn a_boundary_with_no_saga_is_unknown() {
    let mut m = manifest_from_json();
    let w = m
        .workflows
        .iter_mut()
        .find(|w| w.name == "wf_no_saga")
        .expect("wf_no_saga");
    w.boundaries
        .push("unresolved-callback: {closure@x.rs:1:1: 1:2}".to_string());
    let reports = saga::check(&m).expect("check");
    let r = report(&reports, "wf_no_saga");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown.iter().any(|u| u.starts_with("boundary: ")),
        "{r:#?}"
    );
}

#[test]
fn two_sagas_in_one_body_are_unknown() {
    let mut m = manifest_from_json();
    let w = m
        .workflows
        .iter_mut()
        .find(|w| w.name == "wf_covered")
        .expect("wf_covered");
    let root_id = w.root.clone();
    let root = w
        .bodies
        .iter_mut()
        .find(|b| b.id == root_id)
        .expect("the root body");
    let graph = root.flow.as_mut().expect("a flow graph");
    let first = graph
        .nodes
        .iter()
        .find(|n| matches!(n.event, FlowEvent::SagaNew))
        .cloned()
        .expect("a saga-new node");
    // The entry reaches the copy, so the graph stays valid.
    let copy = graph.nodes.len();
    graph.nodes.push(first);
    graph.edges.push(FlowEdge {
        from: 0,
        to: copy,
        label: None,
    });
    let reports = saga::check(&m).expect("check");
    let r = report(&reports, "wf_covered");
    assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
    assert!(
        r.unknown.iter().any(|u| u.starts_with("multiple-sagas: ")),
        "{r:#?}"
    );
}

// ── the write-up ───────────────────────────────────────────────────────────

#[test]
fn the_write_up_states_a_verdict_and_an_unknown_rate() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("docs")
        .join("rnd")
        .join("workflow-graph-spike.md");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    assert!(text.contains("issue #2010"), "names its issue");
    assert!(
        text.contains("**Verdict: go**") || text.contains("**Verdict: no-go**"),
        "states a go / no-go verdict"
    );
    assert!(text.contains("`unknown` rate"), "states the unknown rate");
    assert!(!text.contains("TBD"), "every measurement is filled in");
}
