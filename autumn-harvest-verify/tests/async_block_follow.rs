//! The `async` block a closure returns (issue #2010).
//!
//! The analysis follows that block when a bodyless callee, such as
//! `Saga::step`, polls it. These cases pin two sides of that rule. A clock
//! read in a followed block is found. A case the analysis cannot follow in
//! full stays `unknown`, and never becomes a false `proven-deterministic`.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::analysis;
use autumn_harvest_verify::entry;
use autumn_harvest_verify::mir;
use autumn_harvest_verify::model::Model;
use autumn_harvest_verify::resolve::{Program, SourceRoots};
use autumn_harvest_verify::verdict::{Verdict, WorkflowVerdict};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("async_block_follow")
}

fn verdicts() -> Vec<WorkflowVerdict> {
    let dir = fixture_dir();
    let path = dir.join("flow.mir");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let docs = vec![mir::parse("flow", "flow.mir", &text)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots { roots: vec![dir] }).expect("build");
    let model = Model::builtin().expect("the embedded model must parse");
    analysis::analyze(&program, &model, &entries)
}

fn verdict<'a>(all: &'a [WorkflowVerdict], name: &str) -> &'a Verdict {
    &all.iter()
        .find(|v| v.workflow == name || v.workflow.ends_with(&format!("::{name}")))
        .unwrap_or_else(|| panic!("no verdict for {name}"))
        .verdict
}

#[test]
fn a_clock_read_in_a_saga_step_block_is_found() {
    let all = verdicts();
    for name in [
        "wf_saga_clock",
        "wf_saga_async_fn",
        "wf_step_named_future",
        "wf_step_mut_ref_future",
    ] {
        assert!(
            matches!(verdict(&all, name), Verdict::NondeterminismFound { .. }),
            "{name}: {:?}",
            verdict(&all, name)
        );
    }
}

#[test]
fn a_case_the_analysis_cannot_follow_is_never_proven() {
    let all = verdicts();
    for name in [
        "wf_external",
        "wf_shared_span",
        "wf_block_write",
        "wf_step_write",
        "wf_step_write_tuple",
        "wf_step_write_struct",
        "wf_step_write_cell",
        "wf_step_write_arc",
        "wf_step_named_write_future",
        "wf_step_move_mut",
        "wf_step_boxed_dyn",
    ] {
        assert!(
            !matches!(verdict(&all, name), Verdict::ProvenDeterministic),
            "{name} must not be proven: {:?}",
            verdict(&all, name)
        );
    }
}

#[test]
fn a_future_built_in_another_crate_is_not_an_unresolved_callback() {
    let all = verdicts();
    let Verdict::Unknown { boundaries } = verdict(&all, "wf_foreign_block") else {
        panic!("the stub method is not in the model, so the verdict is unknown");
    };
    assert!(
        boundaries
            .iter()
            .all(|b| b.kind.name() != "unresolved-callback"),
        "{boundaries:?}"
    );
}
