//! Discovery is never silent: a garbled marker header still yields a workflow,
//! and a run that discovers nothing at all says so.
//!
//! `entry::discover` keys on the `__autumn_workflow_info_*` companion the
//! `#[workflow]` macro emits. If MIR syntax drift makes that *header*
//! unparseable, the companion never reaches `MirDoc::bodies` — and a discovery
//! pass that only reads `bodies` would then report `analyzed 0` and exit `0`,
//! even under `--strict`. That is the one failure mode a verifier must not
//! have: "I found nothing" reading as "there is nothing to find".
//!
//! So the two halves are guarded here. Per workflow, a failed marker header is
//! still an entry, whose verdict is `unknown` with the parse failure named. Per
//! run, zero discovered workflows is a reported warning and, under `--strict`,
//! a failing exit.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use autumn_harvest_verify::driver::BuildRequest;
use autumn_harvest_verify::verdict::{Boundary, BoundaryKind, Verdict, WorkflowVerdict};
use autumn_harvest_verify::{Options, Report, entry, mir};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn read_fixture(name: &str) -> String {
    let path = fixtures_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Corrupt **only** the `__autumn_workflow_info_*` fn headers, by opening a
/// parameter list that is never closed. Every other item is left byte-identical,
/// so what the test exercises is a marker the parser cannot read — not a
/// wholesale broken dump.
fn garble_marker_headers(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        if line.starts_with("fn __autumn_workflow_info_") {
            out.push_str(&line.replacen("()", "(_1: u8", 1));
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

fn marker_names(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("fn __autumn_workflow_info_"))
        .map(|rest| {
            rest.split(['(', ':', ' '])
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

fn write_mir(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).expect("write .mir");
    path
}

fn verify_mir(paths: &[PathBuf], roots: &[PathBuf]) -> Report {
    autumn_harvest_verify::verify(
        &BuildRequest::default(),
        &Options {
            mir_paths: paths.to_vec(),
            source_roots: roots.to_vec(),
            ..Options::default()
        },
    )
    .expect("a garbled marker is a boundary, never a tool error")
}

/// Boundaries from both places they can appear.
fn boundaries(v: &WorkflowVerdict) -> Vec<&Boundary> {
    let inner = match &v.verdict {
        Verdict::Unknown { boundaries } => boundaries.as_slice(),
        _ => &[],
    };
    inner.iter().chain(v.boundaries.iter()).collect()
}

// ── a marker whose header does not parse is still a workflow ────────────────

#[test]
fn the_garbled_marker_headers_really_do_fail_to_parse() {
    // The premise of the test below: if this corruption ever parses cleanly,
    // the pipeline assertions would pass for the wrong reason.
    let garbled = garble_marker_headers(&read_fixture("format_and_outparams.mir"));
    let doc = mir::parse("format_and_outparams", "garbled.mir", &garbled);
    let markers = marker_names(&read_fixture("format_and_outparams.mir"));
    assert!(!markers.is_empty(), "the fixture must carry markers");
    assert_eq!(
        doc.parse_failures
            .iter()
            .filter(|f| f.item.contains("__autumn_workflow_info_"))
            .count(),
        markers.len(),
        "every marker header must land in parse_failures: {:?}",
        doc.parse_failures
    );
    assert!(
        !doc.bodies
            .iter()
            .any(|b| b.path.contains("__autumn_workflow_info_")),
        "no marker may survive as a parsed body"
    );
}

#[test]
fn a_marker_that_failed_to_parse_is_still_discovered() {
    let garbled = garble_marker_headers(&read_fixture("format_and_outparams.mir"));
    let doc = mir::parse("format_and_outparams", "garbled.mir", &garbled);
    let discovered: BTreeSet<String> = entry::discover(&[doc])
        .into_iter()
        .map(|e| e.workflow)
        .collect();
    for marker in marker_names(&read_fixture("format_and_outparams.mir")) {
        assert!(
            discovered.contains(&marker),
            "{marker} must be discovered from the parse failure; discovered = {discovered:?}"
        );
    }
}

#[test]
fn garbled_markers_report_every_workflow_as_unknown_not_as_nothing() {
    let source = read_fixture("format_and_outparams.mir");
    let markers = marker_names(&source);
    let dir = tempfile::tempdir().expect("tempdir");
    // The file stem is the crate name, so keep the fixture's.
    let mir = write_mir(
        dir.path(),
        "format_and_outparams.mir",
        &garble_marker_headers(&source),
    );

    let report = verify_mir(&[mir], &[fixtures_dir()]);
    let summary = report.summary();
    assert_eq!(
        summary.analyzed,
        markers.len(),
        "a target whose markers all fail to parse must still report its workflows, \
         not `analyzed 0`: {summary:?}"
    );
    assert_eq!(
        summary.proven, 0,
        "nothing is proven when the marker set did not parse: {summary:?}"
    );
    assert_eq!(
        summary.unknown.saturating_add(summary.found),
        markers.len(),
        "every workflow gets a verdict; a seeded flow still outranks the \
         boundary (D9), so `found` is the other half: {summary:?}"
    );

    for workflow in &report.workflows {
        assert_ne!(
            workflow.verdict.name(),
            "proven-deterministic",
            "{} was never analyzed through its marker; it may not be proven",
            workflow.workflow
        );
        let kinds: BTreeSet<BoundaryKind> = boundaries(workflow).iter().map(|b| b.kind).collect();
        assert!(
            kinds.contains(&BoundaryKind::MirParse) || kinds.contains(&BoundaryKind::MissingBody),
            "{} must carry the parse boundary that made it unknown; kinds = {kinds:?}",
            workflow.workflow
        );
        let own_marker = workflow
            .workflow
            .rsplit("::")
            .next()
            .unwrap_or(&workflow.workflow);
        assert!(
            boundaries(workflow).iter().any(|b| {
                b.detail
                    .contains(&format!("__autumn_workflow_info_{own_marker}"))
            }),
            "{}'s boundaries must name its own unparseable marker and the recorded \
             reason; got {:?}",
            workflow.workflow,
            boundaries(workflow)
                .iter()
                .map(|b| b.detail.as_str())
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(
        report.exit_code(true),
        1,
        "unknown verdicts fail a strict run"
    );
}

#[test]
fn an_entry_whose_body_is_absent_is_unknown_never_absent() {
    // Only the marker is present: the workflow fn itself was never emitted.
    let dir = tempfile::tempdir().expect("tempdir");
    let mir = write_mir(
        dir.path(),
        "lonely.mir",
        "fn __autumn_workflow_info_wf(_1: u8 -> u8 {\n    let mut _0: u8;\n\n\
             bb0: {\n        return;\n    }\n}\n",
    );
    let report = verify_mir(&[mir], &[]);
    assert_eq!(report.summary().analyzed, 1, "{report:?}");
    let workflow = report.workflows.first().expect("one workflow");
    assert_eq!(workflow.verdict.name(), "unknown");
    let kinds: BTreeSet<BoundaryKind> = boundaries(workflow).iter().map(|b| b.kind).collect();
    assert!(
        kinds.contains(&BoundaryKind::MissingBody) || kinds.contains(&BoundaryKind::MirParse),
        "a body that is not in the dump is a boundary, not a pass: {kinds:?}"
    );
}

// ── a run that discovers nothing says so ────────────────────────────────────

#[test]
fn a_run_that_discovers_no_workflow_warns_and_fails_under_strict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mir = write_mir(
        dir.path(),
        "empty.mir",
        "fn helper() -> u8 {\n    let mut _0: u8;\n\n\
             bb0: {\n        _0 = const 0_u8;\n        return;\n    }\n}\n",
    );
    let report = verify_mir(&[mir], &[]);
    assert_eq!(report.summary().analyzed, 0);
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("no #[workflow] entry points were discovered")),
        "a run that analyzed MIR and found no entry point must say so: {:?}",
        report.warnings
    );
    assert_eq!(
        report.exit_code(false),
        0,
        "an empty run is a warning by default"
    );
    assert_eq!(
        report.exit_code(true),
        1,
        "under --strict, `I analyzed nothing` must not read as `all clear`"
    );
    assert!(
        report.render_text().contains("no #[workflow] entry points"),
        "{}",
        report.render_text()
    );
}

#[test]
fn the_empty_run_warning_counts_the_parse_failures() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mir = write_mir(
        dir.path(),
        "garbled.mir",
        "fn helper(_1: u8 -> u8 {\n    let mut _0: u8;\n\n\
             bb0: {\n        return;\n    }\n}\n",
    );
    let report = verify_mir(&[mir], &[]);
    let warning = report
        .warnings
        .iter()
        .find(|w| w.contains("no #[workflow] entry points were discovered"))
        .unwrap_or_else(|| panic!("expected the empty-run warning: {:?}", report.warnings));
    assert!(
        warning.contains("1 parse failure"),
        "the warning must count the parse failures, so a syntax-drift run is \
         distinguishable from a genuinely empty one: {warning}"
    );
}

#[test]
fn discovering_workflows_leaves_the_empty_run_warning_off() {
    let report = verify_mir(
        &[fixtures_dir().join("example_deterministic_primitives.mir")],
        &[Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("parent")
            .to_path_buf()],
    );
    assert_eq!(report.summary().analyzed, 1);
    assert!(
        !report
            .warnings
            .iter()
            .any(|w| w.contains("no #[workflow] entry points")),
        "{:?}",
        report.warnings
    );
    assert_eq!(report.exit_code(true), 0, "the clean baseline stays clean");
}
