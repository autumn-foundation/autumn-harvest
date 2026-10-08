//! Report rendering, the summary triple and the exit-code contract (D9, D10).
//!
//! `Report::summary`, `exit_code` and `render_text` are exercised here, plus
//! the `render_json` round trip that is the machine-readable half of D10.

use autumn_harvest_verify::report::{Report, Summary};
use autumn_harvest_verify::verdict::{
    Boundary, BoundaryKind, Finding, FindingKind, Hop, Site, TaintKind, Verdict, WorkflowVerdict,
};

// ── builders ────────────────────────────────────────────────────────────────

fn site(function: &str, block: &str, what: &str) -> Site {
    Site {
        function: function.to_string(),
        block: block.to_string(),
        what: what.to_string(),
        hint: None,
    }
}

fn hop(function: &str, step: &str) -> Hop {
    Hop {
        function: function.to_string(),
        step: step.to_string(),
    }
}

fn finding() -> Finding {
    Finding {
        kind: FindingKind::TaintedSinkArgument,
        taint: TaintKind::Value,
        source: site("seeded::stamped_name", "bb0", "calls wall_clock_secs"),
        sink: site(
            "seeded::wf_fmt::{closure#0}",
            "bb7",
            "emits execute_activity",
        ),
        trace: vec![
            hop("seeded::wf_fmt::{closure#0}", "calls stamped_name"),
            hop("seeded::stamped_name", "calls wall_clock_secs"),
            hop("seeded::wall_clock_secs", "reads SystemTime::now"),
            hop("seeded::wf_fmt::{closure#0}", "emits execute_activity"),
        ],
        message: "wall-clock time reaches the activity name".to_string(),
    }
}

fn boundary(kind: BoundaryKind, detail: &str) -> Boundary {
    Boundary {
        kind,
        detail: detail.to_string(),
        site: site("boundary::wf::{closure#0}", "bb3", detail),
    }
}

fn proven(workflow: &str) -> WorkflowVerdict {
    WorkflowVerdict {
        workflow: workflow.to_string(),
        crate_name: "clean".to_string(),
        verdict: Verdict::ProvenDeterministic,
        boundaries: Vec::new(),
        allowed: None,
    }
}

fn found(workflow: &str) -> WorkflowVerdict {
    WorkflowVerdict {
        workflow: workflow.to_string(),
        crate_name: "seeded".to_string(),
        verdict: Verdict::NondeterminismFound {
            findings: vec![finding()],
        },
        boundaries: Vec::new(),
        allowed: None,
    }
}

fn unknown(workflow: &str, kind: BoundaryKind, detail: &str) -> WorkflowVerdict {
    WorkflowVerdict {
        workflow: workflow.to_string(),
        crate_name: "boundary".to_string(),
        verdict: Verdict::Unknown {
            boundaries: vec![boundary(kind, detail)],
        },
        boundaries: vec![boundary(kind, detail)],
        allowed: None,
    }
}

fn allowed(mut v: WorkflowVerdict, justification: &str) -> WorkflowVerdict {
    v.allowed = Some(justification.to_string());
    v
}

fn report(workflows: Vec<WorkflowVerdict>) -> Report {
    Report {
        model_version: "1.0.0-962".to_string(),
        rustc_version: "rustc 1.94.1 (e408947bf 2026-03-25)".to_string(),
        workflows,
        unused_allowlist: Vec::new(),
        warnings: Vec::new(),
        discovery_failed: false,
    }
}

// ── summary ─────────────────────────────────────────────────────────────────

#[test]
fn summary_counts_the_metric_triple() {
    let r = report(vec![
        proven("clean::a"),
        proven("clean::b"),
        unknown(
            "boundary::c",
            BoundaryKind::DynDispatch,
            "<dyn Tr as Tr>::f",
        ),
        found("seeded::d"),
        allowed(found("seeded::e"), "tracked in #963"),
    ]);
    assert_eq!(
        r.summary(),
        Summary {
            analyzed: 5,
            proven: 2,
            unknown: 1,
            found: 1,
            allowed: 1
        },
        "an allowlisted workflow counts as `allowed`, not as `found`"
    );
}

#[test]
fn summary_of_an_empty_report_is_all_zero() {
    assert_eq!(report(Vec::new()).summary(), Summary::default());
}

// ── exit codes ──────────────────────────────────────────────────────────────

#[test]
fn exit_code_is_zero_when_everything_is_proven() {
    let r = report(vec![proven("clean::a"), proven("clean::b")]);
    assert_eq!(r.exit_code(false), 0);
    assert_eq!(r.exit_code(true), 0);
}

#[test]
fn unknown_warns_but_does_not_fail_unless_strict() {
    let r = report(vec![
        proven("clean::a"),
        unknown("boundary::b", BoundaryKind::Ffi, "clock_gettime"),
    ]);
    assert_eq!(
        r.exit_code(false),
        0,
        "an honest `unknown` is a warning by default (D10)"
    );
    assert_eq!(r.exit_code(true), 1, "--strict promotes it to a failure");
}

#[test]
fn any_finding_fails_even_without_strict() {
    let r = report(vec![proven("clean::a"), found("seeded::b")]);
    assert_eq!(r.exit_code(false), 1);
    assert_eq!(r.exit_code(true), 1);
}

#[test]
fn an_allowlisted_finding_does_not_fail_the_run() {
    let r = report(vec![
        proven("clean::a"),
        allowed(found("seeded::b"), "tracked in #963"),
    ]);
    assert_eq!(
        r.exit_code(false),
        0,
        "the allowlist is the AC5 escape hatch"
    );
    assert_eq!(r.exit_code(true), 0, "and it is not itself a boundary");
}

#[test]
fn a_finding_alongside_an_allowlisted_one_still_fails() {
    let r = report(vec![
        allowed(found("seeded::a"), "tracked in #963"),
        found("seeded::b"),
    ]);
    assert_eq!(r.exit_code(false), 1);
}

#[test]
fn an_unused_allowlist_entry_only_fails_under_strict() {
    let mut r = report(vec![proven("clean::a")]);
    r.unused_allowlist = vec!["seeded::wf_deleted".to_string()];
    assert_eq!(r.exit_code(false), 0);
    assert_eq!(r.exit_code(true), 1);
}

// ── text rendering ──────────────────────────────────────────────────────────

#[test]
fn render_text_names_every_verdict_and_the_model_line() {
    let r = report(vec![
        proven("clean::a"),
        unknown(
            "boundary::b",
            BoundaryKind::DynDispatch,
            "<dyn Tr as Tr>::f",
        ),
        found("seeded::c"),
    ]);
    let text = r.render_text();
    for name in ["proven-deterministic", "unknown", "nondeterminism-found"] {
        assert!(
            text.contains(name),
            "missing verdict name {name} in:\n{text}"
        );
    }
    assert!(
        text.contains("under model"),
        "the model/boundary line is mandatory (D9):\n{text}"
    );
    assert!(
        text.contains("1.0.0-962"),
        "the model version must be printed:\n{text}"
    );
    for w in ["clean::a", "boundary::b", "seeded::c"] {
        assert!(text.contains(w), "missing workflow {w} in:\n{text}");
    }
}

#[test]
fn render_text_lists_every_boundary_name() {
    let workflows: Vec<WorkflowVerdict> = BoundaryKind::ALL
        .iter()
        .enumerate()
        .map(|(i, kind)| unknown(&format!("boundary::wf_{i}"), *kind, "detail"))
        .collect();
    let text = report(workflows).render_text();
    for kind in BoundaryKind::ALL {
        assert!(
            text.contains(kind.name()),
            "boundary `{}` must be printed by name:\n{text}",
            kind.name()
        );
    }
}

#[test]
fn render_text_renders_the_trace_with_arrows() {
    let text = report(vec![found("seeded::c")]).render_text();
    assert!(
        text.contains("->"),
        "hops must be rendered as an arrow chain:\n{text}"
    );
    for hop_fn in [
        "stamped_name",
        "wall_clock_secs",
        "seeded::wf_fmt::{closure#0}",
    ] {
        assert!(
            text.contains(hop_fn),
            "the trace must name {hop_fn} (AC3):\n{text}"
        );
    }
    assert!(
        text.contains("execute_activity"),
        "the sink must be named:\n{text}"
    );
}

#[test]
fn render_text_prints_the_justification_of_allowed_entries() {
    let text = report(vec![allowed(
        found("seeded::b"),
        "tracked in #963; rewrite lands in #970",
    )])
    .render_text();
    assert!(text.contains("allowed"), "{text}");
    assert!(
        text.contains("tracked in #963; rewrite lands in #970"),
        "an allowlisted workflow must print WHY it is allowed:\n{text}"
    );
}

#[test]
fn render_text_prints_unused_allowlist_entries_and_warnings() {
    let mut r = report(vec![proven("clean::a")]);
    r.unused_allowlist = vec!["seeded::wf_deleted".to_string()];
    r.warnings = vec!["rustc 1.95.0 is untested; the parser is validated on 1.94.x".to_string()];
    let text = r.render_text();
    assert!(text.contains("seeded::wf_deleted"), "{text}");
    assert!(text.contains("1.95.0"), "{text}");
}

// ── JSON round-trip ─────────────────────────────────────────────────────────

#[test]
fn render_json_round_trips_back_to_a_report() {
    let original = {
        let mut r = report(vec![
            proven("clean::a"),
            unknown(
                "boundary::b",
                BoundaryKind::UnmodeledCtxMethod,
                "WorkflowContext::mystery",
            ),
            found("seeded::c"),
            allowed(found("seeded::d"), "tracked in #963"),
        ]);
        r.unused_allowlist = vec!["seeded::wf_deleted".to_string()];
        r.warnings = vec!["one warning".to_string()];
        r
    };
    let json = original.render_json().expect("render_json");
    let parsed: Report = serde_json::from_str(&json).expect("round-trip back to Report");
    assert_eq!(parsed, original);
}

#[test]
fn json_uses_kebab_case_verdict_and_boundary_names() {
    let json = report(vec![
        found("seeded::c"),
        unknown("boundary::b", BoundaryKind::UnsafeRawPointer, "*const u64"),
    ])
    .render_json()
    .expect("render_json");
    assert!(json.contains("nondeterminism-found"), "{json}");
    assert!(json.contains("unsafe-raw-pointer"), "{json}");
    assert!(json.contains("tainted-sink-argument"), "{json}");
    assert!(json.contains("\"taint\": \"value\""), "{json}");
}

#[test]
fn boundary_kind_names_are_unique_and_kebab_case() {
    let mut seen = std::collections::BTreeSet::new();
    for kind in BoundaryKind::ALL {
        let name = kind.name();
        assert!(seen.insert(name), "duplicate boundary name {name}");
        assert!(
            name.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
            "boundary names are kebab-case: {name}"
        );
    }
    assert_eq!(seen.len(), BoundaryKind::ALL.len());
}

#[test]
fn an_unused_entry_on_a_proven_workflow_says_the_workflow_is_clean_now() {
    // The two ways an entry goes unused read very differently to the person who
    // has to act on them: a path that no longer exists is a rename or a
    // deletion, while a path that is now `proven-deterministic` is the good
    // news — the bug the entry was written for is fixed. The warning must not
    // spell the second as "no analyzed workflow has that path".
    let mut r = report(vec![proven("clean::a")]);
    r.unused_allowlist = vec!["clean::a".to_string(), "clean::gone".to_string()];
    let text = r.render_text();
    assert!(
        text.contains(
            "warning: unused allowlist entry: clean::a (that workflow is now \
             proven-deterministic — the entry can be removed)"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "warning: unused allowlist entry: clean::gone (no analyzed workflow \
             has that path)"
        ),
        "{text}"
    );
    assert_eq!(r.exit_code(false), 0);
    assert_eq!(r.exit_code(true), 1, "either way, --strict fails");
}

#[test]
fn a_run_that_discovered_no_workflow_fails_only_under_strict() {
    let mut r = report(Vec::new());
    r.discovery_failed = true;
    assert_eq!(r.summary().analyzed, 0);
    assert_eq!(r.exit_code(false), 0);
    assert_eq!(
        r.exit_code(true),
        1,
        "`analyzed 0` is an absent result, not a clean one"
    );
    assert_eq!(
        report(Vec::new()).exit_code(true),
        0,
        "a report that never claimed to discover anything is unaffected"
    );
}
