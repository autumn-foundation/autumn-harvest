//! Turning findings and boundaries into the three-valued verdict (D9).
//!
//! The precedence is fixed and deliberate:
//! `nondeterminism-found` > `unknown` > `proven-deterministic`. A workflow that
//! hit a boundary *and* found a real flow reports the finding — the boundary is
//! still attached, because a finding does not make the unexplored part of the
//! program explored — but a workflow that hit a boundary and found nothing is
//! never called proven: that is the whole content of AC2.

use std::collections::BTreeSet;

use crate::verdict::{Boundary, Finding, Verdict, WorkflowVerdict};

/// Assemble one workflow's verdict from everything the analyzer collected.
#[must_use]
pub fn assemble(
    workflow: &str,
    crate_name: &str,
    findings: Vec<Finding>,
    boundaries: Vec<Boundary>,
) -> WorkflowVerdict {
    let findings = dedup(findings);
    let boundaries = dedup_boundaries(boundaries);
    let verdict = if !findings.is_empty() {
        Verdict::NondeterminismFound { findings }
    } else if boundaries.is_empty() {
        Verdict::ProvenDeterministic
    } else {
        Verdict::Unknown {
            boundaries: boundaries.clone(),
        }
    };
    WorkflowVerdict {
        workflow: workflow.to_string(),
        crate_name: crate_name.to_string(),
        verdict,
        boundaries,
        allowed: None,
    }
}

/// One finding per `(kind, source, sink)`: the same flow reached through two
/// call contexts is one bug, and printing it twice only hides the others.
fn dedup(findings: Vec<Finding>) -> Vec<Finding> {
    let mut seen: BTreeSet<(String, String, String, String, String)> = BTreeSet::new();
    let mut out = Vec::new();
    for finding in findings {
        let key = (
            format!("{:?}", finding.kind),
            finding.source.function.clone(),
            finding.source.what.clone(),
            finding.sink.function.clone(),
            finding.sink.what.clone(),
        );
        if seen.insert(key) {
            out.push(finding);
        }
    }
    out
}

fn dedup_boundaries(boundaries: Vec<Boundary>) -> Vec<Boundary> {
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out = Vec::new();
    for boundary in boundaries {
        let key = (boundary.kind.name().to_string(), boundary.detail.clone());
        if seen.insert(key) {
            out.push(boundary);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::{BoundaryKind, FindingKind, Hop, Site, TaintKind};

    fn site(function: &str, what: &str) -> Site {
        Site {
            function: function.to_string(),
            block: "bb1".to_string(),
            what: what.to_string(),
            hint: None,
        }
    }

    fn finding(source: &str, sink: &str) -> Finding {
        Finding {
            kind: FindingKind::TaintedSinkArgument,
            taint: TaintKind::Value,
            source: site("helper", source),
            sink: site("wf", sink),
            trace: vec![Hop {
                function: "wf".to_string(),
                step: "calls helper".to_string(),
            }],
            message: "m".to_string(),
        }
    }

    fn boundary(kind: BoundaryKind, detail: &str) -> Boundary {
        Boundary {
            kind,
            detail: detail.to_string(),
            site: site("wf", detail),
        }
    }

    #[test]
    fn a_finding_outranks_a_boundary_but_does_not_hide_it() {
        let verdict = assemble(
            "wf",
            "c",
            vec![finding("SystemTime::now", "execute_activity_raw")],
            vec![boundary(BoundaryKind::Ffi, "abs")],
        );
        assert_eq!(verdict.verdict.name(), "nondeterminism-found");
        assert_eq!(
            verdict.boundaries.len(),
            1,
            "the boundary is still reported"
        );
    }

    #[test]
    fn a_boundary_with_no_finding_is_unknown_never_proven() {
        let verdict = assemble(
            "wf",
            "c",
            Vec::new(),
            vec![boundary(BoundaryKind::Ffi, "abs")],
        );
        assert_eq!(verdict.verdict.name(), "unknown");
    }

    #[test]
    fn nothing_at_all_is_proven() {
        let verdict = assemble("wf", "c", Vec::new(), Vec::new());
        assert_eq!(verdict.verdict, Verdict::ProvenDeterministic);
    }

    #[test]
    fn the_same_flow_found_twice_is_reported_once() {
        let verdict = assemble(
            "wf",
            "c",
            vec![
                finding("SystemTime::now", "execute_activity_raw"),
                finding("SystemTime::now", "execute_activity_raw"),
                finding("rand::random", "execute_activity_raw"),
            ],
            Vec::new(),
        );
        match verdict.verdict {
            Verdict::NondeterminismFound { findings } => assert_eq!(findings.len(), 2),
            other => panic!("expected findings, got {}", other.name()),
        }
    }
}
