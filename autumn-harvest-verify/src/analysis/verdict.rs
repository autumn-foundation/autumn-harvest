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

/// Add boundaries discovered *after* [`assemble`] ran, re-deriving the verdict.
///
/// The pipeline learns some boundaries only once every doc has been parsed (a
/// crate whose dump did not fully parse, a dump that was not valid UTF-8).
/// Appending them to [`WorkflowVerdict::boundaries`] without re-deriving would
/// leave a `proven-deterministic` verdict carrying a non-empty boundary list —
/// the exact self-contradiction AC2 forbids.
pub fn attach_boundaries(verdict: &mut WorkflowVerdict, extra: Vec<Boundary>) {
    if extra.is_empty() {
        return;
    }
    let mut boundaries = std::mem::take(&mut verdict.boundaries);
    boundaries.extend(extra);
    verdict.boundaries = dedup_boundaries(boundaries);
    verdict.verdict = match std::mem::replace(&mut verdict.verdict, Verdict::ProvenDeterministic) {
        Verdict::NondeterminismFound { findings } => Verdict::NondeterminismFound { findings },
        Verdict::Unknown { .. } | Verdict::ProvenDeterministic => Verdict::Unknown {
            boundaries: verdict.boundaries.clone(),
        },
    };
}

/// One finding per `(kind, taint, source, sink)`: the same flow reached through
/// two call contexts is one bug, and printing it twice only hides the others.
///
/// The taint kind is part of the key because one source can reach one sink in
/// two different ways — an ambient comparator decides both the *order* of a
/// collection and, through the branch inside it, the *values* that come out —
/// and collapsing those two onto whichever arrived first would drop the more
/// precise label from the report.
fn dedup(findings: Vec<Finding>) -> Vec<Finding> {
    let mut seen: BTreeSet<(String, String, String, String, String, String)> = BTreeSet::new();
    let mut out = Vec::new();
    for finding in findings {
        let key = (
            format!("{:?}", finding.kind),
            format!("{:?}", finding.taint),
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
    fn a_boundary_attached_after_assembly_downgrades_proven_to_unknown() {
        let mut verdict = assemble("wf", "c", Vec::new(), Vec::new());
        assert_eq!(verdict.verdict, Verdict::ProvenDeterministic);
        attach_boundaries(
            &mut verdict,
            vec![boundary(
                BoundaryKind::MirParse,
                "helper: malformed fn header",
            )],
        );
        assert_eq!(
            verdict.verdict.name(),
            "unknown",
            "a verdict may never be `proven` while carrying a boundary"
        );
        assert_eq!(verdict.boundaries.len(), 1);
    }

    #[test]
    fn a_boundary_attached_after_assembly_keeps_a_finding() {
        let mut verdict = assemble(
            "wf",
            "c",
            vec![finding("SystemTime::now", "execute_activity_raw")],
            Vec::new(),
        );
        attach_boundaries(&mut verdict, vec![boundary(BoundaryKind::MirParse, "x")]);
        assert_eq!(verdict.verdict.name(), "nondeterminism-found");
        assert_eq!(verdict.boundaries.len(), 1);
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
