//! Text and JSON rendering plus the exit-code contract.
//!
//! Two rules shape the text format. Every verdict is printed with the model
//! version it was computed under, and every run ends with the list of analysis
//! boundaries the tool does not see through (D9) — a `proven-deterministic`
//! that does not say what it was proven *modulo* is a claim the analyzer cannot
//! support. And an allowlisted workflow prints the justification that suppressed
//! it, so the escape hatch is visible in the same output as the findings.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::verdict::{BoundaryKind, Verdict, WorkflowVerdict};

/// The whole run's output.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub model_version: String,
    pub rustc_version: String,
    pub workflows: Vec<WorkflowVerdict>,
    /// Allowlist entries that matched no analyzed workflow.
    #[serde(default)]
    pub unused_allowlist: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    /// True when the run discovered no `#[workflow]` entry point at all.
    ///
    /// `analyzed 0` is not a clean result, it is an absent one — a marker the
    /// MIR parser could not read, a target that was never built, a `--package`
    /// that selected the wrong crate. It warns by default and fails under
    /// `--strict`, so a CI gate cannot go green on a run that verified nothing.
    #[serde(default)]
    pub discovery_failed: bool,
}

/// Counts for the success-metric triple.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub analyzed: usize,
    pub proven: usize,
    pub unknown: usize,
    pub found: usize,
    pub allowed: usize,
}

impl Report {
    /// Count the verdicts. An allowlisted workflow counts as `allowed` only —
    /// it is deliberately not also counted as `found`, so the metric triple
    /// reads as "what this run is telling you to fix".
    #[must_use]
    pub fn summary(&self) -> Summary {
        let mut summary = Summary {
            analyzed: self.workflows.len(),
            ..Summary::default()
        };
        for workflow in &self.workflows {
            let slot = if workflow.allowed.is_some() {
                &mut summary.allowed
            } else {
                match workflow.verdict {
                    Verdict::ProvenDeterministic => &mut summary.proven,
                    Verdict::Unknown { .. } => &mut summary.unknown,
                    Verdict::NondeterminismFound { .. } => &mut summary.found,
                }
            };
            *slot = slot.saturating_add(1);
        }
        summary
    }

    /// Exit code: 0 clean; 1 any `nondeterminism-found` (or, under `strict`, any `unknown`,
    /// any unused allowlist entry, or a run that discovered no workflow at all).
    ///
    /// The last of those is [`Report::discovery_failed`]: a run that analyzed no
    /// entry point verified nothing, and under `--strict` "nothing to report"
    /// must not be spelled the same way as "nothing to fix".
    #[must_use]
    pub fn exit_code(&self, strict: bool) -> i32 {
        let summary = self.summary();
        if summary.found > 0 {
            return 1;
        }
        if strict
            && (summary.unknown > 0 || !self.unused_allowlist.is_empty() || self.discovery_failed)
        {
            return 1;
        }
        0
    }

    #[must_use]
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        // `rustc_version` is the whole `rustc -V` line — "rustc 1.98.0 (…)" —
        // so it already carries the word. Prefixing it again printed
        // `rustc rustc 1.98.0 (…)` in every report and every CI log.
        let _ = writeln!(
            out,
            "harvest-verify: model {}, {}",
            self.model_version, self.rustc_version
        );
        if !self.workflows.is_empty() {
            out.push('\n');
        }
        for workflow in &self.workflows {
            render_workflow(workflow, &mut out);
        }
        if !self.unused_allowlist.is_empty() || !self.warnings.is_empty() {
            out.push('\n');
        }
        for entry in &self.unused_allowlist {
            let _ = writeln!(
                out,
                "warning: unused allowlist entry: {entry} ({})",
                self.why_unused(entry)
            );
        }
        for warning in &self.warnings {
            let _ = writeln!(out, "warning: {warning}");
        }

        let summary = self.summary();
        out.push('\n');
        let _ = writeln!(
            out,
            "analyzed {}: proven {}, unknown {}, found {}, allowed {}",
            summary.analyzed, summary.proven, summary.unknown, summary.found, summary.allowed
        );
        let boundaries: Vec<&str> = BoundaryKind::ALL.iter().map(|k| k.name()).collect();
        let _ = writeln!(
            out,
            "verdicts hold under model {}; boundaries not analyzed: {}",
            self.model_version,
            boundaries.join(", ")
        );
        out
    }

    /// Why an allowlist entry went unused — the two cases need different actions.
    ///
    /// An entry whose path is not in the run is a rename, a deletion or a typo.
    /// An entry on a workflow that is *now* `proven-deterministic` is the good
    /// case: the flow it was written for is gone, and saying "no analyzed
    /// workflow has that path" about a workflow that is right there in the same
    /// report would send the reader looking for a rename that never happened.
    fn why_unused(&self, entry: &str) -> &'static str {
        let proven = self
            .workflows
            .iter()
            .any(|w| w.workflow == entry && w.verdict == Verdict::ProvenDeterministic);
        if proven {
            "that workflow is now proven-deterministic — the entry can be removed"
        } else {
            "no analyzed workflow has that path"
        }
    }

    /// # Errors
    /// Never in practice; kept as `Result` for the serializer contract.
    pub fn render_json(&self) -> crate::Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| crate::Error::Other(e.to_string()))
    }
}

/// One workflow's block: the verdict line, its findings and its boundaries.
fn render_workflow(workflow: &WorkflowVerdict, out: &mut String) {
    let _ = write!(out, "{}  {}", workflow.verdict.name(), workflow.workflow);
    if let Some(justification) = &workflow.allowed {
        let _ = write!(out, "  allowed ({justification})");
    }
    out.push('\n');

    if let Verdict::NondeterminismFound { findings } = &workflow.verdict {
        for finding in findings {
            let _ = writeln!(
                out,
                "  {} [{}] {}",
                serde_kind(finding.kind),
                serde_taint(finding.taint),
                finding.message
            );
            let trace: Vec<String> = finding
                .trace
                .iter()
                .map(|hop| format!("{}: {}", hop.function, hop.step))
                .collect();
            if !trace.is_empty() {
                let _ = writeln!(out, "    trace: {}", trace.join(" -> "));
            }
            let _ = writeln!(
                out,
                "    source: {} {} {}",
                finding.source.function, finding.source.block, finding.source.what
            );
            let _ = writeln!(
                out,
                "    sink: {} {} {}",
                finding.sink.function, finding.sink.block, finding.sink.what
            );
        }
    }

    // Boundaries are printed even when the verdict is `nondeterminism-found`,
    // so nothing the analyzer could not see is hidden behind a finding.
    let boundaries = match &workflow.verdict {
        Verdict::Unknown { boundaries } if workflow.boundaries.is_empty() => boundaries,
        _ => &workflow.boundaries,
    };
    for boundary in boundaries {
        let _ = writeln!(
            out,
            "  unknown: {}: {} at {} {}",
            boundary.kind.name(),
            boundary.detail,
            boundary.site.function,
            boundary.site.block
        );
    }
}

/// The kebab-case name of a finding kind, as the JSON format spells it.
const fn serde_kind(kind: crate::FindingKind) -> &'static str {
    match kind {
        crate::FindingKind::TaintedSinkArgument => "tainted-sink-argument",
        crate::FindingKind::ControlDependentSink => "control-dependent-sink",
        crate::FindingKind::ForbiddenEffect => "forbidden-effect",
    }
}

/// The kebab-case name of a taint kind, as the JSON format spells it.
const fn serde_taint(taint: crate::TaintKind) -> &'static str {
    match taint {
        crate::TaintKind::Value => "value",
        crate::TaintKind::Order => "order",
        crate::TaintKind::Control => "control",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::{Boundary, Site};

    fn site() -> Site {
        Site {
            function: "wf".to_string(),
            block: "bb0".to_string(),
            what: "calls thing".to_string(),
            hint: None,
        }
    }

    fn unknown_workflow() -> WorkflowVerdict {
        let boundary = Boundary {
            kind: BoundaryKind::Ffi,
            detail: "clock_gettime".to_string(),
            site: site(),
        };
        WorkflowVerdict {
            workflow: "b::wf".to_string(),
            crate_name: "b".to_string(),
            verdict: Verdict::Unknown {
                boundaries: vec![boundary.clone()],
            },
            boundaries: vec![boundary],
            allowed: None,
        }
    }

    #[test]
    fn an_empty_report_still_prints_the_model_and_boundary_lines() {
        let text = Report {
            model_version: "2026.09.0".to_string(),
            rustc_version: "rustc 1.94.1".to_string(),
            ..Report::default()
        }
        .render_text();
        assert!(
            text.contains("harvest-verify: model 2026.09.0, rustc 1.94.1"),
            "the `rustc -V` line already begins with the word `rustc`; the \
             header must not prefix it a second time:\n{text}"
        );
        assert!(text.contains("analyzed 0: proven 0, unknown 0, found 0, allowed 0"));
        assert!(text.contains("verdicts hold under model 2026.09.0; boundaries not analyzed:"));
    }

    #[test]
    fn a_boundary_line_names_kind_detail_and_site() {
        let report = Report {
            model_version: "m".to_string(),
            rustc_version: "r".to_string(),
            workflows: vec![unknown_workflow()],
            ..Report::default()
        };
        assert!(
            report
                .render_text()
                .contains("  unknown: ffi: clock_gettime at wf bb0"),
            "{}",
            report.render_text()
        );
    }

    #[test]
    fn the_summary_line_and_exit_code_agree() {
        let report = Report {
            workflows: vec![unknown_workflow()],
            ..Report::default()
        };
        assert_eq!(report.summary().unknown, 1);
        assert_eq!(report.exit_code(false), 0);
        assert_eq!(report.exit_code(true), 1);
        assert!(
            report
                .render_text()
                .contains("analyzed 1: proven 0, unknown 1")
        );
    }
}
