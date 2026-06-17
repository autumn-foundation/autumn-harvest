//! Diagnostic reporting for simulated workflow executions.

use crate::analyzer::{AnalyzerWarning, HistoryAnalyzer};
use crate::history_export::export_mermaid_sequence;
use crate::simulator::SimulatorResult;
use serde_json::Value;
use std::fmt::Write;

/// A diagnostic report generated from a simulated workflow execution.
#[derive(Debug, Clone)]
pub struct DiagnosticReport {
    pub final_output: Result<Value, String>,
    pub event_count: usize,
    pub analyzer_warnings: Vec<AnalyzerWarning>,
    pub mermaid_sequence: String,
}

impl DiagnosticReport {
    /// Render the diagnostic report as a Markdown string.
    #[must_use]
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# Diagnostic Report\n\n");

        out.push_str("## Execution Result\n");
        match &self.final_output {
            Ok(val) => {
                out.push_str("**Status:** Success\n");
                let _ = write!(out, "**Output:** `{val}`\n\n");
            }
            Err(err) => {
                out.push_str("**Status:** Failure\n");
                let _ = write!(out, "**Error:** `{err}`\n\n");
            }
        }

        let _ = write!(out, "**Total Events:** {}\n\n", self.event_count);

        if !self.analyzer_warnings.is_empty() {
            out.push_str("## Analyzer Warnings\n");
            for warning in &self.analyzer_warnings {
                let _ = writeln!(out, "- **{}**: {}", warning.rule_name, warning.message);
            }
            out.push('\n');
        }

        out.push_str("## Sequence Diagram\n");
        out.push_str("```mermaid\n");
        out.push_str(&self.mermaid_sequence);
        if !self.mermaid_sequence.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("```\n");

        out
    }
}

/// Extension trait for generating diagnostic reports from a `SimulatorResult`.
pub trait SimulatorResultExt {
    /// Generate a diagnostic report.
    fn diagnostic_report(&self) -> DiagnosticReport;
}

impl SimulatorResultExt for SimulatorResult {
    fn diagnostic_report(&self) -> DiagnosticReport {
        let analyzer = HistoryAnalyzer::new();
        let warnings = analyzer.analyze(&self.history);
        let sequence = export_mermaid_sequence(&self.history).unwrap_or_default();

        DiagnosticReport {
            final_output: self.final_output.clone(),
            event_count: self.history.len(),
            analyzer_warnings: warnings,
            mermaid_sequence: sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WorkflowEvent;
    use chrono::Utc;

    #[test]
    fn test_diagnostic_report_generation() {
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"status": "ok"}),
            },
        ];

        let result = SimulatorResult {
            final_output: Ok(serde_json::json!({"status": "ok"})),
            history,
        };

        let report = result.diagnostic_report();
        let markdown = report.to_markdown();

        assert!(markdown.contains("Diagnostic Report"));
        assert!(markdown.contains("Success"));
        assert!(markdown.contains("sequenceDiagram"));
    }
}
