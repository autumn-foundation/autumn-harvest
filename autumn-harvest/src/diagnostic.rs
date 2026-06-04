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

    /// Render the diagnostic report as a standalone HTML document.
    #[must_use]
    pub fn to_html(&self) -> String {
        let mut out = String::new();
        out.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
        out.push_str("  <meta charset=\"UTF-8\">\n");
        out.push_str("  <title>Workflow Diagnostic Report</title>\n");
        out.push_str("  <style>\n");
        out.push_str("    body { font-family: system-ui, sans-serif; max-width: 900px; margin: 0 auto; padding: 2rem; }\n");
        out.push_str("    .success { color: #059669; font-weight: bold; }\n");
        out.push_str("    .failure { color: #dc2626; font-weight: bold; }\n");
        out.push_str("    .warning { background: #fffbeb; border-left: 4px solid #f59e0b; padding: 1rem; margin-bottom: 1rem; }\n");
        out.push_str("    pre { background: #f3f4f6; padding: 1rem; border-radius: 0.5rem; overflow-x: auto; }\n");
        out.push_str("  </style>\n");
        out.push_str("  <script type=\"module\">\n");
        out.push_str("    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';\n");
        out.push_str("    mermaid.initialize({ startOnLoad: true });\n");
        out.push_str("  </script>\n");
        out.push_str("</head>\n<body>\n");

        out.push_str("  <h1>Diagnostic Report</h1>\n\n");

        out.push_str("  <h2>Execution Result</h2>\n");
        match &self.final_output {
            Ok(val) => {
                out.push_str("  <p>Status: <span class=\"success\">Success</span></p>\n");
                let safe_val = val.to_string().replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
                let _ = writeln!(out, "  <p>Output: <code>{safe_val}</code></p>");
            }
            Err(err) => {
                out.push_str("  <p>Status: <span class=\"failure\">Failure</span></p>\n");
                let safe_err = err.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
                let _ = writeln!(out, "  <p>Error: <code>{safe_err}</code></p>");
            }
        }
        let _ = writeln!(out, "  <p>Total Events: {}</p>\n", self.event_count);

        if !self.analyzer_warnings.is_empty() {
            out.push_str("  <h2>Analyzer Warnings</h2>\n");
            for warning in &self.analyzer_warnings {
                let safe_msg = warning.message.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
                let _ = writeln!(
                    out,
                    "  <div class=\"warning\"><strong>{}</strong>: {}</div>",
                    warning.rule_name, safe_msg
                );
            }
        }

        out.push_str("  <h2>Sequence Diagram</h2>\n");
        out.push_str("  <div class=\"mermaid\">\n");
        out.push_str(&self.mermaid_sequence);
        out.push_str("  </div>\n");

        out.push_str("</body>\n</html>\n");

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

    #[test]
    fn test_diagnostic_report_to_html() {
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
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
        let html = report.to_html();

        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Diagnostic Report"));
        assert!(html.contains("Success"));
        assert!(html.contains("sequenceDiagram"));
        assert!(html.contains("mermaid.esm.min.mjs"));
    }
}
