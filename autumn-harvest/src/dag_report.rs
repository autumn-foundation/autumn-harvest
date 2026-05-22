//! Comprehensive DAG reporting.
//!
//! Aggregates linters, profilers, and critical path analysis into a unified
//! Markdown report containing metrics, warnings, and a Mermaid visualization.

use std::collections::HashMap;
use std::fmt::Write;

use crate::critical_path::{CriticalPathAnalyzer, CriticalPathResult};
use crate::dag::DagDefinition;
use crate::dag_linter::{DagLinter, DagWarning};
use crate::dag_profiler::{DagProfile, DagProfiler};
use crate::info::ActivityInfo;

/// Aggregated report of a DAG's static and simulated execution characteristics.
#[derive(Debug, Clone)]
pub struct DagReport {
    /// Warnings produced by the linter.
    pub warnings: Vec<DagWarning>,
    /// Profiling results from simulating execution.
    pub profile: DagProfile,
    /// Critical path analysis results.
    pub critical_path: CriticalPathResult,
    /// Mermaid diagram string, highlighting critical path nodes.
    pub mermaid_diagram: String,
}

impl DagReport {
    /// Renders the unified report as a Markdown string.
    #[must_use]
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# DAG Analysis Report\n\n");

        out.push_str("## Metrics\n");
        let _ = writeln!(
            out,
            "- **Estimated Duration:** {}s",
            self.profile.total_duration.as_secs_f32()
        );
        let _ = writeln!(
            out,
            "- **Peak Concurrency:** {}",
            self.profile.peak_concurrency
        );
        let _ = writeln!(
            out,
            "- **Critical Path Duration:** {}s",
            self.critical_path.total_duration.as_secs_f32()
        );
        out.push('\n');

        if !self.warnings.is_empty() {
            out.push_str("## Warnings\n");
            for warning in &self.warnings {
                let _ = writeln!(out, "- **{}**: {}", warning.rule_name, warning.message);
            }
            out.push('\n');
        }

        if !self.critical_path.path_names.is_empty() {
            out.push_str("## Critical Path\n");
            let path_str = self.critical_path.path_names.join(" -> ");
            let _ = writeln!(out, "`{path_str}`\n");
        }

        out.push_str("## Graph\n");
        out.push_str("```mermaid\n");
        out.push_str(&self.mermaid_diagram);
        if !self.mermaid_diagram.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("```\n");

        out
    }
}

/// Generates a `DagReport` from a DAG definition and configurations.
pub struct DagReporter {
    dag: DagDefinition,
    activities: HashMap<String, ActivityInfo>,
    durations: HashMap<String, std::time::Duration>,
    default_duration: std::time::Duration,
    linter: DagLinter,
}

impl DagReporter {
    /// Create a new reporter for a given DAG.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activities: HashMap::new(),
            durations: HashMap::new(),
            default_duration: std::time::Duration::from_secs(1),
            linter: DagLinter::new(),
        }
    }

    /// Provide known activity definitions for linting.
    #[must_use]
    pub fn with_activities(mut self, activities: HashMap<String, ActivityInfo>) -> Self {
        self.activities = activities;
        self
    }

    /// Mock the duration of a specific activity for profiling.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: std::time::Duration) -> Self {
        self.durations.insert(name.to_string(), duration);
        self
    }

    /// Set a default duration for unmocked activities.
    #[must_use]
    pub fn with_default_duration(mut self, duration: std::time::Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Configure the linter to use specific rules.
    #[must_use]
    pub fn with_linter(mut self, linter: DagLinter) -> Self {
        self.linter = linter;
        self
    }

    /// Run analysis and generate the report.
    #[must_use]
    pub fn generate(&self) -> DagReport {
        let warnings = self.linter.analyze(&self.dag, &self.activities);

        let mut profiler =
            DagProfiler::new(self.dag.clone()).with_default_duration(self.default_duration);
        let mut cp_analyzer = CriticalPathAnalyzer::new(self.dag.clone())
            .with_default_duration(self.default_duration);

        for (name, &dur) in &self.durations {
            profiler = profiler.mock_duration(name, dur);
            cp_analyzer = cp_analyzer.mock_duration(name, dur);
        }

        let profile = profiler.profile();
        let critical_path = cp_analyzer.analyze();
        let mermaid_diagram = Self::generate_mermaid(&self.dag, &critical_path.path_indices);

        DagReport {
            warnings,
            profile,
            critical_path,
            mermaid_diagram,
        }
    }

    fn generate_mermaid(dag: &DagDefinition, critical_path_indices: &[usize]) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "graph TD");

        let tasks = dag.tasks();

        for (i, task) in tasks.iter().enumerate() {
            let _ = writeln!(out, "    t{i}[\"{}\"]", task.activity_name);
        }

        for (i, task) in tasks.iter().enumerate() {
            for &upstream in &task.upstreams {
                let _ = writeln!(out, "    t{upstream} --> t{i}");
            }
        }

        // Apply styling to nodes in the critical path
        if !critical_path_indices.is_empty() {
            let _ = writeln!(
                out,
                "    classDef critical fill:#f9f,stroke:#333,stroke-width:2px;"
            );
            for &idx in critical_path_indices {
                let _ = writeln!(out, "    class t{idx} critical;");
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_linter::MissingTimeoutRule;
    use std::time::Duration;

    fn act_a() {}
    fn act_b() {}
    fn act_c() {}

    #[test]
    fn test_report_generation() {
        let mut builder = DagBuilder::new();
        let a = builder
            .activity(act_a)
            .start_to_close(Duration::from_secs(1));
        let _b = builder
            .activity(act_b)
            .upstream(&a)
            .start_to_close(Duration::from_secs(2));
        let _c = builder.activity(act_c).upstream(&a); // Missing timeout
        let dag = builder.build().unwrap();

        let linter = DagLinter::new().with_rule(MissingTimeoutRule::new());

        let reporter = DagReporter::new(dag)
            .with_linter(linter)
            .mock_duration("act_a", Duration::from_secs(1))
            .mock_duration("act_b", Duration::from_secs(10))
            .mock_duration("act_c", Duration::from_secs(2));

        let report = reporter.generate();
        let markdown = report.to_markdown();

        // Check Metrics
        assert!(markdown.contains("Estimated Duration"));
        assert!(markdown.contains("Peak Concurrency"));
        assert!(markdown.contains("Critical Path Duration"));

        // Check Warnings
        assert!(markdown.contains("act_c"));
        assert!(markdown.contains("MissingTimeout"));

        // Check Critical Path (act_a -> act_b)
        assert!(markdown.contains("act_a -> act_b"));

        // Check Mermaid
        assert!(markdown.contains("classDef critical fill:#f9f"));
        assert!(markdown.contains("class t0 critical;")); // act_a
        assert!(markdown.contains("class t1 critical;")); // act_b
    }
}
