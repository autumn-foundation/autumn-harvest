//! Unified DAG analysis and reporting.
//!
//! Provides `DagInsight`, a facade that combines multiple discrete analysis
//! tools (`DagLinter`, `DagProfiler`, `CriticalPathAnalyzer`, `DagSimulator`)
//! into a single comprehensive diagnostic report for DAG definitions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::critical_path::{CriticalPathAnalyzer, CriticalPathResult};
use crate::dag::DagDefinition;
use crate::dag_linter::{DagLinter, DagWarning};
use crate::dag_profiler::{DagProfile, DagProfiler};
use crate::dag_simulator::{DagSimulator, DagSimulatorResult};
use crate::info::ActivityInfo;

/// Comprehensive report containing all insights from DAG analysis.
#[derive(Debug, Clone)]
pub struct DagInsightReport {
    /// Lint warnings identifying potential issues (e.g., missing retry policies, excessive parallelism).
    pub warnings: Vec<DagWarning>,
    /// Critical path analysis showing the longest sequence of tasks.
    pub critical_path: CriticalPathResult,
    /// Profiling results including total duration and peak concurrency.
    pub profile: DagProfile,
    /// Result of simulating the DAG execution.
    pub simulation: DagSimulatorResult,
}

/// A unified interface that runs a DAG through all available analysis tools at once.
pub struct DagInsight {
    dag: DagDefinition,
    activities: HashMap<String, ActivityInfo>,
    activity_durations: HashMap<String, Duration>,
    activity_mocks: HashMap<String, Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    default_duration: Duration,
}

impl DagInsight {
    /// Create a new `DagInsight` for the given DAG definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activities: HashMap::new(),
            activity_durations: HashMap::new(),
            activity_mocks: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Register activity metadata for use by the linter.
    #[must_use]
    pub fn with_activities(mut self, activities: HashMap<String, ActivityInfo>) -> Self {
        self.activities = activities;
        self
    }

    /// Set a default duration for activities whose duration is not explicitly mocked.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Mock the duration of a specific activity by name for profiling and critical path analysis.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Register a mock implementation for an activity by name for the simulator.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        self.activity_mocks.insert(name.to_string(), Arc::new(mock));
        self
    }

    /// Run all analysis tools and produce a comprehensive report.
    #[must_use]
    pub fn analyze(&self) -> DagInsightReport {
        // Run Linter
        let linter = DagLinter::new()
            .with_rule(crate::dag_linter::MissingRetryPolicyRule::new())
            .with_rule(crate::dag_linter::MissingTimeoutRule::new())
            .with_rule(crate::dag_linter::ExcessiveParallelismRule::new(10));
        let warnings = linter.analyze(&self.dag, &self.activities);

        // Run Critical Path Analyzer
        let mut cp_analyzer = CriticalPathAnalyzer::new(self.dag.clone())
            .with_default_duration(self.default_duration);
        for (name, &duration) in &self.activity_durations {
            cp_analyzer = cp_analyzer.mock_duration(name, duration);
        }
        let critical_path = cp_analyzer.analyze();

        // Run Profiler
        let mut profiler =
            DagProfiler::new(self.dag.clone()).with_default_duration(self.default_duration);
        for (name, &duration) in &self.activity_durations {
            profiler = profiler.mock_duration(name, duration);
        }
        let profile = profiler.profile();

        // Run Simulator
        let mut simulator = DagSimulator::new(self.dag.clone());
        for (name, mock) in &self.activity_mocks {
            let mock_clone = Arc::clone(mock);
            simulator = simulator.mock_activity(name, move || mock_clone());
        }
        let simulation = simulator.run();

        DagInsightReport {
            warnings,
            critical_path,
            profile,
            simulation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::policy::TaskStatus;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_insight_report_linear_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a);
        let c = builder.activity(activity_c).upstream(&b);
        let dag = builder.build().unwrap();

        let insight = DagInsight::new(dag)
            .with_default_duration(Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(5))
            .mock_activity("activity_b", || Err("Failure!".to_string()));

        let report = insight.analyze();

        // Warnings: no retry/timeout configured
        assert!(!report.warnings.is_empty());

        // Critical path: a(2) -> b(5) -> c(2) = 9
        assert_eq!(report.critical_path.total_duration, Duration::from_secs(9));
        assert_eq!(
            report.critical_path.path_names,
            vec!["activity_a", "activity_b", "activity_c"]
        );

        // Profiler
        assert_eq!(report.profile.total_duration, Duration::from_secs(9));
        assert_eq!(report.profile.peak_concurrency, 1);

        // Simulator
        // A succeeds, B fails, C is skipped
        assert_eq!(
            report.simulation.get_status(a.index()),
            Some(&TaskStatus::Succeeded)
        );
        assert_eq!(
            report.simulation.get_status(b.index()),
            Some(&TaskStatus::Failed)
        );
        assert_eq!(
            report.simulation.get_status(c.index()),
            Some(&TaskStatus::Skipped)
        );
    }
}
