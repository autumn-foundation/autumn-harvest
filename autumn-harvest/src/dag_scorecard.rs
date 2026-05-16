//! DAG Scorecard feature.
//!
//! Evaluates a DAG definition across multiple dimensions (linting, profiling, critical path)
//! to produce a unified health and performance grade.

use crate::critical_path::{CriticalPathAnalyzer, CriticalPathResult};
use crate::dag::DagDefinition;
use crate::dag_linter::{DagLinter, DagWarning};
use crate::dag_profiler::{DagProfile, DagProfiler};
use crate::info::ActivityInfo;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the DAG scorecard evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagScorecardConfig {
    /// The maximum acceptable duration for the DAG before it incurs a score penalty.
    pub target_duration: Duration,
    /// The maximum acceptable peak concurrency before it incurs a score penalty.
    pub target_concurrency: usize,
    /// Base duration to use for activities that don't have a configured or mocked duration.
    pub default_activity_duration: Duration,
}

impl Default for DagScorecardConfig {
    fn default() -> Self {
        Self {
            target_duration: Duration::from_secs(3600), // 1 hour
            target_concurrency: 10,
            default_activity_duration: Duration::from_secs(1),
        }
    }
}

/// The result of a DAG scorecard evaluation.
#[derive(Debug, Clone)]
pub struct DagScorecardResult {
    /// The overall letter grade ('A', 'B', 'C', 'D', 'F').
    pub grade: char,
    /// The numerical score (0-100).
    pub score: u8,
    /// Any lint warnings discovered during evaluation.
    pub lint_warnings: Vec<DagWarning>,
    /// The simulated execution profile.
    pub profile: DagProfile,
    /// The critical path analysis result.
    pub critical_path: CriticalPathResult,
    /// Actionable recommendations to improve the DAG.
    pub recommendations: Vec<String>,
}

/// Evaluator that produces a `DagScorecardResult`.
pub struct DagScorecard<'a> {
    dag: &'a DagDefinition,
    config: DagScorecardConfig,
    linter: DagLinter,
    activity_durations: HashMap<String, Duration>,
    activities: HashMap<String, ActivityInfo>,
}

impl<'a> DagScorecard<'a> {
    /// Create a new scorecard evaluator for the given DAG.
    #[must_use]
    pub fn new(dag: &'a DagDefinition) -> Self {
        Self {
            dag,
            config: DagScorecardConfig::default(),
            linter: DagLinter::new(),
            activity_durations: HashMap::new(),
            activities: HashMap::new(),
        }
    }

    /// Set the configuration for the scorecard.
    #[must_use]
    pub const fn with_config(mut self, config: DagScorecardConfig) -> Self {
        self.config = config;
        self
    }

    /// Provide an instance of a linter to use.
    #[must_use]
    pub fn with_linter(mut self, linter: DagLinter) -> Self {
        self.linter = linter;
        self
    }

    /// Provide mocked durations for specific activities.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Provide registered activity definitions for linting.
    #[must_use]
    pub fn with_activities(mut self, activities: HashMap<String, ActivityInfo>) -> Self {
        self.activities = activities;
        self
    }

    /// Evaluate the DAG and produce the scorecard result.
    #[must_use]
    pub fn evaluate(&self) -> DagScorecardResult {
        // 1. Run Linter
        let lint_warnings = self.linter.analyze(self.dag, &self.activities);

        // 2. Run Profiler
        let mut profiler = DagProfiler::new(self.dag.clone())
            .with_default_duration(self.config.default_activity_duration);
        for (name, duration) in &self.activity_durations {
            profiler = profiler.mock_duration(name, *duration);
        }
        let profile = profiler.profile();

        // 3. Run Critical Path
        let mut cp_analyzer = CriticalPathAnalyzer::new(self.dag.clone())
            .with_default_duration(self.config.default_activity_duration);
        for (name, duration) in &self.activity_durations {
            cp_analyzer = cp_analyzer.mock_duration(name, *duration);
        }
        let critical_path = cp_analyzer.analyze();

        // 4. Calculate Score
        let mut score: i32 = 100;
        let mut recommendations = Vec::new();

        // Penalty for lint warnings
        if !lint_warnings.is_empty() {
            let penalty = i32::try_from(lint_warnings.len()).unwrap_or(i32::MAX / 5) * 5;
            score -= penalty;
            recommendations.push(format!("Fix {} lint warning(s).", lint_warnings.len()));
        }

        // Penalty for duration
        if profile.total_duration > self.config.target_duration {
            score -= 15;
            recommendations.push(format!(
                "DAG duration ({:?}) exceeds target duration ({:?}). Consider optimizing activities on the critical path: {:?}",
                profile.total_duration, self.config.target_duration, critical_path.path_names
            ));
        }

        // Penalty for concurrency
        if profile.peak_concurrency > self.config.target_concurrency {
            score -= 10;
            recommendations.push(format!(
                "Peak concurrency ({}) exceeds target ({}). This may cause resource exhaustion.",
                profile.peak_concurrency, self.config.target_concurrency
            ));
        }

        let final_score = u8::try_from(score.clamp(0, 100)).unwrap_or(0);
        let grade = match final_score {
            90..=100 => 'A',
            80..=89 => 'B',
            70..=79 => 'C',
            60..=69 => 'D',
            _ => 'F',
        };

        DagScorecardResult {
            grade,
            score: final_score,
            lint_warnings,
            profile,
            critical_path,
            recommendations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_linter::MissingTimeoutRule;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_perfect_scorecard() {
        let mut builder = DagBuilder::new();
        let _a = builder
            .activity(activity_a)
            .start_to_close(Duration::from_secs(300))
            .retry(crate::policy::RetryPolicy::exponential(
                3,
                Duration::from_secs(1),
            ));
        let dag = builder.build().unwrap();

        let linter = DagLinter::new().with_rule(MissingTimeoutRule::new());

        let scorecard = DagScorecard::new(&dag).with_linter(linter).evaluate();

        assert_eq!(scorecard.score, 100);
        assert_eq!(scorecard.grade, 'A');
        assert!(scorecard.lint_warnings.is_empty());
        assert!(scorecard.recommendations.is_empty());
    }

    #[test]
    fn test_scorecard_with_penalties() {
        let mut builder = DagBuilder::new();

        // Highly concurrent and long duration without timeouts
        let start = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&start);
        let _c = builder.activity(activity_c).upstream(&start);
        let dag = builder.build().unwrap();

        let linter = DagLinter::new().with_rule(MissingTimeoutRule::new());
        let config = DagScorecardConfig {
            target_duration: Duration::from_secs(5),
            target_concurrency: 1,
            default_activity_duration: Duration::from_secs(10), // Will exceed duration target
        };

        let scorecard = DagScorecard::new(&dag)
            .with_config(config)
            .with_linter(linter)
            .evaluate();

        // 3 missing timeout warnings -> -15 points
        // duration (20s) > target (5s) -> -15 points
        // concurrency (2) > target (1) -> -10 points
        // Total penalty: 40 points
        // Expected score: 60 (Grade D)

        assert_eq!(scorecard.score, 60);
        assert_eq!(scorecard.grade, 'D');
        assert_eq!(scorecard.lint_warnings.len(), 3);
        assert_eq!(scorecard.recommendations.len(), 3); // 1 for lints, 1 for duration, 1 for concurrency
    }
}
