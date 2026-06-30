import re

with open("autumn-harvest/src/analyzer.rs", "r") as f:
    content = f.read()

# Add the rule back
new_rule = """
/// Flags workflows that execute multiple activities sequentially without concurrency.
///
/// Sequential execution of many activities often indicates a missed opportunity
/// to use `futures::join_all` or a DAG to reduce total workflow latency.
pub struct SequentialActivitiesRule {
    threshold: usize,
}

impl SequentialActivitiesRule {
    /// Create a new `SequentialActivitiesRule` with the given sequential threshold.
    #[must_use]
    pub const fn new(threshold: usize) -> Self {
        Self { threshold }
    }
}

impl AnalyzerRule for SequentialActivitiesRule {
    fn name(&self) -> &'static str {
        "SequentialActivities"
    }

    fn analyze(&self, history: &[WorkflowEvent]) -> Vec<AnalyzerWarning> {
        let mut warnings = Vec::new();
        let mut sequential_count = 0;
        let mut active_activities = 0;
        let mut max_sequential = 0;

        for event in history {
            match event {
                WorkflowEvent::ActivityScheduled { .. } | WorkflowEvent::LocalActivityScheduled { .. } => {
                    active_activities += 1;
                    if active_activities > 1 {
                        // Concurrency detected! Reset sequential count.
                        sequential_count = 0;
                    }
                }
                WorkflowEvent::ActivityCompleted { .. } | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. } | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. } | WorkflowEvent::LocalActivityExhausted { .. } => {
                    if active_activities > 0 {
                        active_activities -= 1;
                        if active_activities == 0 {
                            sequential_count += 1;
                            max_sequential = max_sequential.max(sequential_count);
                        }
                    }
                }
                _ => {}
            }
        }

        if max_sequential >= self.threshold {
            warnings.push(AnalyzerWarning {
                rule_name: self.name().to_string(),
                message: format!(
                    "Found {max_sequential} activities executed strictly sequentially. Consider running them concurrently using `futures::join_all` or a DAG."
                ),
            });
        }

        warnings
    }
}
"""

with_defaults = """
    /// Create a new analyzer with the default set of rules.
    #[must_use]
    pub fn with_default_rules() -> Self {
        Self::new()
            .with_rule(ExcessiveRetriesRule::new(3))
            .with_rule(SuspiciousTimerRule::new())
            .with_rule(LargePayloadRule::new(1024 * 1024))
            .with_rule(SequentialActivitiesRule::new(5))
    }
"""

# Check if the rule is missing
if "pub struct SequentialActivitiesRule" not in content:
    content = content.replace("#[cfg(test)]", new_rule + "\n#[cfg(test)]")

if "pub fn with_default_rules" not in content:
    content = re.sub(
        r"(pub fn new\(\) -> Self \{\n\s*Self \{ rules: Vec::new\(\) \}\n\s*\})",
        r"\g<1>\n" + with_defaults,
        content
    )

with open("autumn-harvest/src/analyzer.rs", "w") as f:
    f.write(content)
