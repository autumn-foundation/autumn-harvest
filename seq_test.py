import sys

# Test writing the rule logic
code = """
pub struct SequentialActivitiesRule {
    threshold: usize,
}

impl SequentialActivitiesRule {
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
                    "Found {} activities executed sequentially. Consider running them concurrently using `futures::join_all` or a DAG.",
                    max_sequential
                ),
            });
        }

        warnings
    }
}
"""
