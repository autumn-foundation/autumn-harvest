import sys

content = """
    Analyze {
        /// Workflow execution ID.
        execution_id: String,
        /// Optional rules to apply (comma-separated, e.g. "SuspiciousTimer,ExcessiveRetries").
        /// If omitted, applies all default rules.
        #[arg(long)]
        rules: Option<String>,
    },
"""

print(content)
