import re

with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

analyze_cmd = """
    /// Analyze a workflow execution history for anti-patterns and performance issues.
    Analyze {
        /// Workflow execution ID.
        execution_id: String,
        /// Optional rules to apply (comma-separated).
        #[arg(long)]
        rules: Option<String>,
    },
"""

content = re.sub(
    r"(enum HistoryCommand\s*\{\s*)(/// Export one workflow execution history\.)",
    r"\g<1>" + analyze_cmd + r"    \g<2>",
    content
)

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
