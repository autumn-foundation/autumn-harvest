import re

def fix_external_task_rs():
    with open('autumn-harvest/src/external_task.rs', 'r') as f:
        content = f.read()

    replacements = [
        (
            "pub struct ExternalTaskListFilters {\n    pub states: Vec<String>,\n    pub workflow_name: Option<String>,\n    pub execution_id: Option<ExecutionId>,\n    pub activity_name: Option<String>,\n    pub token: Option<ExternalActivityToken>,\n    pub shard_id: Option<i32>,\n    pub due_before: Option<chrono::DateTime<Utc>>,\n    pub updated_before: Option<chrono::DateTime<Utc>>,\n    pub limit: i64,\n}",
            "pub struct ExternalTaskListFilters {\n    /// Filter by state.\n    pub states: Vec<String>,\n    /// Filter by workflow name.\n    pub workflow_name: Option<String>,\n    /// Filter by execution ID.\n    pub execution_id: Option<ExecutionId>,\n    /// Filter by activity name.\n    pub activity_name: Option<String>,\n    /// Filter by external activity token.\n    pub token: Option<ExternalActivityToken>,\n    /// Filter by shard ID.\n    pub shard_id: Option<i32>,\n    /// Filter by due before time.\n    pub due_before: Option<chrono::DateTime<Utc>>,\n    /// Filter by updated before time.\n    pub updated_before: Option<chrono::DateTime<Utc>>,\n    /// Maximum number of records to return.\n    pub limit: i64,\n}"
        ),
        (
            "    pub const fn with_limit(mut self, limit: i64) -> Self {",
            "    /// Updates the limit and returns `Self`.\n    pub const fn with_limit(mut self, limit: i64) -> Self {"
        ),
        (
            "pub struct ExternalTaskRow {\n    pub token: ExternalActivityToken,\n    pub workflow_exec_id: ExecutionId,\n    pub workflow_id: String,\n    pub workflow_name: String,\n    pub activity_id: ActivityExecId,\n    pub activity_name: String,\n    pub state: String,\n    pub created_at: chrono::DateTime<Utc>,\n    pub updated_at: chrono::DateTime<Utc>,\n    pub deadline_at: chrono::DateTime<Utc>,\n    pub shard_id: i32,\n}",
            "pub struct ExternalTaskRow {\n    /// The external activity token.\n    pub token: ExternalActivityToken,\n    /// The workflow execution ID.\n    pub workflow_exec_id: ExecutionId,\n    /// The workflow ID.\n    pub workflow_id: String,\n    /// The workflow name.\n    pub workflow_name: String,\n    /// The activity execution ID.\n    pub activity_id: ActivityExecId,\n    /// The activity name.\n    pub activity_name: String,\n    /// The state of the external task.\n    pub state: String,\n    /// The creation time.\n    pub created_at: chrono::DateTime<Utc>,\n    /// The updated time.\n    pub updated_at: chrono::DateTime<Utc>,\n    /// The deadline time.\n    pub deadline_at: chrono::DateTime<Utc>,\n    /// The shard ID.\n    pub shard_id: i32,\n}"
        )
    ]
    for old, new in replacements:
        content = content.replace(old, new)

    with open('autumn-harvest/src/external_task.rs', 'w') as f:
        f.write(content)

fix_external_task_rs()
