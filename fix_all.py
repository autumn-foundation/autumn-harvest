import os
import re

files_to_fix = {
    'autumn-harvest/src/batch.rs': [
        (
            "        pub id: String,\n        pub action: String,\n        pub filter: Value,\n        pub signal_name: Option<String>,\n        pub status: String,\n        pub total: i64,\n        pub completed: i64,\n        pub failed: i64,\n        pub started_at: Option<DateTime<Utc>>,\n        pub completed_at: Option<DateTime<Utc>>,\n        pub errors: Vec<BatchTargetError>,\n        pub created_at: DateTime<Utc>,\n        pub created_by: Option<String>,",
            "        /// The job ID.\n        pub id: String,\n        /// The action to perform.\n        pub action: String,\n        /// The filter to match targets.\n        pub filter: Value,\n        /// The signal name if the action is signal.\n        pub signal_name: Option<String>,\n        /// The current status.\n        pub status: String,\n        /// The total number of targets.\n        pub total: i64,\n        /// The number of completed targets.\n        pub completed: i64,\n        /// The number of failed targets.\n        pub failed: i64,\n        /// The start time.\n        pub started_at: Option<DateTime<Utc>>,\n        /// The completion time.\n        pub completed_at: Option<DateTime<Utc>>,\n        /// Any errors encountered.\n        pub errors: Vec<BatchTargetError>,\n        /// The creation time.\n        pub created_at: DateTime<Utc>,\n        /// The creator of the job.\n        pub created_by: Option<String>,"
        ),
        (
            "        pub fn from_row(row: BatchJob) -> Self {",
            "        /// Convert a database row to the API response format.\n        pub fn from_row(row: BatchJob) -> Self {"
        ),
        (
            "    pub async fn mark_failed(\n        conn: &mut AsyncPgConnection,\n        id: Uuid,\n        reason: &str,\n    ) -> HarvestResult<()> {",
            "    /// Mark a batch job as failed with the given reason.\n    pub async fn mark_failed(\n        conn: &mut AsyncPgConnection,\n        id: Uuid,\n        reason: &str,\n    ) -> HarvestResult<()> {"
        )
    ],
    'autumn-harvest/src/build_routing.rs': [
        (
            "    pub id: Uuid,\n    pub queue_name: String,\n    pub build_id: String,\n    pub deployment_name: Option<String>,\n    pub created_at: DateTime<Utc>,\n    pub updated_at: DateTime<Utc>,",
            "    /// The rule ID.\n    pub id: Uuid,\n    /// The queue name this rule applies to.\n    pub queue_name: String,\n    /// The target build ID.\n    pub build_id: String,\n    /// The deployment name.\n    pub deployment_name: Option<String>,\n    /// The creation time.\n    pub created_at: DateTime<Utc>,\n    /// The last updated time.\n    pub updated_at: DateTime<Utc>,"
        ),
        (
            "    pub id: Uuid,\n",
            "    /// The rule ID.\n    pub id: Uuid,\n"
        ),
        (
            "    pub declared_at: DateTime<Utc>,\n",
            "    /// When the build was declared.\n    pub declared_at: DateTime<Utc>,\n"
        ),
        (
            "    pub build_id: String,\n",
            "    /// The build ID.\n    pub build_id: String,\n"
        )
    ],
    'autumn-harvest/src/builder.rs': [
        (
            "pub struct HarvestBuilder {",
            "/// The primary builder for configuring a new harvest engine.\npub struct HarvestBuilder {"
        ),
        (
            "    pub const fn payload_codecs(&self) -> &PayloadCodecs {",
            "    /// Returns a reference to the configured payload codecs.\n    pub const fn payload_codecs(&self) -> &PayloadCodecs {"
        ),
        (
            "    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {",
            "    /// Sets the telemetry configuration.\n    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {"
        )
    ],
    'autumn-harvest/src/context.rs': [
        (
            "    pub fn for_replay_with_state_and_history_policy(\n        exec_id: ExecutionId,\n        events: Vec<WorkflowEvent>,\n        state: SharedState,\n        history_policy: WorkflowHistoryPolicy,\n    ) -> Self {",
            "    /// Creates a workflow context for replay with shared state and a custom history policy.\n    pub fn for_replay_with_state_and_history_policy(\n        exec_id: ExecutionId,\n        events: Vec<WorkflowEvent>,\n        state: SharedState,\n        history_policy: WorkflowHistoryPolicy,\n    ) -> Self {"
        )
    ]
}

for path, rep in files_to_fix.items():
    if os.path.exists(path):
        with open(path, 'r') as f:
            c = f.read()
        for old, new in rep:
            c = c.replace(old, new)
        with open(path, 'w') as f:
            f.write(c)
