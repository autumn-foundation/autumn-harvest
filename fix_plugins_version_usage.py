import re

with open("autumn-harvest-plugin/src/version_usage.rs", "r") as f:
    content = f.read()

replacements = [
    (
        "pub enum VersionUsageShardInspectionStatus {",
        "/// Status of inspecting a specific shard for version usage.\npub enum VersionUsageShardInspectionStatus {"
    ),
    (
        "    Inspected,",
        "    /// The shard was successfully inspected.\n    Inspected,"
    ),
    (
        "    Unavailable,",
        "    /// The shard was unavailable or failed to respond.\n    Unavailable,"
    ),
    (
        "pub struct VersionUsageReport {",
        "/// Report representing version usage across executions.\npub struct VersionUsageReport {"
    ),
    (
        "    pub status: VersionUsageReportStatus,",
        "    /// Overarching status of the report generation.\n    pub status: VersionUsageReportStatus,"
    ),
    (
        "    pub observed_at: DateTime<Utc>,",
        "    /// Timestamp when this report was observed.\n    pub observed_at: DateTime<Utc>,"
    ),
    (
        "    pub filters: VersionUsageReportFilters,",
        "    /// Filters used to generate this report.\n    pub filters: VersionUsageReportFilters,"
    ),
    (
        "    pub items: Vec<VersionUsageReportRow>,",
        "    /// The individual usage records returned by the report.\n    pub items: Vec<VersionUsageReportRow>,"
    ),
    (
        "    pub shards: Vec<VersionUsageShardInspection>,",
        "    /// Details regarding which shards were inspected and their status.\n    pub shards: Vec<VersionUsageShardInspection>,"
    ),
    (
        "pub struct VersionUsageReportFilters {",
        "/// Filters used when querying for version usage reports.\npub struct VersionUsageReportFilters {"
    ),
    (
        "    pub workflow_name: Option<String>,",
        "    /// Optional filter for a specific workflow name.\n    pub workflow_name: Option<String>,"
    ),
    (
        "    pub change_id: Option<String>,",
        "    /// Optional filter for a specific change ID.\n    pub change_id: Option<String>,"
    ),
    (
        "    pub recorded_version: Option<u32>,",
        "    /// Optional filter for a specific recorded version.\n    pub recorded_version: Option<u32>,"
    ),
    (
        "    pub state_group: VersionExecutionStateGroup,",
        "    /// Defines which execution states are included (e.g., active, terminal).\n    pub state_group: VersionExecutionStateGroup,"
    ),
    (
        "    pub shard_id: Option<i32>,",
        "    /// Optional filter to restrict the query to a specific shard ID.\n    pub shard_id: Option<i32>,"
    ),
    (
        "pub struct VersionUsageReportRow {",
        "/// A single row within a version usage report representing usage for a specific workflow, change, and version.\npub struct VersionUsageReportRow {"
    ),
    (
        "    pub workflow_name: String,",
        "    /// The name of the workflow.\n    pub workflow_name: String,"
    ),
    (
        "    pub change_id: String,",
        "    /// The change ID associated with the version usage.\n    pub change_id: String,"
    ),
    (
        "    pub recorded_version: u32,",
        "    /// The recorded version number.\n    pub recorded_version: u32,"
    ),
    (
        "    pub active_executions: i64,",
        "    /// Number of active executions using this version.\n    pub active_executions: i64,"
    ),
    (
        "    pub terminal_executions: i64,",
        "    /// Number of terminal executions using this version.\n    pub terminal_executions: i64,"
    ),
    (
        "    pub oldest_matching_started_at: DateTime<Utc>,",
        "    /// The start time of the oldest execution matching this version.\n    pub oldest_matching_started_at: DateTime<Utc>,"
    ),
    (
        "    pub newest_matching_started_at: DateTime<Utc>,",
        "    /// The start time of the newest execution matching this version.\n    pub newest_matching_started_at: DateTime<Utc>,"
    ),
    (
        "    pub oldest_matching_execution_age_secs: i64,",
        "    /// Age in seconds of the oldest matching execution.\n    pub oldest_matching_execution_age_secs: i64,"
    ),
    (
        "    pub newest_matching_execution_age_secs: i64,",
        "    /// Age in seconds of the newest matching execution.\n    pub newest_matching_execution_age_secs: i64,"
    ),
    (
        "    pub shard_coverage: VersionUsageShardCoverage,",
        "    /// Details on the shard coverage for this row.\n    pub shard_coverage: VersionUsageShardCoverage,"
    ),
    (
        "pub struct VersionUsageShardCoverage {",
        "/// Represents which shards were inspected, matched, or unavailable for a specific usage report row.\npub struct VersionUsageShardCoverage {"
    ),
    (
        "    pub inspected_shards: Vec<i32>,",
        "    /// List of shard IDs that were successfully inspected.\n    pub inspected_shards: Vec<i32>,"
    ),
    (
        "    pub matched_shards: Vec<i32>,",
        "    /// List of shard IDs where this version usage was matched.\n    pub matched_shards: Vec<i32>,"
    ),
    (
        "    pub unavailable_shards: Vec<i32>,",
        "    /// List of shard IDs that were unavailable during inspection.\n    pub unavailable_shards: Vec<i32>,"
    ),
    (
        "pub struct VersionUsageShardInspection {",
        "/// Details regarding the inspection of a single shard.\npub struct VersionUsageShardInspection {"
    ),
    (
        "    pub shard_id: i32,",
        "    /// The ID of the inspected shard.\n    pub shard_id: i32,"
    ),
    (
        "    pub status: VersionUsageShardInspectionStatus,",
        "    /// The inspection status of the shard.\n    pub status: VersionUsageShardInspectionStatus,"
    ),
    (
        "    pub matched_groups: Option<usize>,",
        "    /// The number of matched groups found in the shard, if successfully inspected.\n    pub matched_groups: Option<usize>,"
    ),
    (
        "    pub error: Option<String>,",
        "    /// Any error message encountered during the shard's inspection.\n    pub error: Option<String>,"
    ),
    (
        "pub enum VersionUsageReportStatus {",
        "/// Status of the version usage report.\npub enum VersionUsageReportStatus {"
    ),
    (
        "    Complete,",
        "    /// Report is complete.\n    Complete,"
    ),
    (
        "    NoMatches,",
        "    /// No matches were found for the report.\n    NoMatches,"
    ),
    (
        "    Partial,",
        "    /// The report is partial (e.g., some shards were unavailable).\n    Partial,"
    ),
]

for old, new in replacements:
    content = content.replace(old, new)

with open("autumn-harvest-plugin/src/version_usage.rs", "w") as f:
    f.write(content)
