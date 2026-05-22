import re

def fix_reset_rs():
    with open('autumn-harvest/src/reset.rs', 'r') as f:
        content = f.read()

    replacements = [
        (
            "    pub const fn as_str(self) -> &'static str {",
            "    /// Returns the string representation of the reset signal reapply policy.\n    pub const fn as_str(self) -> &'static str {"
        ),
        (
            "pub struct WorkflowResetRequest {\n    pub reset_to_event_id: i64,\n    pub reason: String,\n    pub operator_id: String,\n    #[serde(default)]\n    pub signal_reapply: ResetSignalReapplyPolicy,\n}",
            "pub struct WorkflowResetRequest {\n    /// The event ID to reset to.\n    pub reset_to_event_id: i64,\n    /// The reason for the reset.\n    pub reason: String,\n    /// The operator performing the reset.\n    pub operator_id: String,\n    /// The policy for reapplying signals.\n    #[serde(default)]\n    pub signal_reapply: ResetSignalReapplyPolicy,\n}"
        ),
        (
            "pub struct ResetUnresolvedSideEffect {\n    pub kind: String,\n    pub side_effect_id: String,\n    pub name: Option<String>,\n    pub scheduled_event_id: i64,\n}",
            "pub struct ResetUnresolvedSideEffect {\n    /// The kind of side effect.\n    pub kind: String,\n    /// The ID of the side effect.\n    pub side_effect_id: String,\n    /// The name of the side effect.\n    pub name: Option<String>,\n    /// The event ID when the side effect was scheduled.\n    pub scheduled_event_id: i64,\n}"
        ),
        (
            "pub struct ResetPlan {\n    pub reset_to_event_id: i64,\n    pub events_carried_over: usize,\n    pub unresolved_side_effects: Vec<ResetUnresolvedSideEffect>,\n    pub nearest_valid_before: Option<i64>,\n    pub nearest_valid_after: Option<i64>,\n    pub source_tasks_to_cancel: usize,\n    pub source_timers_to_remove: usize,\n    pub source_signals_to_drop: usize,\n    pub source_signals_to_buffer: usize,\n}",
            "pub struct ResetPlan {\n    /// The event ID to reset to.\n    pub reset_to_event_id: i64,\n    /// The number of events to carry over.\n    pub events_carried_over: usize,\n    /// Unresolved side effects at the reset point.\n    pub unresolved_side_effects: Vec<ResetUnresolvedSideEffect>,\n    /// The nearest valid boundary before the requested event, if any.\n    pub nearest_valid_before: Option<i64>,\n    /// The nearest valid boundary after the requested event, if any.\n    pub nearest_valid_after: Option<i64>,\n    /// The number of source tasks to cancel.\n    pub source_tasks_to_cancel: usize,\n    /// The number of source timers to remove.\n    pub source_timers_to_remove: usize,\n    /// The number of source signals to drop.\n    pub source_signals_to_drop: usize,\n    /// The number of source signals to buffer.\n    pub source_signals_to_buffer: usize,\n}"
        ),
        (
            "pub struct ResetResult {\n    pub new_exec_id: ExecutionId,\n    pub reset_from_exec_id: ExecutionId,\n    pub reset_to_event_id: i64,\n    pub events_carried_over: usize,\n    pub source_tasks_cancelled: usize,\n    pub source_timers_removed: usize,\n    pub source_signals_dropped: usize,\n    pub source_signals_buffered: usize,\n}",
            "pub struct ResetResult {\n    /// The execution ID of the new reset execution.\n    pub new_exec_id: ExecutionId,\n    /// The execution ID of the original source execution.\n    pub reset_from_exec_id: ExecutionId,\n    /// The event ID that was reset to.\n    pub reset_to_event_id: i64,\n    /// The number of events carried over.\n    pub events_carried_over: usize,\n    /// The number of source tasks cancelled.\n    pub source_tasks_cancelled: usize,\n    /// The number of source timers removed.\n    pub source_timers_removed: usize,\n    /// The number of source signals dropped.\n    pub source_signals_dropped: usize,\n    /// The number of source signals buffered.\n    pub source_signals_buffered: usize,\n}"
        )
    ]
    for old, new in replacements:
        content = content.replace(old, new)

    with open('autumn-harvest/src/reset.rs', 'w') as f:
        f.write(content)

fix_reset_rs()
