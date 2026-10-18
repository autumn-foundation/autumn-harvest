//! Every `WorkflowCommand`, and what the `SQLite` backend does with it.
//!
//! [`kind_of`] and [`support`] match every variant with no wildcard arm. A new
//! variant is therefore a compile error here, so its author must classify it.

use autumn_harvest::context::WorkflowCommand;

/// Declare [`CommandKind`] and [`ALL`] from one list, so the two agree.
macro_rules! command_kinds {
    ($($kind:ident),* $(,)?) => {
        /// The variant of a [`WorkflowCommand`], without its payload.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum CommandKind {
            $($kind),*
        }

        /// Every kind, in declaration order.
        pub const ALL: &[CommandKind] = &[$(CommandKind::$kind),*];
    };
}

command_kinds! {
    ScheduleActivity,
    WaitForActivity,
    StartTimer,
    StartChildWorkflow,
    RecordMarker,
    RecordSideEffect,
    ScheduleExternalActivity,
    WaitForSignal,
    Complete,
    Fail,
    ContinueAsNew,
    RunLocalActivity,
    RecordUpdateResult,
    UpsertSearchAttributes,
    SetCurrentDetails,
    PublishProgress,
    PublishDurableProgress,
    RecordLog,
    SpawnDetachedChildWorkflow,
    SignalExternalWorkflow,
    RequestCancelExternalWorkflow,
    AwaitExternalWorkflow,
    CancelRaceLosers,
    ArmTimer,
    CancelTimer,
    AcquireMutex,
    ReleaseMutex,
}

/// The kind of `command`.
#[allow(dead_code)]
pub const fn kind_of(command: &WorkflowCommand) -> CommandKind {
    match command {
        WorkflowCommand::ScheduleActivity { .. } => CommandKind::ScheduleActivity,
        WorkflowCommand::WaitForActivity { .. } => CommandKind::WaitForActivity,
        WorkflowCommand::StartTimer { .. } => CommandKind::StartTimer,
        WorkflowCommand::StartChildWorkflow { .. } => CommandKind::StartChildWorkflow,
        WorkflowCommand::RecordMarker { .. } => CommandKind::RecordMarker,
        WorkflowCommand::RecordSideEffect { .. } => CommandKind::RecordSideEffect,
        WorkflowCommand::ScheduleExternalActivity { .. } => CommandKind::ScheduleExternalActivity,
        WorkflowCommand::WaitForSignal { .. } => CommandKind::WaitForSignal,
        WorkflowCommand::Complete { .. } => CommandKind::Complete,
        WorkflowCommand::Fail { .. } => CommandKind::Fail,
        WorkflowCommand::ContinueAsNew { .. } => CommandKind::ContinueAsNew,
        WorkflowCommand::RunLocalActivity { .. } => CommandKind::RunLocalActivity,
        WorkflowCommand::RecordUpdateResult { .. } => CommandKind::RecordUpdateResult,
        WorkflowCommand::UpsertSearchAttributes { .. } => CommandKind::UpsertSearchAttributes,
        WorkflowCommand::SetCurrentDetails { .. } => CommandKind::SetCurrentDetails,
        WorkflowCommand::PublishProgress { .. } => CommandKind::PublishProgress,
        WorkflowCommand::PublishDurableProgress { .. } => CommandKind::PublishDurableProgress,
        WorkflowCommand::RecordLog { .. } => CommandKind::RecordLog,
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => {
            CommandKind::SpawnDetachedChildWorkflow
        }
        WorkflowCommand::SignalExternalWorkflow { .. } => CommandKind::SignalExternalWorkflow,
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => {
            CommandKind::RequestCancelExternalWorkflow
        }
        WorkflowCommand::AwaitExternalWorkflow { .. } => CommandKind::AwaitExternalWorkflow,
        WorkflowCommand::CancelRaceLosers { .. } => CommandKind::CancelRaceLosers,
        WorkflowCommand::ArmTimer { .. } => CommandKind::ArmTimer,
        WorkflowCommand::CancelTimer { .. } => CommandKind::CancelTimer,
        WorkflowCommand::AcquireMutex { .. } => CommandKind::AcquireMutex,
        WorkflowCommand::ReleaseMutex { .. } => CommandKind::ReleaseMutex,
    }
}

/// What the `SQLite` backend does with a command kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Support {
    /// `SQLite` runs it. A scenario must emit it on both backends and give
    /// the same result.
    Accepted,
    /// `SQLite` fails the run with `UnsupportedFeature`. A rejection scenario
    /// must show it.
    Rejected,
    /// No `WorkflowContext` method emits it.
    NotEmitted,
}

/// The `SQLite` support of `kind`.
///
/// The match has no wildcard arm, so a new kind is a compile error here.
pub const fn support(kind: CommandKind) -> Support {
    use CommandKind as K;
    match kind {
        K::ScheduleActivity
        | K::WaitForActivity
        | K::StartTimer
        | K::StartChildWorkflow
        | K::RecordMarker
        | K::RecordSideEffect
        | K::WaitForSignal
        | K::ContinueAsNew
        | K::RunLocalActivity
        | K::RecordUpdateResult
        | K::UpsertSearchAttributes
        | K::SetCurrentDetails
        | K::PublishProgress
        | K::PublishDurableProgress
        | K::RecordLog
        | K::SpawnDetachedChildWorkflow
        | K::SignalExternalWorkflow
        | K::RequestCancelExternalWorkflow
        | K::AwaitExternalWorkflow
        | K::CancelRaceLosers
        | K::ArmTimer
        | K::CancelTimer
        | K::AcquireMutex
        | K::ReleaseMutex => Support::Accepted,
        K::ScheduleExternalActivity => Support::Rejected,
        K::Complete | K::Fail => Support::NotEmitted,
    }
}

/// The accepted kinds that `SQLite` rejects when the last cycle starts them
/// and the run returns without an await.
///
/// `persist_terminal_pending_commands` has no arm for them. Each one needs a
/// rejection scenario. A guard checks this list against that function.
pub const REJECTED_IN_LAST_CYCLE: &[CommandKind] = &[
    CommandKind::ScheduleActivity,
    CommandKind::StartTimer,
    CommandKind::WaitForSignal,
];

/// The accepted kinds that a replay does not emit, so the debugger cannot
/// see them. Each entry names the event that shows the command instead, if
/// one exists, and the reason.
pub const NOT_SEEN_IN_REPLAY: &[(CommandKind, Option<&str>, &str)] = &[
    (
        CommandKind::RecordLog,
        None,
        "The logger skips a line while it replays. The line writes no event.",
    ),
    (
        CommandKind::RecordUpdateResult,
        Some("UpdateCompleted"),
        "An update handler runs outside the replay of the workflow body.",
    ),
];
