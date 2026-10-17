//! Every `WorkflowCommand`, and what the `SQLite` backend does with it.
//!
//! [`kind_of`] matches every variant with no wildcard arm. A new variant is
//! therefore a compile error here, so its author must classify it.

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
    /// `SQLite` runs it. The suite must show the same result as Postgres.
    ///
    /// `witness` is the event type that the command writes on both backends.
    /// A scenario that covers the kind must hold that event in both
    /// histories. `None` means the command writes no event. Such a command
    /// is covered by the outcome alone.
    Accepted { witness: Option<&'static str> },
    /// `SQLite` fails the run with `UnsupportedFeature`. A rejection scenario
    /// must show it.
    Rejected,
    /// No `WorkflowContext` method emits it. The executor turns a return
    /// value into a terminal outcome, not into this command.
    NotEmitted,
}

/// The `SQLite` support of `kind`.
///
/// The match has no wildcard arm, so a new kind is a compile error here.
pub const fn support(kind: CommandKind) -> Support {
    use CommandKind as K;
    const fn accepted(witness: &'static str) -> Support {
        Support::Accepted {
            witness: Some(witness),
        }
    }
    const NO_EVENT: Support = Support::Accepted { witness: None };
    match kind {
        K::ScheduleActivity => accepted("ActivityScheduled"),
        K::StartTimer | K::ArmTimer => accepted("TimerStarted"),
        K::StartChildWorkflow => accepted("ChildWorkflowStarted"),
        K::RecordMarker => accepted("MarkerRecorded"),
        K::RecordSideEffect => accepted("SideEffectRecorded"),
        K::WaitForSignal => accepted("SignalReceived"),
        K::ContinueAsNew => accepted("WorkflowContinuedAsNew"),
        K::RunLocalActivity => accepted("LocalActivityCompleted"),
        K::RecordUpdateResult => accepted("UpdateCompleted"),
        K::SpawnDetachedChildWorkflow => accepted("ChildWorkflowSpawnedDetached"),
        K::SignalExternalWorkflow => accepted("ExternalSignalRequested"),
        K::RequestCancelExternalWorkflow => accepted("ExternalCancelRequested"),
        K::AwaitExternalWorkflow => accepted("ExternalAwaitRequested"),
        K::CancelTimer => accepted("TimerCancelled"),
        K::AcquireMutex => accepted("MutexGranted"),
        K::WaitForActivity
        | K::UpsertSearchAttributes
        | K::SetCurrentDetails
        | K::PublishProgress
        | K::PublishDurableProgress
        | K::RecordLog
        | K::CancelRaceLosers
        | K::ReleaseMutex => NO_EVENT,
        K::ScheduleExternalActivity => Support::Rejected,
        K::Complete | K::Fail => Support::NotEmitted,
    }
}
