//! Per-execution stall diagnosis — the root-cause classifier (issue #809).
//!
//! Issue #486 answers *"which runs look stalled?"* with a coarse
//! [`StallReason`](https://github.com/autumn-foundation/autumn-harvest/issues/486)
//! bucket, and `GET /workflows/{id}/stack` lists the raw pending items. Neither
//! answers the question that actually drives MTTR: **is this a healthy wait or a
//! pathological stall, and if it is stalled, what is the actionable root cause?**
//!
//! The killer case is a pending activity. That single bucket hides "running
//! normally", "in retry backoff", "**no live worker is polling this queue**",
//! "**its circuit breaker is open**", "**it is rate-limited / concurrency
//! deferred**", and "**an operator paused the queue**". Reaching a verdict today
//! means correlating four endpoints (`/stack`, `/workers/health`,
//! `/admin/circuits`, `/admin/rate-limits` + `/admin/concurrency`) plus
//! task-queue retry timing, by hand, under incident pressure.
//!
//! This module is the **pure** half of the answer: it takes already-gathered
//! facts (the replay-derived wait set, per-queue worker liveness, the in-process
//! circuit snapshot, rate-limit / concurrency saturation, queue pause state, and
//! the task row's own retry timing) and collapses them into one discriminated
//! [`BlockedOn`] verdict plus a triage-level [`ExecutionHealth`].
//!
//! It performs **no I/O**: no database access, no event append, no task-queue
//! mutation, no `WorkflowEvent` variant, and no migration. Every function here
//! is unit-testable without a database, which is exactly what issue #809 asks
//! for. The plugin's `GET /api/harvest/workflows/{id}/diagnose` handler gathers
//! the inputs shard-locally and calls [`classify_execution`].
//!
//! ## Precedence
//!
//! A run can be blocked by several things at once, but an operator needs **one**
//! actionable answer. The ladder is:
//!
//! 1. terminal execution — nothing to diagnose
//! 2. [`BlockedOn::Paused`] — an operator deliberately parked this run
//! 3. the **worst** verdict across every pending activity (see
//!    [`activity_precedence`])
//! 4. an external handoff, then a pending child, then an awaited signal, then a
//!    sleeping timer — mirroring issue #486's own `StallReason` ordering so the
//!    two surfaces agree about which category "wins"
//! 5. [`BlockedOn::NoPendingWork`]
//!
//! Within the activity bucket the ladder is ordered by **how hard the impediment
//! is to clear**, so the verdict names the thing an operator must act on rather
//! than the thing that happens to self-heal soonest:
//!
//! `queue_paused` > `no_worker` > `circuit_open` > `concurrency_deferred` >
//! `rate_limited` > `retrying` > healthy.
//!
//! In particular `no_worker` deliberately outranks `retrying`: a task in retry
//! backoff on a queue with no live poller will **never** run, so reporting
//! "retrying" would tell the operator to wait for a retry that cannot happen.
//!
//! ## Health mapping
//!
//! [`ExecutionHealth`] is the one-word triage answer:
//!
//! - [`Healthy`](ExecutionHealth::Healthy) — progressing, or blocked on
//!   something that self-heals with no intervention (retry backoff, a rate-limit
//!   refill, a concurrency slot, a durable timer, a child that is itself
//!   running).
//! - [`BlockedExternal`](ExecutionHealth::BlockedExternal) — waiting on a party
//!   outside the engine: a human signal, an external-handoff callback, or an
//!   operator's own pause. Expected, not a page.
//! - [`Stalled`](ExecutionHealth::Stalled) — needs a human now: no worker polls
//!   the queue, a circuit breaker is open, or a `RUNNING` execution has no
//!   pending work at all (the executor-loss / lost-task indicator).
//! - [`Terminal`](ExecutionHealth::Terminal) — the run already finished.
//!
//! A long sleep is **not** a stall: health is derived purely from the verdict,
//! never from how old the last event is, so a workflow sleeping on a
//! month-long timer stays `healthy` (issue #809 AC4).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Triage-level verdict for a single execution.
///
/// See the module docs for the mapping rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExecutionHealth {
    /// Progressing, or blocked on something that clears without intervention.
    Healthy,
    /// Needs a human: nothing will move this run forward on its own.
    Stalled,
    /// Waiting on a party outside the engine (a signal, an external handoff, or
    /// an operator pause). Expected — not a page.
    BlockedExternal,
    /// The execution already reached a terminal state.
    Terminal,
}

impl ExecutionHealth {
    /// Stable, low-cardinality wire string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Stalled => "stalled",
            Self::BlockedExternal => "blocked_external",
            Self::Terminal => "terminal",
        }
    }
}

/// The single, discriminated root cause a run is blocked on.
///
/// Serialized internally tagged (`{"type": "activity_no_worker", "queue": ...}`)
/// so a caller switches on one stable, `snake_case` discriminator.
///
/// Every variant is a *closed* case: adding one is an additive wire change, and
/// callers should treat an unrecognized `type` as "some newer cause" rather than
/// an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum BlockedOn {
    /// Work is genuinely in flight — an activity is claimed/running on a worker,
    /// or is due and unimpeded and simply waiting for a free dispatch slot.
    HealthyInProgress,
    /// Parked on a signal that has not arrived.
    AwaitingSignal {
        /// The awaited signal's name.
        signal_name: String,
        /// When the wait began, where recorded history can date it.
        #[serde(skip_serializing_if = "Option::is_none")]
        since: Option<DateTime<Utc>>,
    },
    /// Sleeping on a durable timer that has not fired yet.
    SleepingTimer {
        /// When the timer is due to fire.
        fires_at: DateTime<Utc>,
    },
    /// Waiting on a child workflow that has not reached a terminal state.
    PendingChild {
        /// The child's execution id.
        child_exec_id: String,
        /// The child's current state.
        child_state: String,
    },
    /// An activity is in retry backoff: it failed and is scheduled to run again.
    ActivityRetrying {
        /// The activity being retried, where the task row names one.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
        /// The attempt number recorded on the task row.
        attempt: i32,
        /// The most recent failure recorded on the task row, if any.
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
        /// When the next attempt becomes claimable (the task's `scheduled_at`).
        #[serde(skip_serializing_if = "Option::is_none")]
        next_attempt_at: Option<DateTime<Utc>>,
    },
    /// An activity was pushed into the future by the dispatcher with no failure
    /// recorded against it — a dispatch-time rate-limit deferral (issue #699 /
    /// #369), a session-capacity deferral (#606), or a capability-miss
    /// redelivery (#804). Distinct from [`Self::ActivityRetrying`], which
    /// requires evidence that an attempt actually failed.
    ActivityDeferred {
        /// The deferred activity, where the task row names one.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
        /// When the task becomes claimable again (the task's `scheduled_at`).
        next_attempt_at: DateTime<Utc>,
    },
    /// **No live worker is polling this activity's task queue.** The condition
    /// issue #486's coarse `pending_activity` bucket cannot express, and the one
    /// that never self-heals.
    ActivityNoWorker {
        /// The task queue with no live poller.
        queue: String,
        /// The activity waiting on it, where the task row names one.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
    },
    /// The activity's per-activity circuit breaker is open (or half-open), so
    /// dispatch fast-fails until it recovers.
    ActivityCircuitOpen {
        /// The activity whose breaker is tripped.
        activity_name: String,
        /// When a half-open probe becomes admissible. `None` when the breaker is
        /// operator-forced open — no probe is admitted on any timer, so recovery
        /// requires an explicit `force-close`.
        #[serde(skip_serializing_if = "Option::is_none")]
        cooldown_until: Option<DateTime<Utc>>,
    },
    /// The activity's rate-limit bucket is exhausted.
    ActivityRateLimited {
        /// The exhausted bucket key.
        key: String,
        /// The activity waiting on it.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
    },
    /// The activity's per-key concurrency cap is saturated.
    ActivityConcurrencyDeferred {
        /// The saturated concurrency key.
        key: String,
        /// The activity waiting on it.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
    },
    /// An operator has paused the activity's task queue (issue #619), so no
    /// worker will claim it until the pause is lifted.
    ActivityQueuePaused {
        /// The held queue.
        queue: String,
        /// The activity waiting on it.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
    },
    /// **A durable timer's deadline has passed but nothing fired it.** Timers
    /// fire only when a worker claims the owning workflow task
    /// (`worker::ingest_due_timers_and_signals`) — there is no independent timer
    /// scanner — so an unfired past-due timer means that task was never claimed.
    /// This is the same lost-task stall [`BlockedOn::NoPendingWork`] reports for
    /// a run with no rows at all, named precisely rather than letting the mere
    /// presence of a timer downgrade it to a healthy sleep.
    TimerOverdue {
        /// The deadline that has already elapsed.
        fires_at: DateTime<Utc>,
        /// How far past the deadline the run is, in whole seconds.
        overdue_by_seconds: i64,
    },
    /// Waiting on an external system to complete a handed-off activity.
    AwaitingExternalHandoff {
        /// The completion token issued to the external system.
        token: String,
        /// The handed-off activity's name.
        #[serde(skip_serializing_if = "Option::is_none")]
        activity_name: Option<String>,
    },
    /// An operator paused this specific execution (issue #383).
    Paused {
        /// Who requested the pause, where recorded.
        #[serde(skip_serializing_if = "Option::is_none")]
        actor: Option<String>,
        /// When the pause took effect.
        #[serde(skip_serializing_if = "Option::is_none")]
        since: Option<DateTime<Utc>>,
    },
    /// Replay found the run parked on a durable wait that leaves no row in any
    /// side table this endpoint reads — a `ctx.await_condition` park, a durable
    /// mutex acquire (issue #691), an admitted update awaiting its handler
    /// result, or a parked external-workflow operation (#492/#757/#751).
    ///
    /// Named by the replay-derived wait set (issue #615) rather than inferred
    /// from the database, and reported only when no side table produced a more
    /// precise cause — so it upgrades what would otherwise be a false
    /// [`Self::NoPendingWork`] and can never mask a specific verdict.
    AwaitingReplayWait {
        /// The awaitable kind replay reported (`condition`, `mutex`, `update`,
        /// `external_workflow`, `activity`, `child_workflow`, `timer`).
        wait_kind: String,
        /// The awaitable's label, where replay names one. `None` for an
        /// `await_condition` park (the API takes no site label).
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// When the wait began, where recorded history can date it.
        #[serde(skip_serializing_if = "Option::is_none")]
        since: Option<DateTime<Utc>>,
    },
    /// **No live worker is polling the queue the run's own workflow task sits
    /// on.** The workflow-task analogue of [`Self::ActivityNoWorker`]: the
    /// decision cycle itself can never be claimed, so the run cannot advance
    /// even though nothing is individually wedged.
    WorkflowNoWorker {
        /// The workflow task queue with no live poller.
        queue: String,
    },
    /// An operator has paused the queue the run's own workflow task sits on
    /// (issue #619), so no worker will claim the decision cycle until the pause
    /// is lifted.
    WorkflowQueuePaused {
        /// The held workflow task queue.
        queue: String,
    },
    /// A non-terminal execution with no pending work of any kind — the
    /// executor-loss / lost-task indicator.
    NoPendingWork,
}

impl BlockedOn {
    /// Stable, low-cardinality discriminator matching the serialized `type`.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::HealthyInProgress => "healthy_in_progress",
            Self::AwaitingSignal { .. } => "awaiting_signal",
            Self::SleepingTimer { .. } => "sleeping_timer",
            Self::PendingChild { .. } => "pending_child",
            Self::ActivityRetrying { .. } => "activity_retrying",
            Self::ActivityDeferred { .. } => "activity_deferred",
            Self::ActivityNoWorker { .. } => "activity_no_worker",
            Self::ActivityCircuitOpen { .. } => "activity_circuit_open",
            Self::ActivityRateLimited { .. } => "activity_rate_limited",
            Self::ActivityConcurrencyDeferred { .. } => "activity_concurrency_deferred",
            Self::ActivityQueuePaused { .. } => "activity_queue_paused",
            Self::TimerOverdue { .. } => "timer_overdue",
            Self::AwaitingExternalHandoff { .. } => "awaiting_external_handoff",
            Self::Paused { .. } => "paused",
            Self::AwaitingReplayWait { .. } => "awaiting_replay_wait",
            Self::WorkflowNoWorker { .. } => "workflow_no_worker",
            Self::WorkflowQueuePaused { .. } => "workflow_queue_paused",
            Self::NoPendingWork => "no_pending_work",
        }
    }

    /// The triage-level health this cause implies.
    ///
    /// Deliberately a pure function of the cause alone — never of how old the
    /// last event is — so a long durable sleep can never read as a stall
    /// (issue #809 AC4).
    #[must_use]
    pub const fn health(&self) -> ExecutionHealth {
        match self {
            // Progressing, or self-healing without intervention.
            Self::HealthyInProgress
            | Self::SleepingTimer { .. }
            | Self::PendingChild { .. }
            | Self::ActivityRetrying { .. }
            | Self::ActivityDeferred { .. }
            | Self::ActivityRateLimited { .. }
            | Self::ActivityConcurrencyDeferred { .. } => ExecutionHealth::Healthy,
            // Waiting on a party outside the engine.
            Self::AwaitingSignal { .. }
            | Self::AwaitingExternalHandoff { .. }
            | Self::AwaitingReplayWait { .. }
            | Self::Paused { .. }
            | Self::ActivityQueuePaused { .. }
            | Self::WorkflowQueuePaused { .. } => ExecutionHealth::BlockedExternal,
            // Nothing will move this forward without a human.
            Self::ActivityNoWorker { .. }
            | Self::WorkflowNoWorker { .. }
            | Self::TimerOverdue { .. }
            | Self::ActivityCircuitOpen { .. }
            | Self::NoPendingWork => ExecutionHealth::Stalled,
        }
    }
}

/// The observable phase of a pending activity's circuit breaker, as read
/// read-only from the in-process registry.
///
/// Only the two dispatch-blocking phases are represented; a closed (or absent)
/// breaker contributes nothing and is modelled as `None` at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlockingCircuitPhase {
    /// Tripped: dispatch fast-fails until the cooldown elapses.
    Open,
    /// Cooldown elapsed: a single probe dispatch is admitted.
    HalfOpen,
}

/// Everything the classifier needs to know about one pending activity task row.
///
/// Deliberately owns its strings so the classifier stays a pure function over a
/// snapshot and never borrows a live database row.
///
/// The four independent booleans are not a state machine: each names a *separate*
/// impediment that can hold simultaneously (a queue can be paused while a breaker
/// is open while a bucket is empty), and the classifier's whole job is to rank
/// them. Collapsing them into an enum would destroy exactly the information the
/// precedence ladder consumes.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingActivityFacts {
    /// The activity's registered name, where the task row carries one.
    pub activity_name: Option<String>,
    /// The task queue this activity was enqueued on.
    pub queue: String,
    /// The task row's state. The engine's own CHECK constraint admits only
    /// `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` and `CANCELLED`; the caller
    /// loads the two non-terminal ones. Retry backoff is NOT a distinct state —
    /// it is `PENDING` with a future `scheduled_at`.
    pub task_state: String,
    /// The task row's attempt counter.
    pub attempt: i32,
    /// The most recent failure text recorded on the task row, if any.
    pub last_error: Option<String>,
    /// The task's `scheduled_at`: when it becomes claimable.
    pub scheduled_at: DateTime<Utc>,
    /// The rate-limit bucket key, where one applies.
    pub rate_limit_key: Option<String>,
    /// The per-key concurrency key, where one applies.
    pub concurrency_key: Option<String>,
    /// `true` when an operator has paused this task's queue (issue #619).
    pub queue_paused: bool,
    /// `true` when at least one live worker polls this queue on this shard.
    pub has_live_worker: bool,
    /// The activity's breaker phase, when it is blocking dispatch.
    pub circuit_phase: Option<BlockingCircuitPhase>,
    /// When a half-open probe becomes admissible, if that is knowable.
    pub circuit_cooldown_until: Option<DateTime<Utc>>,
    /// `true` when the rate-limit bucket has fewer than one token.
    pub rate_limit_saturated: bool,
    /// `true` when the per-key concurrency cap is fully in use.
    pub concurrency_saturated: bool,
}

/// A pending external-handoff activity (issue #92).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalHandoffFacts {
    /// The completion token issued to the external system.
    pub token: String,
    /// The handed-off activity's name.
    pub activity_name: Option<String>,
}

/// A non-terminal child workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChildFacts {
    /// The child's execution id.
    pub child_exec_id: String,
    /// The child's current state.
    pub child_state: String,
}

/// An awaited-but-unfulfilled signal (issue #615's replayed wait set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwaitedSignalFacts {
    /// The awaited signal's name.
    pub signal_name: String,
    /// When the wait began, where recorded history can date it.
    pub since: Option<DateTime<Utc>>,
}

/// An unfired durable timer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTimerFacts {
    /// When the timer is due to fire.
    pub fires_at: DateTime<Utc>,
}

/// The run's OWN workflow task row (`task_type = 'workflow'`).
///
/// Every non-terminal execution has exactly one. It is the decision cycle
/// itself, so it is consulted only as a fallback — when no activity, timer,
/// child, handoff, or awaited signal produced a cause — to distinguish a run
/// that is *currently executing or awaiting dispatch* from the genuinely lost
/// task [`BlockedOn::NoPendingWork`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTaskFacts {
    /// The row's `state` (`PENDING`, `RUNNING`, ...).
    pub state: String,
    /// `true` when `worker_id IS NOT NULL` — a worker holds the claim and the
    /// handler is executing right now. A *parked* row is `RUNNING` with a NULL
    /// worker, so this is what separates executing from parked.
    pub has_worker: bool,
    /// The task queue the decision cycle is dispatched on.
    pub queue_name: String,
    /// When the row becomes claimable.
    pub scheduled_at: DateTime<Utc>,
    /// `true` when an operator has paused `queue_name` (issue #619).
    pub queue_paused: bool,
    /// `true` when at least one live worker polls `queue_name`.
    pub has_live_worker: bool,
}

/// A durable wait that replay named but no side table this endpoint reads can
/// see — an `await_condition` park, a mutex acquire, an admitted update, or a
/// parked external-workflow operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayWaitFacts {
    /// The awaitable kind replay reported.
    pub wait_kind: String,
    /// The awaitable's label, where replay names one.
    pub name: Option<String>,
    /// When the wait began, where recorded history can date it.
    pub since: Option<DateTime<Utc>>,
}

/// The complete, already-gathered fact set for one execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosisInputs {
    /// `true` when the execution has reached a terminal state.
    pub is_terminal: bool,
    /// `true` when the execution is `PAUSED` (issue #383).
    pub is_paused: bool,
    /// Who requested the pause, where recorded.
    pub pause_actor: Option<String>,
    /// When the pause took effect.
    pub paused_since: Option<DateTime<Utc>>,
    /// Every pending activity task row for this execution.
    pub activities: Vec<PendingActivityFacts>,
    /// Every pending external handoff for this execution.
    pub external_handoffs: Vec<ExternalHandoffFacts>,
    /// Every non-terminal child workflow.
    pub children: Vec<PendingChildFacts>,
    /// Every awaited-but-unfulfilled signal.
    pub awaited_signals: Vec<AwaitedSignalFacts>,
    /// Every unfired durable timer.
    pub timers: Vec<PendingTimerFacts>,
    /// Durable waits replay named that leave no side-table row. Consulted only
    /// below every side-table cause, so it can only ever replace a false
    /// [`BlockedOn::NoPendingWork`].
    pub replay_waits: Vec<ReplayWaitFacts>,
    /// The run's own workflow task row, where one was loaded. `None` leaves the
    /// classifier's pre-issue-#1188 behaviour exactly as it was.
    pub workflow_task: Option<WorkflowTaskFacts>,
}

/// Rank a pending-activity verdict by how hard its impediment is to clear.
///
/// Higher wins. Used to pick the **worst** verdict across a fan-out so a single
/// stuck slot among nineteen healthy ones is never reported as healthy.
///
/// Non-activity verdicts rank `0`: this helper is only ever applied to the
/// output of [`classify_pending_activity`].
#[must_use]
pub const fn activity_precedence(blocked: &BlockedOn) -> u8 {
    match blocked {
        BlockedOn::ActivityQueuePaused { .. } => 7,
        BlockedOn::ActivityNoWorker { .. } => 6,
        BlockedOn::ActivityCircuitOpen { .. } => 5,
        BlockedOn::ActivityConcurrencyDeferred { .. } => 4,
        BlockedOn::ActivityRateLimited { .. } => 3,
        // A row with recorded failure evidence outranks a clean dispatcher
        // deferral: the failure is the more actionable of the two.
        BlockedOn::ActivityRetrying { .. } => 2,
        BlockedOn::ActivityDeferred { .. } => 1,
        _ => 0,
    }
}

/// Classify a single pending activity task row.
///
/// A task a worker already holds (`RUNNING`) is [`BlockedOn::HealthyInProgress`]
/// provided a live worker still polls its queue — it *is* progressing, and no
/// claim-time impediment applies to work that has already been claimed. A
/// claimed row with no live poller is an orphan, not healthy.
///
/// For a `PENDING` row the impediment ladder applies (see the module docs).
/// A row that is due, unimpeded and simply waiting for a free dispatch slot is
/// also `HealthyInProgress`.
#[must_use]
pub fn classify_pending_activity(facts: &PendingActivityFacts, now: DateTime<Utc>) -> BlockedOn {
    // A task a worker already holds is progressing — but only while that worker
    // is alive. A claimed row on a queue with no live poller is an orphan
    // (issue #367): the reclaimer heals it within a poll interval EXCEPT when
    // the whole fleet for that queue is down, which is precisely the flagship
    // `activity_no_worker` case. Reporting it healthy would let the endpoint
    // answer "fine" forever for a run that can never move.
    if facts.task_state != "PENDING" {
        if facts.has_live_worker {
            // Every impediment below is a *claim-time* gate; none of them
            // applies to work that has already been claimed.
            return BlockedOn::HealthyInProgress;
        }
        return BlockedOn::ActivityNoWorker {
            queue: facts.queue.clone(),
            activity_name: facts.activity_name.clone(),
        };
    }

    // 1. An operator holds the whole queue (issue #619). Deliberate, and the
    //    one impediment a triaging operator should see before anything else.
    if facts.queue_paused {
        return BlockedOn::ActivityQueuePaused {
            queue: facts.queue.clone(),
            activity_name: facts.activity_name.clone(),
        };
    }

    // 2. Nothing polls this queue. This never self-heals, and it outranks retry
    //    backoff on purpose: a backing-off task on a pollerless queue will never
    //    run, so reporting "retrying" would promise a retry that cannot happen.
    if !facts.has_live_worker {
        return BlockedOn::ActivityNoWorker {
            queue: facts.queue.clone(),
            activity_name: facts.activity_name.clone(),
        };
    }

    // 3. The breaker fast-fails dispatch. Only an activity the task row actually
    //    names can carry one, so an unnamed row can never reach here.
    if let (Some(phase), Some(name)) = (facts.circuit_phase, facts.activity_name.as_ref()) {
        return BlockedOn::ActivityCircuitOpen {
            activity_name: name.clone(),
            // A forced-open breaker admits no probe on any timer, so it has no
            // meaningful cooldown to advertise; the caller passes `None`.
            cooldown_until: match phase {
                BlockingCircuitPhase::Open => facts.circuit_cooldown_until,
                BlockingCircuitPhase::HalfOpen => None,
            },
        };
    }

    // 4. Not due yet. The task has not reached the claim gates at all, so a
    //    momentarily-saturated bucket or concurrency key says nothing about
    //    whether it will be free at `scheduled_at`; reporting one of those
    //    instead would drop `attempt`, `last_error` and `next_attempt_at`, the
    //    three fields an operator actually wants. The three non-self-healing
    //    gates above still outrank it, because a task waiting toward a queue
    //    nothing polls will never run at all.
    //
    //    A future `scheduled_at` alone does NOT mean "failed and retrying".
    //    The dispatcher also pushes a task forward with no failure at all:
    //    `queue::defer_rate_limited_task` (dispatch-time rate limiting, issue
    //    #699 / #369) even decrements the claim-time attempt back down because
    //    "a rate-limit deferral is not an attempt", and the session-capacity
    //    (#606) and capability-miss (#804) paths reuse the same clean
    //    continuation. Calling those `activity_retrying` would send an operator
    //    hunting a failure that never happened. So the verdict turns on
    //    *failure evidence* — a recorded `error` — not on timing alone. The
    //    error column survives a later deferral (`CleanContinuationChangeset`
    //    does not clear it), so its presence is durable proof an attempt really
    //    did fail, and its absence proof that none ever did.
    if facts.scheduled_at > now {
        return if facts.last_error.is_some() {
            BlockedOn::ActivityRetrying {
                activity_name: facts.activity_name.clone(),
                attempt: facts.attempt,
                last_error: facts.last_error.clone(),
                next_attempt_at: Some(facts.scheduled_at),
            }
        } else {
            BlockedOn::ActivityDeferred {
                activity_name: facts.activity_name.clone(),
                next_attempt_at: facts.scheduled_at,
            }
        };
    }

    // 5. Per-key concurrency (issue #247): self-heals as siblings finish.
    if facts.concurrency_saturated
        && let Some(key) = facts.concurrency_key.as_ref()
    {
        return BlockedOn::ActivityConcurrencyDeferred {
            key: key.clone(),
            activity_name: facts.activity_name.clone(),
        };
    }

    // 6. Rate limit (issue #332/#699): self-heals on refill.
    if facts.rate_limit_saturated
        && let Some(key) = facts.rate_limit_key.as_ref()
    {
        return BlockedOn::ActivityRateLimited {
            key: key.clone(),
            activity_name: facts.activity_name.clone(),
        };
    }

    // Due, unimpeded, waiting only for a free dispatch slot.
    BlockedOn::HealthyInProgress
}

/// Classify the run's OWN workflow task row.
///
/// Returns `None` when the row cannot explain the run's state, leaving the
/// caller's lost-task verdict ([`BlockedOn::NoPendingWork`]) standing.
///
/// This is a **fallback**, never a headline: a non-terminal run always has a
/// workflow task row, so consulting it above the specific waits would mask
/// every real cause. It exists to separate three states the row makes plain
/// but no other table does:
///   * **claimed** (`worker_id IS NOT NULL`) — the handler is executing right
///     now, *provided the claimant's queue still has a live poller*. A decision
///     cycle that runs a long local activity (issue #98, up to a minute by
///     default) schedules no queue row of any kind, so without this check a
///     perfectly healthy in-flight run reports `no_pending_work`. A claim left
///     behind by a crashed worker on a queue nothing polls is an orphan
///     (issue #367), not progress, and reports [`BlockedOn::WorkflowNoWorker`].
///   * **awaiting dispatch** (`PENDING`) — about to be claimed. Healthy when a
///     live worker polls the queue; the workflow-task analogue of AC3's
///     flagship "no live worker" stall when one does not, and the operator-held
///     [`BlockedOn::WorkflowQueuePaused`] when the queue is paused (issue #619).
///   * **parked** (`RUNNING` with a NULL worker) — advances on a *wake*, not a
///     claim, so a live worker proves nothing and this returns `None`. That is
///     exactly the lost-task shape, and its verdict is deliberately unchanged.
#[must_use]
pub fn classify_workflow_task(facts: &WorkflowTaskFacts, now: DateTime<Utc>) -> Option<BlockedOn> {
    // A worker holds the claim — but a *stored* claim only means progress while
    // that worker is alive. A crashed worker leaves the row RUNNING with a
    // non-null `worker_id` until the poison-pill reclaimer processes it (issue
    // #367); if nothing polls the queue at all, that stale claim never advances
    // and reporting it healthy would answer "fine" forever for a wedged run.
    // Same rule, and the same fleet-coverage proxy, as a claimed activity row.
    if facts.has_worker {
        if facts.has_live_worker {
            // Every impediment below is a *claim-time* gate — an operator pause
            // included. None of them stops a handler that is already executing.
            return Some(BlockedOn::HealthyInProgress);
        }
        return Some(BlockedOn::WorkflowNoWorker {
            queue: facts.queue_name.clone(),
        });
    }
    // Only a PENDING row is awaiting a claim. A RUNNING row with a NULL worker
    // is *parked*: it advances on a wake, not a claim, so a live poller proves
    // nothing about it. Yield to the replay-wait / lost-task verdicts instead.
    if facts.state != "PENDING" {
        return None;
    }

    // 1. An operator holds the whole queue (issue #619) — deliberate, and the
    //    cause a triaging operator should see before anything else.
    if facts.queue_paused {
        return Some(BlockedOn::WorkflowQueuePaused {
            queue: facts.queue_name.clone(),
        });
    }

    // 2. Nothing polls this queue. This never self-heals, and it outranks the
    //    deferral below on purpose: unlike a timed sleep, a dispatcher deferral
    //    only resolves when something claims the row at `scheduled_at`, so a
    //    deferred row on a pollerless queue will never run at all. Reporting it
    //    healthy would promise a claim that cannot happen.
    if !facts.has_live_worker {
        return Some(BlockedOn::WorkflowNoWorker {
            queue: facts.queue_name.clone(),
        });
    }

    // 3. Deliberately deferred by the dispatcher (retry backoff, a rate-limit
    //    or capability-miss redelivery). With coverage established above, this
    //    genuinely self-heals when the timestamp arrives.
    if facts.scheduled_at > now {
        return Some(BlockedOn::HealthyInProgress);
    }

    // Due, unimpeded, and covered by a live poller: ordinary dispatch latency.
    Some(BlockedOn::HealthyInProgress)
}

/// Did the workflow task's *own* wake go missing?
///
/// A durable timer never fires on its own: it fires when a worker claims the
/// owning workflow task and `worker::ingest_due_timers_and_signals` runs. So an
/// unfired row past its deadline only proves a *wedge* when that timer is what
/// the workflow task is actually scheduled to wake for.
///
/// It usually is not. Every `*_park` persist path in `worker.rs` —
/// `persist_signal_wait_park`, `persist_mutex_acquire_park`,
/// `persist_activity_wait_park`, `persist_scheduled_activities`,
/// `persist_all_started_child_workflows` and `persist_scheduled_external_activity`
/// — discards the armed deadline (`_min_fires_at`) and parks the task on the
/// thing being awaited. Only `persist_started_timer` calls
/// [`crate::queue::reschedule_task`] with the deadline, handing the timer
/// ownership of the wake. A timer armed alongside any of those other waits
/// therefore goes overdue *as a matter of course* while the wait runs, and fires
/// on that wait's completion wake — a healthy run, not a stall.
///
/// The distinguishing fact is the workflow task row itself:
///   * **claimed** — the handler is executing right now, so nothing was missed.
///   * **parked** (`RUNNING`, NULL worker) — some other wake owns this run; the
///     timer is a passenger and its overdue row is expected.
///   * **`PENDING`** — the row is due to be claimed at `scheduled_at`. Only
///     `persist_started_timer` sets that to a deadline, so a `PENDING` row whose
///     own `scheduled_at` is past the grace window is a genuinely missed wake.
///
/// Absent facts (`None`) resolve to `true`, preserving the pre-gate behaviour
/// for callers that do not supply a workflow task row.
#[must_use]
pub fn workflow_wake_was_missed(task: Option<&WorkflowTaskFacts>, now: DateTime<Utc>) -> bool {
    let Some(task) = task else {
        return true;
    };
    if task.has_worker {
        return false;
    }
    if task.state != "PENDING" {
        return false;
    }
    (now - task.scheduled_at).num_seconds() >= TIMER_OVERDUE_GRACE_SECONDS
}

/// Collapse an execution's whole fact set into one root-cause verdict.
///
/// Returns `None` for a terminal execution — there is nothing to diagnose, and
/// the caller reports [`ExecutionHealth::Terminal`] with the terminal outcome
/// instead.
#[must_use]
pub fn classify_execution(inputs: &DiagnosisInputs, now: DateTime<Utc>) -> Option<BlockedOn> {
    // A terminal execution has nothing to diagnose, whatever stale rows linger.
    if inputs.is_terminal {
        return None;
    }

    // An operator deliberately parked this run; every downstream wait is a
    // consequence of that, not an independent cause.
    if inputs.is_paused {
        return Some(BlockedOn::Paused {
            actor: inputs.pause_actor.clone(),
            since: inputs.paused_since,
        });
    }

    // The activity bucket: report the WORST verdict across every pending row so
    // one wedged slot in a fan-out is never masked by nineteen healthy ones.
    // `max_by_key` keeps the LAST maximum, so the scan is written explicitly to
    // keep the FIRST row of an equal-ranked tie — deterministic given the
    // caller's own deterministic row ordering.
    let worst_activity =
        inputs
            .activities
            .iter()
            .fold(None::<BlockedOn>, |worst: Option<BlockedOn>, facts| {
                let candidate = classify_pending_activity(facts, now);
                match worst {
                    Some(current)
                        if activity_precedence(&current) >= activity_precedence(&candidate) =>
                    {
                        Some(current)
                    }
                    _ => Some(candidate),
                }
            });
    if let Some(verdict) = worst_activity {
        return Some(verdict);
    }

    // Category ladder below the activity bucket. Mirrors issue #486's own
    // `StallReason` ordering so the discovery and root-cause surfaces agree
    // about which category wins; external handoffs sit directly under regular
    // activities because they *are* the activity work, just executed elsewhere.
    //
    // The overdue-timer check below sits UNDER the activity bucket and is gated
    // on [`workflow_wake_was_missed`]. Both guards exist for the same reason:
    // an unfired timer past its deadline is only a wedge when that timer is what
    // the workflow task is actually scheduled to wake for.
    //
    // Only `persist_started_timer` hands a timer that ownership, by calling
    // `queue::reschedule_task(task_id, fires_at)`. Every other persist path —
    // `persist_signal_wait_park`, `persist_mutex_acquire_park`,
    // `persist_activity_wait_park`, `persist_scheduled_activities`,
    // `persist_all_started_child_workflows`, `persist_scheduled_external_activity`
    // — discards the armed deadline (`_min_fires_at`) and parks the task on the
    // thing being awaited, so a timer armed alongside that wait goes overdue as a
    // matter of course and fires on the wait's completion wake. Reporting such a
    // healthy run as `timer_overdue`/`stalled` would be a false positive, the one
    // failure mode this endpoint must never have: it sends an operator chasing a
    // non-problem.
    //
    // The gate keys on the workflow task's OWN wake, so it covers every one of
    // those paths at once (pinned by the `overdue_timer_with_a_parked_task_*`
    // tests). The ordering guard is kept as well because it is load-bearing for
    // the activity case specifically: `persist_scheduled_activities` leaves the
    // row PENDING at a *retry* `scheduled_at`, which the gate cannot distinguish
    // from a missed timer wake. Pinned by
    // `healthy_activity_alongside_an_overdue_timer_is_not_a_stall`.
    //
    // What survives both guards is a hard fact, not an inference: timers fire
    // only when a worker claims the owning workflow task
    // (`worker::ingest_due_timers_and_signals`), and there is no independent
    // timer scanner — so a PENDING task long past its own `scheduled_at` with an
    // overdue timer means the engine failed to act. That must outrank the
    // legitimate-looking waits below, or a signal-or-deadline race (issue #476)
    // whose deadline the engine missed would report the healthy-looking
    // `awaiting_signal` instead of the wedge (pinned by
    // `overdue_timer_wins_when_the_workflow_wake_was_genuinely_missed`). This is
    // NOT the event-age heuristic AC4 forbids: a future deadline still reports
    // `sleeping_timer` however old the run is.
    if workflow_wake_was_missed(inputs.workflow_task.as_ref(), now)
        && let Some(overdue) = inputs
            .timers
            .iter()
            .filter(|timer| (now - timer.fires_at).num_seconds() >= TIMER_OVERDUE_GRACE_SECONDS)
            .min_by_key(|timer| timer.fires_at)
    {
        return Some(BlockedOn::TimerOverdue {
            fires_at: overdue.fires_at,
            overdue_by_seconds: (now - overdue.fires_at).num_seconds(),
        });
    }

    if let Some(handoff) = inputs.external_handoffs.first() {
        return Some(BlockedOn::AwaitingExternalHandoff {
            token: handoff.token.clone(),
            activity_name: handoff.activity_name.clone(),
        });
    }
    if let Some(child) = inputs.children.first() {
        return Some(BlockedOn::PendingChild {
            child_exec_id: child.child_exec_id.clone(),
            child_state: child.child_state.clone(),
        });
    }
    if let Some(signal) = inputs.awaited_signals.first() {
        return Some(BlockedOn::AwaitingSignal {
            signal_name: signal.signal_name.clone(),
            since: signal.since,
        });
    }
    // The earliest deadline is the one that governs when the run wakes.
    if let Some(timer) = inputs.timers.iter().min_by_key(|timer| timer.fires_at) {
        return Some(BlockedOn::SleepingTimer {
            fires_at: timer.fires_at,
        });
    }

    // Below every side-table cause: the run's own workflow task row, then the
    // durable waits replay named that leave no side-table row at all. Both sit
    // here — not higher — so they can only ever replace what would otherwise be
    // a false `NoPendingWork`, never mask a more precise verdict above.
    //
    // The workflow task goes first because a *claimed* row is the most current
    // fact there is (the handler is executing this instant, so any replay wait
    // is from the previous suspension). The two never actually compete: a
    // parked row — the shape a replay wait explains — yields `None` here.
    if let Some(verdict) = inputs
        .workflow_task
        .as_ref()
        .and_then(|facts| classify_workflow_task(facts, now))
    {
        return Some(verdict);
    }
    if let Some(wait) = inputs.replay_waits.first() {
        return Some(BlockedOn::AwaitingReplayWait {
            wait_kind: wait.wait_kind.clone(),
            name: wait.name.clone(),
            since: wait.since,
        });
    }

    Some(BlockedOn::NoPendingWork)
}

/// How far past its deadline an unfired durable timer must be before it counts
/// as a stall rather than ordinary scheduling latency.
///
/// A timer fires when a worker claims the owning workflow task, so a deadline
/// that passed moments ago is simply waiting on the next poll. This window is
/// deliberately an order of magnitude above a normal poll interval so that
/// routine latency can never be reported as a wedge; anything past it means no
/// worker claimed the task across many poll cycles.
pub const TIMER_OVERDUE_GRACE_SECONDS: i64 = 60;

/// A one-sentence, human-readable rendering of a verdict, for CLI output and
/// the response's `summary` field.
#[must_use]
/// Render an activity row's name for a human summary, or a neutral phrase
/// when `harvest_task_queue.activity_name` is NULL.
fn activity_phrase(activity_name: Option<&String>) -> String {
    activity_name.map_or_else(
        || "a pending activity".to_string(),
        |name| format!("activity '{name}'"),
    )
}

/// Summarize the activity-row causes of [`summarize`].
///
/// Split out purely so [`summarize`] stays within the function-length lint
/// budget; the two together are exhaustive over [`BlockedOn`].
fn summarize_activity_cause(blocked: &BlockedOn) -> String {
    match blocked {
        BlockedOn::ActivityRetrying {
            activity_name,
            attempt,
            next_attempt_at,
            ..
        } => {
            let who = activity_phrase(activity_name.as_ref());
            next_attempt_at.map_or_else(
                || format!("{who} is in retry backoff after attempt {attempt}"),
                |at| {
                    format!(
                        "{who} is in retry backoff after attempt {attempt}; next attempt at {at}"
                    )
                },
            )
        }
        BlockedOn::ActivityDeferred {
            activity_name,
            next_attempt_at,
        } => {
            let who = activity_phrase(activity_name.as_ref());
            format!(
                "{who} was deferred by the dispatcher with no failure recorded \
                     (rate limit, session capacity, or capability miss); \
                     next attempt at {next_attempt_at}"
            )
        }
        BlockedOn::ActivityNoWorker {
            queue,
            activity_name,
        } => format!(
            "no live worker is polling queue '{queue}', so {} will never be claimed",
            activity_phrase(activity_name.as_ref())
        ),
        BlockedOn::ActivityCircuitOpen {
            activity_name,
            cooldown_until,
        } => cooldown_until.map_or_else(
            || {
                format!(
                    "the circuit breaker for activity '{activity_name}' is open and \
                         operator-forced; it needs an explicit force-close"
                )
            },
            |until| {
                format!(
                    "the circuit breaker for activity '{activity_name}' is open; a probe is \
                         admitted at {until}"
                )
            },
        ),
        BlockedOn::ActivityRateLimited { key, activity_name } => format!(
            "rate-limit bucket '{key}' is exhausted, deferring {}",
            activity_phrase(activity_name.as_ref())
        ),
        BlockedOn::ActivityConcurrencyDeferred { key, activity_name } => format!(
            "concurrency key '{key}' is at its cap, deferring {}",
            activity_phrase(activity_name.as_ref())
        ),
        BlockedOn::ActivityQueuePaused {
            queue,
            activity_name,
        } => format!(
            "queue '{queue}' is paused by an operator, holding {}",
            activity_phrase(activity_name.as_ref())
        ),
        // Every non-activity cause is handled by `summarize` itself.
        other => summarize(other),
    }
}

/// A one-sentence, human-readable rendering of a verdict, for CLI output and
/// the response's `summary` field.
#[must_use]
pub fn summarize(blocked: &BlockedOn) -> String {
    match blocked {
        BlockedOn::HealthyInProgress => {
            "work is in progress; nothing is blocking this execution".to_string()
        }
        BlockedOn::AwaitingSignal { signal_name, .. } => {
            format!("waiting for signal '{signal_name}' to be sent")
        }
        BlockedOn::SleepingTimer { fires_at } => {
            format!("sleeping on a durable timer until {fires_at}")
        }
        BlockedOn::PendingChild {
            child_exec_id,
            child_state,
        } => format!("waiting on child workflow {child_exec_id} (state {child_state})"),
        BlockedOn::TimerOverdue {
            fires_at,
            overdue_by_seconds,
        } => format!(
            "a durable timer was due at {fires_at} ({overdue_by_seconds}s ago) but nothing fired \
             it — the owning workflow task was never claimed"
        ),
        BlockedOn::AwaitingExternalHandoff {
            token,
            activity_name,
        } => format!(
            "{} was handed off to an external system and is awaiting completion of token {token}",
            activity_phrase(activity_name.as_ref())
        ),
        BlockedOn::Paused { actor, .. } => actor.as_ref().map_or_else(
            || "this execution is paused by an operator".to_string(),
            |actor| format!("this execution is paused by an operator ({actor})"),
        ),
        BlockedOn::AwaitingReplayWait {
            wait_kind, name, ..
        } => name.as_ref().map_or_else(
            || format!("parked on a durable {wait_kind} wait"),
            |name| format!("parked on a durable {wait_kind} wait ({name})"),
        ),
        BlockedOn::WorkflowNoWorker { queue } => format!(
            "no live worker is polling workflow task queue '{queue}', so this run's own decision \
             cycle can never be claimed"
        ),
        BlockedOn::WorkflowQueuePaused { queue } => format!(
            "an operator has paused workflow task queue '{queue}', so this run's own decision \
             cycle will not be claimed until the pause is lifted"
        ),
        BlockedOn::NoPendingWork => "this execution has no pending work of any kind; its \
             workflow task may have been lost"
            .to_string(),
        // Activity-row causes are summarized by a dedicated helper so this
        // function stays within the line-length lint budget as variants grow.
        activity_cause => summarize_activity_cause(activity_cause),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    /// A due, unimpeded, single-attempt PENDING activity on a covered queue.
    fn healthy_activity() -> PendingActivityFacts {
        PendingActivityFacts {
            activity_name: Some("send_email".to_string()),
            queue: "email".to_string(),
            task_state: "PENDING".to_string(),
            attempt: 1,
            last_error: None,
            scheduled_at: t(-10),
            rate_limit_key: None,
            concurrency_key: None,
            queue_paused: false,
            has_live_worker: true,
            circuit_phase: None,
            circuit_cooldown_until: None,
            rate_limit_saturated: false,
            concurrency_saturated: false,
        }
    }

    fn inputs_with(activities: Vec<PendingActivityFacts>) -> DiagnosisInputs {
        DiagnosisInputs {
            activities,
            ..Default::default()
        }
    }

    // ── Per-activity classification ────────────────────────────────────────

    #[test]
    fn due_unimpeded_activity_is_healthy_in_progress() {
        assert_eq!(
            classify_pending_activity(&healthy_activity(), t(0)),
            BlockedOn::HealthyInProgress
        );
    }

    #[test]
    fn running_activity_is_healthy_even_with_impediments() {
        // A task a LIVE worker already holds is progressing; claim-time
        // impediments are irrelevant to work that is already claimed.
        let mut facts = healthy_activity();
        facts.task_state = "RUNNING".to_string();
        facts.has_live_worker = true;
        facts.circuit_phase = Some(BlockingCircuitPhase::Open);
        facts.rate_limit_saturated = true;
        facts.concurrency_saturated = true;
        facts.queue_paused = true;
        assert_eq!(
            classify_pending_activity(&facts, t(0)),
            BlockedOn::HealthyInProgress
        );
    }

    #[test]
    fn orphaned_running_activity_is_not_reported_healthy() {
        // A claimed row whose queue has no live poller is a poison-pill orphan
        // (issue #367). The reclaimer heals it within a poll interval EXCEPT
        // when the whole fleet for that queue is down — the flagship
        // `activity_no_worker` case — where reporting healthy would answer
        // "fine" forever for a run that can never move.
        let mut facts = healthy_activity();
        facts.task_state = "RUNNING".to_string();
        facts.has_live_worker = false;
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(verdict.kind(), "activity_no_worker");
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    #[test]
    fn claimed_activity_is_healthy_in_progress() {
        let mut facts = healthy_activity();
        facts.task_state = "CLAIMED".to_string();
        assert_eq!(
            classify_pending_activity(&facts, t(0)),
            BlockedOn::HealthyInProgress
        );
    }

    /// AC3: the case issue #486's coarse `pending_activity` bucket cannot express.
    #[test]
    fn zero_live_workers_on_queue_is_activity_no_worker() {
        let mut facts = healthy_activity();
        facts.has_live_worker = false;
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(
            verdict,
            BlockedOn::ActivityNoWorker {
                queue: "email".to_string(),
                activity_name: Some("send_email".to_string()),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    /// AC5 (first half): an open breaker carries `cooldown_until`.
    #[test]
    fn open_circuit_is_activity_circuit_open_with_cooldown() {
        let mut facts = healthy_activity();
        facts.circuit_phase = Some(BlockingCircuitPhase::Open);
        facts.circuit_cooldown_until = Some(t(30));
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(
            verdict,
            BlockedOn::ActivityCircuitOpen {
                activity_name: "send_email".to_string(),
                cooldown_until: Some(t(30)),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    #[test]
    fn half_open_circuit_is_also_activity_circuit_open() {
        let mut facts = healthy_activity();
        facts.circuit_phase = Some(BlockingCircuitPhase::HalfOpen);
        assert_eq!(
            classify_pending_activity(&facts, t(0)),
            BlockedOn::ActivityCircuitOpen {
                activity_name: "send_email".to_string(),
                cooldown_until: None,
            }
        );
    }

    /// An operator-forced-open breaker admits no probe on any timer, so it has
    /// no meaningful cooldown to advertise.
    #[test]
    fn forced_open_circuit_reports_no_cooldown() {
        let mut facts = healthy_activity();
        facts.circuit_phase = Some(BlockingCircuitPhase::Open);
        facts.circuit_cooldown_until = None;
        assert_eq!(
            classify_pending_activity(&facts, t(0)),
            BlockedOn::ActivityCircuitOpen {
                activity_name: "send_email".to_string(),
                cooldown_until: None,
            }
        );
    }

    /// AC5 (second half): retry backoff carries `attempt`, `last_error` and
    /// `next_attempt_at` (the task row's `scheduled_at`).
    #[test]
    fn future_scheduled_at_is_activity_retrying() {
        let mut facts = healthy_activity();
        facts.attempt = 3;
        facts.last_error = Some("connection refused".to_string());
        facts.scheduled_at = t(45);
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(
            verdict,
            BlockedOn::ActivityRetrying {
                activity_name: Some("send_email".to_string()),
                attempt: 3,
                last_error: Some("connection refused".to_string()),
                next_attempt_at: Some(t(45)),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn saturated_rate_limit_is_activity_rate_limited() {
        let mut facts = healthy_activity();
        facts.rate_limit_key = Some("tenant:acme".to_string());
        facts.rate_limit_saturated = true;
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(
            verdict,
            BlockedOn::ActivityRateLimited {
                key: "tenant:acme".to_string(),
                activity_name: Some("send_email".to_string()),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn saturated_concurrency_is_activity_concurrency_deferred() {
        let mut facts = healthy_activity();
        facts.concurrency_key = Some("tenant:acme".to_string());
        facts.concurrency_saturated = true;
        assert_eq!(
            classify_pending_activity(&facts, t(0)),
            BlockedOn::ActivityConcurrencyDeferred {
                key: "tenant:acme".to_string(),
                activity_name: Some("send_email".to_string()),
            }
        );
    }

    /// A paused queue with live workers and no other impediment would otherwise
    /// be reported "healthy" — a flat-out wrong answer during the exact triage
    /// this endpoint exists for.
    #[test]
    fn paused_queue_is_activity_queue_paused() {
        let mut facts = healthy_activity();
        facts.queue_paused = true;
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(
            verdict,
            BlockedOn::ActivityQueuePaused {
                queue: "email".to_string(),
                activity_name: Some("send_email".to_string()),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::BlockedExternal);
    }

    // ── Impediment precedence ──────────────────────────────────────────────

    /// A task in retry backoff on a queue with no live poller will never run:
    /// reporting "retrying" would promise a retry that cannot happen.
    #[test]
    fn no_worker_outranks_retry_backoff() {
        let mut facts = healthy_activity();
        facts.has_live_worker = false;
        facts.scheduled_at = t(45);
        facts.attempt = 2;
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_no_worker"
        );
    }

    #[test]
    fn queue_paused_outranks_no_worker() {
        let mut facts = healthy_activity();
        facts.queue_paused = true;
        facts.has_live_worker = false;
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_queue_paused"
        );
    }

    #[test]
    fn no_worker_outranks_circuit_open() {
        let mut facts = healthy_activity();
        facts.has_live_worker = false;
        facts.circuit_phase = Some(BlockingCircuitPhase::Open);
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_no_worker"
        );
    }

    #[test]
    fn circuit_open_outranks_concurrency_and_rate_limit() {
        let mut facts = healthy_activity();
        facts.circuit_phase = Some(BlockingCircuitPhase::Open);
        facts.concurrency_key = Some("k".to_string());
        facts.concurrency_saturated = true;
        facts.rate_limit_key = Some("r".to_string());
        facts.rate_limit_saturated = true;
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_circuit_open"
        );
    }

    #[test]
    fn concurrency_outranks_rate_limit() {
        let mut facts = healthy_activity();
        facts.concurrency_key = Some("k".to_string());
        facts.concurrency_saturated = true;
        facts.rate_limit_key = Some("r".to_string());
        facts.rate_limit_saturated = true;
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_concurrency_deferred"
        );
    }

    #[test]
    fn retry_backoff_outranks_rate_limit_for_a_not_yet_due_task() {
        // A not-yet-due task has not reached the claim gates, so a momentarily
        // empty bucket says nothing about it — and reporting the bucket would
        // drop the retry facts the operator needs.
        let mut facts = healthy_activity();
        facts.rate_limit_key = Some("r".to_string());
        facts.rate_limit_saturated = true;
        facts.scheduled_at = t(45);
        facts.last_error = Some("connection reset".to_string());
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_retrying"
        );
    }

    #[test]
    fn not_yet_due_task_without_failure_evidence_is_deferred_not_retrying() {
        // `queue::defer_rate_limited_task` pushes `scheduled_at` forward AND
        // decrements the claim-time attempt back down, recording no error —
        // "a rate-limit deferral is not an attempt". Reporting that as
        // `activity_retrying` would send an operator hunting a failure that
        // never happened, so the verdict turns on failure evidence, not timing.
        let mut facts = healthy_activity();
        facts.scheduled_at = t(45);
        facts.last_error = None;
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(verdict.kind(), "activity_deferred");
        assert!(
            matches!(
                &verdict,
                BlockedOn::ActivityDeferred { next_attempt_at, .. } if *next_attempt_at == t(45)
            ),
            "must carry the deferral deadline: {verdict:?}"
        );
        // A clean deferral self-heals, so it is not a stall.
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn not_yet_due_task_with_failure_evidence_is_retrying() {
        // The mirror control: a recorded error is durable proof an attempt
        // really did fail (`CleanContinuationChangeset` never clears it), so
        // the retry facts are reported.
        let mut facts = healthy_activity();
        facts.scheduled_at = t(45);
        facts.attempt = 3;
        facts.last_error = Some("upstream 503".to_string());
        let verdict = classify_pending_activity(&facts, t(0));
        assert_eq!(verdict.kind(), "activity_retrying");
        assert!(
            matches!(
                &verdict,
                BlockedOn::ActivityRetrying {
                    attempt: 3,
                    last_error: Some(err),
                    next_attempt_at: Some(at),
                    ..
                } if err == "upstream 503" && *at == t(45)
            ),
            "must carry attempt/last_error/next_attempt_at: {verdict:?}"
        );
    }

    #[test]
    fn non_self_healing_gates_still_outrank_a_clean_deferral() {
        // A task deferred toward a queue nothing polls will never run at all,
        // so the no-worker verdict must still win over the deferral.
        let mut facts = healthy_activity();
        facts.scheduled_at = t(45);
        facts.last_error = None;
        facts.has_live_worker = false;
        assert_eq!(
            classify_pending_activity(&facts, t(0)).kind(),
            "activity_no_worker"
        );
    }

    #[test]
    fn healthy_activity_alongside_an_overdue_timer_is_not_a_stall() {
        // REGRESSION PIN. Hoisting the overdue-timer check above the activity
        // bucket looks like a fix ("an overdue timer is a lost-task wedge") but
        // is a false positive for a shape the engine produces routinely:
        // `join!(cancellable_timer.await_fire(), ctx.execute_activity(..))`
        // routes to `persist_scheduled_activities`, which parks the task on the
        // ACTIVITY and discards the armed deadline (`_min_fires_at`). The timer
        // then goes overdue as a matter of course while the activity runs, and
        // fires on the activity's completion wake — nothing is wrong. A false
        // "stalled" is the one verdict this endpoint must never emit, so the
        // activity verdict wins.
        let inputs = DiagnosisInputs {
            activities: vec![healthy_activity()],
            timers: vec![PendingTimerFacts {
                fires_at: t(-600), // ten minutes past due
            }],
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(
            verdict.kind(),
            "healthy_in_progress",
            "an overdue timer alongside a healthy activity is not a stall: {verdict:?}"
        );
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn overdue_timer_still_wins_when_no_activity_is_pending() {
        // The complement: with no activity parking the task, the task WAS
        // rescheduled to the deadline, so an unfired timer past it proves the
        // wake never happened — and it must outrank the healthy-looking waits
        // below (a signal-or-deadline race whose deadline the engine missed).
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "timer_overdue", "{verdict:?}");
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    #[test]
    fn wedged_activity_alongside_an_overdue_timer_reports_the_activity() {
        // When the activity IS the problem, its verdict is the actionable root
        // cause and the overdue timer is downstream of it.
        let mut facts = healthy_activity();
        facts.has_live_worker = false;
        let inputs = DiagnosisInputs {
            activities: vec![facts],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "activity_no_worker", "{verdict:?}");
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    // ── Overdue timers must own the workflow wake (PR #1188 review round 4) ──

    /// Codex's exact counterexample. `persist_scheduled_external_activity`
    /// parks the workflow task and discards the armed deadline, so a
    /// cancellable timer awaited alongside an external activity goes overdue as
    /// a matter of course while the callback is outstanding. Unlike the
    /// activity case, the handoff is ranked BELOW the overdue check, so the
    /// ordering cannot protect it — the check itself must.
    #[test]
    fn overdue_timer_with_a_parked_task_reports_the_external_handoff() {
        let inputs = DiagnosisInputs {
            external_handoffs: vec![ExternalHandoffFacts {
                token: "cb-token".to_string(),
                activity_name: Some("call_partner".to_string()),
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            workflow_task: Some(parked_wf_task()),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(
            verdict.kind(),
            "awaiting_external_handoff",
            "a parked task means the callback owns the wake, not the timer: {verdict:?}"
        );
        assert_eq!(verdict.health(), ExecutionHealth::BlockedExternal);
    }

    /// The same defect for `persist_all_started_child_workflows`, which parks
    /// and discards the deadline identically.
    #[test]
    fn overdue_timer_with_a_parked_task_reports_the_pending_child() {
        let inputs = DiagnosisInputs {
            children: vec![PendingChildFacts {
                child_exec_id: "child-1".to_string(),
                child_state: "RUNNING".to_string(),
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            workflow_task: Some(parked_wf_task()),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "pending_child", "{verdict:?}");
    }

    /// And for `persist_signal_wait_park`: a PURE `wait_for_signal` parks
    /// without rescheduling, so an unrelated armed timer going overdue beside
    /// it is expected, not a missed wake.
    #[test]
    fn overdue_timer_with_a_parked_task_reports_the_awaited_signal() {
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            workflow_task: Some(parked_wf_task()),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "awaiting_signal", "{verdict:?}");
    }

    /// The complement that must NOT regress: a `receive_signal_timeout`
    /// deadline race (issue #476) routes to `persist_started_timer`, which
    /// calls `reschedule_task(task_id, fires_at)` — the task IS due to wake at
    /// the deadline. An unfired timer past it with the task still PENDING and
    /// unclaimed proves the wake never happened, and must still outrank the
    /// healthy-looking `awaiting_signal` beneath it.
    #[test]
    fn overdue_timer_wins_when_the_workflow_wake_was_genuinely_missed() {
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            workflow_task: Some(WorkflowTaskFacts {
                // reschedule_task set scheduled_at = fires_at, and nothing
                // claimed it.
                scheduled_at: t(-600),
                ..wf_task()
            }),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "timer_overdue", "{verdict:?}");
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    #[test]
    fn overdue_timer_suppressed_while_a_worker_holds_the_claim() {
        // A worker is on the decision cycle right now; it ingests due timers
        // when it claims. Nothing was missed.
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            workflow_task: Some(WorkflowTaskFacts {
                state: "RUNNING".to_string(),
                has_worker: true,
                ..wf_task()
            }),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "awaiting_signal", "{verdict:?}");
    }

    #[test]
    fn overdue_timer_suppressed_when_the_next_wake_is_still_ahead() {
        // Two armed timers: the earliest is overdue, but the task is scheduled
        // to wake at the LATER one, which will observe it. Not a missed wake.
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![
                PendingTimerFacts { fires_at: t(-600) },
                PendingTimerFacts { fires_at: t(900) },
            ],
            workflow_task: Some(WorkflowTaskFacts {
                scheduled_at: t(900),
                ..wf_task()
            }),
            ..Default::default()
        };
        let verdict =
            classify_execution(&inputs, t(0)).expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "awaiting_signal", "{verdict:?}");
    }

    #[test]
    fn workflow_wake_was_missed_truth_table() {
        // No evidence supplied: preserve the pre-round-4 unconditional verdict.
        assert!(workflow_wake_was_missed(None, t(0)));
        // Claimed right now.
        assert!(!workflow_wake_was_missed(
            Some(&WorkflowTaskFacts {
                state: "RUNNING".to_string(),
                has_worker: true,
                scheduled_at: t(-600),
                ..wf_task()
            }),
            t(0)
        ));
        // Parked: another wake source owns it.
        assert!(!workflow_wake_was_missed(Some(&parked_wf_task()), t(0)));
        // PENDING and overdue past the grace window: the wake was missed.
        assert!(workflow_wake_was_missed(
            Some(&WorkflowTaskFacts {
                scheduled_at: t(-TIMER_OVERDUE_GRACE_SECONDS),
                ..wf_task()
            }),
            t(0)
        ));
        // PENDING but only just due: ordinary dispatch latency.
        assert!(!workflow_wake_was_missed(
            Some(&WorkflowTaskFacts {
                scheduled_at: t(-TIMER_OVERDUE_GRACE_SECONDS + 1),
                ..wf_task()
            }),
            t(0)
        ));
        // PENDING with the wake still ahead.
        assert!(!workflow_wake_was_missed(
            Some(&WorkflowTaskFacts {
                scheduled_at: t(300),
                ..wf_task()
            }),
            t(0)
        ));
    }

    #[test]
    fn retrying_outranks_a_clean_deferral_across_rows() {
        // Cross-task worst-of: recorded failure evidence is the more
        // actionable of the two, so it wins the fold.
        let mut deferred = healthy_activity();
        deferred.scheduled_at = t(45);
        let mut retrying = healthy_activity();
        retrying.scheduled_at = t(45);
        retrying.last_error = Some("boom".to_string());
        let verdict = classify_execution(&inputs_with(vec![deferred, retrying]), t(0))
            .expect("non-terminal execution must yield a verdict");
        assert_eq!(verdict.kind(), "activity_retrying");
    }

    #[test]
    fn activity_precedence_ladder_is_strictly_ordered() {
        let ranks = [
            BlockedOn::ActivityQueuePaused {
                queue: "q".into(),
                activity_name: None,
            },
            BlockedOn::ActivityNoWorker {
                queue: "q".into(),
                activity_name: None,
            },
            BlockedOn::ActivityCircuitOpen {
                activity_name: "a".into(),
                cooldown_until: None,
            },
            BlockedOn::ActivityConcurrencyDeferred {
                key: "k".into(),
                activity_name: None,
            },
            BlockedOn::ActivityRateLimited {
                key: "r".into(),
                activity_name: None,
            },
            BlockedOn::ActivityRetrying {
                activity_name: Some("a".into()),
                attempt: 1,
                last_error: None,
                next_attempt_at: None,
            },
            BlockedOn::HealthyInProgress,
        ];
        for pair in ranks.windows(2) {
            assert!(
                activity_precedence(&pair[0]) > activity_precedence(&pair[1]),
                "{} must outrank {}",
                pair[0].kind(),
                pair[1].kind()
            );
        }
    }

    // ── Whole-execution classification ─────────────────────────────────────

    #[test]
    fn terminal_execution_has_no_blocked_on() {
        let inputs = DiagnosisInputs {
            is_terminal: true,
            ..Default::default()
        };
        assert_eq!(classify_execution(&inputs, t(0)), None);
    }

    /// A terminal execution reports terminal even if stale rows linger.
    #[test]
    fn terminal_outranks_every_pending_row() {
        let inputs = DiagnosisInputs {
            is_terminal: true,
            activities: vec![healthy_activity()],
            children: vec![PendingChildFacts {
                child_exec_id: "c".into(),
                child_state: "RUNNING".into(),
            }],
            ..Default::default()
        };
        assert_eq!(classify_execution(&inputs, t(0)), None);
    }

    #[test]
    fn paused_execution_outranks_pending_activity() {
        let inputs = DiagnosisInputs {
            is_paused: true,
            pause_actor: Some("oncall".to_string()),
            paused_since: Some(t(-600)),
            activities: vec![healthy_activity()],
            ..Default::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::Paused {
                actor: Some("oncall".to_string()),
                since: Some(t(-600)),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::BlockedExternal);
    }

    /// AC6: a RUNNING execution with no pending work of any kind.
    #[test]
    fn no_pending_work_is_stalled() {
        let verdict = classify_execution(&DiagnosisInputs::default(), t(0)).expect("non-terminal");
        assert_eq!(verdict, BlockedOn::NoPendingWork);
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    /// AC4: a long sleep is not a stall, however old the last event is.
    #[test]
    fn future_timer_is_sleeping_timer_and_healthy() {
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts {
                fires_at: t(86_400),
            }],
            ..Default::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::SleepingTimer {
                fires_at: t(86_400)
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    /// When several timers are open the earliest deadline is the one that
    /// governs when the run wakes.
    #[test]
    fn earliest_timer_wins() {
        let inputs = DiagnosisInputs {
            timers: vec![
                PendingTimerFacts { fires_at: t(900) },
                PendingTimerFacts { fires_at: t(60) },
            ],
            ..Default::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(0)),
            Some(BlockedOn::SleepingTimer { fires_at: t(60) })
        );
    }

    #[test]
    fn awaited_signal_is_blocked_external() {
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: Some(t(-3600)),
            }],
            ..Default::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::AwaitingSignal {
                signal_name: "approval".to_string(),
                since: Some(t(-3600)),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::BlockedExternal);
    }

    #[test]
    fn pending_child_is_healthy() {
        let inputs = DiagnosisInputs {
            children: vec![PendingChildFacts {
                child_exec_id: "child-1".to_string(),
                child_state: "RUNNING".to_string(),
            }],
            ..Default::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::PendingChild {
                child_exec_id: "child-1".to_string(),
                child_state: "RUNNING".to_string(),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn external_handoff_is_blocked_external() {
        let inputs = DiagnosisInputs {
            external_handoffs: vec![ExternalHandoffFacts {
                token: "tok-1".to_string(),
                activity_name: Some("await_carrier".to_string()),
            }],
            ..Default::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::AwaitingExternalHandoff {
                token: "tok-1".to_string(),
                activity_name: Some("await_carrier".to_string()),
            }
        );
        assert_eq!(verdict.health(), ExecutionHealth::BlockedExternal);
    }

    /// Category ladder mirrors issue #486's own `StallReason` ordering, so the
    /// two surfaces agree about which category wins.
    #[test]
    fn category_ladder_activity_then_handoff_then_child_then_signal_then_timer() {
        let full = DiagnosisInputs {
            activities: vec![healthy_activity()],
            external_handoffs: vec![ExternalHandoffFacts {
                token: "tok".into(),
                activity_name: None,
            }],
            children: vec![PendingChildFacts {
                child_exec_id: "c".into(),
                child_state: "RUNNING".into(),
            }],
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "s".into(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(60) }],
            ..Default::default()
        };
        assert_eq!(
            classify_execution(&full, t(0)).map(|b| b.kind()),
            Some("healthy_in_progress")
        );

        let mut no_activity = full;
        no_activity.activities.clear();
        assert_eq!(
            classify_execution(&no_activity, t(0)).map(|b| b.kind()),
            Some("awaiting_external_handoff")
        );

        let mut no_handoff = no_activity.clone();
        no_handoff.external_handoffs.clear();
        assert_eq!(
            classify_execution(&no_handoff, t(0)).map(|b| b.kind()),
            Some("pending_child")
        );

        let mut no_child = no_handoff.clone();
        no_child.children.clear();
        assert_eq!(
            classify_execution(&no_child, t(0)).map(|b| b.kind()),
            Some("awaiting_signal")
        );

        let mut only_timer = no_child.clone();
        only_timer.awaited_signals.clear();
        assert_eq!(
            classify_execution(&only_timer, t(0)).map(|b| b.kind()),
            Some("sleeping_timer")
        );
    }

    /// A fan-out with nineteen healthy slots and one wedged slot must report the
    /// wedged one, not "healthy".
    #[test]
    fn worst_activity_wins_across_a_fan_out() {
        let mut activities: Vec<PendingActivityFacts> =
            (0..19).map(|_| healthy_activity()).collect();
        let mut stuck = healthy_activity();
        stuck.queue = "reports".to_string();
        stuck.has_live_worker = false;
        activities.push(stuck);
        let verdict = classify_execution(&inputs_with(activities), t(0)).expect("non-terminal");
        assert_eq!(
            verdict,
            BlockedOn::ActivityNoWorker {
                queue: "reports".to_string(),
                activity_name: Some("send_email".to_string()),
            }
        );
    }

    #[test]
    fn all_healthy_activities_report_healthy_in_progress() {
        let activities: Vec<PendingActivityFacts> = (0..5).map(|_| healthy_activity()).collect();
        assert_eq!(
            classify_execution(&inputs_with(activities), t(0)),
            Some(BlockedOn::HealthyInProgress)
        );
    }

    /// Deterministic across repeated calls and input permutations of equal rank:
    /// the same fact set always yields the same verdict.
    #[test]
    fn worst_of_selection_is_deterministic_for_equal_ranks() {
        let mut a = healthy_activity();
        a.queue = "alpha".to_string();
        a.has_live_worker = false;
        let mut b = healthy_activity();
        b.queue = "beta".to_string();
        b.has_live_worker = false;
        let first = classify_execution(&inputs_with(vec![a.clone(), b.clone()]), t(0));
        for _ in 0..5 {
            assert_eq!(
                classify_execution(&inputs_with(vec![a.clone(), b.clone()]), t(0)),
                first
            );
        }
        // Equal rank: the first row in the caller's (deterministically ordered)
        // list wins.
        assert_eq!(
            first,
            Some(BlockedOn::ActivityNoWorker {
                queue: "alpha".to_string(),
                activity_name: Some("send_email".to_string()),
            })
        );
    }

    // ── Wire format ────────────────────────────────────────────────────────

    #[test]
    fn blocked_on_serializes_internally_tagged_snake_case() {
        let json = serde_json::to_value(BlockedOn::ActivityNoWorker {
            queue: "email".to_string(),
            activity_name: None,
        })
        .unwrap();
        assert_eq!(json["type"], "activity_no_worker");
        assert_eq!(json["queue"], "email");
        assert!(
            json.get("activity_name").is_none(),
            "absent optional fields are omitted, not null"
        );
    }

    #[test]
    fn unit_variants_serialize_as_tagged_objects() {
        assert_eq!(
            serde_json::to_value(BlockedOn::NoPendingWork).unwrap(),
            serde_json::json!({"type": "no_pending_work"})
        );
        assert_eq!(
            serde_json::to_value(BlockedOn::HealthyInProgress).unwrap(),
            serde_json::json!({"type": "healthy_in_progress"})
        );
    }

    #[test]
    fn blocked_on_round_trips_through_json() {
        let cases = vec![
            BlockedOn::HealthyInProgress,
            BlockedOn::NoPendingWork,
            BlockedOn::AwaitingSignal {
                signal_name: "s".into(),
                since: Some(t(1)),
            },
            BlockedOn::SleepingTimer { fires_at: t(2) },
            BlockedOn::PendingChild {
                child_exec_id: "c".into(),
                child_state: "RUNNING".into(),
            },
            BlockedOn::ActivityRetrying {
                activity_name: Some("a".into()),
                attempt: 2,
                last_error: Some("boom".into()),
                next_attempt_at: Some(t(3)),
            },
            BlockedOn::ActivityNoWorker {
                queue: "q".into(),
                activity_name: Some("a".into()),
            },
            BlockedOn::ActivityCircuitOpen {
                activity_name: "a".into(),
                cooldown_until: Some(t(4)),
            },
            BlockedOn::ActivityRateLimited {
                key: "k".into(),
                activity_name: None,
            },
            BlockedOn::ActivityConcurrencyDeferred {
                key: "k".into(),
                activity_name: None,
            },
            BlockedOn::ActivityQueuePaused {
                queue: "q".into(),
                activity_name: None,
            },
            BlockedOn::AwaitingExternalHandoff {
                token: "t".into(),
                activity_name: None,
            },
            BlockedOn::Paused {
                actor: Some("op".into()),
                since: Some(t(5)),
            },
        ];
        for case in cases {
            let json = serde_json::to_string(&case).unwrap();
            let back: BlockedOn = serde_json::from_str(&json).unwrap();
            assert_eq!(back, case, "round trip failed for {json}");
        }
    }

    #[test]
    fn kind_matches_the_serialized_discriminator() {
        let cases = vec![
            BlockedOn::HealthyInProgress,
            BlockedOn::NoPendingWork,
            BlockedOn::SleepingTimer { fires_at: t(1) },
            BlockedOn::Paused {
                actor: None,
                since: None,
            },
            BlockedOn::ActivityNoWorker {
                queue: "q".into(),
                activity_name: None,
            },
        ];
        for case in cases {
            let json = serde_json::to_value(&case).unwrap();
            assert_eq!(json["type"], case.kind());
        }
    }

    #[test]
    fn execution_health_wire_strings_are_stable() {
        assert_eq!(ExecutionHealth::Healthy.as_str(), "healthy");
        assert_eq!(ExecutionHealth::Stalled.as_str(), "stalled");
        assert_eq!(
            ExecutionHealth::BlockedExternal.as_str(),
            "blocked_external"
        );
        assert_eq!(ExecutionHealth::Terminal.as_str(), "terminal");
        for health in [
            ExecutionHealth::Healthy,
            ExecutionHealth::Stalled,
            ExecutionHealth::BlockedExternal,
            ExecutionHealth::Terminal,
        ] {
            assert_eq!(serde_json::to_value(health).unwrap(), health.as_str());
        }
    }

    // ── Summaries ──────────────────────────────────────────────────────────

    #[test]
    fn summary_names_the_actionable_root_cause() {
        let summary = summarize(&BlockedOn::ActivityNoWorker {
            queue: "email".to_string(),
            activity_name: Some("send_email".to_string()),
        });
        assert!(summary.contains("email"), "{summary}");
        assert!(
            summary.contains("no live worker"),
            "the summary must name the root cause in words: {summary}"
        );
    }

    #[test]
    fn every_variant_has_a_non_empty_summary() {
        let cases = vec![
            BlockedOn::HealthyInProgress,
            BlockedOn::NoPendingWork,
            BlockedOn::AwaitingSignal {
                signal_name: "s".into(),
                since: None,
            },
            BlockedOn::SleepingTimer { fires_at: t(2) },
            BlockedOn::PendingChild {
                child_exec_id: "c".into(),
                child_state: "RUNNING".into(),
            },
            BlockedOn::ActivityRetrying {
                activity_name: Some("a".into()),
                attempt: 2,
                last_error: None,
                next_attempt_at: None,
            },
            BlockedOn::ActivityNoWorker {
                queue: "q".into(),
                activity_name: None,
            },
            BlockedOn::ActivityCircuitOpen {
                activity_name: "a".into(),
                cooldown_until: None,
            },
            BlockedOn::ActivityRateLimited {
                key: "k".into(),
                activity_name: None,
            },
            BlockedOn::ActivityConcurrencyDeferred {
                key: "k".into(),
                activity_name: None,
            },
            BlockedOn::ActivityQueuePaused {
                queue: "q".into(),
                activity_name: None,
            },
            BlockedOn::AwaitingExternalHandoff {
                token: "t".into(),
                activity_name: None,
            },
            BlockedOn::Paused {
                actor: None,
                since: None,
            },
        ];
        for case in cases {
            let summary = summarize(&case);
            assert!(
                !summary.trim().is_empty(),
                "empty summary for {}",
                case.kind()
            );
        }
    }

    // ── Review-round regression tests (RED first) ──────────────────────────

    #[test]
    fn overdue_unfired_timer_is_stalled_not_healthy() {
        // A durable timer fires ONLY when a worker claims the owning workflow
        // task (`worker::ingest_due_timers_and_signals`); there is no separate
        // timer scanner. So an unfired timer whose deadline has already passed
        // means the workflow task was never claimed — the exact lost-task
        // stall this endpoint exists to surface. Reporting `sleeping_timer`
        // (healthy) would let the mere presence of a timer DOWNGRADE the
        // genuine `no_pending_work` stall the same wedge reports without one.
        let now = t(0);
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            ..DiagnosisInputs::default()
        };
        let verdict = classify_execution(&inputs, now).expect("running execution has a verdict");
        assert_eq!(
            verdict.kind(),
            "timer_overdue",
            "an unfired timer past its deadline is a stall, not a healthy sleep"
        );
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
        match verdict {
            BlockedOn::TimerOverdue {
                fires_at,
                overdue_by_seconds,
            } => {
                assert_eq!(fires_at, t(-600));
                assert_eq!(overdue_by_seconds, 600);
            }
            other => panic!("expected TimerOverdue, got {other:?}"),
        }
    }

    #[test]
    fn future_timer_is_still_healthy_however_old_the_run_is() {
        // The AC4 guarantee must survive the overdue fix: a deadline still in
        // the future is a healthy sleep regardless of event age.
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(8_000) }],
            ..DiagnosisInputs::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("running execution");
        assert_eq!(verdict.kind(), "sleeping_timer");
        assert_eq!(verdict.health(), ExecutionHealth::Healthy);
    }

    #[test]
    fn a_just_due_timer_is_still_healthy_inside_the_grace_window() {
        // A deadline that passed moments ago is ordinary scheduling latency —
        // the next worker poll will fire it. Only a deadline missed across many
        // poll cycles is a wedge.
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(0) }],
            ..DiagnosisInputs::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(0)).expect("running").kind(),
            "sleeping_timer"
        );
        // One second inside the window: still healthy.
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(0) }],
            ..DiagnosisInputs::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(TIMER_OVERDUE_GRACE_SECONDS - 1))
                .expect("running")
                .kind(),
            "sleeping_timer"
        );
        // Exactly at the window: a wedge.
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(0) }],
            ..DiagnosisInputs::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(TIMER_OVERDUE_GRACE_SECONDS))
                .expect("running")
                .kind(),
            "timer_overdue"
        );
    }

    #[test]
    fn an_overdue_timer_is_not_masked_by_an_awaited_signal() {
        // The signal-or-deadline race (issue #476) parks on BOTH a signal and a
        // durable deadline. If the engine missed that deadline, reporting the
        // healthy-looking `awaiting_signal` would hide the wedge behind a
        // legitimate-looking external wait.
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(-600) }],
            ..DiagnosisInputs::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("running");
        assert_eq!(verdict.kind(), "timer_overdue");
        assert_eq!(verdict.health(), ExecutionHealth::Stalled);
    }

    #[test]
    fn a_future_deadline_still_yields_to_an_awaited_signal() {
        // The converse: a deadline still ahead is a healthy race, and the signal
        // is the more informative answer.
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            timers: vec![PendingTimerFacts { fires_at: t(8_000) }],
            ..DiagnosisInputs::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(0)).expect("running").kind(),
            "awaiting_signal"
        );
    }

    #[test]
    fn earliest_timer_governs_the_overdue_verdict() {
        // The earliest deadline is the one that should already have woken the
        // run, so it — not a later healthy one — decides the verdict.
        let inputs = DiagnosisInputs {
            timers: vec![
                PendingTimerFacts { fires_at: t(8_000) },
                PendingTimerFacts { fires_at: t(-800) },
            ],
            ..DiagnosisInputs::default()
        };
        let verdict = classify_execution(&inputs, t(0)).expect("running execution");
        match verdict {
            BlockedOn::TimerOverdue { fires_at, .. } => assert_eq!(fires_at, t(-800)),
            other => panic!("expected TimerOverdue, got {other:?}"),
        }
    }

    #[test]
    fn backoff_outranks_self_healing_gates_so_retry_facts_survive() {
        // A task that is not yet due has not reached the claim gates at all, so
        // a momentarily-empty bucket says nothing about it. Reporting
        // `activity_rate_limited` would drop `attempt`, `last_error` and
        // `next_attempt_at` — precisely the three fields an operator wants.
        let now = t(0);
        let mut facts = healthy_activity();
        facts.scheduled_at = t(600);
        facts.attempt = 4;
        facts.last_error = Some("connection reset".to_string());
        facts.rate_limit_saturated = true;
        facts.rate_limit_key = Some("tenant-a".to_string());
        facts.concurrency_saturated = true;
        facts.concurrency_key = Some("tenant-a".to_string());

        match classify_pending_activity(&facts, now) {
            BlockedOn::ActivityRetrying {
                attempt,
                last_error,
                next_attempt_at,
                ..
            } => {
                assert_eq!(attempt, 4);
                assert_eq!(last_error.as_deref(), Some("connection reset"));
                assert_eq!(next_attempt_at, Some(t(600)));
            }
            other => panic!("a not-yet-due task must report its retry facts, got {other:?}"),
        }
    }

    #[test]
    fn a_due_task_still_reports_the_live_rate_limit_gate() {
        // The converse of the test above: once the task IS due, the claim gate
        // is the honest answer.
        let now = t(0);
        let mut facts = healthy_activity();
        facts.scheduled_at = t(-100);
        facts.rate_limit_saturated = true;
        facts.rate_limit_key = Some("tenant-a".to_string());
        assert_eq!(
            classify_pending_activity(&facts, now).kind(),
            "activity_rate_limited"
        );
    }

    #[test]
    fn non_self_healing_gates_still_outrank_backoff() {
        // A backing-off task on a pollerless queue will never run, so promising
        // a retry would be a lie. These three stay above backoff.
        let now = t(0);
        let mut base = healthy_activity();
        base.scheduled_at = t(600);

        let mut no_worker = base.clone();
        no_worker.has_live_worker = false;
        assert_eq!(
            classify_pending_activity(&no_worker, now).kind(),
            "activity_no_worker"
        );

        let mut paused = base.clone();
        paused.queue_paused = true;
        assert_eq!(
            classify_pending_activity(&paused, now).kind(),
            "activity_queue_paused"
        );

        let mut breaker = base;
        breaker.circuit_phase = Some(BlockingCircuitPhase::Open);
        assert_eq!(
            classify_pending_activity(&breaker, now).kind(),
            "activity_circuit_open"
        );
    }

    #[test]
    fn unnamed_activity_in_backoff_is_not_reported_healthy() {
        // `harvest_task_queue.activity_name` is nullable. A nameless row that is
        // provably not due must not fall through to "nothing is blocking this".
        let now = t(0);
        let mut facts = healthy_activity();
        facts.activity_name = None;
        facts.scheduled_at = t(600);

        // Without failure evidence the row is a clean dispatcher deferral.
        match classify_pending_activity(&facts, now) {
            BlockedOn::ActivityDeferred {
                activity_name,
                next_attempt_at,
            } => {
                assert!(activity_name.is_none());
                assert_eq!(next_attempt_at, t(600));
            }
            other => panic!("expected a nameless deferral verdict, got {other:?}"),
        }

        // With failure evidence it is a genuine retry backoff — still nameless,
        // still never "nothing is blocking this".
        facts.last_error = Some("boom".to_string());
        match classify_pending_activity(&facts, now) {
            BlockedOn::ActivityRetrying {
                activity_name,
                next_attempt_at,
                ..
            } => {
                assert!(activity_name.is_none());
                assert_eq!(next_attempt_at, Some(t(600)));
            }
            other => panic!("expected a nameless retry verdict, got {other:?}"),
        }
    }

    // ── The run's own workflow task row (issue #809, PR #1188 review) ──────

    /// A PENDING, due, unimpeded workflow task on a covered queue.
    fn wf_task() -> WorkflowTaskFacts {
        WorkflowTaskFacts {
            state: "PENDING".to_string(),
            has_worker: false,
            queue_name: "default".to_string(),
            scheduled_at: t(-10),
            queue_paused: false,
            has_live_worker: true,
        }
    }

    /// A *parked* workflow task: `RUNNING` with a NULL worker. Produced by
    /// every `*_park` persist path, none of which reschedules the task to an
    /// armed timer's deadline.
    fn parked_wf_task() -> WorkflowTaskFacts {
        WorkflowTaskFacts {
            state: "RUNNING".to_string(),
            has_worker: false,
            ..wf_task()
        }
    }

    fn inputs_with_wf_task(task: WorkflowTaskFacts) -> DiagnosisInputs {
        DiagnosisInputs {
            workflow_task: Some(task),
            ..Default::default()
        }
    }

    #[test]
    fn claimed_workflow_task_is_healthy_not_a_lost_task() {
        // The decision cycle is executing on a worker right now — the shape a
        // long local activity (issue #98) holds for up to a minute while
        // scheduling no queue row of any kind.
        let task = WorkflowTaskFacts {
            state: "RUNNING".to_string(),
            has_worker: true,
            ..wf_task()
        };
        assert_eq!(
            classify_workflow_task(&task, t(0)),
            Some(BlockedOn::HealthyInProgress)
        );
        let verdict = classify_execution(&inputs_with_wf_task(task), t(0));
        assert_eq!(verdict, Some(BlockedOn::HealthyInProgress));
        assert_eq!(
            verdict.map(|v| v.health()),
            Some(ExecutionHealth::Healthy),
            "an executing decision cycle must never read as stalled",
        );
    }

    #[test]
    fn pending_workflow_task_on_a_dead_queue_reports_no_worker() {
        let task = WorkflowTaskFacts {
            has_live_worker: false,
            ..wf_task()
        };
        let verdict = classify_execution(&inputs_with_wf_task(task), t(0));
        match verdict {
            Some(BlockedOn::WorkflowNoWorker { ref queue }) => assert_eq!(queue, "default"),
            other => panic!("expected workflow_no_worker, got {other:?}"),
        }
        assert_eq!(verdict.map(|v| v.health()), Some(ExecutionHealth::Stalled));
    }

    #[test]
    fn pending_workflow_task_on_a_paused_queue_reports_queue_paused() {
        // A paused queue outranks the dead-queue check: the operator's hold is
        // the actionable cause even when no worker happens to be polling.
        let task = WorkflowTaskFacts {
            queue_paused: true,
            has_live_worker: false,
            ..wf_task()
        };
        let verdict = classify_execution(&inputs_with_wf_task(task), t(0));
        match verdict {
            Some(BlockedOn::WorkflowQueuePaused { ref queue }) => assert_eq!(queue, "default"),
            other => panic!("expected workflow_queue_paused, got {other:?}"),
        }
        assert_eq!(
            verdict.map(|v| v.health()),
            Some(ExecutionHealth::BlockedExternal)
        );
    }

    #[test]
    fn pending_workflow_task_with_a_live_worker_is_dispatch_latency_not_a_stall() {
        assert_eq!(
            classify_execution(&inputs_with_wf_task(wf_task()), t(0)),
            Some(BlockedOn::HealthyInProgress)
        );
    }

    #[test]
    fn not_yet_due_pending_workflow_task_with_a_live_worker_is_healthy() {
        // Deliberately deferred by the dispatcher (retry backoff, a rate-limit
        // or capability-miss redelivery). With a live poller on the queue this
        // genuinely self-heals when `scheduled_at` arrives.
        let task = WorkflowTaskFacts {
            scheduled_at: t(30),
            ..wf_task()
        };
        assert_eq!(
            classify_workflow_task(&task, t(0)),
            Some(BlockedOn::HealthyInProgress)
        );
    }

    #[test]
    fn not_yet_due_pending_workflow_task_on_a_dead_queue_reports_no_worker() {
        // A deferral only self-heals if something will claim the row when the
        // timestamp arrives. With no poller on the queue nobody ever will, so
        // the coverage gap is the operative fact and must outrank the deferral
        // — the same hard-impediment ordering `classify_pending_activity` uses.
        let task = WorkflowTaskFacts {
            scheduled_at: t(30),
            has_live_worker: false,
            ..wf_task()
        };
        let verdict = classify_execution(&inputs_with_wf_task(task), t(0));
        match verdict {
            Some(BlockedOn::WorkflowNoWorker { ref queue }) => assert_eq!(queue, "default"),
            other => panic!(
                "expected workflow_no_worker for a deferral nobody will claim, got {other:?}"
            ),
        }
        assert_eq!(verdict.map(|v| v.health()), Some(ExecutionHealth::Stalled));
    }

    #[test]
    fn claimed_workflow_task_on_a_dead_queue_is_an_orphan_not_healthy() {
        // A crashed worker leaves its workflow task RUNNING with a non-null
        // worker_id until the poison-pill reclaimer processes it (issue #367).
        // If nothing polls the queue, that stale claim never advances, so
        // reporting "the handler is executing right now" would answer "fine"
        // forever for a run that cannot move.
        let task = WorkflowTaskFacts {
            state: "RUNNING".to_string(),
            has_worker: true,
            has_live_worker: false,
            ..wf_task()
        };
        let verdict = classify_execution(&inputs_with_wf_task(task), t(0));
        match verdict {
            Some(BlockedOn::WorkflowNoWorker { ref queue }) => assert_eq!(queue, "default"),
            other => panic!("expected workflow_no_worker for a stale claim, got {other:?}"),
        }
        assert_eq!(
            verdict.map(|v| v.health()),
            Some(ExecutionHealth::Stalled),
            "an orphaned claim on a dead queue is a stall, not progress",
        );
    }

    #[test]
    fn claimed_workflow_task_on_a_paused_queue_still_reports_healthy() {
        // A pause is a *claim-time* gate: it stops new claims, it does not stop
        // a handler already executing. A held claim on a covered queue is
        // therefore progressing regardless of the operator hold.
        let task = WorkflowTaskFacts {
            state: "RUNNING".to_string(),
            has_worker: true,
            queue_paused: true,
            ..wf_task()
        };
        assert_eq!(
            classify_workflow_task(&task, t(0)),
            Some(BlockedOn::HealthyInProgress)
        );
    }

    #[test]
    fn parked_workflow_task_with_nothing_pending_is_still_the_lost_task_verdict() {
        // RUNNING with a NULL worker is *parked*: it advances on a wake, not a
        // claim, so a live worker proves nothing. This is exactly the lost-task
        // shape and its verdict must be unchanged.
        let task = WorkflowTaskFacts {
            state: "RUNNING".to_string(),
            has_worker: false,
            ..wf_task()
        };
        assert_eq!(classify_workflow_task(&task, t(0)), None);
        assert_eq!(
            classify_execution(&inputs_with_wf_task(task), t(0)),
            Some(BlockedOn::NoPendingWork)
        );
    }

    #[test]
    fn workflow_task_fallback_never_masks_a_specific_activity_verdict() {
        // A wedged activity AND a dead workflow queue: the activity is the
        // actionable root cause and must win, or the fallback would flatten
        // every specific verdict this endpoint exists to produce.
        let inputs = DiagnosisInputs {
            activities: vec![PendingActivityFacts {
                has_live_worker: false,
                ..healthy_activity()
            }],
            workflow_task: Some(WorkflowTaskFacts {
                has_live_worker: false,
                ..wf_task()
            }),
            ..Default::default()
        };
        match classify_execution(&inputs, t(0)) {
            Some(BlockedOn::ActivityNoWorker { ref queue, .. }) => assert_eq!(queue, "email"),
            other => panic!("expected the activity verdict to win, got {other:?}"),
        }
    }

    #[test]
    fn workflow_task_fallback_never_masks_an_awaited_signal() {
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            workflow_task: Some(wf_task()),
            ..Default::default()
        };
        match classify_execution(&inputs, t(0)) {
            Some(BlockedOn::AwaitingSignal {
                ref signal_name, ..
            }) => {
                assert_eq!(signal_name, "approval");
            }
            other => panic!("expected awaiting_signal, got {other:?}"),
        }
    }

    #[test]
    fn absent_workflow_task_leaves_the_lost_task_verdict_unchanged() {
        // The `None` default keeps every pre-existing caller byte-identical.
        assert_eq!(
            classify_execution(&DiagnosisInputs::default(), t(0)),
            Some(BlockedOn::NoPendingWork)
        );
    }

    // ── Replay-derived waits with no side-table row ────────────────────────

    fn inputs_with_replay_wait(wait: ReplayWaitFacts) -> DiagnosisInputs {
        DiagnosisInputs {
            replay_waits: vec![wait],
            ..Default::default()
        }
    }

    #[test]
    fn replay_condition_park_is_reported_not_a_lost_task() {
        // `ctx.await_condition` is command-less: it leaves no row in any side
        // table, so without this replay is the ONLY source that can see it.
        let verdict = classify_execution(
            &inputs_with_replay_wait(ReplayWaitFacts {
                wait_kind: "condition".to_string(),
                name: None,
                since: Some(t(-60)),
            }),
            t(0),
        );
        match verdict {
            Some(BlockedOn::AwaitingReplayWait {
                ref wait_kind,
                ref name,
                since,
            }) => {
                assert_eq!(wait_kind, "condition");
                assert_eq!(name.as_ref(), None);
                assert_eq!(since, Some(t(-60)));
            }
            other => panic!("expected awaiting_replay_wait, got {other:?}"),
        }
        assert_eq!(
            verdict.map(|v| v.health()),
            Some(ExecutionHealth::BlockedExternal),
        );
    }

    #[test]
    fn replay_mutex_park_names_the_held_key() {
        match classify_execution(
            &inputs_with_replay_wait(ReplayWaitFacts {
                wait_kind: "mutex".to_string(),
                name: Some("ledger:42".to_string()),
                since: None,
            }),
            t(0),
        ) {
            Some(BlockedOn::AwaitingReplayWait {
                ref wait_kind,
                ref name,
                ..
            }) => {
                assert_eq!(wait_kind, "mutex");
                assert_eq!(name.as_deref(), Some("ledger:42"));
            }
            other => panic!("expected awaiting_replay_wait, got {other:?}"),
        }
    }

    #[test]
    fn replay_wait_never_masks_a_sleeping_timer() {
        // The side tables are authoritative and more precise: a DB timer says
        // exactly when the run wakes, which a generic replay wait cannot.
        let inputs = DiagnosisInputs {
            timers: vec![PendingTimerFacts { fires_at: t(600) }],
            replay_waits: vec![ReplayWaitFacts {
                wait_kind: "condition".to_string(),
                name: None,
                since: None,
            }],
            ..Default::default()
        };
        assert_eq!(
            classify_execution(&inputs, t(0)),
            Some(BlockedOn::SleepingTimer { fires_at: t(600) })
        );
    }

    #[test]
    fn awaited_signal_still_outranks_the_generic_replay_wait() {
        let inputs = DiagnosisInputs {
            awaited_signals: vec![AwaitedSignalFacts {
                signal_name: "approval".to_string(),
                since: None,
            }],
            replay_waits: vec![ReplayWaitFacts {
                wait_kind: "condition".to_string(),
                name: None,
                since: None,
            }],
            ..Default::default()
        };
        match classify_execution(&inputs, t(0)) {
            Some(BlockedOn::AwaitingSignal { .. }) => {}
            other => panic!("expected the dedicated signal verdict, got {other:?}"),
        }
    }

    #[test]
    fn every_blocked_on_kind_is_unique_and_snake_case() {
        // The three new variants must not collide with an existing wire label.
        let kinds = [
            BlockedOn::AwaitingReplayWait {
                wait_kind: "condition".to_string(),
                name: None,
                since: None,
            }
            .kind(),
            BlockedOn::WorkflowNoWorker {
                queue: "q".to_string(),
            }
            .kind(),
            BlockedOn::WorkflowQueuePaused {
                queue: "q".to_string(),
            }
            .kind(),
            BlockedOn::NoPendingWork.kind(),
            BlockedOn::ActivityNoWorker {
                queue: "q".to_string(),
                activity_name: None,
            }
            .kind(),
            BlockedOn::ActivityQueuePaused {
                queue: "q".to_string(),
                activity_name: None,
            }
            .kind(),
        ];
        let unique: std::collections::BTreeSet<_> = kinds.iter().copied().collect();
        assert_eq!(unique.len(), kinds.len(), "duplicate kind label: {kinds:?}");
        for kind in kinds {
            assert!(
                kind.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "kind {kind} is not snake_case",
            );
        }
    }

    #[test]
    fn new_verdicts_all_summarize_to_actionable_prose() {
        for verdict in [
            BlockedOn::WorkflowNoWorker {
                queue: "default".to_string(),
            },
            BlockedOn::WorkflowQueuePaused {
                queue: "default".to_string(),
            },
            BlockedOn::AwaitingReplayWait {
                wait_kind: "mutex".to_string(),
                name: Some("ledger:42".to_string()),
                since: None,
            },
            BlockedOn::AwaitingReplayWait {
                wait_kind: "condition".to_string(),
                name: None,
                since: None,
            },
        ] {
            let summary = summarize(&verdict);
            assert!(!summary.is_empty(), "{verdict:?} summarized to nothing");
            assert!(
                !summary.contains("  "),
                "{verdict:?} summary has a doubled space: {summary}",
            );
        }
    }
}
