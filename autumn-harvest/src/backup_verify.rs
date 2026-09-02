//! Post-restore resumability verification for backup/PITR drills (issue #943).
//!
//! "It's just Postgres" is harvest's durability pitch — your DBA's existing
//! backup and point-in-time-recovery tooling applies unchanged. That pitch is
//! only *true* if the restore story is written down and mechanically checkable,
//! because a database restored to time `T` comes back carrying in-flight
//! artifacts that look alarming to an operator seeing them for the first time
//! during an actual disaster:
//!
//! * `RUNNING` task rows referencing `worker_id`s that no longer exist,
//! * schedule fire claims (issue #350), worker-session leases (issue #606) and
//!   durable-mutex leases (issue #691) frozen mid-claim,
//! * `INFLIGHT` completion deliveries (issue #605) whose POST may already have
//!   been delivered after `T`,
//! * an external signal/cancel/await outbox request (issues #244/#492/#757)
//!   whose delivery was rolled back,
//! * and — in a multi-shard deployment restored to *slightly different* points
//!   in time — genuinely broken cross-shard invariants.
//!
//! Every item in the first list is **expected and self-healing**: the reclaim
//! machinery that already ships handles it. Only the last is a real problem.
//! This module encodes that distinction as a three-tier severity model
//! ([`FindingSeverity`]) so a restore drill produces an actionable verdict
//! rather than a wall of scary-looking rows, and so `harvest backup verify`
//! can exit non-zero on exactly the findings that mean "do not start workers".
//!
//! # Scope and safety
//!
//! **Read-only.** Nothing in this module writes. The db-gated probes issue
//! `SELECT`s only, and [`verify_restore`] additionally pins every connection it
//! opens into a read-only Postgres session (see [`READ_ONLY_SESSION_SQL`]) so a
//! future bug *cannot* mutate the database it is inspecting — the guarantee is
//! mechanical, not a code-review promise.
//!
//! **Zero engine impact.** No new `WorkflowEvent` variant, no migration, no
//! change to any runtime path. Every reclaim class below is reported by
//! *replicating or reusing* the corresponding scanner's own **selection**
//! predicate; the mutating half is never called.
//!
//! # The pure/db split
//!
//! Everything above the `#[cfg(feature = "db")]` line is pure vocabulary and
//! decision logic, unit-testable without Postgres. The db half is the probe
//! layer.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Maximum number of sample identifiers retained per [`Finding`].
///
/// A restore of a large fleet can produce thousands of rows in a single class;
/// the report is a triage artifact, not an export, so each finding keeps a
/// bounded, deterministic prefix and reports the true `count` alongside it.
pub const MAX_FINDING_SAMPLES: usize = 10;

/// Default number of non-terminal executions replayed per shard.
pub const DEFAULT_REPLAY_SAMPLE: usize = 50;

/// Upper bound on `--sample`, so one drill cannot ask for an unbounded replay.
pub const MAX_REPLAY_SAMPLE: usize = 2_000;

/// Default cap on rows scanned per coherence probe, per shard.
///
/// For the cross-shard reference scan this is a *page* size, not a ceiling:
/// that scan pages through complete owner groups until the shard is exhausted.
pub const DEFAULT_PROBE_LIMIT: i64 = 1_000;

/// Upper bound on pages the cross-shard reference scan will read per shard.
///
/// A runaway guard, not a tuning knob: at the default `probe_limit` it admits
/// a million reference events, and exceeding it raises a truncation note
/// (`Undetermined`) rather than reporting a clean prefix.
pub const MAX_REFERENCE_SCAN_PAGES: usize = 1_000;

/// Default restore-point skew (seconds) above which a multi-shard restore is
/// flagged as [`FindingClass::RestorePointSkew`].
pub const DEFAULT_MAX_SKEW_SECS: i64 = 60;

/// Default worker-heartbeat staleness threshold, in seconds — mirrors the
/// engine's own `2 x worker_heartbeat_interval` reclaim convention.
pub const DEFAULT_WORKER_STALE_SECS: i64 = 60;

/// Statement that pins a connection into a read-only Postgres session.
///
/// Issued once per connection by [`verify_restore`] immediately after
/// `establish`, *before* any probe runs. Any subsequent `INSERT`/`UPDATE`/
/// `DELETE` on that connection fails with SQLSTATE `25006`
/// (`read_only_sql_transaction`), so the read-only guarantee is enforced by
/// Postgres rather than by reviewer discipline.
///
/// This is a session *setting*, not a data mutation: it changes no row and is
/// discarded when the connection closes.
pub const READ_ONLY_SESSION_SQL: &str = "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY";

// ── Severity ────────────────────────────────────────────────────────────────

/// How much an operator should care about a [`Finding`].
///
/// The whole point of this tier split is that a *correct* restore still
/// produces a long list of [`Reclaimable`](FindingSeverity::Reclaimable)
/// findings. Treating those as failures would make the drill cry wolf and
/// train operators to ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    /// Expected after any restore, and healed automatically by a scanner that
    /// already ships. Reported for visibility; never fails the drill.
    Reclaimable,
    /// Informational: needs operator judgement, but is not by itself proof of
    /// a broken restore.
    Advisory,
    /// A broken invariant. Starting workers against this database risks
    /// wedged or silently-wrong executions. Fails the drill.
    Incoherent,
    /// The check itself could not run, so this condition is **unknown** — not
    /// absent. Distinct from `Incoherent`: we did not determine that the
    /// restore is broken, we determined that we cannot tell. Never reported as
    /// a pass, because "we could not look" must never read as "nothing found".
    Undetermined,
}

impl std::fmt::Display for FindingSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FindingSeverity {
    /// Stable lowercase name, matching the serde representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reclaimable => "reclaimable",
            Self::Advisory => "advisory",
            Self::Incoherent => "incoherent",
            Self::Undetermined => "undetermined",
        }
    }
}

// ── Finding classes ─────────────────────────────────────────────────────────

/// A bounded catalogue of post-restore conditions this module detects.
///
/// Each class maps to exactly one [`FindingSeverity`] via [`Self::severity`],
/// and names the shipped mechanism that heals it (for `Reclaimable`) or the
/// invariant it breaks (for `Incoherent`) in [`Self::explanation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FindingClass {
    // ── Reclaimable: expected after a restore, healed by a shipped scanner ──
    /// A `RUNNING` task row whose `worker_id` has no live heartbeat — the
    /// canonical post-restore artifact.
    DeadWorkerRunningTask,
    /// A task past a per-attempt or cross-retry timeout.
    TimedOutTask,
    /// A `RUNNING` execution past its `deadline_at`/`chain_deadline_at`.
    WorkflowDeadlineExpired,
    /// A schedule frozen mid-claim with an elapsed `fire_claimed_until`.
    ExpiredScheduleClaim,
    /// An `ACTIVE` worker session whose host is gone or whose lease elapsed.
    ExpiredSessionLease,
    /// A durable mutex lock whose lease elapsed.
    ExpiredMutexLease,
    /// A completion delivery frozen `INFLIGHT`.
    ///
    /// Any `INFLIGHT` row in a restored snapshot is frozen by definition — the
    /// worker that claimed it is gone with the process, so the lease is
    /// irrelevant to whether it is stuck.
    InflightCompletionDelivery,
    /// An external signal/cancel/await request with no recorded terminal and a
    /// target that is still resolvable — the outbox will retry it.
    PendingExternalRequest,

    // ── Incoherent: a broken invariant; do not start workers ────────────────
    /// A task-queue row whose owning execution row does not exist.
    DanglingTaskExecution,
    /// A `harvest_events` row whose owning execution row does not exist.
    DanglingEventExecution,
    /// An external request whose target execution is absent from the shard
    /// that owns it.
    ExternalTargetMissing,
    /// A parent recorded `ChildWorkflowStarted` with no terminal, but the
    /// child's execution row is absent from the child's shard.
    ChildExecutionMissing,
    /// A schedule with a claim token but a NULL `fire_claimed_until` — a torn
    /// claim pair. The scheduler's claim predicate is
    /// `fire_claim_token IS NULL OR fire_claimed_until < NOW()`, so such a row
    /// satisfies neither disjunct and the schedule never fires again.
    WedgedScheduleClaim,
    /// A parent recorded a child's terminal, but the child's own shard shows
    /// that child still non-terminal — the signature of a skewed multi-shard
    /// restore.
    ChildTerminalRolledBack,
    /// A caller recorded an external signal/cancel/await as *successfully*
    /// delivered, but the target's own shard shows no trace of the effect —
    /// the external-request analogue of `ChildTerminalRolledBack`, and the
    /// signature of a target shard restored to a point before the effect
    /// landed. Resuming would silently lose an effect the caller believes
    /// already happened.
    ExternalEffectRolledBack,
    /// A sampled history no longer replays cleanly against the deployed
    /// workflow code.
    ReplayDivergence,
    /// A sampled non-terminal history replayed to a workflow *failure* under
    /// the deployed code. The recorded history contains no terminal failure,
    /// so this is the newly deployed handler erroring where the live run had
    /// not — resuming it fails the run immediately.
    ReplayWorkflowFailed,

    // ── Advisory: operator judgement ────────────────────────────────────────
    /// A caller recorded an external *signal* as delivered and the target's
    /// shard does carry a signal of that name, but the delivery carried no
    /// `idempotency_key`, so the exact delivery cannot be proven to have
    /// survived. The engine persists no per-delivery identity for an unkeyed
    /// signal (`SignalReceived` carries only name + payload, and
    /// `harvest_signals` carries a key only when the caller supplied one), so
    /// this is a permanent precision limit of the data model, not a probe
    /// failure — hence Advisory rather than Undetermined. A signal whose name
    /// has *no* trace at all is still reported as `ExternalEffectRolledBack`.
    ExternalEffectUnverifiable,
    /// Shards were restored to materially different points in time.
    RestorePointSkew,
    /// A sampled history could not be replayed because its workflow type is
    /// not registered in the replayer running the check.
    ReplaySkippedNoHandler,
    /// A sampled history could not be read back at all.
    HistoryUnreadable,
    /// A cross-shard reference points at a shard whose DSN was not supplied to
    /// this run, so its coherence could not be checked either way.
    UninspectedShardReference,
    /// A business-key-addressed external request (#751) that carries no
    /// execution id, so this tool cannot adjudicate its target.
    WorkflowIdTargetUnchecked,

    // ── Undetermined: the check could not run ───────────────────────────────
    /// A probe could not execute at all — most commonly a missing table,
    /// i.e. the "restore" produced an unmigrated or empty database.
    ///
    /// This is deliberately NOT advisory: a probe that never ran found nothing
    /// *because it did not look*, and reporting that as a pass is the single
    /// most dangerous false-clean a restore drill can emit.
    ProbeFailed,
}

impl FindingClass {
    /// Every class, in a stable order. Used by tests and by the runbook table.
    pub const ALL: [Self; 24] = [
        Self::DeadWorkerRunningTask,
        Self::TimedOutTask,
        Self::WorkflowDeadlineExpired,
        Self::ExpiredScheduleClaim,
        Self::ExpiredSessionLease,
        Self::ExpiredMutexLease,
        Self::InflightCompletionDelivery,
        Self::PendingExternalRequest,
        Self::DanglingTaskExecution,
        Self::DanglingEventExecution,
        Self::ExternalTargetMissing,
        Self::ChildExecutionMissing,
        Self::WedgedScheduleClaim,
        Self::ChildTerminalRolledBack,
        Self::ExternalEffectRolledBack,
        Self::ReplayDivergence,
        Self::ReplayWorkflowFailed,
        Self::ExternalEffectUnverifiable,
        Self::RestorePointSkew,
        Self::ReplaySkippedNoHandler,
        Self::HistoryUnreadable,
        Self::UninspectedShardReference,
        Self::WorkflowIdTargetUnchecked,
        Self::ProbeFailed,
    ];

    /// The fixed severity of this class.
    ///
    /// Severity is a property of the *class*, never of the individual row, so
    /// a report can never disagree with itself about how bad a given condition
    /// is.
    #[must_use]
    pub const fn severity(self) -> FindingSeverity {
        match self {
            Self::DeadWorkerRunningTask
            | Self::TimedOutTask
            | Self::WorkflowDeadlineExpired
            | Self::ExpiredScheduleClaim
            | Self::ExpiredSessionLease
            | Self::ExpiredMutexLease
            | Self::InflightCompletionDelivery
            | Self::PendingExternalRequest => FindingSeverity::Reclaimable,

            Self::DanglingTaskExecution
            | Self::DanglingEventExecution
            | Self::ExternalTargetMissing
            | Self::ChildExecutionMissing
            | Self::WedgedScheduleClaim
            | Self::ChildTerminalRolledBack
            | Self::ExternalEffectRolledBack
            | Self::ReplayDivergence
            | Self::ReplayWorkflowFailed => FindingSeverity::Incoherent,

            Self::ExternalEffectUnverifiable
            | Self::RestorePointSkew
            | Self::ReplaySkippedNoHandler
            | Self::HistoryUnreadable
            | Self::UninspectedShardReference
            | Self::WorkflowIdTargetUnchecked => FindingSeverity::Advisory,

            Self::ProbeFailed => FindingSeverity::Undetermined,
        }
    }

    /// Stable `snake_case` name, matching the serde representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeadWorkerRunningTask => "dead_worker_running_task",
            Self::TimedOutTask => "timed_out_task",
            Self::WorkflowDeadlineExpired => "workflow_deadline_expired",
            Self::ExpiredScheduleClaim => "expired_schedule_claim",
            Self::WedgedScheduleClaim => "wedged_schedule_claim",
            Self::ExpiredSessionLease => "expired_session_lease",
            Self::ExpiredMutexLease => "expired_mutex_lease",
            Self::InflightCompletionDelivery => "inflight_completion_delivery",
            Self::PendingExternalRequest => "pending_external_request",
            Self::DanglingTaskExecution => "dangling_task_execution",
            Self::DanglingEventExecution => "dangling_event_execution",
            Self::ExternalTargetMissing => "external_target_missing",
            Self::ChildExecutionMissing => "child_execution_missing",
            Self::ChildTerminalRolledBack => "child_terminal_rolled_back",
            Self::ExternalEffectRolledBack => "external_effect_rolled_back",
            Self::ReplayDivergence => "replay_divergence",
            Self::ReplayWorkflowFailed => "replay_workflow_failed",
            Self::ExternalEffectUnverifiable => "external_effect_unverifiable",
            Self::RestorePointSkew => "restore_point_skew",
            Self::ReplaySkippedNoHandler => "replay_skipped_no_handler",
            Self::HistoryUnreadable => "history_unreadable",
            Self::UninspectedShardReference => "uninspected_shard_reference",
            Self::WorkflowIdTargetUnchecked => "workflow_id_target_unchecked",
            Self::ProbeFailed => "probe_failed",
        }
    }

    /// One-line operator explanation: what heals it, or what it breaks.
    #[must_use]
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::DeadWorkerRunningTask => {
                "reclaimed by the poison-pill orphan sweep once workers start (issue #367)"
            }
            Self::TimedOutTask => "reclaimed by the timeout scanner once workers start",
            Self::WorkflowDeadlineExpired => {
                "sealed TIMED_OUT by the execution-timeout scanner once workers start (issue #243)"
            }
            Self::ExpiredScheduleClaim => {
                "the claim expires and a healthy replica re-claims the slot (issue #350)"
            }
            Self::WedgedScheduleClaim => {
                "torn claim pair (token set, fire_claimed_until NULL): the scheduler's \
                 claim predicate can never match it again, so the schedule is permanently \
                 wedged and must be un-claimed by hand (issue #350)"
            }
            Self::ExpiredSessionLease => {
                "marked BROKEN by the session reclaim sweep; member activities fail \
                 non-retryably (issue #606)"
            }
            Self::ExpiredMutexLease => "reclaimed and granted to the next FIFO waiter (issue #691)",
            Self::InflightCompletionDelivery => {
                "claimed by a worker that no longer exists; re-attempted once the lease \
                 lapses. The receiver MUST dedupe on delivery_id (issue #605)"
            }
            Self::PendingExternalRequest => {
                "re-attempted by the external outbox scanner once workers start \
                 (issues #244/#492/#757)"
            }
            Self::DanglingTaskExecution => {
                "task row references an execution that does not exist — a torn restore"
            }
            Self::DanglingEventExecution => {
                "event row references an execution that does not exist — a torn restore"
            }
            Self::ExternalTargetMissing => {
                "the request's target execution is absent from the shard that owns it"
            }
            Self::ChildExecutionMissing => {
                "the parent awaits a child whose execution row is absent from its shard"
            }
            Self::ChildTerminalRolledBack => {
                "the parent recorded the child's terminal but the child's shard rolled \
                 it back — skewed multi-shard restore points"
            }
            Self::ExternalEffectRolledBack => {
                "the caller recorded this external request as delivered but the target's \
                 shard shows no trace of the effect — skewed multi-shard restore points"
            }
            Self::ReplayDivergence => {
                "this history no longer replays against the deployed workflow code"
            }
            Self::ReplayWorkflowFailed => {
                "this non-terminal history replays to a workflow error under the deployed \
                 code — resuming it would fail the run immediately"
            }
            Self::ExternalEffectUnverifiable => {
                "an unkeyed external signal of this name exists on the target, but the \
                 engine records no per-delivery identity for it, so this specific \
                 delivery cannot be proven to have survived"
            }
            Self::RestorePointSkew => "shards carry materially different newest-event timestamps",
            Self::ReplaySkippedNoHandler => {
                "no handler registered for this workflow type in the running replayer"
            }
            Self::HistoryUnreadable => "the recorded history could not be read back",
            Self::UninspectedShardReference => {
                "references a shard whose DSN was not supplied; supply every shard to \
                 check cross-shard coherence"
            }
            Self::WorkflowIdTargetUnchecked => {
                "addressed by business key (#751), so it has no execution id to \
                 look up; verify its target workflow by hand"
            }
            Self::ProbeFailed => {
                "the check could not run (a missing table usually means the restore \
                 produced an unmigrated or empty database)"
            }
        }
    }
}

impl std::fmt::Display for FindingClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Findings ────────────────────────────────────────────────────────────────

/// One detected condition, aggregated by class and shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// Which condition was detected.
    pub class: FindingClass,
    /// Derived from `class`; carried explicitly so a JSON consumer does not
    /// need the class→severity table.
    pub severity: FindingSeverity,
    /// The shard this finding was observed on. `None` for cross-shard findings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<i32>,
    /// How many rows matched. May exceed `samples.len()`.
    pub count: u64,
    /// A bounded, deterministic prefix of matching identifiers.
    pub samples: Vec<String>,
    /// Optional human-readable qualifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// What heals it, or what it breaks.
    pub explanation: &'static str,
    /// True when `samples` does not enumerate the whole population.
    ///
    /// `count` is **always exact** — it comes from `COUNT(*) OVER ()`, computed
    /// before `LIMIT`. Only the sample identifiers are bounded, so `truncated`
    /// means "there are more rows than the ones listed here", never "the count
    /// itself is a lower bound".
    pub truncated: bool,
}

impl Finding {
    /// Builds a finding, deriving `severity`/`explanation` from `class` and
    /// clipping `samples` to [`MAX_FINDING_SAMPLES`].
    #[must_use]
    pub fn new(
        class: FindingClass,
        shard_id: Option<i32>,
        count: u64,
        samples: Vec<String>,
    ) -> Self {
        let truncated = false;
        let mut samples = samples;
        samples.truncate(MAX_FINDING_SAMPLES);
        Self {
            class,
            severity: class.severity(),
            shard_id,
            count,
            samples,
            detail: None,
            explanation: class.explanation(),
            truncated,
        }
    }

    /// Attaches a human-readable qualifier.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Marks `samples` as a partial enumeration (`count` stays exact).
    #[must_use]
    pub const fn with_truncated(mut self, truncated: bool) -> Self {
        self.truncated = truncated;
        self
    }
}

// ── Replay summary ──────────────────────────────────────────────────────────

/// Outcome tally for the sampled-history replay check.
///
/// The `skipped_no_handler` field exists so the report can never claim to have
/// verified replay-safety when it in fact registered no workflow handlers —
/// the failure mode that would make a "clean" verdict a lie. See
/// [`ReplaySummary::verified`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ReplaySummary {
    /// Histories selected for replay.
    pub sampled: u64,
    /// Replayed with no divergence.
    pub clean: u64,
    /// Replayed and diverged.
    pub divergent: u64,
    /// Replayed and the workflow function returned an error.
    ///
    /// Distinct from `divergent`: the history is faithfully reproduced, but the
    /// deployed handler errors where the (non-terminal) recorded run had not.
    /// Resuming such a run fails it immediately, so this is a failed
    /// verification, not a clean one.
    pub failed: u64,
    /// Not replayed: the workflow type has no registered handler.
    pub skipped_no_handler: u64,
    /// Not replayed: the history could not be read back.
    pub unreadable: u64,
}

impl ReplaySummary {
    /// True when at least one history was actually replayed.
    ///
    /// A run whose samples were all skipped has verified *nothing* about
    /// replay-safety, and the report says so rather than implying a pass.
    #[must_use]
    pub const fn verified(&self) -> bool {
        self.clean > 0 || self.divergent > 0 || self.failed > 0
    }

    /// Folds another summary into this one.
    pub const fn merge(&mut self, other: Self) {
        self.sampled += other.sampled;
        self.clean += other.clean;
        self.divergent += other.divergent;
        self.failed += other.failed;
        self.skipped_no_handler += other.skipped_no_handler;
        self.unreadable += other.unreadable;
    }
}

// ── Status ──────────────────────────────────────────────────────────────────

/// The overall verdict of a restore drill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyStatus {
    /// Nothing at all was detected.
    Clean,
    /// Only reclaimable/advisory findings: the restore is resumable and the
    /// shipped scanners will heal what remains. This is the *normal* outcome
    /// of a correct restore.
    ResumableWithReclaim,
    /// At least one broken invariant. Do not start workers.
    Incoherent,
    /// At least one shard could not be inspected, so the verdict could not be
    /// determined. Never mistaken for a pass.
    Unavailable,
}

impl VerifyStatus {
    /// Stable `snake_case` name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::ResumableWithReclaim => "resumable_with_reclaim",
            Self::Incoherent => "incoherent",
            Self::Unavailable => "unavailable",
        }
    }
}

impl std::fmt::Display for VerifyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolves the overall status from the observed findings.
///
/// `Unavailable` wins over everything: a partially-inspected fleet cannot be
/// declared resumable, and reporting an unreachable shard as `Clean` would be
/// exactly the false assurance a restore drill exists to prevent.
#[must_use]
pub fn classify_status<'a>(
    findings: impl IntoIterator<Item = &'a Finding>,
    any_shard_unreachable: bool,
) -> VerifyStatus {
    // "Could not determine" always wins, from either source: an unreachable
    // shard, or a probe that failed to run. Both mean the drill did not
    // actually look, and a pass verdict would be a lie.
    let mut incoherent = false;
    let mut any = false;
    for finding in findings {
        match finding.severity {
            FindingSeverity::Undetermined => return VerifyStatus::Unavailable,
            FindingSeverity::Incoherent => incoherent = true,
            FindingSeverity::Reclaimable | FindingSeverity::Advisory => {}
        }
        any = true;
    }
    if any_shard_unreachable {
        return VerifyStatus::Unavailable;
    }
    if incoherent {
        return VerifyStatus::Incoherent;
    }
    if any {
        VerifyStatus::ResumableWithReclaim
    } else {
        VerifyStatus::Clean
    }
}

// ── Reports ─────────────────────────────────────────────────────────────────

/// Per-shard half of a [`RestoreVerifyReport`].
#[derive(Debug, Clone, Serialize)]
pub struct ShardVerifyReport {
    /// The shard id this DSN was inspected as.
    pub shard_id: i32,
    /// The DSN with any password redacted.
    pub dsn: String,
    /// False when the shard could not be inspected at all.
    pub reachable: bool,
    /// Why the shard could not be inspected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unreachable_reason: Option<String>,
    /// Newest `harvest_events.timestamp` observed — the restore-point proxy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_at: Option<DateTime<Utc>>,
    /// Count of non-terminal (`RUNNING`/`PAUSED`) executions.
    ///
    /// `None` when the count probe itself failed — never `0`, which would read
    /// as a drained fleet. A failure also raises `probe_failed`, so the shard
    /// is `Undetermined` rather than falsely clean.
    pub non_terminal_executions: Option<u64>,
    /// Replay tally for this shard's sampled histories.
    pub replay: ReplaySummary,
    /// Findings observed on this shard.
    pub findings: Vec<Finding>,
}

impl ShardVerifyReport {
    /// Builds an `unreachable` shard report.
    #[must_use]
    pub fn unreachable(shard_id: i32, dsn: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            shard_id,
            dsn: dsn.into(),
            reachable: false,
            unreachable_reason: Some(reason.into()),
            latest_event_at: None,
            non_terminal_executions: None,
            replay: ReplaySummary::default(),
            findings: Vec::new(),
        }
    }
}

/// The machine-readable resumability report.
#[derive(Debug, Clone, Serialize)]
pub struct RestoreVerifyReport {
    /// When the drill ran.
    pub generated_at: DateTime<Utc>,
    /// The overall verdict.
    pub status: VerifyStatus,
    /// Per-shard detail.
    pub shards: Vec<ShardVerifyReport>,
    /// Findings that span shards (child/target coherence, restore-point skew).
    pub cross_shard: Vec<Finding>,
    /// Largest pairwise difference between shards' newest-event timestamps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_point_skew_secs: Option<i64>,
    /// Fleet-wide replay tally.
    pub replay: ReplaySummary,
    /// Whether check (a) — replay — actually ran on anything.
    ///
    /// Surfaced at the top level so a JSON consumer can gate on it without
    /// re-deriving [`ReplaySummary::verified`]. `false` means the `status`
    /// verdict says nothing about whether the deployed workflow code still
    /// replays these histories: the shipped `harvest` CLI links no application
    /// handlers, so it always reports `false` (see the runbook's "Replay
    /// honesty" section).
    pub replay_verified: bool,
    /// Count of findings per severity, for a one-line summary.
    pub totals_by_severity: BTreeMap<String, u64>,
}

impl RestoreVerifyReport {
    /// Assembles a report from per-shard results, deriving status and totals.
    #[must_use]
    pub fn assemble(
        generated_at: DateTime<Utc>,
        shards: Vec<ShardVerifyReport>,
        cross_shard: Vec<Finding>,
    ) -> Self {
        let any_unreachable = shards.iter().any(|s| !s.reachable);
        let mut replay = ReplaySummary::default();
        let mut totals_by_severity: BTreeMap<String, u64> = BTreeMap::new();
        for shard in &shards {
            replay.merge(shard.replay);
            for finding in &shard.findings {
                *totals_by_severity
                    .entry(finding.severity.as_str().to_string())
                    .or_default() += 1;
            }
        }
        for finding in &cross_shard {
            *totals_by_severity
                .entry(finding.severity.as_str().to_string())
                .or_default() += 1;
        }
        let all = shards
            .iter()
            .flat_map(|s| s.findings.iter())
            .chain(cross_shard.iter());
        let status = classify_status(all, any_unreachable);
        let restore_point_skew_secs = compute_skew(shards.iter().map(|s| s.latest_event_at));
        Self {
            generated_at,
            status,
            shards,
            cross_shard,
            restore_point_skew_secs,
            replay_verified: replay.verified(),
            replay,
            totals_by_severity,
        }
    }

    /// Every finding, shard-local first then cross-shard.
    pub fn all_findings(&self) -> impl Iterator<Item = &Finding> {
        self.shards
            .iter()
            .flat_map(|s| s.findings.iter())
            .chain(self.cross_shard.iter())
    }

    /// True when the report contains a finding of the given class.
    #[must_use]
    pub fn detected(&self, class: FindingClass) -> bool {
        self.all_findings().any(|f| f.class == class)
    }

    /// Process exit code.
    ///
    /// `0` = resumable (clean, or only reclaimable/advisory findings);
    /// `1` = a broken invariant — do not start workers;
    /// `2` = the drill could not determine an answer (a shard was unreachable).
    ///
    /// `2` is deliberately distinct from `1`: "we found a problem" and "we
    /// could not tell" demand different operator responses, and collapsing
    /// them would let a connection failure masquerade as a clean bill of
    /// health on retry.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        match self.status {
            VerifyStatus::Clean | VerifyStatus::ResumableWithReclaim => 0,
            VerifyStatus::Incoherent => 1,
            VerifyStatus::Unavailable => 2,
        }
    }
}

/// Largest pairwise gap, in seconds, between the supplied restore-point
/// proxies. `None` when fewer than two shards reported a timestamp.
#[must_use]
pub fn compute_skew(latest: impl IntoIterator<Item = Option<DateTime<Utc>>>) -> Option<i64> {
    let mut seen: Vec<DateTime<Utc>> = latest.into_iter().flatten().collect();
    if seen.len() < 2 {
        return None;
    }
    seen.sort_unstable();
    let first = *seen.first()?;
    let last = *seen.last()?;
    Some((last - first).num_seconds().abs())
}

// ── Production-DSN guard ────────────────────────────────────────────────────

/// True when `candidate` and `live` address the same Postgres database.
///
/// Compares host/`hostaddr`, port and database name only — user, password and
/// query parameters are deliberately ignored, because pointing a "read-only
/// replica user" at the production database is exactly the mistake this guard
/// exists to catch. Ports are normalised so `:5432` and an omitted port
/// compare equal.
///
/// Host and port matching use **overlap**, not set equality, so a multi-host
/// failover DSN that merely lists production among its candidates still trips
/// the guard.
///
/// An unparseable DSN on either side compares **equal**: a guard that cannot
/// read the DSN must fail closed, never wave the drill through.
#[must_use]
#[cfg(feature = "db")]
pub fn dsn_targets_same_database(candidate: &str, live: &str) -> bool {
    let (Some(a), Some(b)) = (parse_dsn_identity(candidate), parse_dsn_identity(live)) else {
        return true;
    };
    if a.database != b.database || !overlaps(&a.ports, &b.ports) {
        return false;
    }
    // `hostaddr` is the TCP destination when present, so a shared address is a
    // match no matter what hostname alias each DSN spells. Only when neither
    // side pins an address do we fall back to comparing hostnames.
    overlaps(&a.hostaddrs, &b.hostaddrs) || overlaps(&a.hosts, &b.hosts)
}

/// True when two sorted, deduped sets share at least one element.
///
/// Overlap rather than equality: a multi-host failover DSN that lists
/// production among its candidates reaches production, so one shared endpoint
/// is enough to trip the guard. Erring toward *more* matches is the safe
/// direction -- a false match costs the operator an explicit acknowledgement,
/// a false miss costs them their production database.
#[cfg(feature = "db")]
fn overlaps<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    a.iter().any(|x| b.contains(x))
}

/// Host, address, port and database identity of a Postgres DSN.
#[cfg(feature = "db")]
#[derive(Debug, PartialEq, Eq)]
struct DsnIdentity {
    /// Lowercased hostnames (and Unix socket paths).
    hosts: Vec<String>,
    /// Literal `hostaddr` values, which override `hosts` at connect time.
    hostaddrs: Vec<String>,
    /// Ports, normalised so an omitted port compares as 5432.
    ports: Vec<u16>,
    /// Database name.
    database: String,
}

/// `(hosts, ports, database)` identity of a Postgres DSN, or `None` when it
/// cannot be parsed.
///
/// Parsed with **`tokio_postgres::Config`** — the exact parser
/// `diesel_async` hands the DSN to when it opens the connection. Using any
/// other parser (`url::Url`, say) is a guard bypass waiting to happen: the two
/// disagree on percent-decoding (`/%68arvest` is database `harvest`), on
/// libpq query parameters (`?dbname=`, `?host=`, `?port=`, `?hostaddr=` all
/// override the URL form), and on comma-separated multi-host failover. Every
/// one of those parses cleanly under `url` while resolving somewhere else
/// entirely at connect time — so a guard built on `url` waves through a DSN
/// that lands on production. Sharing the connector's parser makes that class
/// of disagreement impossible by construction.
///
/// `hostaddr` is kept **separate** from `hosts` rather than folded into one
/// set: tokio-postgres prefers it for the TCP connection, so two DSNs sharing
/// an address are the same database however differently they spell the
/// hostname. Folding them together would make identity require matching
/// hostname *text* as well, and `host=scratch-alias&hostaddr=<prod-ip>` would
/// slip past a live `host=prod-alias&hostaddr=<prod-ip>`.
///
/// A NUMERIC `host` is folded into the address set: it IS an address, so it
/// compares against the other side's `hostaddr` with no DNS involved.
///
/// Known limit: when one side pins only `hostaddr` and the other only a
/// `host` NAME, no comparison is possible without resolving DNS -- which this
/// guard deliberately does not do (a name can resolve differently between the
/// check and the connect). Spell both DSNs the same way, or pass the
/// acknowledgement.
#[cfg(feature = "db")]
fn parse_dsn_identity(dsn: &str) -> Option<DsnIdentity> {
    use std::str::FromStr as _;

    let config = tokio_postgres::Config::from_str(dsn.trim()).ok()?;

    let mut hostaddrs: Vec<String> = config
        .get_hostaddrs()
        .iter()
        .map(ToString::to_string)
        .collect();
    // A NUMERIC `host` is already an address -- comparing it needs no DNS, so
    // it belongs in the address set, not the hostname set. Leaving it in
    // `hosts` meant `host=10.0.0.5` and `host=prod-alias&hostaddr=10.0.0.5`
    // -- the same TCP destination -- overlapped in neither set and the guard
    // waved a production target through. Parsing also normalises IPv6
    // spellings (`[0:0:...:1]` and `::1`) on both sides, since `get_hostaddrs`
    // yields `IpAddr` and we render it the same way.
    let mut hosts: Vec<String> = Vec::new();
    for h in config.get_hosts() {
        match h {
            tokio_postgres::config::Host::Tcp(name) => {
                if let Ok(addr) = std::net::IpAddr::from_str(name) {
                    hostaddrs.push(addr.to_string());
                } else {
                    hosts.push(name.to_ascii_lowercase());
                }
            }
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(path) => {
                hosts.push(path.to_string_lossy().to_lowercase());
            }
        }
    }
    if hosts.is_empty() && hostaddrs.is_empty() {
        return None;
    }
    hosts.sort_unstable();
    hosts.dedup();
    hostaddrs.sort_unstable();
    hostaddrs.dedup();

    // tokio-postgres emits either one port for all hosts or one per host.
    let mut ports: Vec<u16> = config.get_ports().to_vec();
    if ports.is_empty() {
        ports.push(5432);
    }
    ports.sort_unstable();
    ports.dedup();

    // An omitted `dbname` is NOT an empty database name. tokio-postgres simply
    // leaves `database` out of the startup packet, and the server then applies
    // the libpq default: the CONNECTION USER. Comparing `""` against the other
    // side's real name made two DSNs that reach the same database look
    // different, so the guard waved a production target through.
    //
    // With neither `dbname` nor `user` the default is the OS username of
    // whoever connects -- not knowable here, and different on the operator's
    // machine than in the deployed config -- so we fail closed by refusing to
    // produce an identity at all (`None` makes the guard return `true`).
    let database = config
        .get_dbname()
        .or_else(|| config.get_user())?
        .to_string();
    Some(DsnIdentity {
        hosts,
        hostaddrs,
        ports,
        database,
    })
}

/// Returns the DSN with every password-bearing element replaced by `***`, for
/// safe logging.
///
/// A DSN that cannot be parsed is redacted **wholesale** rather than echoed, so
/// a malformed-but-secret-bearing string can never reach a report or a log
/// line. The same applies to any DSN carrying a `password` connection
/// parameter: `tokio_postgres::Config` accepts `?password=` in the query string
/// and in the libpq `key=value` form, and neither survives a naive URL
/// userinfo rewrite — so rather than emit a string we cannot prove is clean,
/// we withhold it. The report needs the DSN only to name which shard is being
/// discussed, and the shard id already does that.
#[must_use]
pub fn redact_dsn(dsn: &str) -> String {
    let trimmed = dsn.trim();
    let Ok(mut url) = url::Url::parse(trimmed) else {
        return "<unparseable dsn>".to_string();
    };
    // A password can hide in the query string as well as in the userinfo. We
    // cannot rewrite what we cannot enumerate, so withhold the whole thing.
    //
    // Any key whose name *carries* `password`, not just the exact word:
    // libpq's `sslpassword` is one, and matching exactly let
    // `?sslpassword=hunter2` through whole. This is the same rule the keyword
    // form uses (`redact_keyword_dsn`), so the two DSN spellings cannot
    // disagree about what counts as a secret.
    if url
        .query_pairs()
        .any(|(k, _)| k.to_ascii_lowercase().contains("password"))
    {
        return "<redacted dsn>".to_string();
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("***"));
    }
    url.to_string()
}

// ── Verification driver (db-gated) ───────────────────────────────────────────

/// One shard to inspect: its logical id and the DSN of the **restored scratch**
/// database that holds it.
#[cfg(all(feature = "db", feature = "testing"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardTarget {
    /// The logical shard id, matching what `ExecutionId::shard()` decodes.
    pub shard_id: i32,
    /// Connection string for the restored scratch database.
    pub dsn: String,
}

#[cfg(all(feature = "db", feature = "testing"))]
impl ShardTarget {
    /// Build a target for `shard_id` at `dsn`.
    #[must_use]
    pub fn new(shard_id: i32, dsn: impl Into<String>) -> Self {
        Self {
            shard_id,
            dsn: dsn.into(),
        }
    }
}

/// Knobs for one verification run. Every field has a conservative default.
#[cfg(all(feature = "db", feature = "testing"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOptions {
    /// How many non-terminal histories to sample and replay per shard.
    pub replay_sample: usize,
    /// Cap on rows returned per probe. Exact counts are always reported; only
    /// the sample identifiers are bounded.
    ///
    /// Floored at `1` at the probe site: a non-positive value would emit
    /// `LIMIT 0` and make every probe read as clean.
    pub probe_limit: i64,
    /// Worker-heartbeat staleness threshold, in seconds, used by the
    /// dead-worker and broken-session probes.
    pub worker_stale_secs: i64,
    /// Cross-shard restore-point skew above which a `RestorePointSkew`
    /// advisory is raised.
    pub max_skew_secs: i64,
    /// The operator has confirmed every DSN points at a scratch database.
    ///
    /// Required by the CLI's live-DSN guard (AC4); the library itself does not
    /// enforce it, since the library caller supplies the DSNs directly.
    pub scratch_ack: bool,
}

#[cfg(all(feature = "db", feature = "testing"))]
impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            replay_sample: DEFAULT_REPLAY_SAMPLE,
            probe_limit: DEFAULT_PROBE_LIMIT,
            worker_stale_secs: DEFAULT_WORKER_STALE_SECS,
            max_skew_secs: DEFAULT_MAX_SKEW_SECS,
            scratch_ack: false,
        }
    }
}

#[cfg(all(feature = "db", feature = "testing"))]
impl VerifyOptions {
    /// Set the per-shard replay sample size (clamped to [`MAX_REPLAY_SAMPLE`]).
    #[must_use]
    pub const fn with_replay_sample(mut self, n: usize) -> Self {
        self.replay_sample = if n > MAX_REPLAY_SAMPLE {
            MAX_REPLAY_SAMPLE
        } else {
            n
        };
        self
    }

    /// Record the operator's scratch-database acknowledgement.
    #[must_use]
    pub const fn with_scratch_ack(mut self, ack: bool) -> Self {
        self.scratch_ack = ack;
        self
    }

    /// Set the worker-heartbeat staleness threshold, in seconds.
    #[must_use]
    pub const fn with_worker_stale_secs(mut self, secs: i64) -> Self {
        self.worker_stale_secs = secs;
        self
    }

    /// Set the per-probe row cap.
    ///
    /// For the cross-shard reference scan this is a *page* size, not a hard
    /// ceiling -- the scan pages through complete owner groups until the shard
    /// is exhausted (see `scan_reference_events`). Raising it trades memory for
    /// fewer round trips.
    #[must_use]
    pub const fn with_probe_limit(mut self, limit: i64) -> Self {
        self.probe_limit = limit;
        self
    }
}

#[cfg(all(feature = "db", feature = "testing"))]
mod probes {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::{DateTime, Utc};
    use diesel::OptionalExtension as _;
    use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
    use uuid::Uuid;

    use super::{
        Finding, FindingClass, MAX_FINDING_SAMPLES, MAX_REFERENCE_SCAN_PAGES,
        READ_ONLY_SESSION_SQL, ReplaySummary, RestoreVerifyReport, ShardTarget, ShardVerifyReport,
        VerifyOptions, compute_skew, redact_dsn,
    };
    use crate::event::WorkflowEvent;
    use crate::testing::WorkflowReplayer;
    use crate::types::{ExecutionId, ExternalTarget};

    /// One row of a bounded probe: a sample identifier plus the exact total
    /// (computed by a window function *before* `LIMIT`, so the count is never
    /// truncated even when only `probe_limit` identifiers are returned).
    #[derive(diesel::QueryableByName)]
    struct ProbeRow {
        #[diesel(sql_type = Text)]
        ident: String,
        #[diesel(sql_type = BigInt)]
        total: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct LatestEventRow {
        #[diesel(sql_type = Nullable<Timestamptz>)]
        latest: Option<DateTime<Utc>>,
    }

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct ExecIdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    #[derive(diesel::QueryableByName)]
    struct EventRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        workflow_exec_id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        event_data: serde_json::Value,
        /// State of the OWNING execution, so the fold can tell a live
        /// dependency from a retained terminal caller's assertion.
        #[diesel(sql_type = diesel::sql_types::Text)]
        owner_state: String,
    }

    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }

    #[derive(diesel::QueryableByName)]
    struct ExistsRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }

    /// The `new_exec_id` a `WorkflowContinuedAsNew` event names, read as text so
    /// a malformed value degrades to `None` rather than failing the query.
    #[derive(diesel::QueryableByName)]
    struct SuccessorRow {
        #[diesel(sql_type = Nullable<Text>)]
        new_exec_id: Option<String>,
    }

    /// Wrap a caller-supplied selection predicate in a bounded, counted probe.
    ///
    /// The inner query is *only ever* one of the engine's own read-only
    /// selection predicates (or a hand-written `SELECT` in this module); it is
    /// never caller input, so the string interpolation carries no injection
    /// surface.
    /// The session-reclaim candidate set the scanner will *act* on.
    ///
    /// `sessions::broken_session_candidates_query()` is only the broad SQL
    /// scan; `sessions::resolve_broken_reason` then applies one Rust-side
    /// suppression the SQL cannot express — a session whose ONLY qualifying
    /// reason is an elapsed lease, but which still has a `RUNNING` member
    /// task, is deliberately NOT broken (the lease is refreshed on member
    /// completion, so a long-running member is independent proof of progress).
    ///
    /// Reusing the scan alone would over-report. Mirror the suppression here
    /// so the report matches what the fleet actually reclaims; the host and
    /// owning-workflow reasons take priority in the scanner and so are
    /// deliberately NOT suppressed.
    fn session_candidates_matching_the_scanner() -> String {
        format!(
            "SELECT c.* FROM ({}) c WHERE NOT ( \
               EXISTS (SELECT 1 FROM harvest_task_queue q \
                       WHERE q.session_id = c.id AND q.state = 'RUNNING') \
               AND NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e2 \
                       WHERE e2.id = c.workflow_exec_id \
                         AND e2.state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', \
                                          'TERMINATED', 'CONTINUED_AS_NEW')) \
               AND EXISTS (SELECT 1 FROM harvest_workers w2 \
                       WHERE w2.worker_id = c.host_worker_id \
                         AND w2.last_heartbeat_at > NOW() - ($1::bigint * INTERVAL '1 second') \
                         AND w2.status NOT IN ('Draining', 'Stopped')) \
             )",
            crate::sessions::broken_session_candidates_query()
        )
    }

    fn bounded(inner: &str, ident_expr: &str, limit: i64) -> String {
        format!(
            "SELECT {ident_expr} AS ident, COUNT(*) OVER () AS total \
             FROM ({inner}) sub ORDER BY 1 LIMIT {limit}"
        )
    }

    /// Establish a connection and pin the whole session **read only**.
    ///
    /// Postgres then rejects any write with SQLSTATE 25006, so AC4's
    /// "never mutates" is enforced mechanically by the server rather than by
    /// code review of every probe.
    async fn connect_read_only(dsn: &str) -> Result<AsyncPgConnection, String> {
        let mut conn = AsyncPgConnection::establish(dsn)
            .await
            .map_err(|e| format!("connect failed: {e}"))?;
        conn.batch_execute(READ_ONLY_SESSION_SQL)
            .await
            .map_err(|e| format!("could not pin session read-only: {e}"))?;
        Ok(conn)
    }

    /// Run one probe and, if it matched anything, produce a [`Finding`].
    async fn probe(
        conn: &mut AsyncPgConnection,
        class: FindingClass,
        shard_id: i32,
        sql: String,
        stale_bind: Option<i64>,
    ) -> Result<Option<Finding>, String> {
        let q = diesel::sql_query(sql);
        let rows: Vec<ProbeRow> = match stale_bind {
            Some(secs) => q.bind::<BigInt, _>(secs).load(conn).await,
            None => q.load(conn).await,
        }
        .map_err(|e| format!("{class} probe failed: {e}"))?;

        // NOTE: `rows.first()` (and `.iter().next()`) resolve to Diesel's
        // `QueryDsl::first` via auto-ref on `Vec`, so go through a plain slice.
        let Some(total) = rows.as_slice().first().map(|r| r.total) else {
            return Ok(None);
        };
        let total = u64::try_from(total).unwrap_or(0);
        if total == 0 {
            return Ok(None);
        }
        let samples: Vec<String> = rows.into_iter().map(|r| r.ident).collect();
        let truncated = samples.len() > MAX_FINDING_SAMPLES || total > samples.len() as u64;
        Ok(Some(
            Finding::new(class, Some(shard_id), total, samples).with_truncated(truncated),
        ))
    }

    /// A cross-shard reference discovered while scanning one shard's history.
    #[derive(Debug, Clone)]
    pub(super) struct PendingRef {
        pub kind: RefKind,
        /// The shard that owns `target`, decoded from the execution id.
        pub owner_shard: i32,
        pub target: Uuid,
        pub source_exec: Uuid,
        pub source_shard: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) enum RefKind {
        /// Parent recorded `ChildWorkflowStarted` with no terminal yet.
        AwaitedChild,
        /// Parent recorded the child's terminal.
        ChildTerminalRecorded,
        /// An external signal/cancel/await request with no terminal.
        ExternalTarget,
        /// The caller recorded a *successful* external terminal, so the target
        /// shard must show the corresponding durable effect. A target restored
        /// to a point before the effect landed is a silent lost-effect break,
        /// which is exactly what the caller's own history says cannot happen.
        ExternalEffectDelivered(ExternalEffect),
    }

    /// Which durable effect a delivered external request asserts on the target.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) enum ExternalEffect {
        /// The named signal must be queued or already recorded on the target.
        ///
        /// `idempotency_key` is the ONLY per-delivery identity the engine
        /// persists on the target side (issue #521): `SignalReceived` carries
        /// just name + payload, and `harvest_signals.idempotency_key` is set
        /// only when the caller supplied one. With a key the check is exact;
        /// without one it can only ask whether *any* signal of that name
        /// survives, which is why an unkeyed hit is reported as
        /// `ExternalEffectUnverifiable` rather than clean.
        Signal {
            name: String,
            idempotency_key: Option<String>,
        },
        /// The target must be terminal.
        Cancel,
        /// The target must be terminal.
        Await,
    }

    /// Whether the target shard still shows a recorded-as-delivered effect.
    ///
    /// Tri-state rather than a bool: for an UNKEYED signal the engine persists
    /// no per-delivery identity, so "a signal of this name is present" is not
    /// proof that *this* delivery survived. Collapsing that to `true` reopens
    /// the false-clean the whole class exists to prevent; collapsing it to
    /// `false` would report a false rollback on every repeated channel name.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum EffectVerdict {
        /// The effect is durably present on the target's shard.
        Survived,
        /// No trace of the effect at all -- definitive rollback.
        Lost,
        /// Same-named evidence exists but this delivery cannot be identified.
        Unverifiable,
    }

    impl ExternalEffect {
        /// Short label for report samples.
        const fn label(&self) -> &'static str {
            match self {
                Self::Signal { .. } => "signal",
                Self::Cancel => "cancel",
                Self::Await => "await",
            }
        }
    }

    /// Probe every shard-local condition, sample + replay histories, and collect
    /// the cross-shard references this shard's history asserts.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn verify_shard(
        target: &ShardTarget,
        options: &VerifyOptions,
        replayer: &WorkflowReplayer,
    ) -> (ShardVerifyReport, Vec<PendingRef>) {
        let shard_id = target.shard_id;
        let dsn = redact_dsn(&target.dsn);

        let mut conn = match connect_read_only(&target.dsn).await {
            Ok(c) => c,
            Err(e) => return (ShardVerifyReport::unreachable(shard_id, dsn, e), Vec::new()),
        };

        let mut findings = Vec::new();
        let mut soft_errors: Vec<String> = Vec::new();
        // A non-positive limit would emit `LIMIT 0`: every probe returns no
        // sample rows, every condition reads as absent, and the run exits 0
        // having looked at nothing. Floor it at 1 so a mis-set knob degrades
        // to "one sample per finding", never to a fabricated all-clear.
        // (Exact counts come from `COUNT(*) OVER ()`, so they are unaffected.)
        let limit = options.probe_limit.max(1);
        // Clamp to the same bound the poison-pill reclaimer applies at its own
        // entry point before binding. Without this an out-of-range
        // `--worker-stale-secs` would have the report evaluate a threshold that
        // scanner would never actually use, so the report would disagree with
        // what the running fleet reclaims. (`sessions::enforce_broken_sessions`
        // binds unclamped; we clamp both uniformly, which only diverges for
        // absurd inputs -- above a year, or negative -- and diverges toward a
        // value Postgres interval arithmetic can actually represent.)
        let stale = options
            .worker_stale_secs
            .clamp(0, crate::poison_pill::MAX_WORKER_STALE_SECS);

        // ── Reclaimable: what the existing scanners will heal ───────────────
        //
        // Each of these reuses the engine's own selection predicate verbatim,
        // so a change to the scanner's definition of "timed out" can never
        // drift from what this drill reports.
        let reclaimable: Vec<(FindingClass, String, Option<i64>)> = vec![
            (
                FindingClass::DeadWorkerRunningTask,
                bounded(
                    crate::poison_pill::orphaned_running_tasks_query(),
                    "sub.id::text",
                    limit,
                ),
                Some(stale),
            ),
            (
                FindingClass::TimedOutTask,
                bounded(
                    &format!(
                        "{} UNION {} UNION {} UNION {}",
                        crate::timeout::heartbeat_timeout_query(),
                        crate::timeout::start_to_close_timeout_query(),
                        crate::timeout::schedule_to_start_timeout_query(),
                        crate::timeout::schedule_to_close_timeout_query(),
                    ),
                    "sub.id::text",
                    limit,
                ),
                None,
            ),
            (
                FindingClass::WorkflowDeadlineExpired,
                // The one predicate reused here that the engine does NOT
                // execute: `workflow_execution_timeout_query` renders `NOW()`
                // illustratively and the real scanner
                // (`enforce_workflow_execution_timeouts`) uses the equivalent
                // Diesel DSL with a Rust-captured `now`. The two are
                // semantically identical and `timeout.rs` pins the const's
                // shape, but a DSL-only change would not break this reuse --
                // so unlike every sibling probe, this one is faithful by
                // review rather than by construction.
                bounded(
                    crate::timeout::workflow_execution_timeout_query(),
                    "sub.id::text",
                    limit,
                ),
                None,
            ),
            (
                FindingClass::ExpiredScheduleClaim,
                bounded(
                    "SELECT s.id FROM harvest_schedules s \
                     WHERE s.fire_claim_token IS NOT NULL AND s.fire_claimed_until < NOW()",
                    "sub.id::text",
                    limit,
                ),
                None,
            ),
            (
                FindingClass::WedgedScheduleClaim,
                bounded(
                    "SELECT s.id FROM harvest_schedules s \
                     WHERE s.fire_claim_token IS NOT NULL AND s.fire_claimed_until IS NULL",
                    "sub.id::text",
                    limit,
                ),
                None,
            ),
            (
                FindingClass::ExpiredSessionLease,
                bounded(
                    &session_candidates_matching_the_scanner(),
                    "sub.id::text",
                    limit,
                ),
                Some(stale),
            ),
            (
                FindingClass::ExpiredMutexLease,
                bounded(crate::mutex::expired_leases_stmt(), "sub.lock_key", limit),
                None,
            ),
            (
                FindingClass::InflightCompletionDelivery,
                bounded(
                    "SELECT d.id FROM harvest_completion_deliveries d \
                     WHERE d.state = 'INFLIGHT'",
                    "sub.id::text",
                    limit,
                ),
                None,
            ),
        ];

        for (class, sql, bind) in reclaimable {
            match probe(&mut conn, class, shard_id, sql, bind).await {
                Ok(Some(f)) => findings.push(f),
                Ok(None) => {}
                Err(e) => soft_errors.push(e),
            }
        }

        // ── Incoherent: shard-local referential breaks ──────────────────────
        let mut dangling: Vec<(FindingClass, String)> = vec![(
            FindingClass::DanglingTaskExecution,
            bounded(
                "SELECT t.id FROM harvest_task_queue t \
                     WHERE t.workflow_exec_id IS NOT NULL \
                       AND NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e \
                                       WHERE e.id = t.workflow_exec_id)",
                "sub.id::text",
                limit,
            ),
        )];
        // Issue #958: on the opt-in PARTITIONED layout an orphan event row is
        // the designed steady state, not a torn restore. That layout drops the
        // `harvest_events` foreign key — its `ON DELETE CASCADE` is the delete
        // storm the partitioning exists to remove — so collecting an execution
        // deliberately leaves its events behind for the partition sweeper to
        // reclaim at whole-partition granularity.
        //
        // Running the probe there would report EVERY healthy partitioned shard
        // as `Incoherent` ("a broken invariant; do not start workers", exit 1)
        // — permanently, and including the cross-region failover runbook's
        // pre-flight. The invariant is real and worth checking; it just is not
        // an invariant of that layout.
        // A FAILED probe is not "unpartitioned". `detect_layout` errors when the
        // catalog cannot be read or when `harvest_event_cohort` is missing or
        // has an unexpected body — and a partitioned shard whose cohort
        // function is damaged is precisely the kind of thing a restore drill
        // should surface. Defaulting to `false` there would run the
        // dangling-event invariant against a partitioned shard, report its
        // designed orphans as `Incoherent` ("do not start workers", exit 1),
        // block the restore, and discard the real catalog error that explains
        // why. Record it as a soft error and skip the layout-dependent probe.
        let layout = match crate::partition::detect_layout(&mut conn).await {
            Ok(layout) => Some(layout),
            Err(e) => {
                soft_errors.push(format!(
                    "could not determine the harvest_events layout, so the \
                     dangling-event invariant was skipped (it does not hold on the \
                     partitioned layout, where orphan event rows are reclaimed by \
                     the partition sweeper): {e}"
                ));
                None
            }
        };
        if layout.is_some_and(|l| !l.is_partitioned()) {
            dangling.push((
                FindingClass::DanglingEventExecution,
                bounded(
                    "SELECT DISTINCT ev.workflow_exec_id AS id FROM harvest_events ev \
                     WHERE NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e \
                                       WHERE e.id = ev.workflow_exec_id)",
                    "sub.id::text",
                    limit,
                ),
            ));
        }
        for (class, sql) in dangling {
            match probe(&mut conn, class, shard_id, sql, None).await {
                Ok(Some(f)) => findings.push(f),
                Ok(None) => {}
                Err(e) => soft_errors.push(e),
            }
        }

        // ── Non-terminal population + newest event timestamp ────────────────
        // Both reads feed operator-visible report fields, and a swallowed
        // error here is the worst possible failure mode: a count that reads
        // `0` looks like a drained fleet, and a `None` timestamp silently
        // disables the cross-shard skew check. Record the error instead so
        // the shard is Undetermined rather than falsely clean.
        let non_terminal: Option<i64> = match diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE state IN ('RUNNING', 'PAUSED', 'SUSPENDED')",
        )
        .get_result::<CountRow>(&mut conn)
        .await
        {
            Ok(r) => Some(r.n),
            Err(e) => {
                soft_errors.push(format!("non-terminal execution count failed: {e}"));
                None
            }
        };

        let latest_event_at =
            match diesel::sql_query("SELECT MAX(timestamp) AS latest FROM harvest_events")
                .get_result::<LatestEventRow>(&mut conn)
                .await
            {
                Ok(r) => r.latest,
                Err(e) => {
                    soft_errors.push(format!("latest event timestamp probe failed: {e}"));
                    None
                }
            };

        // ── Cross-shard references asserted by this shard's history ─────────
        let mut refs = Vec::new();
        match collect_refs(&mut conn, shard_id, limit).await {
            Ok(scan) => {
                refs = scan.refs;
                // A truncated scan did not adjudicate the remainder, so it is
                // Undetermined -- never a silent pass.
                if let Some(note) = scan.truncation {
                    soft_errors.push(note);
                }
                if let Some(note) = scan.undecodable {
                    soft_errors.push(note);
                }
                if let Some(advisory) = scan.workflow_id_targets {
                    findings.push(advisory);
                }
            }
            Err(e) => soft_errors.push(e),
        }

        // ── Replay a bounded sample of non-terminal histories ───────────────
        let replay = match replay_sample(&mut conn, options, replayer, shard_id).await {
            Ok((summary, mut divergences)) => {
                findings.append(&mut divergences);
                summary
            }
            Err(e) => {
                soft_errors.push(e);
                ReplaySummary::default()
            }
        };

        if !soft_errors.is_empty() {
            // Undetermined, NOT advisory: a probe that could not run found
            // nothing because it did not look. The canonical case is a
            // "restore" that produced an unmigrated database -- every probe
            // errors on a missing table, and every condition reads as absent.
            let n = soft_errors.len() as u64;
            findings.push(Finding::new(
                FindingClass::ProbeFailed,
                Some(shard_id),
                n,
                soft_errors,
            ));
        }

        (
            ShardVerifyReport {
                shard_id,
                dsn,
                reachable: true,
                unreachable_reason: None,
                latest_event_at,
                non_terminal_executions: non_terminal.and_then(|n| u64::try_from(n).ok()),
                replay,
                findings,
            },
            refs,
        )
    }

    /// Scan this shard's histories for references that only another shard can
    /// adjudicate: awaited/terminal-recorded children and external requests.
    async fn collect_refs(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        limit: i64,
    ) -> Result<CollectedRefs, String> {
        let (scan, truncation) = scan_reference_events(conn, limit).await?;
        let workflow_id_targets = scan.workflow_id_targets.clone();
        let undecodable = (scan.undecodable_count > 0).then(|| {
            format!(
                "cross-shard reference scan could not decode {} child/external event(s);                  those references were NOT adjudicated: {}",
                scan.undecodable_count,
                scan.undecodable_samples.join(", ")
            )
        });
        Ok(CollectedRefs {
            refs: build_refs(scan, shard_id),
            truncation,
            workflow_id_targets,
            undecodable,
        })
    }

    /// What one shard's reference scan produced: the cross-shard references to
    /// adjudicate, an optional truncation note, and an optional advisory for
    /// business-key-addressed requests that carry no execution id.
    struct CollectedRefs {
        refs: Vec<PendingRef>,
        truncation: Option<String>,
        workflow_id_targets: Option<Finding>,
        /// Fail-closed note when any child/external row could not be decoded.
        undecodable: Option<String>,
    }

    /// The per-owning-execution reference bookkeeping one shard's history
    /// asserts, before any cross-shard resolution.
    #[derive(Default)]
    struct RefScan {
        /// owner -> children seen `ChildWorkflowStarted`.
        awaited: BTreeMap<Uuid, Vec<Uuid>>,
        /// owner -> children whose terminal the owner recorded.
        child_terminal: BTreeMap<Uuid, Vec<Uuid>>,
        /// owner -> external requests, each carrying the correlation id, the
        /// target execution id, and the effect a successful terminal asserts.
        requested: BTreeMap<Uuid, Vec<(Uuid, Uuid, ExternalEffect)>>,
        /// owner -> correlation ids the owner recorded as *successfully*
        /// delivered/resolved. These assert durable state on the target shard.
        delivered: BTreeMap<Uuid, Vec<Uuid>>,
        /// owner -> correlation ids the owner recorded as *failed*. A failed
        /// request applied no effect, so there is nothing to adjudicate.
        failed: BTreeMap<Uuid, Vec<Uuid>>,
        /// Advisory for business-key-addressed external requests, which carry
        /// no execution id to adjudicate.
        workflow_id_targets: Option<Finding>,
        /// Rows whose `event_data` this build could not decode. Fail-closed:
        /// an undecodable child/external event may be hiding a genuine
        /// cross-shard break, so the whole check is reported Undetermined
        /// rather than silently narrowed.
        undecodable_samples: Vec<String>,
        /// Exact count of undecodable rows (samples above are bounded).
        undecodable_count: u64,
        /// Owners whose own execution is already terminal. Their DELIVERED
        /// effects still assert target-shard state and are adjudicated; their
        /// unresolved requests are not live dependencies and are dropped.
        terminal_owners: BTreeSet<Uuid>,
    }

    /// Read this shard's child/external history events and fold them into a
    /// [`RefScan`].
    ///
    /// Returns the fold plus, when the scan was truncated, a note for the
    /// caller to raise as `ProbeFailed`.
    async fn scan_reference_events(
        conn: &mut AsyncPgConnection,
        limit: i64,
    ) -> Result<(RefScan, Option<String>), String> {
        // The owner-state filter is split by event class, because the two
        // classes answer different questions.
        //
        // CHILD events: only a NON-TERMINAL owner matters. A terminal parent's
        // recorded child terminal is history, not a live dependency, and
        // retention may legitimately have collected the child -- scanning
        // those would report `child_execution_missing` (Incoherent, exit 1)
        // on a healthy restore.
        //
        // EXTERNAL events: EVERY owner matters, terminal included. A caller
        // that recorded `ExternalSignalDelivered` asserted durable state on the
        // TARGET shard, and that assertion does not expire when the caller
        // completes. If anything a terminal caller is worse: there is no live
        // caller left to re-drive the delivery, so a target restored to before
        // it waits forever. Filtering them out made that a silent exit 0.
        //
        // Terminal owners' *unresolved* requests are dropped in `build_refs`
        // (see `terminal_owners`) -- only DELIVERED effects are adjudicated
        // from them.
        // The scan PAGES through complete owner groups rather than taking a
        // single `LIMIT probe_limit` slice. A hard limit made any fleet with
        // more reference events than the probe limit report `ProbeFailed` ->
        // `Undetermined` -> exit 2: a false "cannot verify" on a healthy
        // restore, with no operator override.
        //
        // Two invariants hold every page:
        //
        // (1) The cut lands on an execution BOUNDARY. The scan is ordered
        //     `(workflow_exec_id, event_id)`, so a raw `LIMIT` slices through
        //     the middle of the last execution -- keeping its
        //     `ChildWorkflowStarted` but dropping the matching
        //     `ChildWorkflowCompleted`. `build_refs` would then classify that
        //     child as still-awaited rather than terminal-recorded, and
        //     `resolve_refs` would report `ChildExecutionMissing` --
        //     Incoherent, exit 1, "do not start workers" -- on a healthy
        //     restore whose child had simply been collected by retention.
        //     Dropping the partial tail and RE-FETCHING that owner from the
        //     start of the next page makes that false positive impossible
        //     without losing the group.
        //
        // (2) Anything genuinely NOT reached is still Undetermined. Reporting a
        //     clean verdict over an arbitrary prefix of a real fleet is exactly
        //     the false-clean this module exists to refuse, so both bounded
        //     exits below raise a truncation note.
        let page = limit.max(1);
        let mut rows: Vec<EventRow> = Vec::new();
        let mut cursor: Option<Uuid> = None;
        let mut truncation: Option<String> = None;
        let mut exhausted = false;

        for _ in 0..MAX_REFERENCE_SCAN_PAGES {
            // Keyset on the owner id alone: every page starts at an owner
            // boundary, so no group is ever split across two pages.
            let after = match cursor {
                Some(_) => "AND ev.workflow_exec_id > $1 ",
                None => "",
            };
            let sql = format!(
                "SELECT ev.workflow_exec_id, ev.event_data, e.state AS owner_state \
                 FROM harvest_events ev \
                 JOIN harvest_workflow_executions e ON e.id = ev.workflow_exec_id \
                 WHERE ( \
                     ev.event_type IN ( \
                       'ExternalSignalRequested', 'ExternalSignalDelivered', \
                       'ExternalSignalFailed', \
                       'ExternalCancelRequested', 'ExternalCancelDelivered', \
                       'ExternalCancelFailed', \
                       'ExternalAwaitRequested', 'ExternalAwaitResolved', \
                       'ExternalAwaitFailed') \
                     OR ( \
                       e.state IN ('RUNNING', 'PAUSED', 'SUSPENDED') \
                       AND ev.event_type IN ( \
                         'ChildWorkflowStarted', 'ChildWorkflowCompleted', \
                         'ChildWorkflowFailed')) \
                   ) \
                 {after}\
                 ORDER BY ev.workflow_exec_id, ev.event_id \
                 LIMIT {page}"
            );
            let query = diesel::sql_query(sql);
            let loaded: Vec<EventRow> = match cursor {
                Some(c) => query.bind::<diesel::sql_types::Uuid, _>(c).load(conn).await,
                None => query.load(conn).await,
            }
            .map_err(|e| format!("cross-shard reference scan failed: {e}"))?;

            if loaded.is_empty() {
                exhausted = true;
                break;
            }

            // A short page proves the shard is exhausted, so its tail group is
            // whole and must be kept.
            let last_page = i64::try_from(loaded.len()).unwrap_or(i64::MAX) < page;
            let boundary = loaded.last().map(|r| r.workflow_exec_id);
            let kept: Vec<EventRow> = if last_page {
                loaded
            } else {
                loaded
                    .into_iter()
                    .filter(|r| Some(r.workflow_exec_id) != boundary)
                    .collect()
            };

            let kept = if kept.is_empty() {
                // One owner group filled the whole page, so no COMPLETE group
                // could be taken and the cursor cannot advance. That does not
                // yet prove the group is oversized: a group whose size is
                // exactly `page` fills the page while still being whole.
                // Re-read it alone with one extra row to tell the two apart --
                // otherwise every such group would raise a false stall
                // (`probe_failed` -> Undetermined) on a healthy shard.
                let Some(owner) = boundary else { break };
                let Some(group) = read_owner_group(conn, owner, page).await? else {
                    // Genuinely larger than a page. Stopping without a note
                    // would silently skip it; looping would never terminate,
                    // since the cursor still cannot advance.
                    truncation = Some(format!(
                        "cross-shard reference scan stalled: execution {owner} has more \
                         than probe_limit={page} reference events, so its group could \
                         not be read whole; it and everything after it were NOT \
                         adjudicated (raise --probe-limit)"
                    ));
                    break;
                };
                group
            } else {
                kept
            };

            cursor = kept.last().map(|r| r.workflow_exec_id);
            rows.extend(kept);

            if last_page {
                exhausted = true;
                break;
            }
        }

        if !exhausted && truncation.is_none() {
            truncation = Some(format!(
                "cross-shard reference scan hit its page ceiling after {} events \
                 ({MAX_REFERENCE_SCAN_PAGES} pages of probe_limit={page}); the \
                 remainder was NOT adjudicated (raise --probe-limit)",
                rows.len()
            ));
        }

        Ok((fold_reference_events(rows), truncation))
    }

    /// Read ONE owner's reference events whole, or report that it does not fit.
    ///
    /// Used only when a page was filled entirely by a single owner: reading
    /// `page + 1` rows distinguishes a group of exactly `page` events (whole --
    /// returned) from a genuinely oversized one (`None`, which the caller turns
    /// into a truncation note). Without it, a group whose size happens to equal
    /// the page would raise a false stall on a healthy shard.
    async fn read_owner_group(
        conn: &mut AsyncPgConnection,
        owner: Uuid,
        page: i64,
    ) -> Result<Option<Vec<EventRow>>, String> {
        let probe = page.saturating_add(1);
        let loaded: Vec<EventRow> = diesel::sql_query(format!(
            "SELECT ev.workflow_exec_id, ev.event_data, e.state AS owner_state \
             FROM harvest_events ev \
             JOIN harvest_workflow_executions e ON e.id = ev.workflow_exec_id \
             WHERE ev.workflow_exec_id = $1 \
               AND ( \
                 ev.event_type IN ( \
                   'ExternalSignalRequested', 'ExternalSignalDelivered', \
                   'ExternalSignalFailed', \
                   'ExternalCancelRequested', 'ExternalCancelDelivered', \
                   'ExternalCancelFailed', \
                   'ExternalAwaitRequested', 'ExternalAwaitResolved', \
                   'ExternalAwaitFailed') \
                 OR ( \
                   e.state IN ('RUNNING', 'PAUSED', 'SUSPENDED') \
                   AND ev.event_type IN ( \
                     'ChildWorkflowStarted', 'ChildWorkflowCompleted', \
                     'ChildWorkflowFailed')) \
               ) \
             ORDER BY ev.event_id \
             LIMIT {probe}"
        ))
        .bind::<diesel::sql_types::Uuid, _>(owner)
        .load(conn)
        .await
        .map_err(|e| format!("cross-shard reference scan failed: {e}"))?;

        Ok((i64::try_from(loaded.len()).unwrap_or(i64::MAX) <= page).then_some(loaded))
    }

    /// Fold one decoded child/external event into the running [`RefScan`].
    ///
    /// Split out of `fold_reference_events` so the row loop / decode-failure
    /// handling and the per-variant bookkeeping stay separately readable.
    fn fold_reference_event(
        scan: &mut RefScan,
        owner: Uuid,
        event: WorkflowEvent,
        workflow_id_targets: &mut Vec<String>,
    ) {
        match event {
            WorkflowEvent::ChildWorkflowStarted { child_id, .. } => {
                scan.awaited
                    .entry(owner)
                    .or_default()
                    .push(child_id.as_uuid());
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                scan.child_terminal
                    .entry(owner)
                    .or_default()
                    .push(child_id.as_uuid());
            }
            WorkflowEvent::ExternalSignalRequested { .. }
            | WorkflowEvent::ExternalCancelRequested { .. }
            | WorkflowEvent::ExternalAwaitRequested { .. } => {
                fold_reference_request(scan, owner, event, workflow_id_targets);
            }
            _ => fold_reference_terminal(scan, owner, event),
        }
    }

    /// Fold a `*Requested` event -- the record of an effect this shard's caller
    /// asked for against a target that may live on another shard.
    ///
    /// A `WorkflowId`-addressed target (issue #751) is resolved at delivery time
    /// against whichever run is current, so it is not adjudicable from a
    /// restored snapshot: it is collected for an advisory note instead.
    fn fold_reference_request(
        scan: &mut RefScan,
        owner: Uuid,
        event: WorkflowEvent,
        workflow_id_targets: &mut Vec<String>,
    ) {
        match event {
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name,
                idempotency_key,
                ..
            } => match target {
                ExternalTarget::ExecutionId(t) => {
                    scan.requested.entry(owner).or_default().push((
                        signal_id.as_uuid(),
                        t.as_uuid(),
                        ExternalEffect::Signal {
                            name: signal_name,
                            // Mirror the engine's own normalisation: an empty
                            // key is excluded from the partial unique index, so
                            // `send_signal_idempotent` treats it as no key.
                            // Matching on it would look up a value that was
                            // never stored and report a false rollback.
                            idempotency_key: idempotency_key.filter(|k| !k.is_empty()),
                        },
                    ));
                }
                ExternalTarget::WorkflowId {
                    workflow_name,
                    workflow_id,
                } => workflow_id_targets.push(format!(
                    "signal -> {workflow_name}/{workflow_id} (from {owner})"
                )),
            },
            WorkflowEvent::ExternalCancelRequested { cancel_id, target } => match target {
                ExternalTarget::ExecutionId(t) => scan.requested.entry(owner).or_default().push((
                    cancel_id.as_uuid(),
                    t.as_uuid(),
                    ExternalEffect::Cancel,
                )),
                ExternalTarget::WorkflowId {
                    workflow_name,
                    workflow_id,
                } => workflow_id_targets.push(format!(
                    "cancel -> {workflow_name}/{workflow_id} (from {owner})"
                )),
            },
            WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                scan.requested.entry(owner).or_default().push((
                    await_id.as_uuid(),
                    target.as_uuid(),
                    ExternalEffect::Await,
                ));
            }
            _ => {}
        }
    }

    /// Fold a terminal (`*Delivered` / `*Resolved` / `*Failed`) event -- the
    /// record of what the caller believes actually happened on the target.
    fn fold_reference_terminal(scan: &mut RefScan, owner: Uuid, event: WorkflowEvent) {
        match event {
            // Success terminals assert durable state on the target shard.
            WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                scan.delivered
                    .entry(owner)
                    .or_default()
                    .push(signal_id.as_uuid());
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id } => {
                scan.delivered
                    .entry(owner)
                    .or_default()
                    .push(cancel_id.as_uuid());
            }
            WorkflowEvent::ExternalAwaitResolved { await_id, .. } => {
                scan.delivered
                    .entry(owner)
                    .or_default()
                    .push(await_id.as_uuid());
            }
            // Failure terminals applied no effect: nothing to adjudicate.
            WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                scan.failed
                    .entry(owner)
                    .or_default()
                    .push(signal_id.as_uuid());
            }
            WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                scan.failed
                    .entry(owner)
                    .or_default()
                    .push(cancel_id.as_uuid());
            }
            // `ExternalAwaitFailed` is NOT uniformly a failure terminal: it is
            // also the channel through which a NON-`COMPLETED` terminal outcome
            // is RESOLVED (`execution.rs::read_external_await_outcome` ->
            // `ExternalAwaitOutcome::Terminal` -> `worker.rs`/`timeout.rs`). Those
            // reason codes assert that the target reached a chain-head terminal,
            // exactly like `ExternalAwaitResolved`, so a target restored to
            // before that terminal is a genuine rollback. Only the TRANSPORT
            // codes assert nothing about the target's state.
            WorkflowEvent::ExternalAwaitFailed {
                await_id,
                reason_code,
                ..
            } => {
                let bucket = if is_await_transport_failure(&reason_code) {
                    &mut scan.failed
                } else {
                    &mut scan.delivered
                };
                bucket.entry(owner).or_default().push(await_id.as_uuid());
            }
            _ => {}
        }
    }

    /// Is this `ExternalAwaitFailed` reason code a TRANSPORT failure (the await
    /// never observed a target terminal), rather than a resolved non-`COMPLETED`
    /// terminal outcome?
    ///
    /// The engine emits exactly two transport codes: `target_unknown` (the grace
    /// window elapsed with no target row -- `timeout.rs`) and `self_await`
    /// (rejected inline, and never persisted, but matched here so a future
    /// persisted form cannot be mis-adjudicated). Everything else --
    /// `target_failed` / `target_cancelled` / `target_timed_out` /
    /// `target_terminated` -- carries an assertion about the target shard.
    ///
    /// The match is deliberately an ALLOWLIST of transport codes rather than of
    /// outcome codes: an unrecognised future reason code then falls into the
    /// adjudicated bucket, where the worst case is a reported finding an
    /// operator can dismiss -- never a silently skipped check.
    pub(super) fn is_await_transport_failure(reason_code: &str) -> bool {
        matches!(reason_code, "target_unknown" | "self_await")
    }

    /// Fold decoded child/external events into a [`RefScan`].
    ///
    /// Split out of `scan_reference_events` so the SQL/truncation half and the
    /// event-decoding half stay separately readable.
    fn fold_reference_events(rows: Vec<EventRow>) -> RefScan {
        let mut scan = RefScan::default();
        // `ExternalTarget::WorkflowId` requests resolve by business key at
        // delivery time, not by a fixed execution id, so there is no row this
        // check could look up -- they are deliberately not tracked here.
        let mut workflow_id_targets: Vec<String> = Vec::new();

        for row in rows {
            let event = match serde_json::from_value::<WorkflowEvent>(row.event_data) {
                Ok(event) => event,
                Err(e) => {
                    // FAIL CLOSED. A malformed, legacy or newer-version payload
                    // on a child/external row may be the very reference that
                    // would have exposed a missing target, so dropping it can
                    // turn an incoherent restore into a resumable verdict. The
                    // replay sample is not a backstop here: the shipped CLI
                    // links no handlers (so it replays nothing) and an embedded
                    // replayer samples only a bounded subset.
                    if scan.undecodable_samples.len() < MAX_FINDING_SAMPLES {
                        scan.undecodable_samples
                            .push(format!("{}: {e}", row.workflow_exec_id));
                    }
                    scan.undecodable_count += 1;
                    continue;
                }
            };
            let owner = row.workflow_exec_id;
            if crate::erase::is_terminal_state(&row.owner_state) {
                scan.terminal_owners.insert(owner);
            }
            fold_reference_event(&mut scan, owner, event, &mut workflow_id_targets);
        }
        // A business-key-addressed request (#751) resolves at delivery time, so
        // there is no fixed execution id to look up and we cannot adjudicate it.
        // Every other unadjudicable case in this module gets an advisory
        // (`UninspectedShardReference`, `ReplaySkippedNoHandler`); silently
        // discarding these would be the one blind spot with no signal at all.
        if !workflow_id_targets.is_empty() {
            let n = workflow_id_targets.len() as u64;
            scan.workflow_id_targets = Some(Finding::new(
                FindingClass::WorkflowIdTargetUnchecked,
                None,
                n,
                workflow_id_targets,
            ));
        }
        scan
    }

    /// Turn a shard-local [`RefScan`] into the cross-shard references that still
    /// need adjudication (a started child with no recorded terminal, a recorded
    /// child terminal, or an unresolved external request).
    fn build_refs(scan: RefScan, shard_id: i32) -> Vec<PendingRef> {
        let RefScan {
            awaited,
            child_terminal,
            requested,
            delivered,
            failed,
            workflow_id_targets: _,
            undecodable_samples: _,
            undecodable_count: _,
            terminal_owners,
        } = scan;

        let mut out = Vec::new();
        for (owner, children) in &awaited {
            let terminals = child_terminal.get(owner).cloned().unwrap_or_default();
            for child in children {
                let kind = if terminals.contains(child) {
                    RefKind::ChildTerminalRecorded
                } else {
                    RefKind::AwaitedChild
                };
                out.push(PendingRef {
                    kind,
                    owner_shard: owning_shard(*child, shard_id),
                    target: *child,
                    source_exec: *owner,
                    source_shard: shard_id,
                });
            }
        }
        for (owner, reqs) in &requested {
            let ok = delivered.get(owner).cloned().unwrap_or_default();
            let bad = failed.get(owner).cloned().unwrap_or_default();
            for (corr, target, effect) in reqs {
                // A *failed* terminal applied nothing to the target, so there
                // is no effect to verify and no live request to chase.
                if bad.contains(corr) {
                    continue;
                }
                // A *delivered* terminal is not "done with" -- it is an
                // assertion about the target shard's durable state, and a
                // target restored to an earlier point can silently contradict
                // it. Adjudicate it rather than dropping it.
                let kind = if ok.contains(corr) {
                    RefKind::ExternalEffectDelivered(effect.clone())
                } else if terminal_owners.contains(owner) {
                    // The owner is already terminal, so an UNRESOLVED request
                    // is not a live dependency -- nothing is still waiting on
                    // it, and the target may since have been collected by
                    // retention. Reporting it as `external_target_missing`
                    // would be a false Incoherent on a healthy restore. Its
                    // DELIVERED siblings above are still adjudicated.
                    continue;
                } else {
                    RefKind::ExternalTarget
                };
                out.push(PendingRef {
                    kind,
                    owner_shard: owning_shard(*target, shard_id),
                    target: *target,
                    source_exec: *owner,
                    source_shard: shard_id,
                });
            }
        }
        out
    }

    /// Decode the owning shard from an execution id, falling back to the
    /// observing shard for the `UNENCODED` (pre-sharding) sentinel.
    fn owning_shard(id: Uuid, observing: i32) -> i32 {
        let shard = ExecutionId::from_uuid(id).shard();
        if shard.is_unencoded() {
            observing
        } else {
            shard.as_i32()
        }
    }

    /// Replay a bounded sample of this shard's non-terminal histories.
    /// The bounded per-class sample ids collected during a replay sweep.
    struct ReplaySamples {
        divergent: Vec<String>,
        failed: Vec<String>,
        skipped: Vec<String>,
        unreadable: Vec<String>,
    }

    /// Turn a completed [`ReplaySummary`] plus its samples into findings.
    ///
    /// Split out of `replay_sample` so the sampling/classification half and the
    /// finding-construction half stay separately readable.
    fn replay_findings(
        summary: &ReplaySummary,
        shard_id: i32,
        samples: ReplaySamples,
    ) -> Vec<Finding> {
        let mut findings = Vec::new();
        if summary.divergent > 0 {
            findings.push(Finding::new(
                FindingClass::ReplayDivergence,
                Some(shard_id),
                summary.divergent,
                samples.divergent,
            ));
        }
        if summary.failed > 0 {
            findings.push(Finding::new(
                FindingClass::ReplayWorkflowFailed,
                Some(shard_id),
                summary.failed,
                samples.failed,
            ));
        }
        if summary.skipped_no_handler > 0 {
            findings.push(Finding::new(
                FindingClass::ReplaySkippedNoHandler,
                Some(shard_id),
                summary.skipped_no_handler,
                samples.skipped,
            ));
        }
        if summary.unreadable > 0 {
            findings.push(Finding::new(
                FindingClass::HistoryUnreadable,
                Some(shard_id),
                summary.unreadable,
                samples.unreadable,
            ));
        }
        findings
    }

    async fn replay_sample(
        conn: &mut AsyncPgConnection,
        options: &VerifyOptions,
        replayer: &WorkflowReplayer,
        shard_id: i32,
    ) -> Result<(ReplaySummary, Vec<Finding>), String> {
        let n = i64::try_from(options.replay_sample.min(super::MAX_REPLAY_SAMPLE)).unwrap_or(0);
        if n == 0 {
            return Ok((ReplaySummary::default(), Vec::new()));
        }
        let rows: Vec<ExecIdRow> = diesel::sql_query(format!(
            "SELECT id FROM harvest_workflow_executions \
             WHERE state IN ('RUNNING', 'PAUSED', 'SUSPENDED') \
             ORDER BY started_at DESC LIMIT {n}"
        ))
        .load(conn)
        .await
        .map_err(|e| format!("replay sampling failed: {e}"))?;

        let mut summary = ReplaySummary::default();
        let mut divergent_samples = Vec::new();
        let mut skipped_samples = Vec::new();
        let mut unreadable_samples = Vec::new();
        let mut failed_samples = Vec::new();

        for row in rows {
            summary.sampled += 1;
            let exec_id = ExecutionId::from_uuid(row.id);

            let name: Option<String> = diesel::sql_query(
                "SELECT workflow_name AS state FROM harvest_workflow_executions WHERE id = $1",
            )
            .bind::<diesel::sql_types::Uuid, _>(row.id)
            .get_result::<StateRow>(conn)
            .await
            .ok()
            .map(|r| r.state);

            match name {
                Some(n) if !replayer.is_workflow_registered(&n) => {
                    summary.skipped_no_handler += 1;
                    if skipped_samples.len() < MAX_FINDING_SAMPLES {
                        skipped_samples.push(format!("{exec_id} ({n})"));
                    }
                    continue;
                }
                None => {
                    summary.unreadable += 1;
                    if unreadable_samples.len() < MAX_FINDING_SAMPLES {
                        unreadable_samples.push(exec_id.to_string());
                    }
                    continue;
                }
                Some(_) => {}
            }

            // Canary (frontier-tolerant) mode: a parked in-flight run legitimately
            // suspends before consuming its whole recorded history, which strict
            // mode would report as a false `EarlyCompletion` divergence.
            //
            // The FULL status is preserved. Collapsing it to a divergent/clean
            // bool would count `WorkflowFailed` as clean, but these samples are
            // *non-terminal* runs: their recorded history contains no terminal
            // failure, so a replay failure means the deployed handler now errors
            // where the live run had not. The engine's own replay canary
            // (`run_canary`) counts that as `replay_failed`; so do we.
            match replayer.replay_canary_from_db(conn, exec_id).await {
                Ok(report) => match report.status {
                    crate::testing::ReplayStatus::ReplaySucceeded => summary.clean += 1,
                    crate::testing::ReplayStatus::NonDeterminismDetected { .. } => {
                        summary.divergent += 1;
                        if divergent_samples.len() < MAX_FINDING_SAMPLES {
                            divergent_samples.push(exec_id.to_string());
                        }
                    }
                    crate::testing::ReplayStatus::WorkflowFailed { error, .. } => {
                        summary.failed += 1;
                        if failed_samples.len() < MAX_FINDING_SAMPLES {
                            failed_samples.push(format!("{exec_id}: {error}"));
                        }
                    }
                },
                Err(e) => {
                    summary.unreadable += 1;
                    if unreadable_samples.len() < MAX_FINDING_SAMPLES {
                        // Carry the reason: "unreadable" alone leaves an
                        // operator with nothing to act on, and the cause
                        // (undecodable payload vs missing row vs codec) decides
                        // whether the restore or the deployed build is at fault.
                        unreadable_samples.push(format!("{exec_id}: {e}"));
                    }
                }
            }
        }

        let findings = replay_findings(
            &summary,
            shard_id,
            ReplaySamples {
                divergent: divergent_samples,
                failed: failed_samples,
                skipped: skipped_samples,
                unreadable: unreadable_samples,
            },
        );
        Ok((summary, findings))
    }

    /// Resolve every cross-shard reference against the shard that owns it.
    ///
    /// A reference pointing at a shard whose DSN was not supplied cannot be
    /// adjudicated either way, so it is reported as an advisory rather than
    /// silently passing.
    /// The per-reference adjudication buckets for one owning shard.
    #[derive(Default)]
    struct RefBuckets {
        missing_child: Vec<String>,
        rolled_back: Vec<String>,
        missing_external: Vec<String>,
        pending_external: Vec<String>,
        lookup_errors: Vec<String>,
        lost_effect: Vec<String>,
        unverifiable_effect: Vec<String>,
    }

    /// Look up each reference's target on its owning shard and bucket the
    /// verdict. Split out of `resolve_refs` so the per-shard connection
    /// handling and the per-reference verdict logic stay separately readable.
    async fn adjudicate_refs(conn: &mut AsyncPgConnection, owned: &[&PendingRef]) -> RefBuckets {
        let mut out = RefBuckets::default();
        for r in owned {
            // `.optional()` is load-bearing: `get_result` returns
            // `Err(NotFound)` for zero rows AND `Err(DatabaseError)` for a
            // real failure. Collapsing both with `.ok()` would make a
            // transient query error read as "this execution is absent" --
            // an Incoherent verdict (exit 1, "do not start workers") on a
            // perfectly good restore -- and, for a recorded terminal, read
            // as ordinary retention, hiding a genuine rollback.
            let looked_up =
                diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
                    .bind::<diesel::sql_types::Uuid, _>(r.target)
                    .get_result::<StateRow>(conn)
                    .await
                    .optional();

            let state: Option<String> = match looked_up {
                Ok(row) => row.map(|row| row.state),
                Err(e) => {
                    out.lookup_errors
                        .push(format!("{} lookup failed: {e}", r.target));
                    continue;
                }
            };

            match (&r.kind, state) {
                (RefKind::AwaitedChild, None) => {
                    out.missing_child.push(format!(
                        "{} (awaited by {} on shard {})",
                        r.target, r.source_exec, r.source_shard
                    ));
                }
                (RefKind::ChildTerminalRecorded, Some(s))
                    if !crate::erase::is_terminal_state(&s) =>
                {
                    out.rolled_back.push(format!(
                        "{} is {s} on shard {} but {} on shard {} recorded its terminal",
                        r.target, r.owner_shard, r.source_exec, r.source_shard
                    ));
                }
                // A recorded child terminal (or a delivered external effect)
                // whose execution row is gone is ordinary retention, not
                // incoherence.
                (RefKind::ChildTerminalRecorded, _)
                | (RefKind::AwaitedChild, Some(_))
                | (RefKind::ExternalEffectDelivered(_), None) => {}
                (RefKind::ExternalTarget, None) => {
                    out.missing_external.push(format!(
                        "{} (requested by {} on shard {})",
                        r.target, r.source_exec, r.source_shard
                    ));
                }
                (RefKind::ExternalTarget, Some(_)) => {
                    out.pending_external.push(format!(
                        "{} (requested by {} on shard {})",
                        r.target, r.source_exec, r.source_shard
                    ));
                }
                (RefKind::ExternalEffectDelivered(effect), Some(s)) => {
                    match effect_verdict(conn, r.target, effect, &s).await {
                        Ok(EffectVerdict::Survived) => {}
                        Ok(EffectVerdict::Unverifiable) => {
                            out.unverifiable_effect.push(format!(
                                "{} unkeyed signal delivered by {} on shard {} cannot be \
                                 identified on shard {} (a signal of that name is present, \
                                 but the engine records no per-delivery identity)",
                                r.target, r.source_exec, r.source_shard, r.owner_shard
                            ));
                        }
                        Ok(EffectVerdict::Lost) => out.lost_effect.push(format!(
                            "{} {} delivered by {} on shard {} left no trace on shard {} \
                             (target is {s})",
                            effect.label(),
                            r.target,
                            r.source_exec,
                            r.source_shard,
                            r.owner_shard
                        )),
                        Err(e) => out
                            .lookup_errors
                            .push(format!("{} effect check failed: {e}", r.target)),
                    }
                }
            }
        }
        out
    }

    /// Does the target shard still show the effect the caller recorded as
    /// delivered?
    ///
    /// * `Cancel` — delivered either by cancelling the target or as a
    ///   documented no-op against an ALREADY-terminal one (issue #492), so any
    ///   terminal state on the restored shard is consistent with the delivery
    ///   and a non-terminal one means it was rolled back.
    /// * `Await` — resolved only once the target's `CONTINUED_AS_NEW` CHAIN HEAD
    ///   is terminal, so a bare terminal check is not enough (see
    ///   [`await_verdict`]).
    /// * `Signal` — the delivery inserts a `harvest_signals` row, which the
    ///   worker later promotes to a `SignalReceived` event. The row persists
    ///   after consumption (`consumed` is a flag, not a delete), so *neither*
    ///   present means the target was restored to before the delivery.
    async fn effect_verdict(
        conn: &mut AsyncPgConnection,
        target: Uuid,
        effect: &ExternalEffect,
        state: &str,
    ) -> Result<EffectVerdict, diesel::result::Error> {
        match effect {
            ExternalEffect::Cancel => Ok(if crate::erase::is_terminal_state(state) {
                EffectVerdict::Survived
            } else {
                EffectVerdict::Lost
            }),
            ExternalEffect::Await => await_verdict(conn, target, state).await,
            ExternalEffect::Signal {
                name,
                idempotency_key,
            } => {
                // Walk the target's SUCCESSOR CHAIN, not just the target row.
                //
                // Both continue-as-new (`worker.rs`, unconsumed rows only) and
                // workflow-level retry (`signal::forward_signals_to_retry_attempt`,
                // issue #843 -- the WHOLE mailbox, re-arming `consumed`)
                // REASSIGN `harvest_signals.workflow_exec_id` to the successor.
                // An unconsumed signal produces no `SignalReceived` event, so
                // after either transition the original target carries no row
                // AND no event -- checking it alone would report a healthy
                // restore as rolled back and exit 1.
                //
                // `UNION` (not `UNION ALL`) makes the recursion terminate even
                // if a chain link were ever cyclic.
                const CHAIN: &str = "WITH RECURSIVE chain(id) AS ( \
                         SELECT $1::uuid \
                       UNION \
                         SELECT w.id FROM harvest_workflow_executions w \
                           JOIN chain c \
                             ON w.continued_from_exec_id = c.id \
                             OR w.retry_of_exec_id = c.id \
                     ) ";

                if let Some(key) = idempotency_key {
                    // Exact: the key is per-delivery and travels with the row
                    // through every reassignment (only `workflow_exec_id` and
                    // `consumed` are rewritten).
                    let row: ExistsRow = diesel::sql_query(format!(
                        "{CHAIN}SELECT EXISTS ( \
                             SELECT 1 FROM harvest_signals s \
                               JOIN chain c ON s.workflow_exec_id = c.id \
                             WHERE s.idempotency_key = $2 \
                           ) AS present"
                    ))
                    .bind::<diesel::sql_types::Uuid, _>(target)
                    .bind::<diesel::sql_types::Text, _>(key)
                    .get_result(conn)
                    .await?;
                    return Ok(if row.present {
                        EffectVerdict::Survived
                    } else {
                        EffectVerdict::Lost
                    });
                }

                // Unkeyed: the engine persists no per-delivery identity, so the
                // most that can be asked is whether ANY signal of this name
                // survives on the chain. Absence is still definitive (nothing
                // of this name survived); presence is not proof that THIS
                // delivery did -- a repeated channel name would match an older
                // one -- so it is reported as unverifiable, never clean.
                let row: ExistsRow = diesel::sql_query(format!(
                    "{CHAIN}SELECT EXISTS ( \
                         SELECT 1 FROM harvest_signals s \
                           JOIN chain c ON s.workflow_exec_id = c.id \
                         WHERE s.signal_name = $2 \
                       ) OR EXISTS ( \
                         SELECT 1 FROM harvest_events e \
                           JOIN chain c ON e.workflow_exec_id = c.id \
                         WHERE e.event_type = 'SignalReceived' \
                           AND e.event_data->'data'->>'signal_name' = $2 \
                       ) AS present"
                ))
                .bind::<diesel::sql_types::Uuid, _>(target)
                .bind::<diesel::sql_types::Text, _>(name)
                .get_result(conn)
                .await?;
                Ok(if row.present {
                    EffectVerdict::Unverifiable
                } else {
                    EffectVerdict::Lost
                })
            }
        }
    }

    /// Bound on `CONTINUED_AS_NEW` hops the await chain walk follows -- mirrors
    /// `execution::read_external_await_outcome`'s own bound.
    const AWAIT_CHAIN_MAX_HOPS: usize = 128;

    /// Does the target shard still hold the terminal an `ExternalAwait*` terminal
    /// asserted?
    ///
    /// `read_external_await_outcome` FOLLOWS a `CONTINUED_AS_NEW` target through
    /// its successor chain and resolves only once the chain HEAD is terminal, so
    /// the ORIGINAL row reading `CONTINUED_AS_NEW` is not by itself proof the
    /// await resolved -- the successor may be back to `RUNNING`. Because
    /// `is_terminal_state("CONTINUED_AS_NEW")` is true, a bare terminal check
    /// reads exactly that rollback as coherent.
    async fn await_verdict(
        conn: &mut AsyncPgConnection,
        target: Uuid,
        state: &str,
    ) -> Result<EffectVerdict, diesel::result::Error> {
        let mut current = target;
        let mut current_state = state.to_string();

        for _ in 0..AWAIT_CHAIN_MAX_HOPS {
            if current_state != "CONTINUED_AS_NEW" {
                return Ok(if crate::erase::is_terminal_state(&current_state) {
                    EffectVerdict::Survived
                } else {
                    EffectVerdict::Lost
                });
            }

            // Follow the link the engine reader itself follows: the
            // predecessor's own `WorkflowContinuedAsNew` event. The successor's
            // `continued_from_exec_id` back-link is the newer (issue #701)
            // encoding of the same edge and is absent on pre-#701 rows, so the
            // event is the safer source.
            let successor: Option<SuccessorRow> = diesel::sql_query(
                "SELECT e.event_data->'data'->>'new_exec_id' AS new_exec_id \
                 FROM harvest_events e \
                 WHERE e.workflow_exec_id = $1 \
                   AND e.event_type = 'WorkflowContinuedAsNew' \
                 ORDER BY e.event_id DESC LIMIT 1",
            )
            .bind::<diesel::sql_types::Uuid, _>(current)
            .get_result(conn)
            .await
            .optional()?;

            let Some(next) = successor
                .and_then(|row| row.new_exec_id)
                .and_then(|raw| Uuid::parse_str(&raw).ok())
            else {
                // A `CONTINUED_AS_NEW` row naming no readable successor is what
                // the engine reader reports as still-in-flight, so the caller
                // could not have resolved against this shard's current state.
                return Ok(EffectVerdict::Lost);
            };

            let row: Option<StateRow> =
                diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
                    .bind::<diesel::sql_types::Uuid, _>(next)
                    .get_result(conn)
                    .await
                    .optional()?;

            let Some(row) = row else {
                // The successor row is gone. The predecessor's seal and the
                // successor's insert are one transaction, so an absent successor
                // is retention collecting a TERMINAL run -- not a restore point
                // that predates it. Treated as ordinary retention, exactly like
                // an absent target row upstream.
                return Ok(EffectVerdict::Survived);
            };

            current = next;
            current_state = row.state;
        }

        // A chain longer than the engine's own reader follows. Neither verdict
        // is supportable from what we can see, so prefer the one that cannot
        // fail a healthy restore.
        Ok(EffectVerdict::Survived)
    }

    pub(super) async fn resolve_refs(refs: &[PendingRef], targets: &[ShardTarget]) -> Vec<Finding> {
        use std::collections::BTreeSet;

        let mut findings = Vec::new();
        let known: BTreeSet<i32> = targets.iter().map(|t| t.shard_id).collect();

        let uninspected: Vec<String> = refs
            .iter()
            .filter(|r| !known.contains(&r.owner_shard))
            .map(|r| format!("{} -> shard {}", r.target, r.owner_shard))
            .collect();
        if !uninspected.is_empty() {
            findings.push(Finding::new(
                FindingClass::UninspectedShardReference,
                None,
                uninspected.len() as u64,
                uninspected,
            ));
        }

        for target in targets {
            let owned: Vec<&PendingRef> = refs
                .iter()
                .filter(|r| r.owner_shard == target.shard_id)
                .collect();
            if owned.is_empty() {
                continue;
            }
            // This is a SECOND, independent connection: `verify_shard` opened
            // and dropped its own long before we got here. So a failure now is
            // NOT "already reported by verify_shard" -- that shard is recorded
            // `reachable: true` and carries no unreachable finding, meaning a
            // silent `continue` here would drop every cross-shard check for it
            // and still report exit 0. That is precisely the false-clean the
            // Undetermined tier exists to prevent.
            let mut conn = match connect_read_only(&target.dsn).await {
                Ok(conn) => conn,
                Err(e) => {
                    findings.push(Finding::new(
                        FindingClass::ProbeFailed,
                        Some(target.shard_id),
                        1,
                        vec![format!(
                            "cross-shard reference resolution could not connect to \
                             shard {}: {e}",
                            target.shard_id
                        )],
                    ));
                    continue;
                }
            };

            let buckets = adjudicate_refs(&mut conn, &owned).await;
            let RefBuckets {
                missing_child,
                rolled_back,
                missing_external,
                pending_external,
                lookup_errors,
                lost_effect,
                unverifiable_effect,
            } = buckets;

            for (class, samples) in [
                (FindingClass::ChildExecutionMissing, missing_child),
                (FindingClass::ChildTerminalRolledBack, rolled_back),
                (FindingClass::ExternalTargetMissing, missing_external),
                (FindingClass::PendingExternalRequest, pending_external),
                (FindingClass::ExternalEffectRolledBack, lost_effect),
                (
                    FindingClass::ExternalEffectUnverifiable,
                    unverifiable_effect,
                ),
                // A reference we could not adjudicate is Undetermined, never a
                // pass: the row may be missing, or the query may simply have
                // failed, and we must not guess which.
                (FindingClass::ProbeFailed, lookup_errors),
            ] {
                if !samples.is_empty() {
                    findings.push(Finding::new(
                        class,
                        Some(target.shard_id),
                        samples.len() as u64,
                        samples,
                    ));
                }
            }
        }
        findings
    }

    /// Assemble the full report: per-shard probes, then cross-shard resolution.
    pub(super) async fn run(
        targets: &[ShardTarget],
        options: &VerifyOptions,
        replayer: &WorkflowReplayer,
    ) -> RestoreVerifyReport {
        let mut shards = Vec::new();
        let mut refs = Vec::new();
        for target in targets {
            let (report, mut shard_refs) = verify_shard(target, options, replayer).await;
            refs.append(&mut shard_refs);
            shards.push(report);
        }

        let mut cross_shard = resolve_refs(&refs, targets).await;

        let skew = compute_skew(shards.iter().map(|s| s.latest_event_at));
        if let Some(secs) = skew
            && secs > options.max_skew_secs
        {
            cross_shard.push(
                Finding::new(FindingClass::RestorePointSkew, None, 1, Vec::new()).with_detail(
                    format!(
                        "newest-event timestamps differ by {secs}s across shards \
                             (threshold {}s)",
                        options.max_skew_secs
                    ),
                ),
            );
        }

        RestoreVerifyReport::assemble(chrono::Utc::now(), shards, cross_shard)
    }
}

/// Verify that a restored (scratch) fleet is resumable.
///
/// Connects to every supplied shard **read-only** (`SET SESSION CHARACTERISTICS
/// AS TRANSACTION READ ONLY`, so Postgres itself rejects any write with
/// SQLSTATE 25006), probes each shard for the conditions in [`FindingClass`],
/// replays a bounded sample of non-terminal histories through `replayer`, and
/// cross-checks references that span shards.
///
/// Never mutates any database (AC4) and never starts a worker, a scanner, or a
/// scheduler tick — the reclaimable classes are *reported*, never applied.
#[cfg(all(feature = "db", feature = "testing"))]
pub async fn verify_restore(
    targets: &[ShardTarget],
    options: &VerifyOptions,
    replayer: &crate::testing::WorkflowReplayer,
) -> RestoreVerifyReport {
    probes::run(targets, options, replayer).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(class: FindingClass) -> Finding {
        Finding::new(class, Some(0), 1, vec!["x".into()])
    }

    /// Codex round 4, P1. `ExternalAwaitFailed` is the channel for BOTH a
    /// transport failure and a resolved non-`COMPLETED` terminal outcome
    /// (`execution::read_external_await_outcome`). Only the transport codes may
    /// be skipped; the outcome codes assert target-shard state.
    ///
    /// The `unknown_future_code` case pins the allowlist direction: an
    /// unrecognised code must be ADJUDICATED, so the worst case is a finding an
    /// operator can dismiss rather than a silently skipped check.
    #[test]
    #[cfg(all(feature = "db", feature = "testing"))]
    fn await_transport_failures_are_an_allowlist_not_a_denylist() {
        use super::probes::is_await_transport_failure;

        for transport in ["target_unknown", "self_await"] {
            assert!(
                is_await_transport_failure(transport),
                "{transport} asserts nothing about the target shard"
            );
        }
        for outcome in [
            "target_failed",
            "target_cancelled",
            "target_timed_out",
            "target_terminated",
            "unknown_future_code",
        ] {
            assert!(
                !is_await_transport_failure(outcome),
                "{outcome} must be adjudicated, not skipped"
            );
        }
    }

    /// Codex round 1, P2. `host=alias-a&hostaddr=X` and `host=alias-b&hostaddr=X`
    /// reach the SAME database; folding hostaddr into the hostname set made
    /// identity require matching hostname *text*, so the guard waved the run
    /// through against production.
    #[test]
    #[cfg(feature = "db")]
    fn dsn_guard_matches_on_a_shared_hostaddr_despite_different_hostnames() {
        assert!(dsn_targets_same_database(
            "postgres://u@scratch-alias/harvest?hostaddr=10.0.0.5",
            "postgres://u@prod-alias/harvest?hostaddr=10.0.0.5",
        ));
        // A shared address is only a match when port and database agree too.
        assert!(!dsn_targets_same_database(
            "postgres://u@scratch-alias/scratch?hostaddr=10.0.0.5",
            "postgres://u@prod-alias/harvest?hostaddr=10.0.0.5",
        ));
        assert!(!dsn_targets_same_database(
            "postgres://u@a/harvest?hostaddr=10.0.0.6",
            "postgres://u@b/harvest?hostaddr=10.0.0.5",
        ));
    }

    /// Codex round 7, P2. A NUMERIC `host` is an address, not a name: it needs
    /// no DNS to compare against the other side's `hostaddr`. Keeping it in the
    /// hostname set meant `host=10.0.0.5` and `host=prod-alias&hostaddr=10.0.0.5`
    /// -- the same TCP destination -- overlapped in neither set, so the guard
    /// waved a production target through.
    ///
    /// This is NOT the documented name-vs-address limit: that one genuinely
    /// requires resolving a name, which the guard deliberately will not do.
    #[test]
    #[cfg(feature = "db")]
    fn dsn_guard_matches_a_numeric_host_against_a_hostaddr() {
        assert!(
            dsn_targets_same_database(
                "postgres://u@10.0.0.5/harvest",
                "postgres://u@prod-alias/harvest?hostaddr=10.0.0.5",
            ),
            "a numeric host IS the address the other side pins"
        );
        // Symmetric: the pinned side may be the candidate.
        assert!(dsn_targets_same_database(
            "postgres://u@scratch-alias/harvest?hostaddr=10.0.0.5",
            "postgres://u@10.0.0.5/harvest",
        ));
        // IPv6 spellings of one address must not be compared as raw text.
        assert!(
            dsn_targets_same_database(
                "postgres://u@[0:0:0:0:0:0:0:1]/harvest",
                "postgres://u@prod-alias/harvest?hostaddr=::1",
            ),
            "IPv6 forms of the same address must normalise"
        );
        // The control: a DIFFERENT address must still be a genuine scratch
        // target, so the fix widens matching only where the address is shared.
        assert!(!dsn_targets_same_database(
            "postgres://u@10.0.0.6/harvest",
            "postgres://u@prod-alias/harvest?hostaddr=10.0.0.5",
        ));
        // And two numeric hosts still compare to each other as before.
        assert!(dsn_targets_same_database(
            "postgres://u@10.0.0.5/harvest",
            "postgres://u@10.0.0.5/harvest",
        ));
    }

    /// A multi-host failover DSN that merely LISTS production still reaches it,
    /// so one shared endpoint is enough to trip the guard.
    #[test]
    #[cfg(feature = "db")]
    fn dsn_guard_matches_when_a_multi_host_dsn_lists_the_live_host() {
        assert!(dsn_targets_same_database(
            "postgres://u@scratch.internal,prod.internal/harvest",
            "postgres://u@prod.internal/harvest",
        ));
    }

    /// A replayed history that ends in a workflow ERROR is not clean: these
    /// samples are non-terminal runs, so the recorded history contains no
    /// terminal failure and the deployed handler is erroring where the live run
    /// had not. The engine's own canary counts this as `replay_failed`.
    #[test]
    fn replay_workflow_failure_is_incoherent_never_clean() {
        assert_eq!(
            FindingClass::ReplayWorkflowFailed.severity(),
            FindingSeverity::Incoherent
        );
        let summary = ReplaySummary {
            sampled: 1,
            failed: 1,
            ..ReplaySummary::default()
        };
        // It counts as verification actually having happened...
        assert!(summary.verified());
        // ...and it must not be silently folded into `clean`.
        assert_eq!(summary.clean, 0);
        assert_eq!(
            classify_status(&[finding(FindingClass::ReplayWorkflowFailed)], false),
            VerifyStatus::Incoherent
        );
    }

    /// A caller that recorded an external request as DELIVERED asserts durable
    /// state on the target shard; a target restored before the effect landed
    /// contradicts it. Same severity as its child-workflow analogue.
    #[test]
    fn a_lost_external_effect_is_incoherent_like_a_rolled_back_child_terminal() {
        assert_eq!(
            FindingClass::ExternalEffectRolledBack.severity(),
            FindingClass::ChildTerminalRolledBack.severity()
        );
        assert_eq!(
            classify_status(&[finding(FindingClass::ExternalEffectRolledBack)], false),
            VerifyStatus::Incoherent
        );
    }

    #[test]
    fn severity_truth_table_is_pinned_for_every_class() {
        // EXHAUSTIVE by construction: the expected table is matched against
        // `FindingClass::ALL`, so demoting any single class (the mutation that
        // previously survived every test in this module) fails here, and
        // adding a class without deciding its severity fails here too.
        //
        // Severity is the whole product: it decides the exit code, and
        // therefore whether an operator starts workers on a broken fleet.
        use FindingSeverity::{Advisory, Incoherent, Reclaimable, Undetermined};
        let expected: &[(FindingClass, FindingSeverity)] = &[
            // Heals itself once workers start.
            (FindingClass::DeadWorkerRunningTask, Reclaimable),
            (FindingClass::TimedOutTask, Reclaimable),
            (FindingClass::WorkflowDeadlineExpired, Reclaimable),
            (FindingClass::ExpiredScheduleClaim, Reclaimable),
            (FindingClass::ExpiredSessionLease, Reclaimable),
            (FindingClass::ExpiredMutexLease, Reclaimable),
            (FindingClass::InflightCompletionDelivery, Reclaimable),
            (FindingClass::PendingExternalRequest, Reclaimable),
            // A broken invariant: no scanner repairs these.
            (FindingClass::DanglingTaskExecution, Incoherent),
            (FindingClass::DanglingEventExecution, Incoherent),
            (FindingClass::ExternalTargetMissing, Incoherent),
            (FindingClass::ChildExecutionMissing, Incoherent),
            (FindingClass::WedgedScheduleClaim, Incoherent),
            (FindingClass::ChildTerminalRolledBack, Incoherent),
            (FindingClass::ExternalEffectRolledBack, Incoherent),
            (FindingClass::ReplayDivergence, Incoherent),
            (FindingClass::ReplayWorkflowFailed, Incoherent),
            (FindingClass::ExternalEffectUnverifiable, Advisory),
            // Worth an operator's eye; does not fail the gate.
            (FindingClass::RestorePointSkew, Advisory),
            (FindingClass::ReplaySkippedNoHandler, Advisory),
            (FindingClass::HistoryUnreadable, Advisory),
            (FindingClass::UninspectedShardReference, Advisory),
            (FindingClass::WorkflowIdTargetUnchecked, Advisory),
            // Looked at nothing -- never a pass.
            (FindingClass::ProbeFailed, Undetermined),
        ];
        assert_eq!(
            expected.len(),
            FindingClass::ALL.len(),
            "every class must have a pinned severity"
        );
        for (class, want) in expected {
            assert_eq!(
                class.severity(),
                *want,
                "{class} severity changed -- this changes the exit code"
            );
        }
        for class in FindingClass::ALL {
            assert!(
                expected.iter().any(|(c, _)| *c == class),
                "{class} is not pinned in the severity truth table"
            );
        }
    }

    #[test]
    fn every_class_has_a_stable_name_and_explanation() {
        let mut names: Vec<&str> = FindingClass::ALL.iter().map(|c| c.as_str()).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "class names must be unique");
        for class in FindingClass::ALL {
            assert!(
                !class.explanation().is_empty(),
                "{class} needs an explanation"
            );
        }
    }

    #[test]
    fn every_reclaimable_class_names_the_mechanism_that_heals_it() {
        // A "reclaimable" verdict is an assurance to the operator; the report
        // must always say WHAT will heal it, never just "don't worry".
        for class in FindingClass::ALL {
            if class.severity() == FindingSeverity::Reclaimable {
                let text = class.explanation();
                assert!(
                    text.contains("reclaim")
                        || text.contains("re-attempt")
                        || text.contains("re-claim")
                        || text.contains("sealed")
                        || text.contains("BROKEN")
                        || text.contains("granted"),
                    "{class}: reclaimable explanation must name the healing mechanism, got {text:?}"
                );
            }
        }
    }

    #[test]
    fn class_severity_is_a_total_function_of_the_class() {
        // Calling severity() twice on the same class must agree; this is what
        // lets Finding carry a denormalised severity safely.
        for class in FindingClass::ALL {
            assert_eq!(class.severity(), class.severity());
            assert_eq!(
                Finding::new(class, None, 0, vec![]).severity,
                class.severity()
            );
        }
    }

    #[test]
    fn an_unrunnable_probe_is_undetermined_never_a_pass() {
        // The regression this guards: a "restore" that produced an unmigrated
        // (empty) database makes every probe error on a missing table. Folding
        // that into an advisory made the report read `resumable_with_reclaim`
        // and exit 0 -- telling an operator to start workers against a database
        // with no harvest schema at all.
        assert_eq!(
            FindingClass::ProbeFailed.severity(),
            FindingSeverity::Undetermined
        );
        let f = finding(FindingClass::ProbeFailed);
        assert_eq!(classify_status([&f], false), VerifyStatus::Unavailable);
    }

    #[test]
    fn undetermined_outranks_incoherent_and_reclaimable() {
        let probe = finding(FindingClass::ProbeFailed);
        let broken = finding(FindingClass::DanglingTaskExecution);
        let reclaim = finding(FindingClass::DeadWorkerRunningTask);
        assert_eq!(
            classify_status([&reclaim, &broken, &probe], false),
            VerifyStatus::Unavailable,
            "a run that could not look must never claim to have found the whole picture"
        );
    }

    #[test]
    fn advisory_classes_never_escalate_the_verdict() {
        // The mirror of the above: a genuinely-advisory class (a single
        // unreadable history among many replayed) must NOT be undetermined, or
        // every partially-readable restore would fail the drill.
        for class in [
            FindingClass::HistoryUnreadable,
            FindingClass::ReplaySkippedNoHandler,
            FindingClass::RestorePointSkew,
            FindingClass::UninspectedShardReference,
        ] {
            let f = finding(class);
            assert_eq!(
                classify_status([&f], false),
                VerifyStatus::ResumableWithReclaim,
                "{class} must stay advisory"
            );
        }
    }

    #[test]
    fn classify_status_prefers_unavailable_over_everything() {
        let f = finding(FindingClass::DanglingTaskExecution);
        assert_eq!(classify_status([&f], true), VerifyStatus::Unavailable);
        assert_eq!(classify_status([], true), VerifyStatus::Unavailable);
    }

    #[test]
    fn classify_status_incoherent_beats_reclaimable() {
        let ok = finding(FindingClass::DeadWorkerRunningTask);
        let bad = finding(FindingClass::ChildTerminalRolledBack);
        assert_eq!(
            classify_status([&ok, &bad], false),
            VerifyStatus::Incoherent
        );
    }

    #[test]
    fn a_normal_restore_is_resumable_not_a_failure() {
        // The load-bearing anti-cry-wolf property: a restore that produced ONLY
        // the expected in-flight artifacts must exit 0.
        let expected: Vec<Finding> = [
            FindingClass::DeadWorkerRunningTask,
            FindingClass::ExpiredScheduleClaim,
            FindingClass::ExpiredSessionLease,
            FindingClass::ExpiredMutexLease,
            FindingClass::InflightCompletionDelivery,
            FindingClass::PendingExternalRequest,
        ]
        .into_iter()
        .map(finding)
        .collect();
        assert_eq!(
            classify_status(expected.iter(), false),
            VerifyStatus::ResumableWithReclaim
        );
        let report = RestoreVerifyReport::assemble(Utc::now(), vec![], expected);
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn classify_status_clean_when_nothing_found() {
        assert_eq!(classify_status([], false), VerifyStatus::Clean);
    }

    #[test]
    fn exit_codes_distinguish_incoherent_from_undetermined() {
        let shard = ShardVerifyReport::unreachable(1, "postgres://h/db", "connection refused");
        let unavailable = RestoreVerifyReport::assemble(Utc::now(), vec![shard], vec![]);
        assert_eq!(unavailable.status, VerifyStatus::Unavailable);
        assert_eq!(unavailable.exit_code(), 2);

        let incoherent = RestoreVerifyReport::assemble(
            Utc::now(),
            vec![],
            vec![finding(FindingClass::ChildExecutionMissing)],
        );
        assert_eq!(incoherent.exit_code(), 1);
    }

    #[test]
    fn assemble_tallies_severities_and_merges_replay() {
        let shard = ShardVerifyReport {
            shard_id: 0,
            dsn: "postgres://h/db".into(),
            reachable: true,
            unreachable_reason: None,
            latest_event_at: None,
            non_terminal_executions: Some(3),
            replay: ReplaySummary {
                sampled: 3,
                clean: 2,
                divergent: 1,
                ..ReplaySummary::default()
            },
            findings: vec![finding(FindingClass::DeadWorkerRunningTask)],
        };
        let report = RestoreVerifyReport::assemble(
            Utc::now(),
            vec![shard],
            vec![finding(FindingClass::ReplayDivergence)],
        );
        assert_eq!(report.replay.clean, 2);
        assert_eq!(report.replay.divergent, 1);
        assert_eq!(report.totals_by_severity.get("reclaimable"), Some(&1));
        assert_eq!(report.totals_by_severity.get("incoherent"), Some(&1));
        assert!(report.detected(FindingClass::ReplayDivergence));
        assert!(!report.detected(FindingClass::ExpiredMutexLease));
    }

    #[test]
    fn replay_summary_is_not_verified_when_everything_was_skipped() {
        let skipped = ReplaySummary {
            sampled: 10,
            skipped_no_handler: 10,
            ..ReplaySummary::default()
        };
        assert!(
            !skipped.verified(),
            "a run that replayed nothing must not claim replay was verified"
        );
        let real = ReplaySummary {
            sampled: 1,
            clean: 1,
            ..ReplaySummary::default()
        };
        assert!(real.verified());
    }

    #[test]
    fn replay_summary_merge_is_additive() {
        let mut a = ReplaySummary {
            sampled: 2,
            clean: 1,
            divergent: 1,
            ..ReplaySummary::default()
        };
        a.merge(ReplaySummary {
            sampled: 3,
            clean: 1,
            skipped_no_handler: 2,
            ..ReplaySummary::default()
        });
        assert_eq!(a.sampled, 5);
        assert_eq!(a.clean, 2);
        assert_eq!(a.divergent, 1);
        assert_eq!(a.skipped_no_handler, 2);
    }

    #[test]
    fn compute_skew_needs_two_timestamps() {
        let t0 = Utc::now();
        assert_eq!(compute_skew([]), None);
        assert_eq!(compute_skew([Some(t0)]), None);
        assert_eq!(compute_skew([Some(t0), None]), None);
        assert_eq!(
            compute_skew([Some(t0), Some(t0 + chrono::Duration::seconds(90))]),
            Some(90)
        );
        // Order-independent, and uses the widest pair, not adjacent pairs.
        assert_eq!(
            compute_skew([
                Some(t0 + chrono::Duration::seconds(120)),
                Some(t0),
                Some(t0 + chrono::Duration::seconds(30)),
            ]),
            Some(120)
        );
    }

    #[test]
    fn finding_samples_are_bounded() {
        let many: Vec<String> = (0..100).map(|i| i.to_string()).collect();
        let f = Finding::new(FindingClass::TimedOutTask, Some(0), 100, many);
        assert_eq!(f.samples.len(), MAX_FINDING_SAMPLES);
        assert_eq!(f.count, 100, "count must report the true population");
    }

    #[test]
    // `tokio-postgres` (the guard's parser) is a `db`-feature dependency, so
    // the guard itself only exists in a `db` build.
    #[cfg(feature = "db")]
    fn dsn_guard_matches_on_host_port_database_only() {
        // Same database reached with a different user/password is STILL the
        // same database — that is the mistake the guard exists to catch.
        assert!(dsn_targets_same_database(
            "postgres://readonly:pw@db.prod:5432/harvest",
            "postgres://app:other@db.prod:5432/harvest"
        ));
        // Default port is normalised.
        assert!(dsn_targets_same_database(
            "postgres://db.prod/harvest",
            "postgres://db.prod:5432/harvest"
        ));
        // Host case is normalised.
        assert!(dsn_targets_same_database(
            "postgres://DB.PROD:5432/harvest",
            "postgres://db.prod:5432/harvest"
        ));
        // Query parameters are ignored.
        assert!(dsn_targets_same_database(
            "postgres://db.prod/harvest?sslmode=require",
            "postgres://db.prod/harvest"
        ));
    }

    #[test]
    // `tokio-postgres` (the guard's parser) is a `db`-feature dependency, so
    // the guard itself only exists in a `db` build.
    #[cfg(feature = "db")]
    fn dsn_guard_distinguishes_a_genuine_scratch_target() {
        assert!(!dsn_targets_same_database(
            "postgres://db.prod:5432/harvest_scratch",
            "postgres://db.prod:5432/harvest"
        ));
        assert!(!dsn_targets_same_database(
            "postgres://scratch.host:5432/harvest",
            "postgres://db.prod:5432/harvest"
        ));
        assert!(!dsn_targets_same_database(
            "postgres://db.prod:5433/harvest",
            "postgres://db.prod:5432/harvest"
        ));
    }

    #[test]
    // `tokio-postgres` (the guard's parser) is a `db`-feature dependency, so
    // the guard itself only exists in a `db` build.
    #[cfg(feature = "db")]
    fn dsn_guard_fails_closed_on_an_unreadable_dsn() {
        // A guard that cannot parse the DSN must refuse, not wave through.
        assert!(dsn_targets_same_database(
            "not a dsn",
            "postgres://db/harvest"
        ));
        assert!(dsn_targets_same_database("postgres://db/harvest", ""));
        assert!(dsn_targets_same_database(
            "mysql://db/harvest",
            "postgres://db/harvest"
        ));
    }

    /// Codex round 3, P2. libpq (and therefore the server, since
    /// tokio-postgres simply omits `database` from the startup packet when
    /// `dbname` is unset) defaults the database name to the CONNECTION USER.
    /// Storing `""` for an omitted `dbname` made `postgres://harvest@prod-host`
    /// compare unequal to `postgres://ops@prod-host/harvest` even though both
    /// land on database `harvest` -- so the guard waved a production target
    /// through without the acknowledgement.
    // `tokio-postgres` (the guard's parser) is a `db`-feature dependency, so
    // the guard itself only exists in a `db` build.
    #[test]
    #[cfg(feature = "db")]
    fn dsn_guard_defaults_an_omitted_database_to_the_connection_user() {
        // The mistake this exists to catch: user `harvest` with no dbname IS
        // database `harvest`.
        assert!(dsn_targets_same_database(
            "postgres://harvest@prod-host",
            "postgres://ops@prod-host/harvest"
        ));
        // Symmetric -- the omission may be on either side.
        assert!(dsn_targets_same_database(
            "postgres://ops@prod-host/harvest",
            "postgres://harvest@prod-host"
        ));
        // Both omitted, same user: same database.
        assert!(dsn_targets_same_database(
            "postgres://harvest@prod-host",
            "postgres://harvest@prod-host"
        ));
        // Still discriminating: a different user with no dbname is a
        // different database, so a genuine scratch target is not blocked.
        assert!(!dsn_targets_same_database(
            "postgres://scratch@prod-host",
            "postgres://ops@prod-host/harvest"
        ));
    }

    /// With neither `dbname` nor `user` present the database name is the OS
    /// username of whoever connects -- not knowable here, and different on the
    /// operator's machine than in the deployed config. A guard that cannot
    /// determine the identity must refuse, not guess.
    // `tokio-postgres` (the guard's parser) is a `db`-feature dependency, so
    // the guard itself only exists in a `db` build.
    #[test]
    #[cfg(feature = "db")]
    fn dsn_guard_fails_closed_when_the_database_name_is_unknowable() {
        assert!(dsn_targets_same_database(
            "postgres://prod-host",
            "postgres://ops@prod-host/scratch"
        ));
        assert!(dsn_targets_same_database(
            "postgres://ops@prod-host/scratch",
            "postgres://prod-host"
        ));
    }

    #[test]
    fn redact_dsn_removes_the_password() {
        let redacted = redact_dsn("postgres://app:hunter2@db.prod:5432/harvest");
        assert!(!redacted.contains("hunter2"), "got {redacted}");
        assert!(redacted.contains("db.prod"));
        assert!(redacted.contains("harvest"));
        assert_eq!(redact_dsn("::: not a dsn :::"), "<unparseable dsn>");
    }

    #[test]
    fn redact_dsn_withholds_any_password_bearing_query_key() {
        // `sslpassword` is a libpq option like `password`, and matching the
        // exact word let it through whole. The keyword-form scanner already
        // used a `contains` rule; the URL form must not be laxer, or the same
        // secret is redacted or not depending on how the DSN was spelled.
        for dsn in [
            "postgres://db.prod/harvest?password=hunter2",
            "postgres://db.prod/harvest?sslpassword=hunter2",
            "postgres://db.prod/harvest?sslmode=require&sslpassword=hunter2",
            "postgres://db.prod/harvest?SSLPassword=hunter2",
        ] {
            let redacted = redact_dsn(dsn);
            assert!(
                !redacted.contains("hunter2"),
                "credential leaked from {dsn}: {redacted}"
            );
            assert_eq!(redacted, "<redacted dsn>", "from {dsn}");
        }

        // A DSN with no secret in the query keeps its identity: an operator
        // needs to know which database a failure was about.
        let plain = redact_dsn("postgres://db.prod/harvest?sslmode=require");
        assert!(plain.contains("db.prod"), "got {plain}");
    }

    #[test]
    fn read_only_session_sql_is_a_session_setting_not_a_write() {
        let sql = READ_ONLY_SESSION_SQL.to_ascii_uppercase();
        assert!(sql.starts_with("SET SESSION"));
        for forbidden in ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER"] {
            assert!(!sql.contains(forbidden), "must not contain {forbidden}");
        }
        assert!(sql.contains("READ ONLY"));
    }
}
