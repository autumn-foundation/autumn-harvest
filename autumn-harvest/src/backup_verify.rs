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
pub const DEFAULT_PROBE_LIMIT: i64 = 1_000;

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
    /// A completion delivery frozen `INFLIGHT` with an elapsed lease.
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
    /// A parent recorded a child's terminal, but the child's own shard shows
    /// that child still non-terminal — the signature of a skewed multi-shard
    /// restore.
    ChildTerminalRolledBack,
    /// A sampled history no longer replays cleanly against the deployed
    /// workflow code.
    ReplayDivergence,

    // ── Advisory: operator judgement ────────────────────────────────────────
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
    pub const ALL: [Self; 19] = [
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
        Self::ChildTerminalRolledBack,
        Self::ReplayDivergence,
        Self::RestorePointSkew,
        Self::ReplaySkippedNoHandler,
        Self::HistoryUnreadable,
        Self::UninspectedShardReference,
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
            | Self::ChildTerminalRolledBack
            | Self::ReplayDivergence => FindingSeverity::Incoherent,

            Self::RestorePointSkew
            | Self::ReplaySkippedNoHandler
            | Self::HistoryUnreadable
            | Self::UninspectedShardReference => FindingSeverity::Advisory,

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
            Self::ExpiredSessionLease => "expired_session_lease",
            Self::ExpiredMutexLease => "expired_mutex_lease",
            Self::InflightCompletionDelivery => "inflight_completion_delivery",
            Self::PendingExternalRequest => "pending_external_request",
            Self::DanglingTaskExecution => "dangling_task_execution",
            Self::DanglingEventExecution => "dangling_event_execution",
            Self::ExternalTargetMissing => "external_target_missing",
            Self::ChildExecutionMissing => "child_execution_missing",
            Self::ChildTerminalRolledBack => "child_terminal_rolled_back",
            Self::ReplayDivergence => "replay_divergence",
            Self::RestorePointSkew => "restore_point_skew",
            Self::ReplaySkippedNoHandler => "replay_skipped_no_handler",
            Self::HistoryUnreadable => "history_unreadable",
            Self::UninspectedShardReference => "uninspected_shard_reference",
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
            Self::ExpiredSessionLease => {
                "marked BROKEN by the session reclaim sweep; member activities fail \
                 non-retryably (issue #606)"
            }
            Self::ExpiredMutexLease => "reclaimed and granted to the next FIFO waiter (issue #691)",
            Self::InflightCompletionDelivery => {
                "re-attempted after the lease lapses; the receiver MUST dedupe on \
                 delivery_id (issue #605)"
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
            Self::ReplayDivergence => {
                "this history no longer replays against the deployed workflow code"
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
    /// True when `count` was clipped by the probe's row limit, so the real
    /// population may be larger.
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

    /// Marks the finding's `count` as clipped by a probe row limit.
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
        self.clean > 0 || self.divergent > 0
    }

    /// Folds another summary into this one.
    pub const fn merge(&mut self, other: Self) {
        self.sampled += other.sampled;
        self.clean += other.clean;
        self.divergent += other.divergent;
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
    pub non_terminal_executions: u64,
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
            non_terminal_executions: 0,
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
/// Compares host, port and database name only — user, password and query
/// parameters are deliberately ignored, because pointing a "read-only replica
/// user" at the production database is exactly the mistake this guard exists
/// to catch. Ports are normalised so `:5432` and an omitted port compare equal.
///
/// An unparseable DSN on either side compares **equal**: a guard that cannot
/// read the DSN must fail closed, never wave the drill through.
#[must_use]
pub fn dsn_targets_same_database(candidate: &str, live: &str) -> bool {
    let (Some(a), Some(b)) = (parse_dsn_identity(candidate), parse_dsn_identity(live)) else {
        return true;
    };
    a == b
}

/// `(host, port, database)` identity of a Postgres DSN, or `None` when it
/// cannot be parsed.
fn parse_dsn_identity(dsn: &str) -> Option<(String, u16, String)> {
    let url = url::Url::parse(dsn.trim()).ok()?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return None;
    }
    let host = url.host_str()?.to_ascii_lowercase();
    let port = url.port().unwrap_or(5432);
    let database = url.path().trim_start_matches('/').to_string();
    Some((host, port, database))
}

/// Returns the DSN with its password replaced by `***`, for safe logging.
///
/// A DSN that cannot be parsed is redacted wholesale rather than echoed, so a
/// malformed-but-secret-bearing string can never reach a report or a log line.
#[must_use]
pub fn redact_dsn(dsn: &str) -> String {
    match url::Url::parse(dsn.trim()) {
        Ok(mut url) if url.password().is_some() => {
            let _ = url.set_password(Some("***"));
            url.to_string()
        }
        Ok(url) => url.to_string(),
        Err(_) => "<unparseable dsn>".to_string(),
    }
}

// ── Verification driver (db-gated) ───────────────────────────────────────────

/// One shard to inspect: its logical id and the DSN of the **restored scratch**
/// database that holds it.
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardTarget {
    /// The logical shard id, matching what `ExecutionId::shard()` decodes.
    pub shard_id: i32,
    /// Connection string for the restored scratch database.
    pub dsn: String,
}

#[cfg(feature = "db")]
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
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOptions {
    /// How many non-terminal histories to sample and replay per shard.
    pub replay_sample: usize,
    /// Cap on rows returned per probe. Exact counts are always reported; only
    /// the sample identifiers are bounded.
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

#[cfg(feature = "db")]
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

#[cfg(feature = "db")]
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
}

#[cfg(feature = "db")]
mod probes {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
    use uuid::Uuid;

    use super::{
        Finding, FindingClass, MAX_FINDING_SAMPLES, READ_ONLY_SESSION_SQL, ReplaySummary,
        RestoreVerifyReport, ShardTarget, ShardVerifyReport, VerifyOptions, compute_skew,
        redact_dsn,
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
    }

    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }

    /// Wrap a caller-supplied selection predicate in a bounded, counted probe.
    ///
    /// The inner query is *only ever* one of the engine's own read-only
    /// selection predicates (or a hand-written `SELECT` in this module); it is
    /// never caller input, so the string interpolation carries no injection
    /// surface.
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

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum RefKind {
        /// Parent recorded `ChildWorkflowStarted` with no terminal yet.
        AwaitedChild,
        /// Parent recorded the child's terminal.
        ChildTerminalRecorded,
        /// An external signal/cancel/await request with no terminal.
        ExternalTarget,
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
        let limit = options.probe_limit;
        let stale = options.worker_stale_secs;

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
                FindingClass::ExpiredSessionLease,
                bounded(
                    crate::sessions::broken_session_candidates_query(),
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
        let dangling: Vec<(FindingClass, String)> = vec![
            (
                FindingClass::DanglingTaskExecution,
                bounded(
                    "SELECT t.id FROM harvest_task_queue t \
                     WHERE t.workflow_exec_id IS NOT NULL \
                       AND NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e \
                                       WHERE e.id = t.workflow_exec_id)",
                    "sub.id::text",
                    limit,
                ),
            ),
            (
                FindingClass::DanglingEventExecution,
                bounded(
                    "SELECT DISTINCT ev.workflow_exec_id AS id FROM harvest_events ev \
                     WHERE NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e \
                                       WHERE e.id = ev.workflow_exec_id)",
                    "sub.id::text",
                    limit,
                ),
            ),
        ];
        for (class, sql) in dangling {
            match probe(&mut conn, class, shard_id, sql, None).await {
                Ok(Some(f)) => findings.push(f),
                Ok(None) => {}
                Err(e) => soft_errors.push(e),
            }
        }

        // ── Non-terminal population + newest event timestamp ────────────────
        let non_terminal: i64 = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE state IN ('RUNNING', 'PAUSED', 'SUSPENDED')",
        )
        .get_result::<CountRow>(&mut conn)
        .await
        .map(|r| r.n)
        .unwrap_or(0);

        let latest_event_at =
            diesel::sql_query("SELECT MAX(timestamp) AS latest FROM harvest_events")
                .get_result::<LatestEventRow>(&mut conn)
                .await
                .ok()
                .and_then(|r| r.latest);

        // ── Cross-shard references asserted by this shard's history ─────────
        let mut refs = Vec::new();
        match collect_refs(&mut conn, shard_id, limit).await {
            Ok(r) => refs = r,
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
                non_terminal_executions: u64::try_from(non_terminal).unwrap_or(0),
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
    ) -> Result<Vec<PendingRef>, String> {
        let scan = scan_reference_events(conn, limit).await?;
        Ok(build_refs(scan, shard_id))
    }

    /// The per-owning-execution reference bookkeeping one shard's history
    /// asserts, before any cross-shard resolution.
    #[derive(Default)]
    struct RefScan {
        /// owner -> children seen `ChildWorkflowStarted`.
        awaited: BTreeMap<Uuid, Vec<Uuid>>,
        /// owner -> children whose terminal the owner recorded.
        child_terminal: BTreeMap<Uuid, Vec<Uuid>>,
        /// owner -> (correlation id, target execution id) for external requests.
        requested: BTreeMap<Uuid, Vec<(Uuid, Uuid)>>,
        /// owner -> correlation ids whose terminal the owner recorded.
        resolved: BTreeMap<Uuid, Vec<Uuid>>,
    }

    /// Read this shard's child/external history events and fold them into a
    /// [`RefScan`].
    async fn scan_reference_events(
        conn: &mut AsyncPgConnection,
        limit: i64,
    ) -> Result<RefScan, String> {
        // Only non-terminal owning executions matter — a terminal parent's
        // recorded child terminal is history, not a live dependency, and
        // retention may legitimately have collected the child.
        let sql = format!(
            "SELECT ev.workflow_exec_id, ev.event_data \
             FROM harvest_events ev \
             JOIN harvest_workflow_executions e ON e.id = ev.workflow_exec_id \
             WHERE e.state IN ('RUNNING', 'PAUSED', 'SUSPENDED') \
               AND ev.event_type IN ( \
                 'ChildWorkflowStarted', 'ChildWorkflowCompleted', 'ChildWorkflowFailed', \
                 'ExternalSignalRequested', 'ExternalSignalDelivered', 'ExternalSignalFailed', \
                 'ExternalCancelRequested', 'ExternalCancelDelivered', 'ExternalCancelFailed', \
                 'ExternalAwaitRequested', 'ExternalAwaitResolved', 'ExternalAwaitFailed') \
             ORDER BY ev.workflow_exec_id, ev.event_id \
             LIMIT {limit}"
        );
        let rows: Vec<EventRow> = diesel::sql_query(sql)
            .load(conn)
            .await
            .map_err(|e| format!("cross-shard reference scan failed: {e}"))?;

        let mut scan = RefScan::default();
        // `ExternalTarget::WorkflowId` requests resolve by business key at
        // delivery time, not by a fixed execution id, so there is no row this
        // check could look up -- they are deliberately not tracked here.
        let mut workflow_id_targets: u64 = 0;

        for row in rows {
            let Ok(event) = serde_json::from_value::<WorkflowEvent>(row.event_data) else {
                // A payload this build cannot decode is not a coherence break;
                // it is surfaced separately by the replay sample.
                continue;
            };
            let owner = row.workflow_exec_id;
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
                WorkflowEvent::ExternalSignalRequested {
                    signal_id, target, ..
                } => match target {
                    ExternalTarget::ExecutionId(t) => scan
                        .requested
                        .entry(owner)
                        .or_default()
                        .push((signal_id.as_uuid(), t.as_uuid())),
                    ExternalTarget::WorkflowId { .. } => workflow_id_targets += 1,
                },
                WorkflowEvent::ExternalCancelRequested { cancel_id, target } => match target {
                    ExternalTarget::ExecutionId(t) => scan
                        .requested
                        .entry(owner)
                        .or_default()
                        .push((cancel_id.as_uuid(), t.as_uuid())),
                    ExternalTarget::WorkflowId { .. } => workflow_id_targets += 1,
                },
                WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                    scan.requested
                        .entry(owner)
                        .or_default()
                        .push((await_id.as_uuid(), target.as_uuid()));
                }
                WorkflowEvent::ExternalSignalDelivered { signal_id }
                | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                    scan.resolved
                        .entry(owner)
                        .or_default()
                        .push(signal_id.as_uuid());
                }
                WorkflowEvent::ExternalCancelDelivered { cancel_id }
                | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                    scan.resolved
                        .entry(owner)
                        .or_default()
                        .push(cancel_id.as_uuid());
                }
                WorkflowEvent::ExternalAwaitResolved { await_id, .. }
                | WorkflowEvent::ExternalAwaitFailed { await_id, .. } => {
                    scan.resolved
                        .entry(owner)
                        .or_default()
                        .push(await_id.as_uuid());
                }
                _ => {}
            }
        }
        let _ = workflow_id_targets;
        Ok(scan)
    }

    /// Turn a shard-local [`RefScan`] into the cross-shard references that still
    /// need adjudication (a started child with no recorded terminal, a recorded
    /// child terminal, or an unresolved external request).
    fn build_refs(scan: RefScan, shard_id: i32) -> Vec<PendingRef> {
        let RefScan {
            awaited,
            child_terminal,
            requested,
            resolved,
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
            let done = resolved.get(owner).cloned().unwrap_or_default();
            for (corr, target) in reqs {
                if done.contains(corr) {
                    continue;
                }
                out.push(PendingRef {
                    kind: RefKind::ExternalTarget,
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
            // `WorkflowFailed` is counted CLEAN: a recorded terminal failure is
            // the run's own outcome, faithfully reproduced -- not a replay defect.
            let outcome = replayer
                .replay_canary_from_db(conn, exec_id)
                .await
                .map(|report| {
                    matches!(
                        report.status,
                        crate::testing::ReplayStatus::NonDeterminismDetected { .. }
                    )
                });
            match outcome {
                Ok(false) => summary.clean += 1,
                Ok(true) => {
                    summary.divergent += 1;
                    if divergent_samples.len() < MAX_FINDING_SAMPLES {
                        divergent_samples.push(exec_id.to_string());
                    }
                }
                Err(_) => {
                    summary.unreadable += 1;
                    if unreadable_samples.len() < MAX_FINDING_SAMPLES {
                        unreadable_samples.push(exec_id.to_string());
                    }
                }
            }
        }

        let mut findings = Vec::new();
        if summary.divergent > 0 {
            findings.push(Finding::new(
                FindingClass::ReplayDivergence,
                Some(shard_id),
                summary.divergent,
                divergent_samples,
            ));
        }
        if summary.skipped_no_handler > 0 {
            findings.push(Finding::new(
                FindingClass::ReplaySkippedNoHandler,
                Some(shard_id),
                summary.skipped_no_handler,
                skipped_samples,
            ));
        }
        if summary.unreadable > 0 {
            findings.push(Finding::new(
                FindingClass::HistoryUnreadable,
                Some(shard_id),
                summary.unreadable,
                unreadable_samples,
            ));
        }
        Ok((summary, findings))
    }

    /// Resolve every cross-shard reference against the shard that owns it.
    ///
    /// A reference pointing at a shard whose DSN was not supplied cannot be
    /// adjudicated either way, so it is reported as an advisory rather than
    /// silently passing.
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
            let Ok(mut conn) = connect_read_only(&target.dsn).await else {
                // Unreachability is already reported by `verify_shard`.
                continue;
            };

            let mut missing_child = Vec::new();
            let mut rolled_back = Vec::new();
            let mut missing_external = Vec::new();
            let mut pending_external = Vec::new();

            for r in owned {
                let state: Option<String> = diesel::sql_query(
                    "SELECT state FROM harvest_workflow_executions WHERE id = $1",
                )
                .bind::<diesel::sql_types::Uuid, _>(r.target)
                .get_result::<StateRow>(&mut conn)
                .await
                .ok()
                .map(|row| row.state);

                match (r.kind, state) {
                    (RefKind::AwaitedChild, None) => {
                        missing_child.push(format!(
                            "{} (awaited by {} on shard {})",
                            r.target, r.source_exec, r.source_shard
                        ));
                    }
                    (RefKind::ChildTerminalRecorded, Some(s))
                        if !crate::erase::is_terminal_state(&s) =>
                    {
                        rolled_back.push(format!(
                            "{} is {s} on shard {} but {} on shard {} recorded its terminal",
                            r.target, r.owner_shard, r.source_exec, r.source_shard
                        ));
                    }
                    // A recorded child terminal whose execution row is gone is
                    // ordinary retention, not incoherence.
                    (RefKind::ChildTerminalRecorded, _) | (RefKind::AwaitedChild, Some(_)) => {}
                    (RefKind::ExternalTarget, None) => {
                        missing_external.push(format!(
                            "{} (requested by {} on shard {})",
                            r.target, r.source_exec, r.source_shard
                        ));
                    }
                    (RefKind::ExternalTarget, Some(_)) => {
                        pending_external.push(format!(
                            "{} (requested by {} on shard {})",
                            r.target, r.source_exec, r.source_shard
                        ));
                    }
                }
            }

            for (class, samples) in [
                (FindingClass::ChildExecutionMissing, missing_child),
                (FindingClass::ChildTerminalRolledBack, rolled_back),
                (FindingClass::ExternalTargetMissing, missing_external),
                (FindingClass::PendingExternalRequest, pending_external),
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
#[cfg(feature = "db")]
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

    #[test]
    fn severity_truth_table() {
        assert_eq!(
            FindingClass::DeadWorkerRunningTask.severity(),
            FindingSeverity::Reclaimable
        );
        assert_eq!(
            FindingClass::DanglingTaskExecution.severity(),
            FindingSeverity::Incoherent
        );
        assert_eq!(
            FindingClass::RestorePointSkew.severity(),
            FindingSeverity::Advisory
        );
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
            non_terminal_executions: 3,
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

    #[test]
    fn redact_dsn_removes_the_password() {
        let redacted = redact_dsn("postgres://app:hunter2@db.prod:5432/harvest");
        assert!(!redacted.contains("hunter2"), "got {redacted}");
        assert!(redacted.contains("db.prod"));
        assert!(redacted.contains("harvest"));
        assert_eq!(redact_dsn("::: not a dsn :::"), "<unparseable dsn>");
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
