//! Streaming the management-API audit trail to an external sink (issue #953).
//!
//! # Overview
//!
//! Every mutating management-API operation writes a row to
//! `harvest_audit_log` ([`crate::audit`], issue #158) — but those rows live
//! *per-shard, inside the same Postgres database they describe*, readable
//! only through Harvest's own API. For a security/compliance team that is
//! backwards: SIEM pipelines need privileged-action logs centralized,
//! off-box, and gap-detectable.
//!
//! This module ships that export as a deliberate replay of the durable
//! completion-callback design (issue #605): a boxed async transport trait in
//! core with **no HTTP client dependency** ([`AuditSink`]), a `reqwest`-based
//! signed-webhook implementation in `autumn-harvest-plugin`, a
//! two-transaction claim/deliver scanner that never holds a row lock across
//! network I/O, at-least-once delivery, and an operator redrive path.
//!
//! # Determinism contract
//!
//! **No new [`crate::event::WorkflowEvent`] variant. Zero replay-determinism
//! impact.** Audit rows are operational metadata, never event history;
//! `harvest_events` is never read or written here. The exporter only *reads*
//! `harvest_audit_log` and writes its own bookkeeping
//! (`harvest_audit_log.export_seq`, `harvest_audit_export_cursor`).
//!
//! # Opt-in and zero-cost when unconfigured
//!
//! With no sink registered, [`GLOBAL_AUDIT_EXPORT_CONFIG`] is `None`, the
//! scanner returns `Ok(0)` before issuing a single query, no cursor row is
//! ever created, and `export_seq` stays `NULL` on every audit row. Behavior
//! is byte-identical to before this module existed.
//!
//! # Where the monotonic sequence comes from (and why not `BIGSERIAL`)
//!
//! AC4 requires a *strictly monotonic per-shard sequence* so a receiving SIEM
//! can detect gaps. The obvious implementation — a `BIGSERIAL` column on
//! `harvest_audit_log` — is wrong twice over:
//!
//! 1. **It would lose records.** A serial value is handed out *before* the
//!    transaction commits, so two concurrent audited operations can take
//!    sequence 5 and 6 and commit in the order 6, 5. A cursor of the form
//!    `WHERE seq > last_exported` that ships 6 first would then skip 5
//!    forever, which is precisely the silent loss AC2 forbids "by
//!    construction". `occurred_at` has the same defect (it is transaction
//!    *start* time, so it can even move backwards between concurrent
//!    inserts).
//! 2. **It would break under DR failover.** As
//!    `migrations/20260726000000_harvest_shard_generation/up.sql` records for
//!    issue #954, logical replication does not replicate sequence values — a
//!    promoted standby would re-issue sequence numbers it had already
//!    exported, corrupting the receiver's `(shard, seq)` accounting.
//!
//! Instead the exporter assigns the sequence itself, under the per-shard
//! cursor row lock, to rows it can actually *see* (`export_seq IS NULL`).
//! A late-committing row is still `NULL` when the next tick runs and simply
//! receives a later sequence: skipping is not representable. The counter
//! lives in `harvest_audit_export_cursor` — ordinary replicated table data,
//! not a sequence object. Sequences come out **dense**, so a receiver can
//! check contiguity rather than merely detect gaps.
//!
//! # At-least-once, never at-most-once
//!
//! Unlike a completion callback, an audit record may **never** be dropped
//! after N attempts — the export *is* the compliance artifact. There is
//! therefore no dead-letter arm in [`classify_export_outcome`]: a failing
//! sink backs off (capped exponential) and retries forever, and the cursor
//! never advances past the failure. A batch can consequently be delivered
//! more than once (a process death between the POST and the cursor write
//! re-sends it); receivers deduplicate on `(shard, seq)`, exactly as #605
//! specifies for callbacks.
//!
//! # OTLP-logs mapping
//!
//! [`AuditExportRecord`] is deliberately flat and maps 1:1 onto an
//! OpenTelemetry log record for embedders bridging to a collector:
//!
//! | `AuditExportRecord` field | OTLP log record field |
//! |---|---|
//! | `occurred_at` | `timeObservedUnixNano` / `timeUnixNano` |
//! | `operation` | `body` (or `event.name`) |
//! | `status` | `severityText` (`SUCCEEDED` -> `INFO`, `FAILED` -> `ERROR`) |
//! | `error_summary` | `attributes["exception.message"]` |
//! | `shard`, `seq` | `attributes["harvest.shard.id"]`, `attributes["harvest.audit.seq"]` |
//! | `actor`, `target_type`, `target_id`, `route_or_command`, `request_id`, `idempotency_key`, `source` | `attributes["harvest.audit.<field>"]` |
//! | `id` | `attributes["harvest.audit.id"]` |
//!
//! See `docs/audit-export.md` for the full receiver contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::completion_callback::{CallbackSecret, SIGNATURE_HEADER, TIMESTAMP_HEADER};

// ---------------------------------------------------------------------
// M1: wire shape
// ---------------------------------------------------------------------

/// HTTP header naming the shard a batch came from.
///
/// A receiver that dedupes on `(shard, seq)` can read the pair from headers
/// without parsing the body, mirroring #605's `X-Harvest-Delivery-Id`.
pub const SHARD_HEADER: &str = "X-Harvest-Audit-Shard";
/// HTTP header carrying the first (lowest) `seq` in the batch.
pub const FIRST_SEQ_HEADER: &str = "X-Harvest-Audit-First-Seq";
/// HTTP header carrying the last (highest) `seq` in the batch.
pub const LAST_SEQ_HEADER: &str = "X-Harvest-Audit-Last-Seq";

/// Default number of audit records claimed and delivered per batch.
pub const DEFAULT_EXPORT_BATCH_SIZE: i64 = 500;

/// Hard ceiling on the configurable batch size.
///
/// A batch is buffered in memory and `POSTed` as one body; an unbounded value
/// would let a misconfiguration try to serialize an entire retention window
/// into a single request.
pub const MAX_EXPORT_BATCH_SIZE: i64 = 5_000;

/// One exported audit record.
///
/// **The field set and JSON shape are a public contract** — a receiver's
/// index mappings depend on them. Do not reorder, rename, or drop a field
/// without a compatibility plan. Optional fields serialize as an explicit
/// `null` rather than being omitted, so a SIEM's schema inference sees a
/// stable object shape across every batch.
///
/// Carries no workflow payloads, activity inputs, or signal bodies: the
/// audit trail deliberately is not a second PII store ([`crate::audit`]),
/// and exporting it must not make it one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditExportRecord {
    /// The shard whose database this record was read from — half of the
    /// receiver's dedup key and of the gap-detection tuple (AC4).
    pub shard: i32,
    /// Strictly monotonic, dense, per-shard sequence assigned by the
    /// exporter. The other half of `(shard, seq)`.
    pub seq: i64,
    /// The audit row's own primary key.
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub operation: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub route_or_command: String,
    pub request_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub error_summary: Option<String>,
    pub source: String,
}

/// Serialize a batch as JSON lines — one compact JSON object per line,
/// newline-terminated.
///
/// These are the exact bytes that get signed and delivered; a caller must
/// sign *these* bytes, never a re-serialization, so the receiver verifies
/// precisely what it received. `serde_json` emits struct fields in
/// declaration order with no interior newlines, so the output is
/// deterministic — which is what makes a redrive byte-identical (AC6).
///
/// An empty slice produces zero bytes (callers never deliver an empty
/// batch).
///
/// # Errors
/// Returns `Err` only if a record fails to serialize, which cannot happen
/// for [`AuditExportRecord`]'s field types; kept fallible so a future field
/// change cannot silently panic in the delivery path.
pub fn serialize_batch(records: &[AuditExportRecord]) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::with_capacity(records.len() * 256);
    for record in records {
        serde_json::to_writer(&mut out, record)?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Build the signed header set for one exported batch.
///
/// Uses the same `X-Harvest-Signature` HMAC-SHA256 scheme as issue #605
/// (delegating to [`crate::completion_callback::sign`], so the two can never
/// drift), plus the shard and sequence range a receiver needs for
/// `(shard, seq)` dedup and contiguity checking.
#[must_use]
pub fn export_headers(
    secret: &CallbackSecret,
    body: &[u8],
    shard: i32,
    first_seq: i64,
    last_seq: i64,
    now: DateTime<Utc>,
) -> Vec<(&'static str, String)> {
    vec![
        (
            SIGNATURE_HEADER,
            crate::completion_callback::sign(secret, body),
        ),
        (TIMESTAMP_HEADER, now.to_rfc3339()),
        (SHARD_HEADER, shard.to_string()),
        (FIRST_SEQ_HEADER, first_seq.to_string()),
        (LAST_SEQ_HEADER, last_seq.to_string()),
    ]
}

// ---------------------------------------------------------------------
// M2: the embedder transport seam
// ---------------------------------------------------------------------

/// The result of one sink delivery attempt.
///
/// Either a response status was observed (`status`, whatever it was), or the
/// request never got one (`transport_error` — connect failure, timeout, TLS
/// or DNS error). Mirrors
/// [`crate::completion_callback::DeliveryAttempt`] rather than reusing it so
/// a future change to callback delivery semantics cannot silently alter
/// audit-export semantics, where the cost of a misclassification is a
/// compliance gap rather than a missed notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkAttempt {
    pub status: Option<u16>,
    pub transport_error: Option<String>,
}

impl SinkAttempt {
    /// A response was received with this status (success or not).
    #[must_use]
    pub const fn success(status: u16) -> Self {
        Self {
            status: Some(status),
            transport_error: None,
        }
    }

    /// No response was received at all.
    #[must_use]
    pub const fn transport_error(message: String) -> Self {
        Self {
            status: None,
            transport_error: Some(message),
        }
    }

    /// `true` only for a 2xx response status.
    ///
    /// A 3xx is deliberately *not* a success: the plugin sink never follows
    /// redirects (`redirect::Policy::none()`), so a 3xx means the batch was
    /// not accepted and must be retried, not acknowledged.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self.status, Some(s) if (200..300).contains(&s))
    }
}

/// One batch handed to an [`AuditSink`].
///
/// Carries both the canonical `body` (the signed bytes an HTTP sink POSTs
/// verbatim) and the parsed `records`, so a non-HTTP sink — Kinesis, a file,
/// an OTLP-logs bridge — can map the structured form without re-parsing what
/// core just serialized.
pub struct AuditBatch<'a> {
    /// Shard this batch was read from.
    pub shard: i32,
    /// Lowest `seq` in the batch.
    pub first_seq: i64,
    /// Highest `seq` in the batch; the position the cursor advances to on
    /// acknowledgement.
    pub last_seq: i64,
    /// The records, ascending by `seq`.
    pub records: &'a [AuditExportRecord],
    /// Canonical JSON-lines body — exactly the bytes covered by the
    /// signature in `headers`.
    pub body: &'a [u8],
    /// Signature, timestamp, shard, and sequence-range headers.
    pub headers: &'a [(&'static str, String)],
}

/// Future returned by [`AuditSink::deliver`].
pub type SinkFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = SinkAttempt> + Send + 'a>>;

/// An embedder-supplied (or plugin-default) transport for audit-record
/// export.
///
/// Implementations are a thin transport: hand `batch` to the destination and
/// report what happened. Batching, sequencing, HMAC signing, cursor
/// management, retry/backoff, and lag accounting all happen in core, above
/// this trait — an implementation does not need to reason about any of it,
/// and **must not** acknowledge a batch it did not durably accept: a
/// `success` return advances the cursor past those records.
///
/// Core ships no HTTP client, so there is no default implementation here;
/// `autumn-harvest-plugin` supplies the `reqwest`-based signed-webhook sink,
/// exactly as it does for [`crate::completion_callback::CompletionCallbackDeliverer`]
/// and [`crate::payload_store::PayloadStore`].
pub trait AuditSink: Send + Sync + 'static {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a>;
}

// ---------------------------------------------------------------------
// M3: pure retry/cursor decisions
// ---------------------------------------------------------------------

/// Capped exponential backoff for a failing sink.
///
/// Deliberately *not* [`crate::policy::RetryPolicy`]: that type carries a
/// `max_attempts` ceiling whose whole purpose is to eventually give up and
/// dead-letter, which must never happen to an audit record. Only the
/// backoff math is shared (via [`crate::policy::compute_retry_delay`]), so
/// the two cannot drift.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportBackoff {
    /// Delay after the first failure.
    pub initial_interval: std::time::Duration,
    /// Multiplier applied per consecutive failure.
    pub backoff_coefficient: f64,
    /// Hard ceiling on the delay, so a long sink outage still retries on a
    /// predictable cadence (and recovers promptly when the sink returns).
    pub max_interval: std::time::Duration,
}

impl Default for ExportBackoff {
    /// 1s -> 2s -> 4s ... capped at 60s.
    ///
    /// The cap is what bounds recovery time after an outage: the success
    /// metric asks for zero gaps after a 10-minute sink outage, and a
    /// 60-second ceiling means at most one minute of extra lag once the sink
    /// returns.
    fn default() -> Self {
        Self {
            initial_interval: std::time::Duration::from_secs(1),
            backoff_coefficient: 2.0,
            max_interval: std::time::Duration::from_secs(60),
        }
    }
}

/// What the scanner should write after one delivery attempt.
///
/// Note the absence of a dead-letter arm — see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportOutcome {
    /// 2xx — advance the shard's cursor to `through_seq` and clear the error
    /// state.
    Advance { through_seq: i64, status: u16 },
    /// Anything else — **hold the cursor exactly where it is** and retry at
    /// `next_attempt_at`.
    Backoff {
        next_attempt_at: DateTime<Utc>,
        last_status: Option<u16>,
        last_error: Option<String>,
        consecutive_failures: i32,
    },
}

/// Decide what to write after one delivery attempt.
///
/// Pure, so the "never advance past a failure" invariant (AC2) is pinned by
/// a unit test rather than requiring a database and a broken sink to
/// observe.
///
/// `consecutive_failures` is the count *before* this attempt; the returned
/// [`ExportOutcome::Backoff`] carries the incremented value (saturating).
#[must_use]
pub fn classify_export_outcome(
    attempt: &SinkAttempt,
    through_seq: i64,
    consecutive_failures: i32,
    backoff: &ExportBackoff,
    now: DateTime<Utc>,
) -> ExportOutcome {
    if let Some(status) = attempt.status
        && attempt.is_success()
    {
        return ExportOutcome::Advance {
            through_seq,
            status,
        };
    }

    // `compute_retry_delay` treats `attempt` as 1-based and exponentiates on
    // `attempt - 1`, so the first failure (0 prior failures) must map to 1.
    let failures = consecutive_failures.saturating_add(1);
    let delay = crate::policy::compute_retry_delay(
        backoff.initial_interval,
        backoff.backoff_coefficient,
        backoff.max_interval,
        u32::try_from(failures).unwrap_or(u32::MAX),
    );
    let next_attempt_at = now
        + chrono::Duration::from_std(delay).unwrap_or_else(|_| {
            chrono::Duration::from_std(backoff.max_interval)
                .unwrap_or_else(|_| chrono::Duration::seconds(60))
        });

    ExportOutcome::Backoff {
        next_attempt_at,
        last_status: attempt.status,
        last_error: attempt.transport_error.clone(),
        consecutive_failures: failures,
    }
}

/// Result of resolving an operator's redrive request against the live
/// cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindOutcome {
    /// The cursor moves backwards from `from` to `to`; every record with
    /// `seq > to` re-exports.
    Rewound { from: i64, to: i64 },
    /// Nothing to do — the request did not move the cursor backwards.
    NoOp { cursor: i64, requested: i64 },
    /// No cursor exists for this shard: audit export has never run here, so
    /// there is nothing to rewind. Returned only by the database-backed
    /// [`rewind_cursor`]; [`resolve_rewind`] is pure over an existing cursor.
    NotConfigured,
}

/// Resolve a redrive request. **A cursor may only ever move backwards.**
///
/// Moving it forward would mark records delivered that never were — the
/// exact gap this feature exists to make impossible — so a forward request
/// is refused outright rather than clamped and applied, and the caller
/// reports the refusal to the operator instead of silently doing nothing
/// useful.
///
/// A negative request is clamped to `0` (re-export everything still
/// retained) rather than rejected: `0` is its only sane reading, and writing
/// a negative cursor would violate the table's `CHECK (last_acked_seq >= 0)`.
#[must_use]
pub const fn resolve_rewind(current_acked: i64, requested: i64) -> RewindOutcome {
    let target = if requested < 0 { 0 } else { requested };
    if target >= current_acked {
        return RewindOutcome::NoOp {
            cursor: current_acked,
            requested,
        };
    }
    RewindOutcome::Rewound {
        from: current_acked,
        to: target,
    }
}

// ---------------------------------------------------------------------
// M3b: builder-time configuration
// ---------------------------------------------------------------------

/// Everything an embedder can set through `HarvestBuilder`'s
/// `audit_export_*` methods, resolved into an [`AuditExportRuntimeConfig`] at
/// startup.
///
/// With `sink` and `webhook_url` both `None` — the default — audit export is
/// never installed and the feature is entirely inert (AC8).
#[derive(Clone)]
pub struct AuditExportBuilderConfig {
    /// Allowed sink hosts. Required (non-empty) for a `webhook_url` to
    /// validate, mirroring the completion-callback SSRF posture (#605).
    pub allowlist: crate::completion_callback::HostAllowlist,
    /// Permit `http://` sink URLs. Off by default: audit records name who did
    /// what to which tenant, and shipping them in cleartext is a finding in
    /// its own right.
    pub allow_http: bool,
    /// Permit IP-literal sink hosts.
    pub allow_ip_literals: bool,
    /// Signed-webhook endpoint for the plugin's default sink.
    pub webhook_url: Option<String>,
    /// HMAC key for `X-Harvest-Signature`.
    pub secret: Option<CallbackSecret>,
    /// Embedder-supplied sink. Takes precedence over `webhook_url`.
    pub sink: Option<std::sync::Arc<dyn AuditSink>>,
    /// Records per batch.
    pub batch_size: i64,
    /// Capped exponential backoff after a sink failure.
    pub backoff: ExportBackoff,
}

impl std::fmt::Debug for AuditExportBuilderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditExportBuilderConfig")
            .field("allowlist", &self.allowlist)
            .field("allow_http", &self.allow_http)
            .field("allow_ip_literals", &self.allow_ip_literals)
            .field("webhook_url", &self.webhook_url)
            .field("secret", &self.secret)
            .field("sink", &self.sink.as_ref().map(|_| "<AuditSink>"))
            .field("batch_size", &self.batch_size)
            .field("backoff", &self.backoff)
            .finish()
    }
}

impl Default for AuditExportBuilderConfig {
    fn default() -> Self {
        Self {
            allowlist: crate::completion_callback::HostAllowlist::new(),
            allow_http: false,
            allow_ip_literals: false,
            webhook_url: None,
            secret: None,
            sink: None,
            batch_size: DEFAULT_EXPORT_BATCH_SIZE,
            backoff: ExportBackoff::default(),
        }
    }
}

impl AuditExportBuilderConfig {
    /// `true` when audit export should be installed at all.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.sink.is_some() || self.webhook_url.is_some()
    }

    /// The [`crate::completion_callback::SsrfPolicy`] implied by this config.
    #[must_use]
    pub fn ssrf_policy(&self) -> crate::completion_callback::SsrfPolicy {
        crate::completion_callback::SsrfPolicy {
            allowlist: self.allowlist.clone(),
            allow_http: self.allow_http,
            allow_ip_literals: self.allow_ip_literals,
        }
    }

    /// Validate the configured webhook URL against this config's own SSRF
    /// policy. Called at `HarvestBuilder::try_build()` time so a bad sink URL
    /// fails startup rather than silently never delivering.
    ///
    /// An embedder-supplied [`AuditSink`] is not validated here — it is not a
    /// URL, and where it ships is the embedder's decision.
    ///
    /// # Errors
    /// Returns the `(url, rejection)` pair when the webhook URL fails
    /// validation.
    pub fn validate_webhook_url(
        &self,
    ) -> Result<(), (String, crate::completion_callback::SsrfRejection)> {
        let Some(url) = &self.webhook_url else {
            return Ok(());
        };
        let policy = self.ssrf_policy();
        crate::completion_callback::validate_target_url(url, &policy)
            .map(|_| ())
            .map_err(|rejection| (url.clone(), rejection))
    }

    /// Batch size clamped into the supported range.
    #[must_use]
    pub const fn effective_batch_size(&self) -> i64 {
        if self.batch_size < 1 {
            1
        } else if self.batch_size > MAX_EXPORT_BATCH_SIZE {
            MAX_EXPORT_BATCH_SIZE
        } else {
            self.batch_size
        }
    }
}

// ---------------------------------------------------------------------
// M4: process-global runtime config (opt-in; `None` == fully inert)
// ---------------------------------------------------------------------

/// Default lease held on a shard's cursor while a batch is in flight.
///
/// Long enough to cover a slow sink, short enough that a crashed exporter's
/// shard resumes promptly. A lease expiring early is safe — it costs a
/// duplicate delivery, which the receiver dedupes on `(shard, seq)` — while a
/// lease expiring late costs export lag.
#[cfg(feature = "db")]
pub const DEFAULT_EXPORT_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Everything the exporter needs at runtime, installed once at startup.
///
/// `None` (the default, before any builder wiring runs) means the feature is
/// fully inert: [`fire_due_audit_exports`] returns `Ok(0)` before issuing a
/// single query, so an embedder who never configures an audit sink sees zero
/// behavior change and zero scanner work (AC8).
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct AuditExportRuntimeConfig {
    /// Embedder-supplied (or plugin-default) transport.
    pub sink: std::sync::Arc<dyn AuditSink>,
    /// HMAC key for `X-Harvest-Signature`.
    pub secret: CallbackSecret,
    /// Records claimed and delivered per batch, clamped to
    /// `[1, MAX_EXPORT_BATCH_SIZE]` at read time.
    pub batch_size: i64,
    /// Capped exponential backoff after a sink failure.
    pub backoff: ExportBackoff,
    /// How long a claim holds a shard's cursor.
    pub lease: std::time::Duration,
}

// `Arc`-wrapped for the same reason as `GLOBAL_CALLBACK_CONFIG` (issue #605
// review): every read clones the value out of the lock, and the struct carries
// owned fields that would otherwise be deep-copied on every scanner tick for a
// value that only ever changes at startup.
#[cfg(feature = "db")]
pub static GLOBAL_AUDIT_EXPORT_CONFIG: std::sync::RwLock<
    Option<std::sync::Arc<AuditExportRuntimeConfig>>,
> = std::sync::RwLock::new(None);

/// Read [`GLOBAL_AUDIT_EXPORT_CONFIG`], tolerating a poisoned lock.
///
/// Mirrors `completion_callback::read_global_callback_config`: a poisoned
/// `RwLock` means some other thread panicked while holding the write guard,
/// but the single `*lock = Some(..)` that write path performs is not a
/// multi-step invariant a panic could leave half-applied, so the data behind
/// the guard is still valid. Recovering it avoids the failure mode of a bare
/// `.read().ok()`, where one unrelated panic would silently stop exporting
/// audit records — a compliance gap — for the rest of the process's life.
#[cfg(feature = "db")]
fn read_global_audit_export_config() -> Option<std::sync::Arc<AuditExportRuntimeConfig>> {
    match GLOBAL_AUDIT_EXPORT_CONFIG.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            tracing::error!(
                "GLOBAL_AUDIT_EXPORT_CONFIG lock was poisoned by a panic elsewhere in the \
                 process; recovering the last-written config rather than treating it as \
                 unconfigured (which would silently stop audit export)"
            );
            poisoned.into_inner().clone()
        }
    }
}

/// Install [`GLOBAL_AUDIT_EXPORT_CONFIG`] for an embedder using the core
/// `HarvestBuilder::build()` -> `into_worker_parts()` path directly.
///
/// Mirrors
/// [`crate::completion_callback::install_global_callback_config_for_direct_worker`]
/// and exists for the same reason (issue #921 review): the plugin's runner is
/// the only installer this crate ships, so a direct core embedder would
/// otherwise get an `audit_export_*` builder API that silently did nothing —
/// and a silently-inert audit export is a compliance gap discovered at audit
/// time.
///
/// Only an **embedder-supplied [`AuditSink`]** can be installed here: core
/// ships no HTTP client, so a bare `audit_export_webhook(...)` has no
/// transport on this path. That case is logged rather than ignored, since the
/// embedder plainly intended export to happen.
///
/// **Clears any prior config when this runtime configures none**, for the
/// same reason as the callback installer: the config is a single process-wide
/// static, so a second runtime built without a sink must not keep shipping
/// audit records to the first runtime's destination.
#[cfg(feature = "db")]
pub fn install_global_audit_export_config_for_direct_worker(config: &AuditExportBuilderConfig) {
    let Some(sink) = config.sink.clone() else {
        if config.webhook_url.is_some() {
            tracing::warn!(
                "audit_export_webhook(...) was configured but this runtime was built \
                 through the direct core worker path, which ships no HTTP client -- no \
                 audit records will be exported. Supply audit_export_sink(...) with your \
                 own AuditSink, or run through autumn-harvest-plugin, which provides the \
                 default reqwest signed-webhook sink."
            );
        }
        if let Ok(mut lock) = GLOBAL_AUDIT_EXPORT_CONFIG.write() {
            *lock = None;
        }
        return;
    };
    let secret = config.secret.clone().unwrap_or_else(|| {
        tracing::warn!(
            "audit-export HMAC secret was never configured via \
             HarvestBuilder::audit_export_secret(...) -- every exported batch will be \
             signed with an empty key, which defeats the X-Harvest-Signature \
             tamper-evidence guarantee for any receiver relying on it"
        );
        CallbackSecret::new(Vec::new())
    });
    if let Ok(mut lock) = GLOBAL_AUDIT_EXPORT_CONFIG.write() {
        *lock = Some(std::sync::Arc::new(AuditExportRuntimeConfig {
            sink,
            secret,
            batch_size: config.effective_batch_size(),
            backoff: config.backoff.clone(),
            lease: DEFAULT_EXPORT_LEASE,
        }));
    }
}

/// `true` when an audit sink is configured in this process.
///
/// Used by the retention sweep to decide whether the "never purge an
/// unexported record" guard is live.
#[cfg(feature = "db")]
#[must_use]
pub fn is_configured() -> bool {
    read_global_audit_export_config().is_some()
}

// ---------------------------------------------------------------------
// M5: cursor + claim (transaction 1 — no network I/O, no lock held across it)
// ---------------------------------------------------------------------

/// What a successful claim hands back to the delivery step.
#[cfg(feature = "db")]
#[derive(Debug, Clone)]
pub struct ClaimedBatch {
    /// The shard this batch belongs to.
    pub shard: i32,
    /// The epoch the post-delivery write must be guarded on.
    pub claim_epoch: i64,
    /// Failure count before this attempt, threaded into
    /// [`classify_export_outcome`].
    pub consecutive_failures: i32,
    /// Records to deliver, ascending by `seq`. Never empty — a claim with
    /// nothing to send is released instead.
    pub records: Vec<AuditExportRecord>,
}

/// Create this shard's cursor row if it does not exist yet.
///
/// Deliberately not seeded by the migration: a shard's database cannot know
/// its own shard id (see the migration's comment, and
/// `replication::ensure_generation_row` for the same pattern in #954).
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn ensure_cursor_row(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    diesel::sql_query(
        "INSERT INTO harvest_audit_export_cursor (shard_id) VALUES ($1) \
         ON CONFLICT (shard_id) DO NOTHING",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Claim a shard for export: assign sequences to newly-visible audit rows and
/// load the next batch, all under the cursor row's lock.
///
/// This is transaction 1 of the two-transaction shape (#605): it takes the
/// cursor row lock, does only local work, and commits **before** any network
/// I/O happens. No row lock is ever held across a sink call.
///
/// Returns `Ok(None)` when the shard is not due (backing off), is already
/// leased by another exporter, or has nothing to deliver.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)] // claim + assign + load is one atomic unit
pub async fn claim_shard(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    batch_size: i64,
    lease: std::time::Duration,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<ClaimedBatch>> {
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    let batch_size = batch_size.clamp(1, MAX_EXPORT_BATCH_SIZE);
    let lease_until =
        now + chrono::Duration::from_std(lease).unwrap_or_else(|_| chrono::Duration::seconds(60));

    Box::pin(
        conn.transaction::<Option<ClaimedBatch>, crate::error::HarvestError, _>(async |conn| {
            let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(crate::models::AuditExportCursor::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            let Some(cursor) = cursor else {
                return Ok(None);
            };

            // Backing off after a sink failure, or another exporter (or this
            // one, on a previous tick whose HTTP call has not returned) holds
            // a live lease. Either way this shard is not ours right now.
            if cursor.next_attempt_at > now {
                return Ok(None);
            }
            if cursor.lease_until.is_some_and(|until| until > now) {
                return Ok(None);
            }

            // ── Assign sequences to rows that are now visible ──────────────
            //
            // Ordered by `(occurred_at, id)` purely for a stable, index-backed
            // scan; correctness does NOT depend on that order matching commit
            // order, because a row that commits later is still `NULL` here and
            // simply receives a later sequence on a later tick. That is the
            // whole reason the sequence is stamped by the exporter rather than
            // handed out by a `BIGSERIAL` before commit — see the module docs.
            let assigned = diesel::sql_query(
                "WITH claimed AS ( \
                     SELECT id, row_number() OVER (ORDER BY occurred_at, id) AS rn \
                     FROM harvest_audit_log \
                     WHERE export_seq IS NULL \
                     ORDER BY occurred_at, id \
                     LIMIT $2 \
                 ) \
                 UPDATE harvest_audit_log a \
                 SET export_seq = $1 + claimed.rn \
                 FROM claimed \
                 WHERE a.id = claimed.id",
            )
            .bind::<diesel::sql_types::BigInt, _>(cursor.last_assigned_seq)
            .bind::<diesel::sql_types::BigInt, _>(batch_size)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

            let last_assigned_seq = cursor.last_assigned_seq + i64::try_from(assigned).unwrap_or(0);

            // ── Load the batch to deliver ─────────────────────────────────
            //
            // Everything above the cursor, not merely what was just assigned:
            // a retry after a failed delivery, and a redrive that rewound the
            // cursor, both re-send already-sequenced records. Reading by
            // `export_seq` (never re-stamping) is what makes a re-export
            // byte-identical (AC6).
            let rows: Vec<crate::models::AuditExportRow> = log::harvest_audit_log
                .filter(log::export_seq.gt(cursor.last_acked_seq))
                .select(crate::models::AuditExportRow::as_select())
                .order(log::export_seq.asc())
                .limit(batch_size)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;

            if last_assigned_seq != cursor.last_assigned_seq {
                diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                    .set((
                        cur::last_assigned_seq.eq(last_assigned_seq),
                        cur::updated_at.eq(now),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            if rows.is_empty() {
                return Ok(None);
            }

            let claim_epoch = cursor.claim_epoch + 1;
            diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                .set((
                    cur::claim_epoch.eq(claim_epoch),
                    cur::lease_until.eq(Some(lease_until)),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            let records = rows
                .into_iter()
                .filter_map(|row| {
                    // `export_seq` is non-NULL by the filter above; a row
                    // without one is skipped rather than exported with a
                    // fabricated sequence, which would corrupt the receiver's
                    // gap accounting.
                    row.export_seq.map(|seq| AuditExportRecord {
                        shard: shard_id,
                        seq,
                        id: row.id,
                        occurred_at: row.occurred_at,
                        actor: row.actor,
                        operation: row.operation,
                        target_type: row.target_type,
                        target_id: row.target_id,
                        route_or_command: row.route_or_command,
                        request_id: row.request_id,
                        idempotency_key: row.idempotency_key,
                        status: row.status,
                        error_summary: row.error_summary,
                        source: row.source,
                    })
                })
                .collect::<Vec<_>>();

            if records.is_empty() {
                return Ok(None);
            }

            Ok(Some(ClaimedBatch {
                shard: shard_id,
                claim_epoch,
                consecutive_failures: cursor.consecutive_failures,
                records,
            }))
        }),
    )
    .await
}

// ---------------------------------------------------------------------
// M6: apply the outcome (transaction 2)
// ---------------------------------------------------------------------

/// Apply a delivery outcome to a shard's cursor.
///
/// Every write is guarded on `claim_epoch = $epoch`, so an attempt whose sink
/// call outlived its lease — and whose batch a later claim has already
/// re-delivered — can never apply a stale outcome over the fresher one, and a
/// redrive that ran mid-flight (which bumps the epoch) can never be silently
/// undone by the in-flight batch's acknowledgement.
///
/// Returns `true` when the guarded write applied.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn apply_outcome(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    claim_epoch: i64,
    outcome: &ExportOutcome,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<bool> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let target = cur::harvest_audit_export_cursor
        .find(shard_id)
        .filter(cur::claim_epoch.eq(claim_epoch));

    let updated = match outcome {
        ExportOutcome::Advance {
            through_seq,
            status,
        } => {
            // Plain assignment, not `GREATEST(...)`: the epoch guard already
            // rules out a stale attempt writing here, and a `GREATEST` would
            // defeat a legitimate redrive by re-raising a cursor an operator
            // deliberately rewound.
            diesel::update(target)
                .set((
                    cur::last_acked_seq.eq(*through_seq),
                    cur::lease_until.eq(None::<DateTime<Utc>>),
                    cur::next_attempt_at.eq(now),
                    cur::consecutive_failures.eq(0),
                    cur::last_status.eq(Some(i32::from(*status))),
                    cur::last_error.eq(None::<String>),
                    cur::last_delivered_at.eq(Some(now)),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?
        }
        ExportOutcome::Backoff {
            next_attempt_at,
            last_status,
            last_error,
            consecutive_failures,
        } => {
            // `last_acked_seq` is deliberately untouched: a failed delivery
            // never advances the cursor, so the same records are re-sent on
            // the next attempt. This is the AC2 "never advances past the
            // failure" invariant, enforced by simply not writing the column.
            diesel::update(target)
                .set((
                    cur::lease_until.eq(None::<DateTime<Utc>>),
                    cur::next_attempt_at.eq(*next_attempt_at),
                    cur::consecutive_failures.eq(*consecutive_failures),
                    cur::last_status.eq(last_status.map(i32::from)),
                    cur::last_error.eq(last_error.clone()),
                    cur::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?
        }
    };

    Ok(updated > 0)
}

/// Drop a claim without changing the cursor (nothing was delivered).
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn release_claim(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    diesel::update(
        cur::harvest_audit_export_cursor
            .find(shard_id)
            .filter(cur::claim_epoch.eq(claim_epoch)),
    )
    .set((
        cur::lease_until.eq(None::<DateTime<Utc>>),
        cur::updated_at.eq(now),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

// ---------------------------------------------------------------------
// M7: observability primitives
// ---------------------------------------------------------------------

/// Delivery state reported by `GET /admin/audit-export`.
///
/// Derived from the cursor row rather than stored, so it can never disagree
/// with the columns it summarizes.
#[must_use]
pub fn delivery_state(
    lease_until: Option<DateTime<Utc>>,
    consecutive_failures: i32,
    next_attempt_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> &'static str {
    if lease_until.is_some_and(|until| until > now) {
        return "DELIVERING";
    }
    if consecutive_failures > 0 && next_attempt_at > now {
        return "BACKOFF";
    }
    if consecutive_failures > 0 {
        return "RETRYING";
    }
    "IDLE"
}

/// One shard's export status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditExportShardStatus {
    pub shard: i32,
    /// The cursor: every record at or below this sequence has been
    /// acknowledged by the sink at least once.
    pub cursor_seq: i64,
    /// High-water mark of sequences handed out.
    pub last_assigned_seq: i64,
    /// Records not yet acknowledged (assigned-but-unacked plus not-yet-assigned).
    pub pending_records: i64,
    /// Age in seconds of the oldest record not yet acknowledged; `0.0` when
    /// nothing is pending. This is `harvest.audit.export_lag`.
    pub lag_seconds: f64,
    /// `IDLE` | `DELIVERING` | `BACKOFF` | `RETRYING`.
    pub delivery_state: String,
    pub consecutive_failures: i32,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub last_delivered_at: Option<DateTime<Utc>>,
    pub next_attempt_at: DateTime<Utc>,
}

/// Pending-record count and lag for one shard.
///
/// "Pending" is `export_seq IS NULL OR export_seq > last_acked_seq` — both a
/// record the exporter has not sequenced yet and one it sequenced but has not
/// had acknowledged are equally undelivered.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::cast_precision_loss)] // millisecond lag never approaches 2^53
pub async fn pending_and_lag(
    conn: &mut diesel_async::AsyncPgConnection,
    last_acked_seq: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<(i64, f64)> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct PendingRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        pending: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        oldest: Option<DateTime<Utc>>,
    }

    let row: PendingRow = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS pending, MIN(occurred_at) AS oldest \
         FROM harvest_audit_log \
         WHERE export_seq IS NULL OR export_seq > $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(last_acked_seq)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;

    let lag = row.oldest.map_or(0.0, |oldest| {
        let secs = (now - oldest).num_milliseconds() as f64 / 1000.0;
        if secs.is_finite() && secs > 0.0 {
            secs
        } else {
            0.0
        }
    });
    Ok((row.pending, lag))
}

/// Read one shard's export status.
///
/// Returns `Ok(None)` when this shard has no cursor row — audit export has
/// never run here.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn export_status(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<AuditExportShardStatus>> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(crate::models::AuditExportCursor::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let Some(cursor) = cursor else {
        return Ok(None);
    };

    let (pending_records, lag_seconds) = pending_and_lag(conn, cursor.last_acked_seq, now).await?;

    Ok(Some(AuditExportShardStatus {
        shard: shard_id,
        cursor_seq: cursor.last_acked_seq,
        last_assigned_seq: cursor.last_assigned_seq,
        pending_records,
        lag_seconds,
        delivery_state: delivery_state(
            cursor.lease_until,
            cursor.consecutive_failures,
            cursor.next_attempt_at,
            now,
        )
        .to_string(),
        consecutive_failures: cursor.consecutive_failures,
        last_status: cursor.last_status,
        last_error: cursor.last_error,
        last_delivered_at: cursor.last_delivered_at,
        next_attempt_at: cursor.next_attempt_at,
    }))
}

// ---------------------------------------------------------------------
// M8: redrive
// ---------------------------------------------------------------------

/// What an operator asked the cursor to be rewound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewindRequest {
    /// Rewind to an explicit sequence: records with `seq > n` re-export.
    Seq(i64),
    /// Rewind so that every record that occurred at or after this instant
    /// re-exports.
    Before(DateTime<Utc>),
}

/// Rewind a shard's export cursor so already-delivered records re-export
/// (AC6), after sink-side data loss.
///
/// Re-exported records are **byte-identical**: `export_seq` is never
/// re-stamped, so a record carries the same `(shard, seq)` and the same JSON
/// on every delivery, and the receiver dedupes.
///
/// The cursor may only ever move **backwards** — see [`resolve_rewind`].
/// Bumps `claim_epoch` and clears the lease, so a delivery already in flight
/// when the rewind lands cannot acknowledge over it.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn rewind_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    request: RewindRequest,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<RewindOutcome> {
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    Box::pin(
        conn.transaction::<RewindOutcome, crate::error::HarvestError, _>(async |conn| {
            let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(crate::models::AuditExportCursor::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            let Some(cursor) = cursor else {
                return Ok(RewindOutcome::NotConfigured);
            };

            let requested = match request {
                RewindRequest::Seq(seq) => seq,
                RewindRequest::Before(instant) => {
                    // The highest sequence among records that occurred strictly
                    // before `instant`; rewinding there re-exports everything from
                    // `instant` onwards. No such record (the instant predates the
                    // whole retained window) means rewind to the beginning.
                    let highest: Option<Option<i64>> = log::harvest_audit_log
                        .filter(log::occurred_at.lt(instant))
                        .filter(log::export_seq.is_not_null())
                        .select(diesel::dsl::max(log::export_seq))
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;
                    highest.flatten().unwrap_or(0)
                }
            };

            let outcome = resolve_rewind(cursor.last_acked_seq, requested);
            if let RewindOutcome::Rewound { to, .. } = outcome {
                diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                    .set((
                        cur::last_acked_seq.eq(to),
                        // Invalidate any in-flight delivery's acknowledgement.
                        cur::claim_epoch.eq(cursor.claim_epoch + 1),
                        cur::lease_until.eq(None::<DateTime<Utc>>),
                        // Re-export immediately rather than serving out a backoff
                        // that belonged to a since-resolved sink failure.
                        cur::next_attempt_at.eq(now),
                        cur::consecutive_failures.eq(0),
                        cur::last_error.eq(None::<String>),
                        cur::updated_at.eq(now),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            Ok(outcome)
        }),
    )
    .await
}

// ---------------------------------------------------------------------
// M9: the scanner
// ---------------------------------------------------------------------

/// Export one batch for one shard on one connection.
///
/// Three phases, deliberately separated: claim (transaction 1), deliver (no
/// transaction, no locks), apply (transaction 2). Returns the number of
/// records delivered.
#[cfg(feature = "db")]
async fn export_once_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    config: &AuditExportRuntimeConfig,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    ensure_cursor_row(conn, shard_id).await?;

    let now = Utc::now();
    let Some(claim) = claim_shard(conn, shard_id, config.batch_size, config.lease, now).await?
    else {
        // Nothing claimed, but the lag gauge must still be emitted: an
        // operator's "is the export keeping up?" signal has to stay live
        // exactly when deliveries are NOT happening.
        emit_lag(conn, shard_id, metrics).await;
        return Ok(0);
    };

    let first_seq = claim.records.first().map_or(0, |r| r.seq);
    let last_seq = claim.records.last().map_or(0, |r| r.seq);

    let body = match serialize_batch(&claim.records) {
        Ok(body) => body,
        Err(error) => {
            // Cannot serialize what we claimed — release the claim and back
            // off rather than silently advancing past records we never sent.
            tracing::error!(
                shard = shard_id,
                error = %error,
                "failed to serialize an audit export batch; holding the cursor"
            );
            release_claim(conn, shard_id, claim.claim_epoch, Utc::now()).await?;
            return Ok(0);
        }
    };

    let delivered_at = Utc::now();
    let headers = export_headers(
        &config.secret,
        &body,
        shard_id,
        first_seq,
        last_seq,
        delivered_at,
    );
    let batch = AuditBatch {
        shard: shard_id,
        first_seq,
        last_seq,
        records: &claim.records,
        body: &body,
        headers: &headers,
    };

    let attempt = config.sink.deliver(&batch).await;
    let outcome = classify_export_outcome(
        &attempt,
        last_seq,
        claim.consecutive_failures,
        &config.backoff,
        Utc::now(),
    );

    let applied = apply_outcome(conn, shard_id, claim.claim_epoch, &outcome, Utc::now()).await?;

    let delivered = match &outcome {
        ExportOutcome::Advance { .. } if applied => {
            let count = claim.records.len();
            metrics
                .record_audit_exported(u16::try_from(shard_id).unwrap_or(u16::MAX), count as u64);
            count
        }
        ExportOutcome::Advance { .. } => {
            // The guarded write did not apply: this attempt's lease expired
            // and a fresher claim owns the shard, or a redrive bumped the
            // epoch. The batch was delivered (the receiver dedupes on
            // `(shard, seq)`) but this attempt must not move the cursor.
            tracing::warn!(
                shard = shard_id,
                claim_epoch = claim.claim_epoch,
                "audit export batch was acknowledged by the sink but its claim had \
                 already been superseded; the cursor was not advanced and the batch \
                 will be re-delivered (at-least-once)"
            );
            0
        }
        ExportOutcome::Backoff {
            last_status,
            last_error,
            consecutive_failures,
            ..
        } => {
            tracing::warn!(
                shard = shard_id,
                status = ?last_status,
                error = ?last_error,
                consecutive_failures,
                "audit export delivery failed; cursor held at its current position"
            );
            0
        }
    };

    emit_lag(conn, shard_id, metrics).await;
    Ok(delivered)
}

/// Emit `harvest.audit.export_lag` for one shard, best-effort.
#[cfg(feature = "db")]
async fn emit_lag(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let acked: Result<Option<i64>, _> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(cur::last_acked_seq)
        .first(conn)
        .await
        .optional();
    let Ok(Some(acked)) = acked else {
        return;
    };
    if let Ok((_, lag)) = pending_and_lag(conn, acked, Utc::now()).await {
        metrics.record_audit_export_lag(u16::try_from(shard_id).unwrap_or(u16::MAX), lag);
    }
}

/// Export due audit batches across every assigned shard.
///
/// Follows the established scanner pattern: called from
/// [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is spawned
/// (AC3). Mirrors
/// [`crate::completion_callback::fire_due_completion_deliveries`]'s per-shard
/// fan-out, because audit rows live on the shard whose database recorded
/// them, and a single-connection scan would never see the others.
///
/// A no-op (returns `Ok(0)` **before any query**) when no audit sink has been
/// configured (AC8).
///
/// # Errors
/// Returns `HarvestError` if a database query fails. A sink's transport
/// failure is never an `Err` here — it is captured as a [`SinkAttempt`] and
/// classified into a backoff write.
#[cfg(feature = "db")]
pub async fn fire_due_audit_exports(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    let Some(config) = read_global_audit_export_config() else {
        return Ok(0);
    };

    let mut total = 0usize;

    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            for shard in shard_assignments {
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    continue;
                };
                let mut shard_conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "[audit_export] failed to get connection to shard {shard:?}: {e:?}"
                        );
                        continue;
                    }
                };
                // One shard's failure must never stop the others: an
                // unreachable shard is an availability problem, but silently
                // skipping every *later* shard's export would be a compliance
                // one.
                match export_once_on_conn(&mut shard_conn, &config, shard.as_i32(), metrics).await {
                    Ok(n) => total += n,
                    Err(e) => tracing::error!(
                        shard = shard.as_i32(),
                        error = %e,
                        "[audit_export] shard export failed"
                    ),
                }
            }
        }
        _ => {
            // Unsharded deployment: the default shard is 0, matching how the
            // rest of the system labels an unsharded install.
            total += export_once_on_conn(conn, &config, 0, metrics).await?;
        }
    }

    Ok(total)
}

// ── Unit tests (pure, no DB) ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn rec(seq: i64) -> AuditExportRecord {
        AuditExportRecord {
            shard: 3,
            seq,
            id: Uuid::from_u128(u128::try_from(seq).unwrap_or(0)),
            occurred_at: DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts"),
            actor: "alice".to_string(),
            operation: "workflow.cancel".to_string(),
            target_type: "workflow".to_string(),
            target_id: Some("exec-1".to_string()),
            route_or_command: "POST /workflows/{id}/cancel".to_string(),
            request_id: None,
            idempotency_key: None,
            status: "SUCCEEDED".to_string(),
            error_summary: None,
            source: "api".to_string(),
        }
    }

    // ── JSON-lines batch shape ──────────────────────────────────────────────

    #[test]
    fn serialize_batch_emits_one_compact_json_object_per_line() {
        let body = serialize_batch(&[rec(1), rec(2), rec(3)]).expect("serializes");
        let text = String::from_utf8(body).expect("utf8");
        assert!(
            text.ends_with('\n'),
            "JSON-lines bodies must end with a newline so a log-ingest tail never \
             merges the last record with the next batch; got {text:?}"
        );
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3, "one line per record");
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("each line is JSON");
            assert_eq!(
                parsed["seq"],
                serde_json::json!(i64::try_from(i).unwrap() + 1)
            );
            assert!(
                !line.contains('\n'),
                "a record must never contain a raw newline or it would split a line"
            );
        }
    }

    #[test]
    fn serialize_batch_of_no_records_is_empty() {
        let body = serialize_batch(&[]).expect("serializes");
        assert!(
            body.is_empty(),
            "an empty batch must produce no bytes at all"
        );
    }

    // Redrive byte-identity (AC6): re-exporting the same (shard, seq) records
    // must produce exactly the same bytes, or the receiver's dedup on
    // (shard, seq) would be checking a different payload than it stored.
    #[test]
    fn serialize_batch_is_byte_identical_across_calls() {
        let first = serialize_batch(&[rec(7), rec(8)]).expect("serializes");
        let second = serialize_batch(&[rec(7), rec(8)]).expect("serializes");
        assert_eq!(first, second, "re-export must be byte-identical");
    }

    // A SIEM maps this to a fixed schema, so an absent optional field must
    // still appear as an explicit `null` rather than vanishing from the object.
    #[test]
    fn absent_optional_fields_serialize_as_explicit_null() {
        let body = serialize_batch(&[rec(1)]).expect("serializes");
        let line = String::from_utf8(body).expect("utf8");
        let parsed: serde_json::Value =
            serde_json::from_str(line.trim_end_matches('\n')).expect("json");
        for field in ["request_id", "idempotency_key", "error_summary"] {
            assert!(
                parsed.get(field).is_some_and(serde_json::Value::is_null),
                "{field} must serialize as an explicit null, not be omitted"
            );
        }
        // Tamper-evidence identity fields (AC4) are always present.
        assert_eq!(parsed["shard"], serde_json::json!(3));
        assert_eq!(parsed["seq"], serde_json::json!(1));
    }

    // ── Signing (the #605 X-Harvest-Signature scheme, reused verbatim) ───────

    #[test]
    fn export_headers_sign_the_exact_body_with_the_605_scheme() {
        let secret = CallbackSecret::new(b"topsecret".to_vec());
        let body = serialize_batch(&[rec(1), rec(2)]).expect("serializes");
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let headers = export_headers(&secret, &body, 3, 1, 2, now);

        let signature = headers
            .iter()
            .find(|(name, _)| *name == SIGNATURE_HEADER)
            .map(|(_, v)| v.clone())
            .expect("signature header present");
        assert_eq!(
            signature,
            crate::completion_callback::sign(&secret, &body),
            "must be the same HMAC scheme as issue #605, over the exact POSTed bytes"
        );
        assert!(signature.starts_with("sha256="));
    }

    #[test]
    fn export_headers_carry_the_shard_and_sequence_range() {
        let secret = CallbackSecret::new(Vec::new());
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let headers = export_headers(&secret, b"body", 3, 10, 42, now);
        let get = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| *n == name)
                .map_or_else(|| panic!("{name} present"), |(_, value)| value.clone())
        };
        assert_eq!(get(SHARD_HEADER), "3");
        assert_eq!(get(FIRST_SEQ_HEADER), "10");
        assert_eq!(get(LAST_SEQ_HEADER), "42");
        assert_eq!(get(TIMESTAMP_HEADER), now.to_rfc3339());
    }

    // ── Sink attempt classification ─────────────────────────────────────────

    #[test]
    fn sink_attempt_is_success_only_for_2xx() {
        assert!(SinkAttempt::success(200).is_success());
        assert!(SinkAttempt::success(204).is_success());
        assert!(SinkAttempt::success(299).is_success());
        assert!(!SinkAttempt::success(199).is_success());
        assert!(!SinkAttempt::success(302).is_success());
        assert!(!SinkAttempt::success(500).is_success());
        assert!(!SinkAttempt::transport_error("refused".to_string()).is_success());
    }

    // ── The core AC2 invariant: never advance past a failure ────────────────

    #[test]
    fn a_2xx_advances_the_cursor_through_the_delivered_batch() {
        let now = Utc::now();
        let outcome = classify_export_outcome(
            &SinkAttempt::success(202),
            42,
            3,
            &ExportBackoff::default(),
            now,
        );
        assert_eq!(
            outcome,
            ExportOutcome::Advance {
                through_seq: 42,
                status: 202
            }
        );
    }

    #[test]
    fn a_failure_never_advances_the_cursor() {
        let now = Utc::now();
        for attempt in [
            SinkAttempt::success(500),
            SinkAttempt::success(404),
            SinkAttempt::success(301),
            SinkAttempt::transport_error("timeout".to_string()),
        ] {
            let outcome = classify_export_outcome(&attempt, 42, 0, &ExportBackoff::default(), now);
            assert!(
                matches!(outcome, ExportOutcome::Backoff { .. }),
                "a failed delivery must hold the cursor, never advance it: {attempt:?}"
            );
        }
    }

    // There is deliberately no dead-letter arm: unlike a completion callback
    // (#605), an audit record may never be dropped after N attempts — the
    // export is the compliance artifact. Retry forever, capped.
    #[test]
    fn repeated_failures_keep_backing_off_and_never_give_up() {
        let now = Utc::now();
        let backoff = ExportBackoff::default();
        for failures in [1_i32, 5, 50, 5_000, i32::MAX] {
            let outcome =
                classify_export_outcome(&SinkAttempt::success(503), 7, failures, &backoff, now);
            match outcome {
                ExportOutcome::Backoff {
                    next_attempt_at,
                    consecutive_failures,
                    ..
                } => {
                    assert!(next_attempt_at >= now, "backoff is always in the future");
                    assert!(
                        next_attempt_at
                            <= now + chrono::Duration::from_std(backoff.max_interval).unwrap(),
                        "backoff is capped at max_interval even after {failures} failures"
                    );
                    assert_eq!(
                        consecutive_failures,
                        failures.saturating_add(1),
                        "the failure counter increments (saturating at i32::MAX)"
                    );
                }
                ExportOutcome::Advance { .. } => {
                    panic!("a 503 must never advance the cursor")
                }
            }
        }
    }

    #[test]
    fn backoff_grows_exponentially_before_the_cap() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let backoff = ExportBackoff {
            initial_interval: Duration::from_secs(1),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(60),
        };
        let delay_after = |failures: i32| match classify_export_outcome(
            &SinkAttempt::success(500),
            1,
            failures,
            &backoff,
            now,
        ) {
            ExportOutcome::Backoff {
                next_attempt_at, ..
            } => (next_attempt_at - now).num_seconds(),
            ExportOutcome::Advance { .. } => panic!("not a success"),
        };
        assert_eq!(delay_after(0), 1, "first failure waits initial_interval");
        assert_eq!(delay_after(1), 2);
        assert_eq!(delay_after(2), 4);
        assert_eq!(delay_after(3), 8);
        assert_eq!(delay_after(30), 60, "capped at max_interval");
    }

    #[test]
    fn a_transport_error_is_recorded_as_the_last_error() {
        let now = Utc::now();
        let outcome = classify_export_outcome(
            &SinkAttempt::transport_error("connection refused".to_string()),
            9,
            0,
            &ExportBackoff::default(),
            now,
        );
        match outcome {
            ExportOutcome::Backoff {
                last_status,
                last_error,
                ..
            } => {
                assert_eq!(last_status, None);
                assert_eq!(last_error.as_deref(), Some("connection refused"));
            }
            ExportOutcome::Advance { .. } => panic!("transport error is never a success"),
        }
    }

    // ── Redrive: a rewind may only ever move the cursor backwards ───────────

    #[test]
    fn rewind_moves_the_cursor_backwards() {
        assert_eq!(
            resolve_rewind(100, 40),
            RewindOutcome::Rewound { from: 100, to: 40 }
        );
        assert_eq!(
            resolve_rewind(100, 0),
            RewindOutcome::Rewound { from: 100, to: 0 },
            "rewinding to 0 re-exports every retained record"
        );
    }

    // The dangerous direction: moving a cursor FORWARD would skip records that
    // were never delivered, silently creating exactly the gap this feature
    // exists to make impossible. It must be refused, not clamped-and-applied.
    #[test]
    fn rewind_refuses_to_move_the_cursor_forward() {
        assert_eq!(
            resolve_rewind(100, 101),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: 101
            }
        );
        assert_eq!(
            resolve_rewind(100, i64::MAX),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: i64::MAX
            }
        );
    }

    #[test]
    fn rewind_to_the_current_position_is_a_noop() {
        assert_eq!(
            resolve_rewind(100, 100),
            RewindOutcome::NoOp {
                cursor: 100,
                requested: 100
            }
        );
    }

    #[test]
    fn rewind_clamps_a_negative_request_to_zero() {
        assert_eq!(
            resolve_rewind(100, -5),
            RewindOutcome::Rewound { from: 100, to: 0 },
            "a negative position is meaningless; clamp to the beginning rather \
             than writing a negative cursor the CHECK constraint would reject"
        );
    }

    // ── Trait shape ─────────────────────────────────────────────────────────

    struct NoopSink;

    impl AuditSink for NoopSink {
        fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
            Box::pin(async { SinkAttempt::success(200) })
        }
    }

    #[test]
    fn audit_sink_is_object_safe_and_send_sync() {
        fn assert_bounds<T: AuditSink>() {}
        assert_bounds::<NoopSink>();
        let boxed: Box<dyn AuditSink> = Box::new(NoopSink);
        let _arc: std::sync::Arc<dyn AuditSink> = std::sync::Arc::from(boxed);
    }
}
