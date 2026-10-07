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
//! # Opt-in, but one cost is not zero
//!
//! With no sink registered, [`GLOBAL_AUDIT_EXPORT_CONFIG`] is `None`. The
//! scanner returns `Ok(0)` before it issues a single query. No cursor row is
//! ever created, and `export_seq` stays `NULL` on every audit row. No new
//! query runs and no new row exists: read behavior matches the code before
//! this module existed.
//!
//! One exception exists (issue #1506). If a live config is removed, each tick
//! still returns before any query. It then reports `export_observed = 0` for
//! each shard it serves, because the gauges would otherwise keep a healthy value.
//!
//! Write behavior matched that before issue #1667. The partial index
//! `harvest_audit_log_unexported_idx` matches every row while `export_seq` stays
//! `NULL`. It then added maintenance cost to each audit insert and served no
//! read (issue #1272). The index is now built lazily. [`ensure_unexported_index`]
//! builds it on the first export tick, and a migration drops it from databases
//! that never ran export. An unconfigured deployment pays no index maintenance
//! cost. Once export runs, the index size is bounded only while retention
//! reclaims unexported rows. See "Retention interaction" in
//! `docs/audit-export.md`. Retention can never
//! purge a decommission or reactivation record, exported or not.
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
//! | `status` | `severityText` (`"succeeded"` -> `INFO`, `"failed"` -> `ERROR`). **Lowercase on the wire** — these are `audit::STATUS_SUCCEEDED` / `STATUS_FAILED` passed through verbatim, so a receiver matching `"FAILED"` silently files every failed privileged action as `INFO` |
//! | `error_summary` | `attributes["exception.message"]` |
//! | `shard` | `attributes["harvest.audit.source_shard"]` — the database the record was read from, and half the dedup key |
//! | `seq` | `attributes["harvest.audit.seq"]` |
//! | `shard_id` | `attributes["harvest.shard.id"]` — the shard the operation itself named, which can differ from `shard` |
//! | `actor`, `target_type`, `target_id`, `route_or_command`, `request_id`, `idempotency_key`, `source` | `attributes["harvest.audit.<field>"]` |
//! | `id` | `attributes["harvest.audit.id"]` |
//! | `chain_prev`, `chain_newest_before`, `chain_hash` | `attributes["harvest.audit.chain_prev"]`, `attributes["harvest.audit.chain_newest_before"]`, `attributes["harvest.audit.chain_hash"]`, when present |
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
/// **Routing and triage metadata only — not covered by the signature.** The
/// signature (per #605's scheme) is over the request body alone, so a
/// receiver must read the authoritative `(shard, seq)` pair from each record
/// *in the body*. Deduplicating on these headers would mean deduplicating on
/// unauthenticated input: an attacker replaying a captured, validly-signed
/// batch with a shifted range could mark a real range as already-seen and
/// create exactly the silent gap this feature exists to prevent.
pub const SHARD_HEADER: &str = "X-Harvest-Audit-Shard";
/// HTTP header carrying the first (lowest) `seq` in the batch. Unsigned — see
/// [`SHARD_HEADER`].
pub const FIRST_SEQ_HEADER: &str = "X-Harvest-Audit-First-Seq";
/// HTTP header carrying the last (highest) `seq` in the batch. Unsigned — see
/// [`SHARD_HEADER`].
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
/// stable object shape across every batch. The two chain fields are the
/// exception: they are omitted when absent (issue #1838).
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
    /// The shard the *operation* recorded itself against, straight from
    /// `harvest_audit_log.shard_id`.
    ///
    /// Distinct from [`Self::shard`], which names the database the record was
    /// read from and is the dedup dimension. The two normally agree, but a
    /// control-plane mutation writes its audit row on the default shard while
    /// naming the shard it acted on, so exporting only one of them would make
    /// a receiver's correlation silently wrong.
    pub shard_id: Option<i32>,
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
    /// Lowercase hex of the link before this row (issue #1838).
    ///
    /// Present only on rows the chain covers. The field is omitted, not
    /// `null`, when absent. So a deployment without a chain key ships the
    /// same bytes as before. See [`crate::audit_chain`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_prev: Option<String>,
    /// The newest `occurred_at` of every row chained before this one (issue
    /// #1838). The link covers it. Omitted when absent, as
    /// [`Self::chain_prev`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_newest_before: Option<DateTime<Utc>>,
    /// Lowercase hex of the row's audit-chain link (issue #1838). Omitted
    /// when absent, as [`Self::chain_prev`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_hash: Option<String>,
}

#[cfg(feature = "db")]
impl AuditExportRecord {
    /// Build the record the exporter ships for `row`, read from `shard`.
    ///
    /// Returns `None` for a row with no `export_seq`. A fabricated sequence
    /// would corrupt the receiver's gap accounting.
    #[must_use]
    ///
    /// The chain fields come along only when `shard` made the links. Another
    /// shard on the same database ships the row without them.
    pub fn from_row(shard: i32, row: crate::models::AuditExportRow) -> Option<Self> {
        let own = row.chain_shard == Some(shard);
        Some(Self {
            shard,
            seq: row.export_seq?,
            id: row.id,
            shard_id: row.shard_id,
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
            chain_prev: chain_hex(row.chain_prev.as_deref().filter(|_| own)),
            chain_newest_before: row.chain_newest_before.filter(|_| own),
            chain_hash: chain_hex(row.chain_hash.as_deref().filter(|_| own)),
        })
    }
}

/// Lowercase hex of a stored 32-byte link.
#[cfg(feature = "db")]
fn chain_hex(bytes: Option<&[u8]>) -> Option<String> {
    bytes
        .and_then(crate::audit_chain::from_bytes)
        .map(|hash| crate::audit_chain::to_hex(&hash))
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
/// drift). **The signature covers the body only** — the shard/sequence/
/// timestamp headers are unauthenticated routing metadata; see
/// [`SHARD_HEADER`].
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewindOutcome {
    /// The cursor moves backwards from `from` to `to`; every record with
    /// `seq > to` re-exports.
    ///
    /// Not every record in `(to, from]` is guaranteed to still exist. Retention
    /// can purge part of that window first — see
    /// [`count_redrive_recoverable`] (issue #1267).
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
/// never installed and the scanner is entirely inert (AC8). The partial
/// index does not exist until export first runs; see the module-level note
/// above (issues #1272 and #1667).
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
    /// How long a claim holds a shard's cursor, and the timeout applied to the
    /// sink call itself. Must exceed the sink's own per-request timeout — see
    /// [`DEFAULT_EXPORT_LEASE`].
    pub lease: std::time::Duration,
    /// HMAC key for the audit hash chain (issue #1838). `None` turns the chain
    /// off. See [`crate::audit_chain`].
    pub chain_key: Option<CallbackSecret>,
    /// Keys the exporter accepts on a stored chain checkpoint, but never signs
    /// with. Set them for a key rotation.
    pub chain_accept_keys: Vec<CallbackSecret>,
}

/// A webhook URL reduced to its origin, for diagnostics.
///
/// A SIEM ingest endpoint routinely carries its credential in the path or the
/// query string (`.../services/collector/<token>`, `?api_key=...`), so the full
/// URL is a secret in the same way the HMAC key is (issue #953, Codex review
/// round 23 P1). `AuditExportBuilderConfig`'s `Debug` is hand-written precisely
/// to keep secrets out of logs and panic messages — it already redacts the HMAC
/// key and the sink — but it printed the URL verbatim, which is the same leak
/// `audit_sink::describe_without_url` exists to prevent on the error path. The
/// origin is what makes a config dump useful ("which host is this pointed at?")
/// and is the part that is not a credential.
///
/// Falls back to a marker rather than the input when the URL cannot be parsed:
/// an unparseable string must not be echoed on the assumption it is harmless.
///
/// Reads the origin from a full [`url::Url::parse`], not a hand-rolled
/// split (issue #1274). A manual split trusts the input's shape. It
/// missed a backslash standing in for `/`. It also read a malformed
/// `host:port` authority as valid — `s3cr3t` is not a port, so the real
/// parser rejects the whole string. Either way, the secret rode along
/// after it. Delegating to the parser closes the whole class: this
/// function can only render a host the parser itself accepted.
pub(crate) fn redact_webhook_url(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return "<unparseable webhook url redacted>".to_string();
    };
    let Some(host) = parsed.host_str() else {
        return "<unparseable webhook url redacted>".to_string();
    };
    // A port is part of the origin, not the credential (issue #1274). Two
    // targets at the same host on different ports are different
    // endpoints. `Url::port()` already omits the scheme's default port,
    // so this adds nothing for a bare `https://host/...` origin.
    let port = parsed.port().map_or_else(String::new, |p| format!(":{p}"));
    format!("{}://{host}{port}/<redacted>", parsed.scheme())
}

impl std::fmt::Debug for AuditExportBuilderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditExportBuilderConfig")
            .field("allowlist", &self.allowlist)
            .field("allow_http", &self.allow_http)
            .field("allow_ip_literals", &self.allow_ip_literals)
            .field(
                "webhook_url",
                &self.webhook_url.as_deref().map(redact_webhook_url),
            )
            .field("secret", &self.secret)
            .field("sink", &self.sink.as_ref().map(|_| "<AuditSink>"))
            .field("batch_size", &self.batch_size)
            .field("backoff", &self.backoff)
            .field("lease", &self.lease)
            .field("chain_key", &self.chain_key)
            .field("chain_accept_keys", &self.chain_accept_keys)
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
            lease: DEFAULT_EXPORT_LEASE,
            chain_key: None,
            chain_accept_keys: Vec::new(),
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
    /// `true` when a signed webhook is configured but no HMAC key is.
    ///
    /// Fails the build rather than warning (issue #953 review): the signature
    /// *is* the tamper-evidence control this feature promises, and HMAC-SHA256
    /// accepts a zero-length key happily — so an unconfigured secret does not
    /// produce a missing header, it produces a well-formed
    /// `X-Harvest-Signature` that anyone can recompute. A receiver verifying
    /// it would see a valid signature on a batch any third party could have
    /// forged, which is worse than no signature at all. A startup
    /// `tracing::warn!` on a fleet is not a control.
    ///
    /// Only the *webhook* path is gated. An embedder-supplied [`AuditSink`]
    /// may authenticate however it likes (IAM, mTLS, a local file), so an
    /// absent HMAC key there is a legitimate configuration and only warns.
    #[must_use]
    pub fn webhook_is_missing_a_secret(&self) -> bool {
        if self.sink.is_some() || self.webhook_url.is_none() {
            return false;
        }
        // An *explicitly empty* secret is missing, not configured (issue #953,
        // Codex review round 3 P1). `audit_export_secret(env::var("...")
        // .unwrap_or_default())` with the variable unset yields
        // `Some(CallbackSecret(b""))`, which an `is_none()` check waves through
        // — and produces exactly the publicly-reproducible signature this
        // check exists to reject. Failing closed has to mean the bytes, not
        // the `Option`.
        self.secret
            .as_ref()
            .is_none_or(|secret| secret.as_bytes().is_empty())
    }

    /// Validate the webhook URL against the SSRF policy.
    ///
    /// Skipped entirely when an embedder-supplied `sink` is set, matching
    /// [`Self::webhook_is_missing_a_secret`] and the documented precedence:
    /// the sink wins, no `ReqwestAuditSink` is ever constructed, and no request
    /// can reach the URL. Failing startup on a stale or non-allowlisted URL
    /// that nothing will call is a false positive (issue #953, Codex review
    /// P2) — the two webhook checks must agree about when a webhook is live,
    /// or a config that skips one still trips the other.
    ///
    /// # Errors
    /// Returns the `(url, rejection)` pair when the webhook URL fails
    /// validation.
    pub fn validate_webhook_url(
        &self,
    ) -> Result<(), (String, crate::completion_callback::SsrfRejection)> {
        if self.sink.is_some() {
            return Ok(());
        }
        let Some(url) = &self.webhook_url else {
            return Ok(());
        };
        let policy = self.ssrf_policy();
        crate::completion_callback::validate_target_url(url, &policy)
            .map(|_| ())
            // The REDACTED origin, never the full URL (issue #953, Codex
            // review round 24 P1). This pair becomes
            // `HarvestBuilderError::AuditSinkRejected`, whose `Display` a
            // startup failure writes straight to the logs -- and a SIEM ingest
            // URL carries its credential in the path or query. Redacting here
            // rather than at the construction site means the full URL cannot
            // escape through this result at all.
            //
            // Nothing diagnostic is lost: every `SsrfRejection` variant
            // discriminates on an ORIGIN property -- scheme, host, port, IP
            // literal, userinfo -- so the origin is exactly what explains the
            // rejection, and the part that is dropped is exactly the part that
            // is a secret.
            .map_err(|rejection| (redact_webhook_url(url), rejection))
    }

    /// The claim lease, floored at one second.
    ///
    /// A zero lease would make every claim immediately reclaimable and give
    /// the sink call a zero timeout, so every batch would fail before it was
    /// sent.
    #[must_use]
    pub fn effective_lease(&self) -> std::time::Duration {
        self.lease.max(std::time::Duration::from_secs(1))
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
// M4: process-global runtime config (opt-in; `None` == scanner fully inert)
// ---------------------------------------------------------------------

/// Bound on acquiring a shard's connection inside the scanner (issue #953,
/// Codex review P1).
///
/// `pool.get()` can be an **unbounded** wait — a pool may have no deadpool
/// `Timeouts`. The scanner is already holding a connection when it runs (the
/// timeout checker checks one out before calling `enforce_timeouts_once`), and
/// for a per-shard checker that connection comes from the very pool this
/// function then asks for a second one. On a one-connection pool that is a
/// self-deadlock: the wait never returns, audit export never runs, and every
/// scanner resident sequenced after it is wedged with it.
///
/// Bounding converts the indefinite park into "skip this shard for one tick",
/// which is visible in the log and self-heals, mirroring
/// `worker::shard_acquire_bound` (added for the same class of bug in #961).
/// Deliberately generous: the bound exists to stop an *indefinite* block, not
/// to police normal contention on a busy pool.
pub const SHARD_ACQUIRE_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// Bound reserved for the acknowledgement query itself, on top of
/// [`SHARD_ACQUIRE_BOUND`] (Codex review on PR #1520, follow-up P2).
///
/// `SHARD_ACQUIRE_BOUND` alone only reserves time for the post-delivery
/// connection checkout. A checkout that uses nearly all of that bound
/// leaves `apply_outcome` no margin before `lease_until`. A second exporter
/// could then reclaim the shard first, and the acknowledgement would be
/// rejected even though the batch was genuinely delivered.
///
/// This reserves additional time for that query alone. Deliberately
/// generous for a single guarded `UPDATE`, matching `SHARD_ACQUIRE_BOUND`'s
/// own margin above normal-case latency.
pub const ACK_QUERY_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// Splits a reserve between the post-delivery checkout and the
/// acknowledgement query. The split keeps their uncapped proportion
/// (Codex review on PR #1520, follow-up P1).
///
/// The caller never passes a `reserve` above `SHARD_ACQUIRE_BOUND` plus
/// `ACK_QUERY_BOUND`, only at or below it. So this only ever shrinks the
/// two bounds together, never widens either one. A short lease can cap
/// `reserve` well below their sum.
///
/// Using the fixed, uncapped bounds for each step regardless would let the
/// checkout alone consume a reserve meant to cover both steps. The
/// acknowledgement would then get no margin at all.
#[cfg(feature = "db")]
fn split_reserve(reserve: std::time::Duration) -> (std::time::Duration, std::time::Duration) {
    let total = SHARD_ACQUIRE_BOUND + ACK_QUERY_BOUND;
    let checkout_nanos = reserve.as_nanos() * SHARD_ACQUIRE_BOUND.as_nanos() / total.as_nanos();
    let checkout =
        std::time::Duration::from_nanos(u64::try_from(checkout_nanos).unwrap_or(u64::MAX));
    (checkout, reserve.saturating_sub(checkout))
}

/// Default lease held on a shard's cursor while a batch is in flight.
///
/// Long enough to cover a slow sink, short enough that a crashed exporter's
/// shard resumes promptly. A lease expiring early is safe — it costs a
/// duplicate delivery, which the receiver dedupes on `(shard, seq)` — while a
/// lease expiring late costs export lag.
///
/// **It also bounds the sink call itself.** The exporter wraps every
/// `deliver` in a timeout of exactly the configured lease, so a sink can
/// never outlive its own claim: without that, a sink slower than the lease
/// would livelock (each attempt superseded by the next claim, the cursor
/// never advancing) and would additionally wedge the shared background
/// scanner behind an embedder-supplied `await`.
pub const DEFAULT_EXPORT_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Everything the exporter needs at runtime, installed once at startup.
///
/// `None` (the default, before any builder wiring runs) means the scanner is
/// fully inert. [`fire_due_audit_exports`] returns `Ok(0)` before issuing a
/// query, so an embedder who never configures a sink sees zero query
/// behavior change and zero scanner work (AC8). The partial index is built
/// lazily on the first tick; see the module-level note above (issue #1667).
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
    /// Key for the audit hash chain (issue #1838). `None` stamps no chain.
    pub chain_key: Option<crate::audit_chain::AuditChainKey>,
}

#[cfg(feature = "db")]
impl AuditExportRuntimeConfig {
    /// Claim a batch on `shard_id` with this config's batch size, lease and
    /// chain key. See [`claim_shard_chained`].
    ///
    /// # Errors
    /// Returns `HarvestError` on a database failure.
    pub async fn claim(
        &self,
        conn: &mut diesel_async::AsyncPgConnection,
        shard_id: i32,
        now: DateTime<Utc>,
    ) -> crate::error::HarvestResult<Option<ClaimedBatch>> {
        claim_shard_chained(
            conn,
            shard_id,
            self.batch_size,
            self.lease,
            now,
            self.chain_key.as_ref(),
        )
        .await
    }
}

// Write this static through [`set_global_audit_export_config`]. A direct write
// skips the disable tracking of issue #1506, and the gauges then keep their
// last value.
//
// `Arc`-wrapped for the same reason as `GLOBAL_CALLBACK_CONFIG` (issue #605
// review): every read clones the value out of the lock, and the struct carries
// owned fields that would otherwise be deep-copied on every scanner tick for a
// value that only ever changes at startup.
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
///
/// That clear marks export as disabled (issue #1506). Each export tick then
/// reports its shards as unobserved until a sink is configured again.
pub fn install_global_audit_export_config_for_direct_worker(config: &AuditExportBuilderConfig) {
    if config.sink.is_none() && config.webhook_url.is_some() {
        tracing::warn!(
            "audit_export_webhook(...) was configured but this runtime was built \
             through the direct core worker path, which ships no HTTP client -- no \
             audit records will be exported. Supply audit_export_sink(...) with your \
             own AuditSink, or run through autumn-harvest-plugin, which provides the \
             default reqwest signed-webhook sink."
        );
    }
    set_global_audit_export_config(direct_worker_runtime_config(config).map(std::sync::Arc::new));
}

/// The runtime config the direct core worker path installs for `config`.
///
/// `None` without an embedder-supplied sink.
fn direct_worker_runtime_config(
    config: &AuditExportBuilderConfig,
) -> Option<AuditExportRuntimeConfig> {
    let sink = config.sink.clone()?;
    let secret = config.secret.clone().unwrap_or_else(|| {
        tracing::warn!(
            "audit-export HMAC secret was never configured via \
             HarvestBuilder::audit_export_secret(...) -- every exported batch will be \
             signed with an empty key, which defeats the X-Harvest-Signature \
             tamper-evidence guarantee for any receiver relying on it"
        );
        CallbackSecret::new(Vec::new())
    });
    Some(AuditExportRuntimeConfig {
        sink,
        secret,
        batch_size: config.effective_batch_size(),
        backoff: config.backoff.clone(),
        lease: config.effective_lease(),
        chain_key: runtime_chain_key(config),
    })
}

/// The validated chain key for the runtime config (issue #1838).
///
/// It carries the accepted keys of `config` too. `try_build` already rejects
/// a short key, so this drops only a key that skipped the builder. It logs
/// that, because the chain or that accepted key is then off.
#[must_use]
pub fn runtime_chain_key(
    config: &AuditExportBuilderConfig,
) -> Option<crate::audit_chain::AuditChainKey> {
    let validate = |key: &CallbackSecret| {
        crate::audit_chain::AuditChainKey::new(key.as_bytes().to_vec())
            .inspect_err(|e| {
                tracing::error!(error = %e, "an audit chain key is too short and is ignored");
            })
            .ok()
    };
    let active = validate(config.chain_key.as_ref()?)?;
    Some(
        config
            .chain_accept_keys
            .iter()
            .filter_map(validate)
            .fold(active, crate::audit_chain::AuditChainKey::with_accepted_key),
    )
}

/// `true` after a live export config was removed and none replaced it.
///
/// Only [`set_global_audit_export_config`] writes it. A process that never
/// installed a sink never sets it, so it emits no new series (AC8).
static EXPORT_DISABLED_AFTER_ENABLE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Publish or clear [`GLOBAL_AUDIT_EXPORT_CONFIG`], and track the edge.
///
/// Replacing a `Some` with `None` marks export as disabled (issue #1506).
/// The exporter then reports each shard as unobserved. Without this, the
/// gauges keep their last value and the process looks healthy. Publishing a
/// `Some` clears the mark. The mark changes under the write lock. A tick reads
/// the mark without the lock, so it can lag one tick behind. The next tick
/// corrects it.
pub fn set_global_audit_export_config(config: Option<std::sync::Arc<AuditExportRuntimeConfig>>) {
    let mut lock = GLOBAL_AUDIT_EXPORT_CONFIG
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    apply_config_edge(&mut lock, &EXPORT_DISABLED_AFTER_ENABLE, config);
}

/// Store `config` in `slot` and update `disabled` for the edge.
///
/// Takes the flag as an argument so tests can use a private one.
fn apply_config_edge(
    slot: &mut Option<std::sync::Arc<AuditExportRuntimeConfig>>,
    disabled: &std::sync::atomic::AtomicBool,
    config: Option<std::sync::Arc<AuditExportRuntimeConfig>>,
) {
    use std::sync::atomic::Ordering;

    let was_configured = slot.is_some();
    *slot = config;
    if slot.is_some() {
        disabled.store(false, Ordering::Relaxed);
    } else if was_configured {
        disabled.store(true, Ordering::Relaxed);
    }
}

#[cfg(feature = "db")]
fn export_disabled_after_enable() -> bool {
    EXPORT_DISABLED_AFTER_ENABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Shards an unconfigured tick reports on.
///
/// Mirrors the shard choice of [`fire_due_audit_exports`]: the assignments
/// when a sharded pool has any, else the pool default (or `0` when unsharded).
#[cfg(feature = "db")]
fn disabled_tick_shards(
    pool_default: Option<i32>,
    assignments: &[crate::types::ShardId],
) -> Vec<i32> {
    if pool_default.is_some() && !assignments.is_empty() {
        assignments.iter().map(|s| s.as_i32()).collect()
    } else {
        vec![pool_default.unwrap_or(0)]
    }
}

/// Mark one shard unobserved when export was disabled (issue #1506).
///
/// Does nothing for a process that never had export configured.
#[cfg(feature = "db")]
fn report_export_disabled(
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    shard: u16,
) {
    report_if_disabled(export_disabled_after_enable(), metrics, shard);
}

#[cfg(feature = "db")]
fn report_if_disabled(
    disabled: bool,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    shard: u16,
) {
    if disabled {
        metrics.record_audit_export_observed(shard, false);
    }
}

/// `true` when an audit sink is configured in this process.
///
/// Two callers, and both matter:
/// - [`crate::audit::purge_old_audit_records`] uses it to decide whether the
///   "never purge an unexported record" guard is live. The guard cannot be
///   inferred from the cursor table alone: an empty table means *either*
///   "export was never configured" (purge freely) *or* "export is configured
///   but has not ticked on this shard yet" (purge nothing), and a stale
///   cursor row left behind by a since-disabled exporter would otherwise
///   block retention forever.
/// - `GET /admin/audit-export` reports it as `sink_configured`, so an
///   operator can see export configured on the web app but not on the worker
///   fleet (or vice versa).
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
    /// When this claim's database lease expires — the same `lease_until`
    /// written to the cursor row inside the claim transaction.
    ///
    /// Delivery is bounded by what **remains** of this, not by the full lease
    /// (issue #953, Codex review round 19 P1). The lease starts running when
    /// the claim commits; serializing the batch and reaching the sink consumes
    /// some of it. Bounding the sink call by the whole lease therefore lets a
    /// delivery outlive the row lease, at which point another exporter can
    /// reclaim the shard and bump the epoch, so this attempt's acknowledgement
    /// is refused. With sink latency consistently near the lease that repeats
    /// indefinitely: the cursor never advances and the sink is handed the same
    /// batch over and over.
    pub lease_until: DateTime<Utc>,
}

/// Create this shard's cursor row if it does not exist, and heartbeat it if it
/// does. **Never reactivates a retired cursor** — see issue #1273.
///
/// Deliberately not seeded by the migration: a shard's database cannot know
/// its own shard id (see the migration's comment, and
/// `replication::ensure_generation_row` for the same pattern in #954).
///
/// **`updated_at` doubles as an exporter heartbeat** (issue #953, Codex review
/// P1). Called on every scanner tick, including ticks that claim nothing, so a
/// fresh `updated_at` means "an exporter is running against this shard right
/// now" — durable, shared state that a *different process* can read. That is
/// what lets [`crate::audit::purge_old_audit_records`] protect unexported rows
/// in a split web/worker deployment, where the process running retention may
/// have no sink configured and so cannot answer the question locally.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn ensure_cursor_row(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    // Two defences for the sequence high-water mark, because re-issuing a
    // `(shard, seq)` pair that names a *different* record is the one way to
    // make a receiver deduping on that pair discard genuine audit events.
    //
    // 1. The row is retired rather than deleted ([`decommission_cursor`]), so
    //    `last_assigned_seq` survives even when retention later purges every
    //    stamped row (issue #953).
    // 2. The INSERT arm still seeds from `MAX(export_seq)` rather than 0, for
    //    the paths where the row genuinely went missing anyway: a manual
    //    DELETE, a partial restore (round 4 P1). Restarting at 0 there would
    //    also violate the cursor's `last_acked_seq <= last_assigned_seq` CHECK
    //    the moment a retained batch was acknowledged.
    //
    // On a fresh INSERT `last_acked_seq` stays 0, so a cursor rebuilt from
    // stamped rows re-delivers them rather than assuming they shipped:
    // at-least-once, deduped by the receiver on a now-stable pair, erring
    // toward re-export over silent loss.
    //
    // The ON CONFLICT arm is guarded by `WHERE retired_at IS NULL` (issue
    // #1273). This call used to clear `retired_at` unconditionally.
    //
    // That was a race. A scanner tick reads its config, then calls this
    // function, with no lock held in between. A tick already under way when
    // an operator runs [`decommission_cursor`] can still reach this call
    // afterwards, and used to silently un-retire the shard.
    //
    // Postgres checks an `ON CONFLICT DO UPDATE ... WHERE` predicate under
    // the same lock that resolves the conflict. So this is race-free by
    // construction: whichever of this call and a decommission commits first
    // wins, and a retired row is now a no-op here. It gets no heartbeat and
    // no un-retire.
    //
    // Resuming a retired shard is [`reactivate_cursor`]: an explicit, audited
    // operator action, not a side effect of the exporter noticing new work.
    //
    // A rebuilt row has no chain state. The next keyed claim seeds it from the
    // stored rows, inside its own transaction (issue #1838).
    diesel::sql_query(
        "INSERT INTO harvest_audit_export_cursor (shard_id, last_assigned_seq) \
         SELECT $1, COALESCE(MAX(export_seq), 0) FROM harvest_audit_log \
         ON CONFLICT (shard_id) DO UPDATE SET updated_at = NOW() \
         WHERE harvest_audit_export_cursor.retired_at IS NULL",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// The statement that builds the claim-scan index.
///
/// The exporter runs it on a dedicated connection. An operator runs it once,
/// as the table owner, when the worker role cannot.
pub const UNEXPORTED_INDEX_DDL: &str = "CREATE INDEX CONCURRENTLY IF NOT EXISTS \
     harvest_audit_log_unexported_idx ON harvest_audit_log (occurred_at, id) \
     WHERE export_seq IS NULL";

/// The statement that clears an invalid claim-scan index.
///
/// An operator runs it before [`UNEXPORTED_INDEX_DDL`] when the index exists
/// but is invalid. `IF NOT EXISTS` skips an invalid index, so the build
/// statement alone leaves it in place. Run each statement on its own: a
/// concurrent index statement cannot share a transaction.
pub const UNEXPORTED_INDEX_DROP_DDL: &str =
    "DROP INDEX CONCURRENTLY IF EXISTS harvest_audit_log_unexported_idx";

/// [`UNEXPORTED_INDEX_DDL`] for a named schema.
///
/// `CREATE INDEX` takes an unqualified index name. The index lands in the
/// schema of its table, so only the table is qualified.
#[must_use]
pub fn unexported_index_ddl_in(schema: &str) -> String {
    format!(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_audit_log_unexported_idx \
         ON {schema}.harvest_audit_log (occurred_at, id) WHERE export_seq IS NULL"
    )
}

/// [`UNEXPORTED_INDEX_DROP_DDL`] for a named schema.
#[must_use]
pub fn unexported_index_drop_ddl_in(schema: &str) -> String {
    format!("DROP INDEX CONCURRENTLY IF EXISTS {schema}.harvest_audit_log_unexported_idx")
}

/// Advisory-lock class for builds of the claim-scan index.
///
/// The lock takes two keys: this class and [`UNEXPORTED_INDEX_LOCK_OBJECT_SQL`].
pub const UNEXPORTED_INDEX_LOCK_CLASS: i32 = 0x6175_6469;

/// SQL for the second lock key: the identity of the audit table.
///
/// Advisory locks are scoped to a database. Tenant schemas in one database each
/// hold their own table and index, so the key names the table the session
/// resolves. Only builders of the same table then serialize.
pub const UNEXPORTED_INDEX_LOCK_OBJECT_SQL: &str =
    "hashtext(to_regclass('harvest_audit_log')::oid::text)";

/// What [`ensure_unexported_index`] found or did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnexportedIndexOutcome {
    /// The index exists and `pg_index.indisvalid` is true.
    Ready,
    /// Another session holds the lock for this audit table. Nothing was built.
    /// The caller must try again later.
    LockBusy,
}

/// Build the claim-scan index if it is missing or invalid (issue #1667).
///
/// The index `harvest_audit_log_unexported_idx` matches every audit row while
/// no sink is configured. It would add cost to each insert and serve no read.
/// So no migration creates it. The exporter builds it here, off the export
/// tick.
///
/// The build uses `CREATE INDEX CONCURRENTLY`, so audit inserts continue. A
/// failed concurrent build leaves an invalid index. This function drops and
/// rebuilds such an index. A session advisory lock keeps two exporters from
/// dropping each other's build. A caller that loses the race gets
/// [`UnexportedIndexOutcome::LockBusy`] at once. The claim scan is correct
/// without the index, only slower.
///
/// `conn` must be a dedicated connection, outside a transaction. The build can
/// hold it for minutes, so a pooled connection would starve the pool. On `Err`
/// the caller must drop the connection: the advisory lock may still be held,
/// and `statement_timeout` may still be off.
///
/// [`UnexportedIndexOutcome::Ready`] is returned only after a fresh read of
/// `pg_index.indisvalid` confirms the index.
///
/// # Errors
/// Returns `HarvestError` on a database failure. A role that does not own the
/// table gets a privilege error; see [`index_build_needs_owner`].
#[cfg(feature = "db")]
pub async fn ensure_unexported_index(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<UnexportedIndexOutcome> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Flag {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        flag: bool,
    }

    if unexported_index_valid(conn).await? == Some(true) {
        return Ok(UnexportedIndexOutcome::Ready);
    }
    let locked: Vec<Flag> = diesel::sql_query(format!(
        "SELECT pg_try_advisory_lock($1, {UNEXPORTED_INDEX_LOCK_OBJECT_SQL}) AS flag"
    ))
    .bind::<diesel::sql_types::Integer, _>(UNEXPORTED_INDEX_LOCK_CLASS)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    if !locked.into_iter().next().is_some_and(|row| row.flag) {
        return Ok(UnexportedIndexOutcome::LockBusy);
    }
    let built = build_unexported_index(conn).await;
    // The lock belongs to the session. A failed unlock leaves the lock on a
    // live session, so this error takes precedence over the build result.
    diesel::sql_query(format!(
        "SELECT pg_advisory_unlock($1, {UNEXPORTED_INDEX_LOCK_OBJECT_SQL})"
    ))
    .bind::<diesel::sql_types::Integer, _>(UNEXPORTED_INDEX_LOCK_CLASS)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    built?;
    // `CREATE INDEX CONCURRENTLY` can return without an error and still leave
    // an invalid index. Only the catalog proves the build.
    match unexported_index_valid(conn).await? {
        Some(true) => Ok(UnexportedIndexOutcome::Ready),
        state => Err(crate::error::HarvestError::Database(format!(
            "harvest_audit_log_unexported_idx is not valid after the build: {state:?}"
        ))),
    }
}

/// Whether an [`ensure_unexported_index`] error means the role cannot build.
///
/// `CREATE INDEX` on `harvest_audit_log` needs table ownership. A worker role
/// with DML rights only gets SQLSTATE `42501`. A retry cannot fix that. The
/// caller logs the statements an operator must run and waits
/// [`INDEX_BUILD_REFUSED_RETRY`].
#[cfg(feature = "db")]
#[must_use]
pub fn index_build_needs_owner(error: &crate::error::HarvestError) -> bool {
    match error {
        crate::error::HarvestError::Database(message) => {
            message.contains("42501")
                || message.contains("must be owner")
                || message.contains("permission denied")
        }
        _ => false,
    }
}

/// `None` when the index is missing, else whether it is valid.
#[cfg(feature = "db")]
async fn unexported_index_valid(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Option<bool>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        valid: bool,
    }

    // Bind the index to the table this session resolves. An index of the same
    // name in another schema of `search_path` must not count.
    let rows: Vec<State> = diesel::sql_query(
        "SELECT i.indisvalid AS valid FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE c.relname = 'harvest_audit_log_unexported_idx' \
           AND i.indrelid = to_regclass('harvest_audit_log')",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(rows.into_iter().next().map(|row| row.valid))
}

/// Build the index. The caller holds the advisory lock for this audit table.
#[cfg(feature = "db")]
async fn build_unexported_index(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Setting {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    // A role or database timeout shorter than the build would fail it on every
    // try. A short `lock_timeout` fails the concurrent statements the same
    // way while they wait for a conflicting lock. The drop of an invalid index
    // waits like the build, so the override covers both statements. Capture
    // each session value first and restore that exact value after. `RESET`
    // would restore the role or database default instead.
    const OVERRIDDEN: [&str; 2] = ["statement_timeout", "lock_timeout"];
    let mut previous = Vec::with_capacity(OVERRIDDEN.len());
    for name in OVERRIDDEN {
        let value: Vec<Setting> =
            diesel::sql_query(format!("SELECT current_setting('{name}') AS value"))
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;
        previous.push(
            value
                .into_iter()
                .next()
                .map_or_else(|| "0".to_owned(), |row| row.value),
        );
    }
    for name in OVERRIDDEN {
        diesel::sql_query(format!("SET {name} = 0"))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }
    let built = repair_and_build_unexported_index(conn).await;
    let mut restored = Ok(());
    for (name, value) in OVERRIDDEN.into_iter().zip(previous) {
        let result = diesel::sql_query(format!("SELECT set_config('{name}', $1, false)"))
            .bind::<diesel::sql_types::Text, _>(value)
            .execute(conn)
            .await
            .map(|_| ())
            .map_err(crate::error::database_error);
        restored = restored.and(result);
    }
    built?;
    restored?;
    Ok(())
}

/// Drop an invalid index, then build it. The caller disables the timeouts.
#[cfg(feature = "db")]
async fn repair_and_build_unexported_index(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    // Re-check under the lock: another exporter may have finished the build.
    match unexported_index_valid(conn).await? {
        Some(true) => return Ok(()),
        Some(false) => {
            diesel::sql_query("DROP INDEX CONCURRENTLY IF EXISTS harvest_audit_log_unexported_idx")
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        }
        None => {}
    }
    tracing::info!("[audit_export] building harvest_audit_log_unexported_idx (issue #1667)");
    diesel::sql_query(UNEXPORTED_INDEX_DDL)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// A fingerprint of a database URL for use in a gate key.
///
/// The gates are process-wide statics. A URL can carry a password, and a key
/// outlives the worker that made it. So a key holds this hash and never the
/// URL text. The hash is stable inside one process, which is all a gate needs.
#[cfg(feature = "db")]
fn dsn_fingerprint(dsn: &str) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    dsn.hash(&mut hasher);
    hasher.finish()
}

/// A shard of one audit table. The URL separates databases that share a shard
/// number inside one process. The schema separates tenant tables in one
/// database.
#[cfg(feature = "db")]
type BuildKey = (i32, u64, String);

/// Earliest time each shard may try another background index build.
#[cfg(feature = "db")]
static INDEX_BUILD_GATE: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<BuildKey, std::time::Instant>>,
> = std::sync::LazyLock::new(Default::default);

/// Longest wait for the build connection to open.
#[cfg(feature = "db")]
const INDEX_BUILD_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Wait between repeats of an operator notice about the index.
#[cfg(feature = "db")]
const INDEX_NOTICE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// A shard of one audit table in one database. The identity names the database
/// and the schema, so neither merges with another that shares a shard number.
#[cfg(feature = "db")]
type NoticeKey = (i32, String);

/// When each shard in each database last logged an operator notice.
#[cfg(feature = "db")]
static INDEX_NOTICE_GATE: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<NoticeKey, std::time::Instant>>,
> = std::sync::LazyLock::new(Default::default);

/// Whether this shard may log an operator notice about the index now.
#[cfg(feature = "db")]
fn index_notice_due(key: &NoticeKey) -> bool {
    let mut gate = INDEX_NOTICE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    if gate
        .get(key)
        .is_some_and(|last| now.duration_since(*last) < INDEX_NOTICE_INTERVAL)
    {
        return false;
    }
    gate.retain(|_, last| now.duration_since(*last) < INDEX_NOTICE_INTERVAL);
    gate.insert(key.clone(), now);
    true
}

/// Join the parts that name a database cluster.
///
/// A Unix-socket connection has no address and no port. The start time of the
/// postmaster then tells two clusters with the same database name apart.
#[cfg(feature = "db")]
fn database_identity_of(name: &str, addr: &str, port: &str, started: &str) -> String {
    format!("{name}@{addr}:{port}@{started}")
}

/// Name the database behind a connection: its name, host address, port and
/// postmaster start time.
///
/// An empty name on error only merges the gates of the databases it cannot
/// name.
#[cfg(feature = "db")]
async fn database_identity(conn: &mut diesel_async::AsyncPgConnection) -> String {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Identity {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        addr: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        port: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        started: String,
    }

    let rows: Result<Vec<Identity>, _> = diesel::sql_query(
        "SELECT current_database()::text AS name, \
                COALESCE(inet_server_addr()::text, '') AS addr, \
                COALESCE(inet_server_port()::text, '') AS port, \
                pg_postmaster_start_time()::text AS started",
    )
    .load(conn)
    .await;
    rows.ok()
        .and_then(|rows| rows.into_iter().next())
        .map(|row| database_identity_of(&row.name, &row.addr, &row.port, &row.started))
        .unwrap_or_default()
}

/// Run `future` unless `cancel` fires first. `None` means shutdown won.
///
/// A catalog probe on a stalled connection never returns. Graceful shutdown
/// awaits the checker, so each probe must race the cancel token.
#[cfg(feature = "db")]
async fn until_cancelled<F: std::future::Future>(
    cancel: &tokio_util::sync::CancellationToken,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        output = future => Some(output),
        () = cancel.cancelled() => None,
    }
}

/// Log an operator notice once per interval, only while the index is not valid.
///
/// A valid index never uses the gate, so a healthy database cannot hide the
/// notice of another database that has the same shard number.
#[cfg(feature = "db")]
async fn index_notice_wanted(conn: &mut diesel_async::AsyncPgConnection, shard_id: i32) -> bool {
    if matches!(unexported_index_valid(conn).await, Ok(Some(true))) {
        return false;
    }
    // Tenant schemas in one database share its identity and may share a shard
    // number, so the resolved schema is part of the key.
    let schema = audit_table_schema(conn).await.unwrap_or_default();
    let key = notice_key(shard_id, &database_identity(conn).await, &schema);
    index_notice_due(&key)
}

/// The throttle key for one shard of one audit table in one database.
#[cfg(feature = "db")]
fn notice_key(shard_id: i32, database: &str, schema: &str) -> NoticeKey {
    (shard_id, format!("{database}/{schema}"))
}

/// A shard, build URL and pool. The pool separates tenant schemas that share a
/// shard number and a build URL.
#[cfg(feature = "db")]
type ProbeKey = (i32, u64, usize);

/// Shortest wait between two catalog probes for one [`ProbeKey`].
#[cfg(feature = "db")]
const INDEX_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// When each [`ProbeKey`] may next probe the catalogs.
#[cfg(feature = "db")]
static INDEX_PROBE_GATE: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<ProbeKey, std::time::Instant>>,
> = std::sync::LazyLock::new(Default::default);

/// Whether this checker may probe the index catalogs now.
///
/// A healthy exporter ticks every poll interval. Two catalog reads per tick
/// would run for as long as the worker lives. One probe per
/// [`INDEX_PROBE_INTERVAL`] bounds that cost, and still finds a dropped or
/// invalid index within the interval.
#[cfg(feature = "db")]
fn index_probe_due(key: &ProbeKey) -> bool {
    let mut gate = INDEX_PROBE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    if gate.get(key).is_some_and(|not_before| now < *not_before) {
        return false;
    }
    // An expired entry means the same as no entry. Evict it, so churn in
    // workers and pools cannot grow the map.
    gate.retain(|_, not_before| now < *not_before);
    gate.insert(*key, now + INDEX_PROBE_INTERVAL);
    true
}

/// Wait after a failed or skipped background build before the next attempt.
#[cfg(feature = "db")]
const INDEX_BUILD_RETRY: std::time::Duration = std::time::Duration::from_secs(300);

/// Gate wait while a build task is alive. The end of the task replaces it.
#[cfg(feature = "db")]
const INDEX_BUILD_IN_FLIGHT: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 3600);

/// Wait after a build the role may not run. Only an operator can fix it.
#[cfg(feature = "db")]
pub const INDEX_BUILD_REFUSED_RETRY: std::time::Duration = std::time::Duration::from_secs(3600);

/// How a background build ended, and so when the shard may try again.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildEnd {
    /// The catalog confirmed a valid index. The gate opens at once.
    Ready,
    /// A failure, a lost lock race, or a shutdown. Wait [`INDEX_BUILD_RETRY`].
    Retry,
    /// The role cannot build. Wait [`INDEX_BUILD_REFUSED_RETRY`].
    Refused,
}

/// Holds the in-flight mark of a build gate.
///
/// Shutdown can drop a build future at any await point. A dropped guard turns
/// the gate into an ordinary retry wait, so the in-flight period never
/// outlives the future that owns it. `disarm` hands the end of the build to
/// the caller.
#[cfg(feature = "db")]
struct InFlightGuard(Option<BuildKey>);

#[cfg(feature = "db")]
impl InFlightGuard {
    const fn new(key: BuildKey) -> Self {
        Self(Some(key))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

#[cfg(feature = "db")]
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(key) = self.0.take() {
            index_build_finished(&key, BuildEnd::Retry);
        }
    }
}

/// Claim the right to start a build for this shard. Marks it in flight.
#[cfg(feature = "db")]
fn index_build_due(key: &BuildKey) -> bool {
    let mut gate = INDEX_BUILD_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    if gate.get(key).is_some_and(|not_before| now < *not_before) {
        return false;
    }
    // The task is in flight until it calls `index_build_finished`. A task can
    // stall after it connects, and a build can outlive any retry wait. A timed
    // gate would then reopen and stack another task per wait. So the gate stays
    // closed while the task lives, and its end sets the next wait.
    gate.retain(|_, not_before| now < *not_before);
    gate.insert(key.clone(), now + INDEX_BUILD_IN_FLIGHT);
    true
}

/// Record how a build ended. Only [`BuildEnd::Ready`] opens the gate.
#[cfg(feature = "db")]
fn index_build_finished(key: &BuildKey, end: BuildEnd) {
    let mut gate = INDEX_BUILD_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    match end {
        BuildEnd::Ready => {
            gate.remove(key);
        }
        BuildEnd::Retry => {
            gate.insert(key.clone(), now + INDEX_BUILD_RETRY);
        }
        BuildEnd::Refused => {
            gate.insert(key.clone(), now + INDEX_BUILD_REFUSED_RETRY);
        }
    }
}

/// Open a dedicated connection to `dsn` and run [`ensure_unexported_index`].
///
/// The connection is never pooled. It closes when this function returns, or
/// when the caller drops the future. Postgres then cancels a running build and
/// leaves an invalid index, which the next attempt drops and rebuilds.
#[cfg(feature = "db")]
async fn build_unexported_index_on_dedicated_connection(
    dsn: &str,
    schema: &str,
    connect_timeout: std::time::Duration,
) -> crate::error::HarvestResult<UnexportedIndexOutcome> {
    use diesel_async::RunQueryDsl;

    // A host can accept the socket and never finish the handshake. Without a
    // bound, the stalled task outlives the retry gate and tasks pile up. The
    // connector follows the listener's transport rule, so `sslmode=require`
    // gets verified TLS.
    let mut conn = tokio::time::timeout(connect_timeout, crate::notify::connect_async_pg(dsn))
        .await
        .map_err(|_| {
            crate::error::HarvestError::Database(format!(
                "connect to the index build URL timed out after {connect_timeout:?}"
            ))
        })??;
    // The pool may select its relations through `search_path`, and the build
    // role can differ from the pool role. `schema` names the schema that holds
    // the pool session's table, so no `"$user"` token is re-expanded here.
    diesel::sql_query("SELECT set_config('search_path', $1, false)")
        .bind::<diesel::sql_types::Text, _>(schema)
        .execute(&mut conn)
        .await
        .map_err(crate::error::database_error)?;
    let outcome = ensure_unexported_index(&mut conn).await;
    // On `Err` the session may still hold the advisory lock. Closing the
    // connection releases it either way.
    drop(conn);
    outcome
}

/// The quoted name of the schema that holds `harvest_audit_log` for the
/// pool's own session.
#[cfg(feature = "db")]
async fn audit_table_schema(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<String> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Schema {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    let rows: Vec<Schema> = diesel::sql_query(
        "SELECT quote_ident(n.nspname)::text AS value FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = to_regclass('harvest_audit_log')",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    rows.into_iter().next().map(|row| row.value).ok_or_else(|| {
        crate::error::HarvestError::Database(
            "harvest_audit_log is not visible to the pool session".to_owned(),
        )
    })
}

/// The build and cleanup statements for the schema this session resolves.
///
/// An operator may run them through a role with another default path. Without
/// a resolved schema they fall back to the unqualified statements.
#[cfg(feature = "db")]
async fn operator_ddl(conn: &mut diesel_async::AsyncPgConnection) -> (String, String) {
    audit_table_schema(conn).await.map_or_else(
        |_| {
            (
                UNEXPORTED_INDEX_DDL.to_owned(),
                UNEXPORTED_INDEX_DROP_DDL.to_owned(),
            )
        },
        |schema| {
            (
                unexported_index_ddl_in(&schema),
                unexported_index_drop_ddl_in(&schema),
            )
        },
    )
}

/// [`build_unexported_index_on_dedicated_connection`] under its own DR fence
/// (issue #1823).
///
/// The build outlives the export tick and its fence. So it takes a fence of
/// its own, and a bump waits for the DDL. A fenced or held shard, or a lost
/// guard, returns an error, and the caller then waits to retry.
#[cfg(feature = "db")]
async fn build_unexported_index_fenced(
    pool: &crate::worker::DbPool,
    fence_key: crate::types::ShardId,
    dsn: &str,
    schema: &str,
) -> crate::error::HarvestResult<UnexportedIndexOutcome> {
    let fence = crate::replication::begin_fenced_group(pool, fence_key).await?;
    crate::replication::run_fenced_pass(
        &fence,
        Box::pin(build_unexported_index_on_dedicated_connection(
            dsn,
            schema,
            INDEX_BUILD_CONNECT_TIMEOUT,
        )),
    )
    .await
    .and_then(|built| built)
}

/// Start the index build in a detached task, off the export tick (issue #1667).
///
/// The build can take minutes on a large table. It must not delay a claim,
/// the lag gauge or the lease. It must not hold a pooled connection either: a
/// shard pool may have one connection, and that connection serves the export.
/// So the task opens its own connection to `build_dsn`, the database URL of
/// this shard, and closes it when the build ends. A pool of any size exports
/// while the build runs.
///
/// `conn` is the tick's own connection. It serves one catalog read here and
/// nothing longer.
///
/// With no `build_dsn` the exporter cannot build. It logs
/// [`UNEXPORTED_INDEX_DDL`] for an operator once per
/// [`INDEX_NOTICE_INTERVAL`]. Export is correct without the index, only
/// slower.
///
/// The gate for this shard opens only after the catalog confirms a valid
/// index. A lost lock race, a failure or a shutdown keeps it closed for
/// [`INDEX_BUILD_RETRY`]. A role that cannot build keeps it closed for
/// [`INDEX_BUILD_REFUSED_RETRY`], after one `error!` line that names the
/// statements an operator must run.
///
/// `cancel` stops a running build. Dropping the build future closes its
/// connection, and Postgres cancels the statement.
#[cfg(feature = "db")]
async fn spawn_unexported_index_build_if_due(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    pool: &crate::worker::DbPool,
    fence_key: crate::types::ShardId,
    build_dsn: Option<&str>,
    cancel: &tokio_util::sync::CancellationToken,
) {
    let pool_id = std::ptr::from_ref(pool.manager()) as usize;
    // One probe per interval. Without it, every tick of a healthy exporter
    // would read the catalogs.
    if !index_probe_due(&(shard_id, build_dsn.map_or(0, dsn_fingerprint), pool_id)) {
        return;
    }
    let Some(dsn) = build_dsn else {
        if index_notice_wanted(conn, shard_id).await {
            let (statement, cleanup) = operator_ddl(conn).await;
            tracing::warn!(
                shard = shard_id,
                statement = %statement,
                cleanup = %cleanup,
                "[audit_export] no database URL for a dedicated connection, so the exporter \
                 cannot build the claim-scan index; export continues without it. Set \
                 WorkerConfig::with_notification_database_url or \
                 with_shard_notification_database_urls, or run the statement once as the \
                 table owner. If the index exists but is invalid, run `cleanup` first"
            );
        }
        return;
    };
    // The schema is part of the key: tenant schemas can share a shard number
    // and a build URL while they manage different indexes.
    let schema = match audit_table_schema(conn).await {
        Ok(schema) => schema,
        Err(error) => {
            tracing::warn!(shard = shard_id, %error, "[audit_export] could not resolve the schema of harvest_audit_log");
            return;
        }
    };
    let key: BuildKey = (shard_id, dsn_fingerprint(dsn), schema.clone());
    if !index_build_due(&key) {
        return;
    }
    // Dropping this future mid-probe, or panicking, leaves a retry wait.
    let guard = InFlightGuard::new(key.clone());
    match unexported_index_valid(conn).await {
        Ok(Some(true)) => {
            index_build_finished(&key, BuildEnd::Ready);
            guard.disarm();
            return;
        }
        Ok(_) => {}
        Err(error) => {
            // The guard turns the gate into a retry wait.
            tracing::warn!(shard = shard_id, %error, "[audit_export] could not inspect the claim-scan index");
            return;
        }
    }
    let dsn = dsn.to_owned();
    let cancel = cancel.clone();
    let pool = pool.clone();
    tokio::spawn(async move {
        // The task owns the in-flight mark now. A panic or an abort that drops
        // the task leaves a retry wait too.
        let guard = guard;
        let end = tokio::select! {
            result = build_unexported_index_fenced(&pool, fence_key, &dsn, &schema) => match result {
                Ok(UnexportedIndexOutcome::Ready) => BuildEnd::Ready,
                Ok(UnexportedIndexOutcome::LockBusy) => {
                    tracing::debug!(
                        shard = shard_id,
                        "[audit_export] another session is building the claim-scan index"
                    );
                    BuildEnd::Retry
                }
                Err(error) if index_build_needs_owner(&error) => {
                    tracing::error!(
                        shard = shard_id,
                        %error,
                        statement = %unexported_index_ddl_in(&schema),
                        cleanup = %unexported_index_drop_ddl_in(&schema),
                        retry_in_secs = INDEX_BUILD_REFUSED_RETRY.as_secs(),
                        "[audit_export] the worker role cannot build the claim-scan index; \
                         export continues without it. Run `statement` once through the \
                         role that owns the table, such as the migration role. If the \
                         index exists but is invalid, run `cleanup` first"
                    );
                    BuildEnd::Refused
                }
                Err(error) => {
                    tracing::warn!(
                        shard = shard_id,
                        %error,
                        statement = %unexported_index_ddl_in(&schema),
                        "[audit_export] could not build the claim-scan index; export continues \
                         without it. The next attempt follows after the retry wait"
                    );
                    BuildEnd::Retry
                }
            },
            () = cancel.cancelled() => {
                tracing::info!(
                    shard = shard_id,
                    "[audit_export] shutdown requested during the claim-scan index build; \
                     closing its connection"
                );
                BuildEnd::Retry
            }
        };
        index_build_finished(&key, end);
        guard.disarm();
    });
}

/// Tell an operator once per [`INDEX_NOTICE_INTERVAL`] that the index is
/// missing (issue #1667).
///
/// [`fire_due_audit_exports`] runs on a connection its caller owns. It never
/// builds the index there. The build would hold that connection for minutes,
/// and an error could leave the session with the advisory lock held. The
/// dedicated export task builds the index on its own connection. An embedder
/// that drives this primitive by hand runs [`ensure_unexported_index`] on a
/// dedicated connection, or runs [`UNEXPORTED_INDEX_DDL`] as the table owner.
///
/// Each call reads the index catalog once. A valid index ends the call there.
/// This path has no pool and no state handle, so it has no key that names a
/// database without a query. A throttle keyed by the connection address would
/// alias unrelated databases. The caller sets the cost with its own cadence.
/// The pooled exporter throttles its probes per shard, URL and pool.
#[cfg(feature = "db")]
async fn notice_missing_unexported_index(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) {
    if !index_notice_wanted(conn, shard_id).await {
        return;
    }
    let (statement, cleanup) = operator_ddl(conn).await;
    tracing::warn!(
        shard = shard_id,
        statement = %statement,
        cleanup = %cleanup,
        "[audit_export] the claim-scan index is missing; fire_due_audit_exports never builds \
         it on its caller's connection. Use the dedicated export task, run \
         ensure_unexported_index on a dedicated connection, or run the statement once as \
         the table owner. If the index exists but is invalid, run `cleanup` first"
    );
}

/// Result of resolving a decommission or reactivate request against the live
/// cursor. The two share a shape because they are inverse transitions of the
/// same state machine — see issue #1273.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecommissionOutcome {
    /// The cursor moved from active to retired.
    Retired,
    /// The cursor was already retired. Idempotent: no write happened.
    AlreadyRetired,
    /// No cursor exists for this shard. There is nothing to retire.
    NotConfigured,
}

/// The reactivate-side counterpart of [`DecommissionOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactivateOutcome {
    /// The cursor moved from retired back to active.
    Reactivated,
    /// The cursor was already active. Idempotent: no write happened.
    AlreadyActive,
    /// No cursor exists for this shard. There is nothing to reactivate.
    NotConfigured,
}

/// Retire a shard's export cursor, re-enabling audit retention there.
///
/// Opens its own transaction. The management route pairs the retirement with
/// its own audit record, so it uses [`decommission_cursor_locked`] instead —
/// see that function.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn decommission_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<DecommissionOutcome> {
    use diesel_async::AsyncConnection;

    Box::pin(
        conn.transaction::<DecommissionOutcome, crate::error::HarvestError, _>(async |conn| {
            decommission_cursor_locked(conn, shard_id, now).await
        }),
    )
    .await
}

/// [`decommission_cursor`] without the surrounding transaction.
///
/// The **explicit decommission step** for audit export (issue #953, Codex
/// review round 3 P1). While a cursor row exists,
/// [`crate::audit::purge_old_audit_records`] will not delete a record that
/// shard still owes the sink — the row's presence, not a timeout, is what says
/// "an exporter is responsible for this shard".
///
/// This deliberately replaced a 24-hour heartbeat TTL. A TTL cannot tell
/// "export was intentionally removed" from "the worker has been down since
/// Friday". It used to resolve that ambiguity by *deleting audit records* —
/// the one outcome this feature exists to prevent, arriving precisely during
/// an outage. Unbounded table growth is the strictly better failure: it is
/// loud (`harvest.audit.export_lag`, the `last_error` on
/// `GET /admin/audit-export`), it is bounded by the genuine unexported
/// backlog rather than by the whole table (fully-acknowledged records are
/// purged normally), and it is *reversible* — deleted audit records are not.
///
/// So retiring an export configuration is an operator action, not an inferred
/// one. See `docs/audit-export.md`.
///
/// The row is **retired, never deleted** (issue #953, Codex review round 7 P1).
/// `last_assigned_seq` has to outlive the audit rows themselves, and retiring
/// the cursor is precisely what lets retention purge them: seeding a recreated
/// cursor from `MAX(export_seq)` preserves the counter only while at least one
/// stamped row survives, so a decommission followed by a full purge would reset
/// it to 0 and re-issue `(shard, seq)` pairs the SIEM still holds against
/// different records. Keeping the row makes the high-water mark durable
/// independently of retention.
///
/// Reversible via [`reactivate_cursor`], an explicit, audited operator action
/// that resumes from the preserved `last_assigned_seq`. Records purged while
/// retired are gone and are not re-delivered — `last_acked_seq` is preserved
/// too.
///
/// **Must be called inside a transaction.** On an autocommit connection the
/// row lock below is released before the caller can pair it with anything.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn decommission_cursor_locked(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<DecommissionOutcome> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(crate::models::AuditExportCursor::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let Some(cursor) = cursor else {
        return Ok(DecommissionOutcome::NotConfigured);
    };
    if cursor.retired_at.is_some() {
        return Ok(DecommissionOutcome::AlreadyRetired);
    }

    // Bumping `claim_epoch` invalidates any delivery still in flight (issue
    // #953, Codex review round 13 P2). `apply_outcome` is guarded on
    // `(shard_id, claim_epoch)` alone, so without this an attempt claimed
    // before the retirement could land after it -- advancing the cursor or
    // writing backoff state onto a row the status route now reports as a
    // frozen `RETIRED` snapshot, and racing retention, which is permitted to
    // purge the shard the moment it is retired. Clearing `lease_until` in the
    // same statement means the row does not also read as mid-delivery.
    diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
        .set((
            cur::retired_at.eq(Some(now)),
            cur::updated_at.eq(now),
            cur::claim_epoch.eq(cursor.claim_epoch + 1),
            cur::lease_until.eq(None::<DateTime<Utc>>),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(DecommissionOutcome::Retired)
}

/// Reactivate a shard's retired export cursor.
///
/// Opens its own transaction. The management route pairs the reactivation
/// with its own audit record, so it uses [`reactivate_cursor_locked`] instead
/// — see that function.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn reactivate_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<ReactivateOutcome> {
    use diesel_async::AsyncConnection;

    Box::pin(
        conn.transaction::<ReactivateOutcome, crate::error::HarvestError, _>(async |conn| {
            reactivate_cursor_locked(conn, shard_id, now).await
        }),
    )
    .await
}

/// [`reactivate_cursor`] without the surrounding transaction.
///
/// The **explicit reactivate step** for audit export (issue #1273). It
/// resumes a shard [`decommission_cursor_locked`] retired, from the
/// preserved `last_assigned_seq`. New records continue the sequence, rather
/// than re-issuing numbers a receiver already holds against different
/// records.
///
/// This used to happen as a side effect of [`ensure_cursor_row`]: the next
/// scanner tick after a re-enable un-retired the row on its own. That made
/// resumption racy and silent. A scanner tick already under way when an
/// operator retired a shard could un-retire it moments later. Neither
/// transition left a record of who asked for it. Reactivation is now its
/// own operator action, audited exactly like [`decommission_cursor_locked`].
///
/// **Must be called inside a transaction.** On an autocommit connection the
/// row lock below is released before the caller can pair it with anything.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn reactivate_cursor_locked(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<ReactivateOutcome> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(crate::models::AuditExportCursor::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let Some(cursor) = cursor else {
        return Ok(ReactivateOutcome::NotConfigured);
    };
    if cursor.retired_at.is_none() {
        return Ok(ReactivateOutcome::AlreadyActive);
    }

    // Bumps `claim_epoch` for the same reason decommission does: every
    // lifecycle transition invalidates a delivery attempt claimed under an
    // older one. No claim can be outstanding on a retired row today, since
    // a retired cursor is not claimable. So this guards a future change to
    // that rule, not a live hazard.
    diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
        .set((
            cur::retired_at.eq(None::<DateTime<Utc>>),
            cur::updated_at.eq(now),
            cur::claim_epoch.eq(cursor.claim_epoch + 1),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(ReactivateOutcome::Reactivated)
}

/// The `MAX(export_seq)` read back from the sequence-assignment statement —
/// the high-water mark of sequences actually handed out.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct HighWater {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    high_water: i64,
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
pub async fn claim_shard(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    batch_size: i64,
    lease: std::time::Duration,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<ClaimedBatch>> {
    claim_shard_chained(conn, shard_id, batch_size, lease, now, None).await
}

/// [`claim_shard`], and stamp the audit hash chain with `chain_key`
/// (issue #1838).
///
/// The chain covers exactly the rows this claim sequences. It is stamped in
/// the same transaction, under the same cursor row lock, so two exporters
/// can never fork the chain. `None` stamps no chain, as [`claim_shard`].
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)] // claim + assign + load is one atomic unit
pub async fn claim_shard_chained(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    batch_size: i64,
    lease: std::time::Duration,
    now: DateTime<Utc>,
    chain_key: Option<&crate::audit_chain::AuditChainKey>,
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

            // A retired cursor is inert (issue #953; issue #1273).
            // `export_once_on_conn` calls `ensure_cursor_row` first. That
            // call never un-retires a row. So a decommissioned shard reaches
            // this check on every tick, not just in a narrow race window.
            //
            // A decommission that commits between `ensure_cursor_row` and
            // this locked read hits the same check. Without it, the scanner
            // would take a new claim and deliver a batch after the
            // retirement. Bumping the epoch on retirement only invalidates
            // claims taken *before* it. So this is the other half of that
            // fix. It matters because retention may purge the shard's
            // records the moment it is retired.
            if cursor.retired_at.is_some() {
                return Ok(None);
            }

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
            // The high-water mark is read back from the rows actually
            // stamped (`MAX(export_seq)`), never inferred from the affected
            // row count (issue #953 review): the two diverge if any row in
            // the CTE's snapshot disappears before the UPDATE takes its
            // lock, and a high-water mark one short of the highest sequence
            // handed out would let the NEXT tick reissue that sequence to a
            // different record -- two audit records sharing a `(shard, seq)`,
            // which a receiver deduping on that key silently drops.
            //
            // The outer `AND a.export_seq IS NULL` repeats the CTE's
            // predicate under the row lock for the same reason: without it
            // the UPDATE would overwrite a sequence another writer assigned
            // between the CTE's evaluation and the lock.
            let assigned: HighWater = diesel::sql_query(
                "WITH claimed AS ( \
                     SELECT id, row_number() OVER (ORDER BY occurred_at, id) AS rn \
                     FROM harvest_audit_log \
                     WHERE export_seq IS NULL \
                     ORDER BY occurred_at, id \
                     LIMIT $2 \
                 ), stamped AS ( \
                     UPDATE harvest_audit_log a \
                     SET export_seq = $1 + claimed.rn \
                     FROM claimed \
                     WHERE a.id = claimed.id AND a.export_seq IS NULL \
                     RETURNING a.export_seq \
                 ) \
                 SELECT COALESCE(MAX(export_seq), $1) AS high_water FROM stamped",
            )
            .bind::<diesel::sql_types::BigInt, _>(cursor.last_assigned_seq)
            .bind::<diesel::sql_types::BigInt, _>(batch_size)
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;

            let last_assigned_seq = assigned.high_water.max(cursor.last_assigned_seq);

            // ── Stamp the hash chain over the rows just sequenced ─────────
            //
            // Rows sequenced on an earlier tick keep whatever they had. So a
            // key set later starts the chain at the next sequence.
            let stamped = match chain_key {
                Some(key) if last_assigned_seq > cursor.last_assigned_seq => {
                    // Extend only a checkpoint the key accepts. Otherwise a
                    // writer could move the head and have it re-signed.
                    match crate::audit_chain::chain_anchor(conn, &cursor, key).await? {
                        Ok(anchor) => crate::audit_chain::stamp_chain(
                            conn,
                            shard_id,
                            key.secret(),
                            cursor.last_assigned_seq,
                            last_assigned_seq,
                            &anchor,
                        )
                        .await?
                        .map(|stamped| (key, stamped, anchor.start_seq)),
                        Err(crate::audit_chain::ChainRefusal::Checkpoint) => {
                            tracing::error!(
                                shard_id,
                                "the audit chain checkpoint is missing or does not verify; \
                                 new rows stay unchained until an operator calls \
                                 audit_chain::reanchor_shard_chain"
                            );
                            None
                        }
                        Err(crate::audit_chain::ChainRefusal::SharedDatabase) => {
                            tracing::error!(
                                shard_id,
                                "the audit chain is off: another shard's live export cursor \
                                 shares this database, so the two share the export_seq space"
                            );
                            None
                        }
                    }
                }
                _ => None,
            };
            // The keyed checkpoint moves with the head (issue #1838).
            let checkpoint = stamped.map(|(key, stamped, start_seq)| {
                let checkpoint = crate::audit_chain::ChainCheckpoint {
                    start_seq: start_seq.unwrap_or(stamped.first_seq),
                    head_seq: stamped.head_seq,
                    head: stamped.head,
                    newest_at: stamped.newest_at,
                };
                (checkpoint, key)
            });

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
                if let Some((checkpoint, key)) = checkpoint {
                    crate::audit_chain::write_checkpoint(conn, shard_id, &checkpoint, key).await?;
                }
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

            // `export_seq` is non-NULL by the filter above. A row without one
            // is skipped, not exported with a fabricated sequence.
            let records = rows
                .into_iter()
                .filter_map(|row| AuditExportRecord::from_row(shard_id, row))
                .collect::<Vec<_>>();

            if records.is_empty() {
                return Ok(None);
            }

            Ok(Some(ClaimedBatch {
                shard: shard_id,
                claim_epoch,
                consecutive_failures: cursor.consecutive_failures,
                records,
                lease_until,
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
    retired_at: Option<DateTime<Utc>>,
) -> &'static str {
    // A retired cursor's other columns are a frozen snapshot of whatever the
    // exporter last did (issue #953, Codex review round 9 P2). Deriving IDLE,
    // BACKOFF or RETRYING from them would tell an operator that records are
    // pending delivery when no exporter owes them and retention is free to
    // purge them. `RETIRED` is checked first because it overrides every other
    // reading of those columns.
    if retired_at.is_some() {
        return "RETIRED";
    }
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
    /// `IDLE` | `DELIVERING` | `BACKOFF` | `RETRYING` | `RETIRED`, from
    /// [`delivery_state`]. `GET /admin/audit-export` additionally synthesizes
    /// `NOT_STARTED` for a shard with no cursor row at all, which this struct
    /// cannot represent (there is no cursor to describe).
    ///
    /// `RETIRED` means an operator ran `decommission_cursor`: the row survives
    /// only to preserve the sequence high-water mark, no exporter owes this
    /// shard records, and retention may purge them. The remaining fields are a
    /// frozen snapshot of the last export activity, not a live one.
    pub delivery_state: String,
    pub consecutive_failures: i32,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub last_delivered_at: Option<DateTime<Utc>>,
    pub next_attempt_at: DateTime<Utc>,
}

/// How many of the lowest-sequence pending rows [`export_lag_seconds`] scans
/// for the true oldest `occurred_at` (issue #1271).
///
/// `occurred_at` is transaction start time. A long transaction can commit
/// after a shorter one that started later. The exporter then sees the long
/// transaction later and assigns it a higher sequence. Its `occurred_at`
/// stays older than the short transaction's.
///
/// A lookup over only the single lowest-sequence pending row misses this
/// skew. It reports the short transaction's age instead of the true lag.
///
/// This bound trades exactness for a fixed cost. It scans the lowest
/// [`EXPORT_LAG_LOOKBACK_ROWS`] pending rows and takes their minimum
/// `occurred_at`. This finds the true oldest row whenever the skew resolves
/// within that many rows, which covers every ordinary case. The scan cost
/// never grows with the total backlog.
pub const EXPORT_LAG_LOOKBACK_ROWS: i64 = 1000;

/// The bounded pending-window scan's single output column.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct OldestInWindow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    oldest: Option<DateTime<Utc>>,
}

/// Age in seconds of the oldest audit record the sink has not acknowledged,
/// or `0.0` when nothing is pending. This is `harvest.audit.export_lag`.
///
/// Deliberately **two bounded lookups**, not the single query the admin
/// view uses: `MIN(occurred_at) WHERE export_seq IS NULL OR export_seq > $1`
/// (issue #953 review). That disjunction spans both partial indexes. It
/// degenerates into visiting every pending heap tuple. This runs on every
/// scanner tick. The pending set grows largest during a sink outage, when
/// the database is already under stress. So the per-tick query must not
/// scale with the backlog.
///
/// - Not-yet-sequenced rows: `MIN(occurred_at) WHERE export_seq IS NULL`.
///   An index-min on `harvest_audit_log_unexported_idx` serves this once export runs.
/// - Sequenced-but-unacknowledged rows: `MIN(occurred_at)` over the lowest
///   [`EXPORT_LAG_LOOKBACK_ROWS`] pending sequences (issue #1271). The
///   covering index `harvest_audit_log_export_seq_idx` on
///   `(export_seq, occurred_at)` serves this without a heap fetch, on a
///   page whose visibility map bit is already set. An unvacuumed page
///   still costs one fetch. Sequences are assigned in `(occurred_at, id)` order
///   within one exporter tick. So skew between sequence and `occurred_at`
///   comes only from a row a later tick sequenced, while an earlier tick's
///   row stayed invisible. See [`EXPORT_LAG_LOOKBACK_ROWS`] for the
///   accepted bound.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
#[allow(clippy::cast_precision_loss)] // millisecond lag never approaches 2^53
pub async fn export_lag_seconds(
    conn: &mut diesel_async::AsyncPgConnection,
    last_acked_seq: i64,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<f64> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_log::dsl as log;

    let unsequenced: Option<DateTime<Utc>> = log::harvest_audit_log
        .filter(log::export_seq.is_null())
        .select(diesel::dsl::min(log::occurred_at))
        .first::<Option<DateTime<Utc>>>(conn)
        .await
        .map_err(crate::error::database_error)?;

    // The oldest `occurred_at` among the lowest `EXPORT_LAG_LOOKBACK_ROWS`
    // pending sequences, not merely the single lowest one. See the module
    // doc above and issue #1271.
    let window: OldestInWindow = diesel::sql_query(
        "SELECT MIN(occurred_at) AS oldest FROM ( \
             SELECT occurred_at FROM harvest_audit_log \
             WHERE export_seq > $1 \
             ORDER BY export_seq ASC \
             LIMIT $2 \
         ) AS pending_window",
    )
    .bind::<diesel::sql_types::BigInt, _>(last_acked_seq)
    .bind::<diesel::sql_types::BigInt, _>(EXPORT_LAG_LOOKBACK_ROWS)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;

    let oldest = match (unsequenced, window.oldest) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        (None, None) => None,
    };

    Ok(oldest.map_or(0.0, |oldest| {
        let secs = (now - oldest).num_milliseconds() as f64 / 1000.0;
        if secs.is_finite() && secs > 0.0 {
            secs
        } else {
            0.0
        }
    }))
}

/// Pending-record count and lag for one shard.
///
/// "Pending" is `export_seq IS NULL OR export_seq > last_acked_seq` — both a
/// record the exporter has not sequenced yet and one it sequenced but has not
/// had acknowledged are equally undelivered.
///
/// The `COUNT(*)` makes this **O(backlog)**, so it is deliberately confined to
/// the operator-triggered `GET /admin/audit-export` read. The scanner's
/// per-tick gauge uses [`export_lag_seconds`], which is not.
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

    // A retired cursor owes nothing, so there is no backlog to report (issue
    // #953, Codex review round 10 P2). Recomputing it from the live audit table
    // would keep `pending_records` and `lag_seconds` climbing for a shard no
    // exporter is responsible for -- firing backlog alerts that no action can
    // clear, and contradicting the contract's promise that a retired entry's
    // fields are a frozen snapshot. Skipping the query is also the honest
    // answer to what the fields mean: how much the exporter still owes.
    let (pending_records, lag_seconds) = if cursor.retired_at.is_some() {
        (0, 0.0)
    } else {
        pending_and_lag(conn, cursor.last_acked_seq, now).await?
    };

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
            cursor.retired_at,
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
/// Opens its own transaction. A caller that must commit the rewind together
/// with something else — the management route pairs it with its own audit
/// record, so a rewind can never be applied unaudited — should open the
/// transaction itself and call [`rewind_cursor_locked`].
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
    use diesel_async::AsyncConnection;

    Box::pin(
        conn.transaction::<RewindOutcome, crate::error::HarvestError, _>(async |conn| {
            rewind_cursor_locked(conn, shard_id, request, now).await
        }),
    )
    .await
}

/// [`rewind_cursor`] without the surrounding transaction.
///
/// Takes the cursor row's `FOR UPDATE` lock and applies the rewind but leaves
/// the commit to the caller, so a caller can bind the rewind to another write
/// in the same unit. The management route uses this to write its
/// `audit_export.redrive` record *before* committing, which makes an applied
/// but unaudited redrive unrepresentable rather than merely unlikely.
///
/// **Must be called inside a transaction.** On an autocommit connection each
/// statement commits independently and the atomicity this exists to provide is
/// gone.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn rewind_cursor_locked(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    request: RewindRequest,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<RewindOutcome> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    {
        {
            let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(crate::models::AuditExportCursor::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            // A *retired* cursor is not a rewindable one (issue #953, Codex
            // review round 8 P2). The row now outlives a decommission so its
            // sequence high-water mark survives — but retention deliberately
            // ignores retired cursors and is free to purge the very records a
            // rewind would target, and no exporter is running to ship them.
            // Answering 200 "rewound" there is a promise the system cannot
            // keep, so a retired shard is reported exactly as an unconfigured
            // one was before the row began to persist.
            let Some(cursor) = cursor.filter(|c| c.retired_at.is_none()) else {
                return Ok(RewindOutcome::NotConfigured);
            };

            let requested = match request {
                RewindRequest::Seq(seq) => seq,
                RewindRequest::Before(instant) => {
                    // The LOWEST sequence among records that occurred at or
                    // after `instant`, minus one -- never `MAX(seq)` over the
                    // records *before* it (issue #953 review).
                    //
                    // The two are not equivalent, and the difference is the
                    // same hazard this whole design is built around.
                    // `occurred_at` is transaction START time, so a
                    // long-running request's audit row can carry an earlier
                    // `occurred_at` than a row that committed sooner and yet
                    // be sequenced later. Taking the max over the "before"
                    // side would then land ABOVE records the operator asked to
                    // re-export, and they would silently never be re-sent --
                    // the exact skew the forward path is immune to,
                    // reintroduced in the recovery path.
                    //
                    // Anchoring on the "at or after" side is monotone in the
                    // safe direction: any skew makes the rewind reach FURTHER
                    // back, costing duplicate deliveries the receiver dedupes
                    // on `(shard, seq)`, never a missing record. No sequenced
                    // record at or after the instant means there is nothing to
                    // re-export from there, so the cursor stays put.
                    //
                    // This MIN sees only SURVIVING rows. Retention may have
                    // purged the earliest records at or after `instant`.
                    // `redrive_window_truncated` reports that case from the
                    // purge watermark (issue #1508).
                    let lowest: Option<Option<i64>> = log::harvest_audit_log
                        .filter(log::occurred_at.ge(instant))
                        .filter(log::export_seq.is_not_null())
                        .select(diesel::dsl::min(log::export_seq))
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;
                    lowest
                        .flatten()
                        .map_or(cursor.last_acked_seq, |seq| seq.saturating_sub(1))
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
        }
    }
}

/// Rows still present that a [`RewindOutcome::Rewound`] window can actually
/// redeliver.
///
/// Counts `harvest_audit_log` rows with `to < export_seq <= from`. A retention
/// sweep does not take the cursor row's `FOR UPDATE` lock (issue #1267). It
/// can read the pre-rewind cursor and purge part of this window. The rewind
/// then still commits a lower one. `from - to` is the count the redrive was
/// asked for; this function is the count it can actually deliver. A caller
/// compares the two to report a gap instead of a recovery the database
/// cannot back up.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn count_redrive_recoverable(
    conn: &mut diesel_async::AsyncPgConnection,
    from: i64,
    to: i64,
) -> crate::error::HarvestResult<i64> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_log::dsl as log;

    log::harvest_audit_log
        .filter(log::export_seq.gt(to))
        .filter(log::export_seq.le(from))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)
}

/// `(recoverable_records, already_purged_records)` for a
/// [`rewind_cursor_locked`] outcome (issue #1267).
///
/// `(0, 0)` for [`RewindOutcome::NoOp`] and [`RewindOutcome::NotConfigured`]:
/// a refused rewind moved nothing, so there is no window to measure. For
/// [`RewindOutcome::Rewound`], see [`count_redrive_recoverable`].
///
/// Exact for a [`RewindRequest::Seq`] rewind: `to` is the operator's own
/// number, independent of what still exists. For a [`RewindRequest::Before`]
/// rewind, `to` comes from surviving rows, so this count misses a purged
/// prefix. [`redrive_window_truncated`] covers that case (issue #1508).
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn redrive_recovery_counts(
    conn: &mut diesel_async::AsyncPgConnection,
    outcome: RewindOutcome,
) -> crate::error::HarvestResult<(i64, i64)> {
    let RewindOutcome::Rewound { from, to } = outcome else {
        return Ok((0, 0));
    };
    let recoverable = count_redrive_recoverable(conn, from, to).await?;
    Ok((recoverable, (from - to - recoverable).max(0)))
}

/// Whether retention already purged records the redrive named (issue #1508).
///
/// A [`RewindRequest::Before`] window comes from surviving rows only. A purged
/// row leaves nothing to count. Retention therefore records the latest
/// `occurred_at` it purged in `harvest_audit_purge_watermark`. The window is
/// truncated when that value is at or after the requested instant.
///
/// The watermark is database-wide, like the `Before` resolver. Shards that
/// share a database share it, so the flag can be `true` for a shard that
/// lost nothing. A purge that commits after this check is not seen. A purge
/// from before the migration left no trace.
///
/// Always `false` for [`RewindRequest::Seq`]: `to` there is the operator's own
/// number, so `already_purged_records` is exact. Always `false` for
/// [`RewindOutcome::NotConfigured`], which has no window.
///
/// The watermark shows that a record at or after the instant is gone. It does
/// not show where that record sat in the window, and it does not count them.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn redrive_window_truncated(
    conn: &mut diesel_async::AsyncPgConnection,
    request: RewindRequest,
    outcome: RewindOutcome,
) -> crate::error::HarvestResult<bool> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_purge_watermark::dsl as mark;

    let RewindRequest::Before(instant) = request else {
        return Ok(false);
    };
    if matches!(outcome, RewindOutcome::NotConfigured) {
        return Ok(false);
    }
    mark::harvest_audit_purge_watermark
        .filter(mark::max_purged_occurred_at.ge(instant))
        .count()
        .get_result::<i64>(conn)
        .await
        .map(|n| n > 0)
        .map_err(crate::error::database_error)
}

// ---------------------------------------------------------------------
// M9: the scanner
// ---------------------------------------------------------------------

/// Deliver `batch`, bounded by what remains of this claim's database lease.
///
/// Never the full `config.lease` (issue #953, Codex review round 19 P1). The
/// lease starts running when the claim commits, and claiming, serializing and
/// signing the batch have already consumed part of it. Bounding by the whole
/// lease would let the call outlive the row lease, after which another exporter
/// can reclaim the shard and bump the epoch — so this attempt's acknowledgement
/// is refused and the batch is redelivered. At sink latencies consistently near
/// the lease that never converges: the cursor stays put while the sink receives
/// the same batch forever.
///
/// An already-elapsed remainder is not skipped, it is an immediate timeout —
/// classified like any other transport failure, so the cursor is held and the
/// batch retried under a fresh claim. Never a loss.
#[cfg(feature = "db")]
async fn deliver_within_lease(
    config: &AuditExportRuntimeConfig,
    batch: &AuditBatch<'_>,
    lease_until: DateTime<Utc>,
) -> SinkAttempt {
    let remaining = (lease_until - Utc::now())
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);
    tokio::time::timeout(remaining, config.sink.deliver(batch))
        .await
        .unwrap_or_else(|_| {
            SinkAttempt::transport_error(format!(
                "audit sink did not respond within the {}s remaining on this claim's {}s \
                 export lease",
                remaining.as_secs(),
                config.lease.as_secs()
            ))
        })
}

/// Export one batch for one shard on one connection.
///
/// Three phases, deliberately separated: claim (transaction 1), deliver (no
/// transaction, no locks), apply (transaction 2). Returns the number of
/// records delivered.
/// Discard a delivery whose sink was replaced while it was in flight.
///
/// `read_global_audit_export_config` clones the `Arc` at the top of a tick, so
/// a second runtime publishing a different sink does not disturb that clone and
/// the batch reaches the OLD destination (issue #953, Codex review round 25
/// P1). `apply_outcome` is guarded on `claim_epoch`, which a config swap does
/// not change, so a 2xx from the old sink would advance the cursor: the records
/// would be delivered, marked delivered, and absent from the SIEM the operator
/// actually configured.
///
/// Comparing the `Arc` this attempt used against the one now installed closes
/// that window. A mismatch becomes a transport failure, so the cursor is held
/// and the batch is redelivered under the new sink. The old sink keeps its
/// copy — that is the at-least-once contract, not a leak.
///
/// A `current` of `None` also counts as swapped: export was turned off
/// mid-flight, and advancing the cursor on the strength of the retired sink's
/// answer would strand those records.
///
/// Takes `current` rather than reading the global itself, so the decision is a
/// pure function of its inputs — the same shape as [`classify_export_outcome`].
/// That is deliberate: a version of this that read the process-wide static
/// internally could only be tested by mutating that static, and two tests in
/// this PR have already failed on CI for exactly that reason while passing
/// locally.
#[cfg(feature = "db")]
fn fence_against_sink_swap(
    attempt: SinkAttempt,
    delivered_with: &std::sync::Arc<AuditExportRuntimeConfig>,
    current: Option<&std::sync::Arc<AuditExportRuntimeConfig>>,
) -> SinkAttempt {
    if current.is_some_and(|current| std::sync::Arc::ptr_eq(current, delivered_with)) {
        return attempt;
    }
    SinkAttempt::transport_error(
        "the audit sink was replaced while this batch was in flight; holding the cursor so \
         the batch is redelivered to the newly configured sink"
            .to_string(),
    )
}

#[cfg(feature = "db")]
async fn export_once_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    config_arc: &std::sync::Arc<AuditExportRuntimeConfig>,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    let config = config_arc.as_ref();
    notice_missing_unexported_index(conn, shard_id).await;
    ensure_cursor_row(conn, shard_id).await?;

    let now = Utc::now();
    let Some(claim) = config.claim(conn, shard_id, now).await? else {
        // Nothing claimed, but the lag gauge must still be emitted: an
        // operator's "is the export keeping up?" signal has to stay live
        // exactly when deliveries are NOT happening.
        emit_lag_and_observed(conn, shard_id, metrics).await;
        return Ok(0);
    };

    let first_seq = claim.records.first().map_or(0, |r| r.seq);
    let last_seq = claim.records.last().map_or(0, |r| r.seq);

    let body = match serialize_batch(&claim.records) {
        Ok(body) => body,
        Err(error) => {
            // Cannot serialize what we claimed. Hold the cursor AND write a
            // real backoff (issue #953 review): a bare `release_claim` leaves
            // `next_attempt_at` in the past, so the next tick re-claims the
            // same unserializable batch immediately and the shard hot-spins
            // once per poll interval with the cursor frozen and nothing but a
            // log line per iteration. A backoff paces the failure and surfaces
            // it on `GET /admin/audit-export`.
            tracing::error!(
                shard = shard_id,
                error = %error,
                "failed to serialize an audit export batch; holding the cursor"
            );
            let now = Utc::now();
            let outcome = classify_export_outcome(
                &SinkAttempt::transport_error(format!("batch serialization failed: {error}")),
                last_seq,
                claim.consecutive_failures,
                &config.backoff,
                now,
            );
            apply_outcome(conn, shard_id, claim.claim_epoch, &outcome, now).await?;
            // A serialization failure says nothing about the shard's cursor
            // and lag. Both were readable this tick, moments ago, in
            // `claim_shard`. So this tick still counts as observed
            // (issue #1268).
            emit_lag_and_observed(conn, shard_id, metrics).await;
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

    // Bound the embedder-supplied call by the claim's own lease (issue #953
    // review). Two failures this closes:
    //
    //   * A sink slower than the lease livelocks: its claim is superseded by
    //     the next tick's before its 2xx lands, that acknowledgement is
    //     refused by the epoch guard, and the cursor never advances while the
    //     sink receives the same batch forever.
    //   * `fire_due_audit_exports` is awaited inline in
    //     `enforce_timeouts_once`, so an un-timed `await` on embedder code
    //     would wedge the shared scanner -- and every resident sequenced after
    //     it -- for as long as the sink hangs.
    //
    // A timeout is classified exactly like any other transport failure: the
    // cursor is held and the batch is retried. Never a loss.
    let attempt = deliver_within_lease(config, &batch, claim.lease_until).await;

    let attempt = fence_against_sink_swap(
        attempt,
        config_arc,
        read_global_audit_export_config().as_ref(),
    );

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

    emit_lag_and_observed(conn, shard_id, metrics).await;
    Ok(delivered)
}

/// Emit `harvest.audit.export_lag` and `harvest.audit.export_observed` for
/// one shard, best-effort.
///
/// **Always records `export_observed`, success or failure** (issue #1268).
/// Before this, a failed cursor read or lag query returned silently, and
/// `export_lag` simply kept its last value — commonly `0`, the caught-up
/// reading. Prometheus then saw neither a high value nor an absent series
/// while the shard went unexported: the two alerts the feature documents
/// both stayed quiet. `export_observed` going to `0` is the signal a rule can
/// alert on instead.
///
/// `export_lag` itself is untouched on failure, deliberately. A fabricated
/// reading would conflate "behind" with "unreadable". The stale value is
/// left exactly as before, and `export_observed` is the availability signal.
#[cfg(feature = "db")]
async fn emit_lag_and_observed(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let shard_u16 = u16::try_from(shard_id).unwrap_or(u16::MAX);

    let acked: Result<Option<i64>, _> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(cur::last_acked_seq)
        .first(conn)
        .await
        .optional();
    let Ok(Some(acked)) = acked else {
        metrics.record_audit_export_observed(shard_u16, false);
        return;
    };

    match export_lag_seconds(conn, acked, Utc::now()).await {
        Ok(lag) => {
            metrics.record_audit_export_observed(shard_u16, true);
            metrics.record_audit_export_lag(shard_u16, lag);
        }
        Err(_) => metrics.record_audit_export_observed(shard_u16, false),
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
/// Returns `Ok(0)` **before any query** when no audit sink is configured (AC8).
/// It emits no metric, unless a sink was removed after it was enabled. Then it
/// reports each shard unobserved (issue #1506).
///
/// # Connection handling
///
/// `conn` is used **only** for the unsharded fallback. When a `sharded_pool` is
/// present, every assigned shard — including a lone one — is exported through
/// that shard's own pool, acquired under [`SHARD_ACQUIRE_BOUND`].
///
/// Deliberately no inference that a single `shard_assignments` entry means
/// `conn` belongs to it. This is a `pub` primitive an embedder may drive by
/// hand, nothing here can verify which database `conn` points at, and being
/// wrong stamps one shard's audit rows under another shard's key — silent
/// corruption of the `(shard, seq)` identity the whole feature rests on.
/// Acquiring the exact pool can at worst skip a shard, loudly. See the comment
/// in the match arm for the full reasoning and its cost.
///
/// # Claim-scan index (issue #1667)
///
/// On the unsharded fallback this primitive never builds the claim-scan index.
/// Each call reads the index catalog once, and logs the build statement hourly
/// while the index is missing. The caller sets the cost with its call cadence.
/// The dedicated export task throttles the same probe and builds the index.
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
        // Issue #1506: report a disabled export instead of freezing the gauges.
        if export_disabled_after_enable() {
            let pool_default = sharded_pool.as_ref().map(|sp| sp.default_shard().as_i32());
            for shard in disabled_tick_shards(pool_default, shard_assignments) {
                report_export_disabled(metrics, u16::try_from(shard).unwrap_or(u16::MAX));
            }
        }
        return Ok(0);
    };

    let mut total = 0usize;

    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            // EVERY shard is exported through its own pool, including a single
            // assignment. An earlier revision reused the caller's `conn` when
            // `shard_assignments.len() == 1`, inferring from that alone that
            // `conn` belonged to the named shard (issue #953, Codex review
            // rounds 3, 14 and 18 -- the same question, answered three times).
            //
            // The inference is not sound for a `pub` primitive. A caller that
            // hands over a default-pool connection with a single NON-default
            // assignment -- which `enforce_timeouts_once` cannot detect --
            // would have its rows stamped in the default database under
            // another shard's key, a cursor created there under that key, the
            // real shard left unexported, and the mislabelled records deduped
            // against the true shard's sequence stream by the receiver.
            //
            // That is silent cross-shard corruption of the very `(shard, seq)`
            // identity this feature exists to guarantee. Acquiring the exact
            // pool can at worst SKIP a shard, loudly and recoverably. For an
            // audit exporter a visible skip beats invisible corruption, so the
            // inference is gone.
            //
            // The cost is that a shard pool sized 1 cannot export while the
            // scanner holds its only connection. That is already true of every
            // other per-shard resident of this loop (#605's delivery included),
            // so such a pool is not a supported configuration; the skip is
            // logged with that cause, and
            // autumn-foundation/autumn-harvest#1269 removes the constraint
            // outright by giving export its own task.
            for shard in shard_assignments {
                // A shard the scanner cannot reach is unobserved, not merely
                // silent (issue #1268). Every `continue` below must mark it
                // so before moving on. Otherwise `export_lag` is the only
                // signal left, and it just keeps its stale reading.
                let shard_u16 = u16::try_from(shard.as_i32()).unwrap_or(u16::MAX);
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    metrics.record_audit_export_observed(shard_u16, false);
                    continue;
                };
                // Bounded, never a bare `pool.get()` — see `SHARD_ACQUIRE_BOUND`.
                let mut shard_conn =
                    match tokio::time::timeout(SHARD_ACQUIRE_BOUND, pool.get()).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => {
                            tracing::error!(
                                "[audit_export] failed to get connection to shard {shard:?}: {e:?}"
                            );
                            metrics.record_audit_export_observed(shard_u16, false);
                            continue;
                        }
                        Err(_elapsed) => {
                            tracing::error!(
                                shard = shard.as_i32(),
                                bound = ?SHARD_ACQUIRE_BOUND,
                                "[audit_export] timed out acquiring a connection for this shard; \
                                 skipping it for one tick. If this repeats, the shard's pool is \
                                 saturated or too small: the scanner already holds one connection \
                                 from it, so a max_size of 1 can never yield a second"
                            );
                            metrics.record_audit_export_observed(shard_u16, false);
                            continue;
                        }
                    };
                // One shard's failure must never stop the others: an
                // unreachable shard is an availability problem, but silently
                // skipping every *later* shard's export would be a compliance
                // one.
                match export_once_on_conn(&mut shard_conn, &config, shard.as_i32(), metrics).await {
                    Ok(n) => total += n,
                    Err(e) => {
                        tracing::error!(
                            shard = shard.as_i32(),
                            error = %e,
                            "[audit_export] shard export failed"
                        );
                        // `export_once_on_conn` returned before it could emit
                        // either gauge for this tick (issue #1268).
                        metrics.record_audit_export_observed(shard_u16, false);
                    }
                }
            }
        }
        _ => {
            // Unsharded deployment, or a pool with no assignments. Label the
            // records with the pool's own default shard rather than a hardcoded
            // `0` (issue #953 review): a sharded pool whose default is not 0
            // would otherwise write a cursor row keyed `0` into that shard's
            // database, alongside the correctly-keyed cursor a worker assigned
            // to it maintains -- two independent counters stamping `export_seq`
            // on the same rows from different bases, which breaks per-shard
            // density and the receiver's contiguity check.
            let shard = sharded_pool
                .as_ref()
                .map_or(0, |sp| sp.default_shard().as_i32());
            // Logged and swallowed for the same reason as the sharded arm
            // above: `fire_due_audit_exports` is one resident of
            // `enforce_timeouts_once`, and a transient audit-export database
            // error must not abort the timeout/SLA/session residents
            // sequenced after it.
            match export_once_on_conn(conn, &config, shard, metrics).await {
                Ok(n) => total += n,
                Err(e) => {
                    tracing::error!(
                        shard,
                        error = %e,
                        "[audit_export] export failed on the default shard"
                    );
                    // See the sharded arm above (issue #1268): a propagated
                    // error means neither gauge was emitted this tick.
                    metrics.record_audit_export_observed(
                        u16::try_from(shard).unwrap_or(u16::MAX),
                        false,
                    );
                }
            }
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------
// M8: a dedicated per-shard export task (issue #1269)
// ---------------------------------------------------------------------

/// Acquire one shard's connection, bounded, for the dedicated export task.
///
/// `bound` is normally [`SHARD_ACQUIRE_BOUND`], but the post-delivery
/// reacquire passes a narrower, lease-derived bound instead (Codex review
/// on PR #1520, follow-up P1) — see [`split_reserve`].
///
/// A connection-acquisition failure or timeout is logged and marks the
/// shard unobserved, then returns `None`. It is not a database error, so
/// the caller never turns it into an `Err`.
#[cfg(feature = "db")]
async fn acquire_shard_conn_for_export(
    pool: &crate::worker::DbPool,
    shard_id: i32,
    shard_u16: u16,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    bound: std::time::Duration,
) -> Option<
    deadpool::managed::Object<
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
> {
    // Bounded, never a bare `pool.get()` — see `SHARD_ACQUIRE_BOUND`. Under
    // the tick's fence the wait stays below a bump's lock timeout, and a
    // failed checkout drops the guards (issue #1823).
    let checkout = tokio::time::timeout(crate::replication::fenced_wait(bound), pool.get()).await;
    if !matches!(checkout, Ok(Ok(_))) {
        crate::replication::abandon_fenced_pass();
    }
    match checkout {
        Ok(Ok(conn)) => Some(conn),
        Ok(Err(error)) => {
            tracing::error!(
                shard = shard_id,
                error = %error,
                "[audit_export] failed to acquire a connection for the export tick"
            );
            metrics.record_audit_export_observed(shard_u16, false);
            None
        }
        Err(_elapsed) => {
            tracing::error!(
                shard = shard_id,
                bound = ?bound,
                "[audit_export] timed out acquiring a connection for the export tick; \
                 skipping it for one cycle"
            );
            metrics.record_audit_export_observed(shard_u16, false);
            None
        }
    }
}

/// Export one shard's due batch, acquiring connections from `pool` rather
/// than holding one across the whole call.
///
/// [`export_once_on_conn`] holds its caller's connection from the claim
/// through the acknowledgement, network delivery included. That is fine
/// when the caller already owns the connection for other reasons — an
/// embedder driving [`fire_due_audit_exports`] by hand.
///
/// It is wrong for [`spawn_audit_export_checker_for_shard`]'s own dedicated
/// task (Codex review on PR #1520 P1). On a `max_size(1)` shard pool,
/// holding the connection for the whole delivery would block every other
/// user of that pool. The timeout checker is one of them, for up to the
/// claim lease. That is exactly the coupling issue #1269 exists to remove.
///
/// This checks a connection out for the claim transaction, then releases it
/// before the network call. It checks one out again — not necessarily the
/// same physical connection — for the acknowledgement transaction. No
/// connection is held during delivery at all.
///
/// The delivery deadline reserves `SHARD_ACQUIRE_BOUND` plus
/// `ACK_QUERY_BOUND` off the lease, capped at half the configured lease.
/// Both the reserve and its cap follow Codex review on PR #1520 (follow-up
/// P1 and follow-up P2).
///
/// The reserve covers the second checkout and the acknowledgement query it
/// runs. Both steps are bounded, but bounded is not free. A successful
/// delivery finishing right at `lease_until` would otherwise leave no time
/// for either step. A fresher claim could then reclaim the shard. The cap
/// also keeps a configured lease as short as one second from losing its
/// entire delivery window to a fixed reserve.
///
/// [`split_reserve`] splits that same reserve between the two steps, so a
/// capped reserve shrinks both bounds together (Codex review on PR #1520,
/// follow-up P1). Reacquiring under the fixed, uncapped `SHARD_ACQUIRE_BOUND`
/// regardless of the cap could let the checkout alone consume a reserve
/// meant to cover both steps.
///
/// `cancel` races the delivery wait, never the claim or the acknowledgement
/// (Codex review on PR #1520, follow-up P1). A shutdown mid-delivery
/// abandons the wait and leaves the claim exactly where it was, for the
/// next attempt to redeliver. It never blocks the caller's shutdown for up
/// to the full lease.
///
/// `config_arc` is a snapshot the caller already read, not re-read here
/// (Codex review on PR #1520, follow-up P2). The caller uses that same
/// snapshot to decide the registered liveness interval. A second, separate
/// read inside this function could observe a runtime swap the caller's
/// read missed. That would register this tick's own tolerance against a
/// lease it is not actually using.
///
/// `fence_against_sink_swap` reads the global fresh, deliberately, and runs
/// immediately before the acknowledgement write, not right after delivery
/// (Codex review on PR #1520, follow-up P1). A swap could otherwise land
/// during the reacquire wait between the two, after an earlier fence check
/// but before a stale `Advance` outcome commits.
///
/// Returns `Ok(0)` before any query when no sink is configured (AC8). It
/// reports the shard unobserved if a sink was removed after it was enabled
/// (issue #1506).
///
/// # Errors
/// Returns `HarvestError` on a genuine database failure inside a claimed
/// transaction. A sink transport failure, and a connection-acquisition
/// failure, are never an `Err` here — see [`fire_due_audit_exports`].
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)] // claim + release + deliver + reacquire + apply is one unit
async fn export_once_via_pool(
    pool: &crate::worker::DbPool,
    shard_id: i32,
    fence_key: crate::types::ShardId,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    cancel: &tokio_util::sync::CancellationToken,
    config_arc: Option<std::sync::Arc<AuditExportRuntimeConfig>>,
    index_build_dsn: Option<&str>,
) -> crate::error::HarvestResult<usize> {
    let shard_u16 = u16::try_from(shard_id).unwrap_or(u16::MAX);
    let Some(config_arc) = config_arc else {
        // Issue #1506: report a disabled export instead of freezing the gauges.
        report_export_disabled(metrics, shard_u16);
        return Ok(0);
    };
    let config = config_arc.as_ref();

    let Some(mut conn) =
        acquire_shard_conn_for_export(pool, shard_id, shard_u16, metrics, SHARD_ACQUIRE_BOUND)
            .await
    else {
        return Ok(0);
    };
    // The pre-build probes race shutdown. A stalled catalog read must not hold
    // the checker, and the claim phase below observes the same token.
    let _ = until_cancelled(
        cancel,
        spawn_unexported_index_build_if_due(
            &mut conn,
            shard_id,
            pool,
            fence_key,
            index_build_dsn,
            cancel,
        ),
    )
    .await;
    // Raced against `cancel` (Codex review on PR #1520, follow-up P2, fifth
    // round). `claim_shard`'s locked read can wait indefinitely behind
    // another session holding the cursor row. A bare await here would then
    // block graceful shutdown for as long as that lock is held. That is
    // exactly the failure mode the delivery wait below is raced against.
    // Nothing has been claimed yet at this point. Abandoning the wait
    // leaves no state to clean up: the next tick's `claim_shard` retries
    // the same locked row from scratch.
    let claim = tokio::select! {
        result = async {
            ensure_cursor_row(&mut conn, shard_id).await?;
            let now = Utc::now();
            config.claim(&mut conn, shard_id, now).await
        } => result?,
        () = cancel.cancelled() => {
            tracing::warn!(
                shard = shard_id,
                "[audit_export] shutdown requested while claiming a batch; discarding \
                 the connection and abandoning the wait for the next attempt to retry"
            );
            // `claim_shard` runs inside its own Diesel transaction (Codex
            // review on PR #1520, follow-up P2, sixth round -- P1).
            // Dropping this future mid-flight sends no ROLLBACK. Silently
            // returning `conn` to the pool could then hand the next
            // checkout a connection still holding an open transaction and
            // the cursor row's lock. `Object::take` detaches it from the
            // pool instead of recycling it. Dropping the raw connection
            // then closes the socket, and Postgres rolls back whatever
            // that session still had open.
            drop(deadpool::managed::Object::take(conn));
            return Ok(0);
        }
    };
    let Some(claim) = claim else {
        // Nothing claimed, but the lag gauge must still be emitted (issue
        // #1268). An operator's "is export keeping up?" signal has to stay
        // live exactly when deliveries are NOT happening.
        emit_lag_and_observed(&mut conn, shard_id, metrics).await;
        return Ok(0);
    };
    // Released before any network I/O -- the whole point of this function.
    drop(conn);

    let first_seq = claim.records.first().map_or(0, |r| r.seq);
    let last_seq = claim.records.last().map_or(0, |r| r.seq);

    let body = match serialize_batch(&claim.records) {
        Ok(body) => body,
        Err(error) => {
            // Cannot serialize what we claimed. Hold the cursor AND write a
            // real backoff, mirroring `export_once_on_conn`'s own handling
            // of this case.
            tracing::error!(
                shard = shard_id,
                error = %error,
                "failed to serialize an audit export batch; holding the cursor"
            );
            let now = Utc::now();
            let outcome = classify_export_outcome(
                &SinkAttempt::transport_error(format!("batch serialization failed: {error}")),
                last_seq,
                claim.consecutive_failures,
                &config.backoff,
                now,
            );
            let Some(mut conn) = acquire_shard_conn_for_export(
                pool,
                shard_id,
                shard_u16,
                metrics,
                SHARD_ACQUIRE_BOUND,
            )
            .await
            else {
                return Ok(0);
            };
            apply_outcome(&mut conn, shard_id, claim.claim_epoch, &outcome, now).await?;
            emit_lag_and_observed(&mut conn, shard_id, metrics).await;
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

    // The delivery deadline reserves `SHARD_ACQUIRE_BOUND` plus
    // `ACK_QUERY_BOUND` off the end of the lease. That covers the
    // post-delivery reacquire-and-acknowledge step below (Codex review on
    // PR #1520, follow-up P1 and follow-up P2). Without a reserve, a sink
    // finishing near `lease_until` could leave that step to run PAST the
    // lease.
    //
    // A second exporter would then see the lease already expired. It would
    // reclaim the shard and bump `claim_epoch`. This attempt's
    // `apply_outcome` below would be guarded out even though the batch was
    // genuinely delivered. Under sustained near-lease latency that repeats
    // forever: delivered, but never acknowledged.
    //
    // The reserve is capped at half the configured lease (Codex review on
    // PR #1520, follow-up P1). A builder-configured lease can be as short
    // as one second. Subtracting the full, fixed reserve from a lease that
    // short leaves no delivery window at all. Every batch would then time
    // out immediately and back off forever. Capping the reserve instead
    // shrinks the delivery window and the reacquire-and-acknowledge window
    // together on a short lease. A short lease then always keeps some
    // genuine delivery time.
    let reserve = (SHARD_ACQUIRE_BOUND + ACK_QUERY_BOUND).min(config.lease / 2);
    let delivery_deadline = claim.lease_until
        - chrono::Duration::from_std(reserve).unwrap_or_else(|_| chrono::Duration::zero());

    // The reserve is split between the reacquire below and the
    // acknowledgement query that follows it (Codex review on PR #1520,
    // follow-up P1). The split keeps their uncapped proportion. A capped
    // `reserve` can be smaller than the fixed `SHARD_ACQUIRE_BOUND`.
    // Reacquiring under that fixed bound regardless could let the checkout
    // alone consume the whole capped reserve, leaving the acknowledgement
    // no margin at all. See [`split_reserve`].
    let (checkout_bound, ack_bound) = split_reserve(reserve);

    // No connection held during this await. A timeout is classified exactly
    // like any other transport failure: the cursor is held and the batch is
    // retried. Never a loss.
    //
    // Raced against `cancel` (Codex review on PR #1520 P1). A bare await
    // here does not return until the sink finishes or the delivery deadline
    // elapses. Both worker shutdown paths join every export task's handle.
    // An in-flight delivery could otherwise hold up a graceful shutdown for
    // the whole lease. That is 60s by default, longer if configured -- well
    // past a typical deployment's termination grace period.
    //
    // Cancellation drops the delivery future without recording an outcome.
    // The claim's cursor and lease are untouched. The batch is safely
    // redelivered once this shard's lease expires or the process restarts.
    let attempt = tokio::select! {
        attempt = deliver_within_lease(config, &batch, delivery_deadline) => attempt,
        () = cancel.cancelled() => {
            tracing::warn!(
                shard = shard_id,
                "[audit_export] shutdown requested mid-delivery; abandoning the wait \
                 and leaving the claim for the next attempt to redeliver"
            );
            return Ok(0);
        }
    };
    // Reacquired under `checkout_bound`, the reserve's own share for this
    // step (Codex review on PR #1520, follow-up P1). The fixed
    // `SHARD_ACQUIRE_BOUND` above is for the claim checkout only.
    let Some(mut conn) =
        acquire_shard_conn_for_export(pool, shard_id, shard_u16, metrics, checkout_bound).await
    else {
        // The batch was delivered (or the attempt failed) but the outcome
        // cannot be recorded this tick. At-least-once: the next tick that
        // can reach this shard re-claims and re-attempts, so nothing is
        // silently lost either way.
        return Ok(0);
    };

    // Fenced and classified here, immediately before the acknowledgement
    // write below, not right after delivery (Codex review on PR #1520,
    // follow-up P1). A sink swap landing during the reacquire wait just
    // above must still be caught before a stale `Advance` outcome can
    // commit. Fencing any earlier would miss exactly that swap.
    //
    // A swap landing during the acknowledgement write itself, after this
    // comparison, is not caught (Codex review on PR #1520, follow-up P1,
    // second round). Closing that too means holding this read across the
    // `apply_outcome` await below, so no writer can land in between.
    //
    // `GLOBAL_AUDIT_EXPORT_CONFIG` is a `std::sync::RwLock`. Its read guard
    // is not `Send`, confirmed by `cargo test`, not assumed. It cannot
    // survive an await point inside a spawned, `Send`-bound task. Closing
    // the gap for real needs an async-aware lock, across every reader and
    // writer of the global -- a wider change than this fix.
    //
    // The residual window is bounded by `ack_bound`. It also requires a
    // second runtime's `build()` to land inside that window. That only
    // happens at process startup or an embedder's own rebuild, never on a
    // request path.
    let attempt = fence_against_sink_swap(
        attempt,
        &config_arc,
        read_global_audit_export_config().as_ref(),
    );
    let outcome = classify_export_outcome(
        &attempt,
        last_seq,
        claim.consecutive_failures,
        &config.backoff,
        Utc::now(),
    );

    // Bounded by `ack_bound`, the reserve's own share for this query
    // (Codex review on PR #1520, follow-up P1). A timeout here is treated
    // exactly like the failed-reacquire case above: the outcome cannot be
    // recorded this tick, but nothing is lost.
    let applied = match tokio::time::timeout(
        ack_bound,
        apply_outcome(&mut conn, shard_id, claim.claim_epoch, &outcome, Utc::now()),
    )
    .await
    {
        Ok(result) => result?,
        Err(_elapsed) => {
            tracing::error!(
                shard = shard_id,
                bound = ?ack_bound,
                "[audit_export] timed out acknowledging an export batch; the next tick \
                 will redeliver it"
            );
            // The shard must not still look observed after this (Codex
            // review on PR #1520, follow-up P2, third round). Without it,
            // `export_observed` keeps its last-good value. The cursor then
            // stalls silently behind repeated acknowledgement timeouts.
            //
            // Recorded directly, not via `emit_lag_and_observed` (Codex
            // review on PR #1520, follow-up P2, fourth round). That
            // helper's own success path reports `true` whenever it can
            // read the cursor row, which it almost always can. This
            // acknowledgement is genuinely indeterminate: the write may
            // have landed on the database side after the client gave up
            // waiting. `false` is the only honest reading here, not
            // whatever the row happens to show right now.
            metrics.record_audit_export_observed(shard_u16, false);
            // Discarded, not returned to the pool (Codex review on PR
            // #1520, follow-up P2, seventh round -- P1). This mirrors the
            // claim-cancellation fix above. Dropping this future does not
            // cancel the already-dispatched `UPDATE`. A connection stuck
            // behind a locked row would otherwise be recycled anyway. Every
            // later user of a size-one shard pool would then queue behind
            // that same blocked statement.
            drop(deadpool::managed::Object::take(conn));
            return Ok(0);
        }
    };

    let delivered = match &outcome {
        ExportOutcome::Advance { .. } if applied => {
            let count = claim.records.len();
            metrics.record_audit_exported(shard_u16, count as u64);
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

    emit_lag_and_observed(&mut conn, shard_id, metrics).await;
    Ok(delivered)
}

/// Spawn a dedicated background task that exports one shard's due audit
/// batches on its own cadence (issue #1269).
///
/// [`fire_due_audit_exports`] used to run inline inside
/// `crate::timeout::enforce_timeouts_once`, sharing that loop's connection
/// and cadence. A slow or unresponsive sink then delayed every other
/// resident of that loop. Timeout enforcement, SLA checks, and session
/// cleanup all waited, for up to one claim lease. Worse, on a shard pool
/// sized for one connection, the export call's own connection request
/// competed with the checker's already-held connection. It could never
/// succeed.
///
/// This task owns its connection lifecycle end to end, through
/// [`export_once_via_pool`]. It never runs nested inside another resident's
/// checkout. That nesting is what made the old failure permanent: the
/// checker always held the pool's only connection when it tried to claim a
/// second one. Every single tick failed the same way, forever.
///
/// A `max_size(1)` shard pool now works, in the sense that matters. This
/// task never holds a connection across a network call. It never blocks
/// the timeout checker, or anything else sharing the pool, for the
/// duration of a delivery. Export is no longer permanently wedged, and a
/// slow sink no longer starves its neighbors on a one-connection pool
/// either.
///
/// Registers under [`crate::scanner_health::Scanner::AuditExport`], so a
/// wedged export task is visible to `scanner_liveness`, exactly like the
/// timeout checker and the poison-pill reclaimer.
///
/// The registered interval accounts for the configured export lease, not
/// just the poll interval (Codex review on PR #1520 P2). A single tick can
/// legitimately run as long as the lease allows. A bare poll-interval
/// threshold would flag a healthy, still-within-lease delivery as `Stale`
/// or `Wedged`. Re-checked every tick and re-registered on an actual
/// change (Codex review on PR #1520, follow-up). A later runtime
/// publishing a longer lease is picked up without needing this task
/// restarted.
///
/// The three stages are summed, not maxed (Codex review on PR #1520,
/// follow-up P2, second round). One tick sleeps for `interval`. It then
/// may spend up to `SHARD_ACQUIRE_BOUND` acquiring its initial connection.
/// Only then does the lease-bounded claim/delivery/acknowledge cycle
/// start. These are three sequential stages, not alternatives, so their
/// worst cases add. The prior formula registered only
/// `interval.max(lease)`. That understated the true worst case by up to
/// `SHARD_ACQUIRE_BOUND`, plus whichever of `interval` or `lease` was not
/// the max. `scanner_liveness` grants `2 x` the registered interval before
/// paging. That grace could then be shorter than one legitimately slow
/// tick.
///
/// Pass `shard` to attribute this instance to one shard in the liveness
/// snapshot, mirroring
/// [`crate::timeout::spawn_timeout_checker_for_shard`]. `sharded_pool`
/// resolves the shard actually stamped on exported records when `shard` is
/// `None` (the unsharded fallback). This is the same rule
/// [`fire_due_audit_exports`] applies in its own unsharded arm.
///
/// `index_build_dsn` is the database URL of this shard (issue #1667). The
/// task opens one dedicated connection to it for the claim-scan index build,
/// so no pooled connection is held for the build. The worker passes the
/// shard's notification URL, which reaches the same database. With `None`
/// the task never builds the index and logs [`UNEXPORTED_INDEX_DDL`] for an
/// operator instead. See [`ensure_unexported_index`].
/// The worst-case wall-clock span of one audit-export checker tick.
///
/// See [`spawn_audit_export_checker_for_shard`]'s doc comment for why the
/// three stages sum rather than max. Saturating throughout: a
/// pathological configuration degrades instead of overflowing.
#[cfg(feature = "db")]
fn audit_export_liveness_interval(
    interval: std::time::Duration,
    config: Option<&AuditExportRuntimeConfig>,
) -> std::time::Duration {
    config.map_or(interval, |config| {
        interval
            .saturating_add(SHARD_ACQUIRE_BOUND)
            .saturating_add(config.lease)
    })
}

#[must_use]
#[cfg(feature = "db")]
pub fn spawn_audit_export_checker_for_shard(
    pool: crate::worker::DbPool,
    cancel: tokio_util::sync::CancellationToken,
    interval: std::time::Duration,
    telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
    shard: Option<crate::types::ShardId>,
    sharded_pool: Option<&crate::shard::ShardedDbPool>,
    index_build_dsn: Option<String>,
) -> tokio::task::JoinHandle<()> {
    // See this function's doc comment: the registered threshold must cover
    // the worst legitimate tick, not just the poll cadence.
    let mut registered_interval =
        audit_export_liveness_interval(interval, read_global_audit_export_config().as_deref());
    // Issue #797: declare the loop before its first iteration so the
    // `scanner_liveness` check expects it and grants it boot grace.
    let mut owner = crate::scanner_health::register_scanner_for_shard(
        &*telemetry.metrics,
        crate::scanner_health::Scanner::AuditExport,
        registered_interval,
        shard,
    );
    let shard_id = shard.map_or_else(
        || sharded_pool.map_or(0, |sp| sp.default_shard().as_i32()),
        crate::types::ShardId::as_i32,
    );
    let shard_u16 = u16::try_from(shard_id).unwrap_or(u16::MAX);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            // A held shard gets no write until the resolver releases it (issue #1823).
            // The loop is still alive, so it still ticks.
            if crate::replication::shard_writes_held(shard) {
                crate::scanner_health::record_scanner_tick(&*telemetry.metrics, owner);
                continue;
            }

            // Read the config ONCE. Use this same snapshot for both the
            // registration decision below and the tick itself (Codex review
            // on PR #1520, follow-up P2). Two independent reads could
            // observe different configs across a mid-tick runtime swap,
            // registering this tick's tolerance against a lease it is not
            // actually using.
            let config_snapshot = read_global_audit_export_config();

            // The registered threshold must track the CURRENTLY configured
            // lease, not just the one in effect at spawn time (Codex review
            // on PR #1520 P2). A second runtime can publish a longer lease
            // at any point (`fence_against_sink_swap`'s doc comment).
            // A stale, too-tight threshold would misclassify a healthy
            // delivery under the new lease as `Stale` or `Wedged`. Cheap to
            // check every tick; only re-registers on an actual change.
            let desired_interval =
                audit_export_liveness_interval(interval, config_snapshot.as_deref());
            if desired_interval != registered_interval {
                crate::scanner_health::deregister_scanner(owner);
                owner = crate::scanner_health::register_scanner_for_shard(
                    &*telemetry.metrics,
                    crate::scanner_health::Scanner::AuditExport,
                    desired_interval,
                    shard,
                );
                registered_interval = desired_interval;
            }

            // Issue #1823: the tick claims and moves the export cursor, so
            // it holds a fence barrier on its shard and every pinned shard
            // colocated there. A fenced process skips the tick. A lost
            // barrier stops it before its next write.
            let fence_key = shard.unwrap_or(crate::types::ShardId::UNENCODED);
            let fence = match crate::replication::begin_fenced_group(&pool, fence_key).await {
                Ok(guards) => guards,
                Err(error) => {
                    tracing::warn!(
                        shard = shard_id,
                        error = %error,
                        "[audit_export] tick skipped: this process is fenced"
                    );
                    crate::scanner_health::record_scanner_tick(&*telemetry.metrics, owner);
                    continue;
                }
            };
            let exported = crate::replication::run_fenced_pass(
                &fence,
                Box::pin(export_once_via_pool(
                    &pool,
                    shard_id,
                    fence_key,
                    &*telemetry.metrics,
                    &cancel,
                    config_snapshot,
                    index_build_dsn.as_deref(),
                )),
            )
            .await
            .and_then(|done| done);
            drop(fence);
            if let Err(error) = exported {
                tracing::error!(
                    shard = shard_id,
                    error = %error,
                    "[audit_export] scheduled export tick failed"
                );
                telemetry
                    .metrics
                    .record_audit_export_observed(shard_u16, false);
            }

            // Issue #797: unconditional end-of-iteration liveness tick, same
            // as every other spawned scanner loop.
            crate::scanner_health::record_scanner_tick(&*telemetry.metrics, owner);
            if cancel.is_cancelled() {
                break;
            }
        }
        // Issue #797: a graceful stop retires this loop from the expected
        // scanner set. A panic unwinds past this point, so a panicked loop
        // stays registered and correctly ages into `Wedged`.
        crate::scanner_health::deregister_scanner(owner);
    })
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
            shard_id: Some(3),
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
            chain_prev: None,
            chain_newest_before: None,
            chain_hash: None,
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

    #[test]
    fn the_direct_worker_config_keeps_the_chain_key() {
        struct Nowhere;
        impl AuditSink for Nowhere {
            fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
                Box::pin(async { SinkAttempt::success(200) })
            }
        }
        let config = AuditExportBuilderConfig {
            sink: Some(std::sync::Arc::new(Nowhere)),
            secret: Some(CallbackSecret::new(b"s".to_vec())),
            chain_key: Some(CallbackSecret::new(vec![1_u8; 32])),
            ..AuditExportBuilderConfig::default()
        };
        let runtime = direct_worker_runtime_config(&config).expect("a sink is set");
        assert_eq!(
            runtime
                .chain_key
                .as_ref()
                .map(|key| key.secret().as_bytes()),
            Some(&[1_u8; 32][..])
        );
        assert!(direct_worker_runtime_config(&AuditExportBuilderConfig::default()).is_none());
    }

    #[test]
    fn a_short_chain_key_never_reaches_the_runtime_config() {
        let with = |key: Option<Vec<u8>>| AuditExportBuilderConfig {
            chain_key: key.map(CallbackSecret::new),
            ..AuditExportBuilderConfig::default()
        };
        assert!(runtime_chain_key(&with(Some(vec![1_u8; 31]))).is_none());
        assert!(runtime_chain_key(&with(Some(Vec::new()))).is_none());
        assert!(runtime_chain_key(&with(None)).is_none());
        assert!(runtime_chain_key(&with(Some(vec![1_u8; 32]))).is_some());
    }

    #[test]
    fn the_runtime_chain_key_accepts_the_configured_accept_keys() {
        let checkpoint = crate::audit_chain::ChainCheckpoint {
            start_seq: 1,
            head_seq: 1,
            head: crate::audit_chain::GENESIS,
            newest_at: DateTime::<Utc>::UNIX_EPOCH,
        };
        let old = CallbackSecret::new(vec![2_u8; 32]);
        let short = CallbackSecret::new(vec![3_u8; 31]);
        let config = AuditExportBuilderConfig {
            chain_key: Some(CallbackSecret::new(vec![1_u8; 32])),
            chain_accept_keys: vec![old.clone(), short.clone()],
            ..AuditExportBuilderConfig::default()
        };
        let key = runtime_chain_key(&config).expect("an active key");
        assert_eq!(key.secret().as_bytes(), &[1_u8; 32][..]);
        assert!(key.accepts(&checkpoint, 0, &checkpoint.mac(&old, 0)));
        assert!(!key.accepts(&checkpoint, 0, &checkpoint.mac(&short, 0)));
        let mut unrelated = config;
        unrelated.chain_accept_keys.clear();
        let key = runtime_chain_key(&unrelated).expect("an active key");
        assert!(!key.accepts(&checkpoint, 0, &checkpoint.mac(&old, 0)));
    }

    #[test]
    fn a_record_without_a_chain_hash_omits_the_field() {
        let body = serialize_batch(&[rec(1)]).expect("serializes");
        let line = String::from_utf8(body).expect("utf8");
        assert!(!line.contains("chain_hash"), "{line}");
    }

    #[test]
    fn a_chained_record_carries_its_chain_hash_last() {
        let mut record = rec(1);
        record.chain_hash = Some("ab".repeat(32));
        let body = serialize_batch(&[record]).expect("serializes");
        let line = String::from_utf8(body).expect("utf8");
        let expected = format!(",\"chain_hash\":\"{}\"}}\n", "ab".repeat(32));
        assert!(line.ends_with(&expected), "{line}");
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

    // ── Delivery state (pure; four branches, all reachable) ─────────────────

    #[test]
    fn delivery_state_reports_every_branch() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).expect("valid ts");
        let future = now + chrono::Duration::seconds(30);
        let past = now - chrono::Duration::seconds(30);

        assert_eq!(
            delivery_state(Some(future), 0, past, now, None),
            "DELIVERING",
            "a live lease means a batch is in flight right now"
        );
        assert_eq!(
            delivery_state(Some(future), 4, future, now, None),
            "DELIVERING",
            "a live lease outranks a pending backoff"
        );
        assert_eq!(
            delivery_state(Some(past), 3, future, now, None),
            "BACKOFF",
            "an expired lease with failures and a future retry is backing off"
        );
        assert_eq!(
            delivery_state(None, 3, past, now, None),
            "RETRYING",
            "failures with the retry deadline already passed is a due retry, \
             not a healthy idle"
        );
        assert_eq!(delivery_state(None, 0, past, now, None), "IDLE");

        // Retirement overrides every other reading of the row: those columns
        // are a frozen snapshot, and reporting BACKOFF or RETRYING from them
        // would say records are pending that nothing owes and retention may
        // purge.
        for (lease, failures, attempt) in [
            (Some(future), 0, past),
            (Some(past), 3, future),
            (None, 3, past),
            (None, 0, past),
        ] {
            assert_eq!(
                delivery_state(lease, failures, attempt, now, Some(past)),
                "RETIRED",
                "a retired cursor must never report a live delivery state"
            );
        }
        assert_eq!(
            delivery_state(Some(now), 0, past, now, None),
            "IDLE",
            "a lease expiring exactly now is expired, not live"
        );
    }

    // ── Batch-size and lease clamping ───────────────────────────────────────

    #[test]
    fn batch_size_is_clamped_into_the_supported_range() {
        let with = |size| {
            AuditExportBuilderConfig {
                batch_size: size,
                ..AuditExportBuilderConfig::default()
            }
            .effective_batch_size()
        };

        assert_eq!(with(0), 1, "a zero batch would never make progress");
        assert_eq!(with(-7), 1);
        assert_eq!(with(i64::MIN), 1);
        assert_eq!(with(1), 1);
        assert_eq!(with(250), 250);
        assert_eq!(with(MAX_EXPORT_BATCH_SIZE), MAX_EXPORT_BATCH_SIZE);
        assert_eq!(
            with(MAX_EXPORT_BATCH_SIZE + 1),
            MAX_EXPORT_BATCH_SIZE,
            "a batch is buffered in memory and POSTed as one body; the ceiling \
             is what stops a misconfiguration serializing a whole retention \
             window into one request"
        );
        assert_eq!(with(i64::MAX), MAX_EXPORT_BATCH_SIZE);
        assert_eq!(
            AuditExportBuilderConfig::default().effective_batch_size(),
            DEFAULT_EXPORT_BATCH_SIZE
        );
    }

    #[test]
    fn the_lease_is_floored_at_one_second() {
        let with = |lease| {
            AuditExportBuilderConfig {
                lease,
                ..AuditExportBuilderConfig::default()
            }
            .effective_lease()
        };

        assert_eq!(
            with(std::time::Duration::ZERO),
            std::time::Duration::from_secs(1),
            "a zero lease also becomes a zero sink timeout, so every batch \
             would fail before it was sent"
        );
        assert_eq!(
            with(std::time::Duration::from_millis(1)),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            with(std::time::Duration::from_secs(120)),
            std::time::Duration::from_secs(120)
        );
        assert_eq!(
            AuditExportBuilderConfig::default().effective_lease(),
            DEFAULT_EXPORT_LEASE
        );
    }

    // ── `split_reserve` shrinks both post-delivery bounds together ───────────
    //
    // Codex review on PR #1520, follow-up P1: a capped reserve must not let
    // the checkout alone consume it, leaving the acknowledgement no margin.

    #[test]
    #[cfg(feature = "db")]
    fn split_reserve_returns_the_fixed_bounds_at_the_uncapped_reserve() {
        let (checkout, ack) = split_reserve(SHARD_ACQUIRE_BOUND + ACK_QUERY_BOUND);
        assert_eq!(checkout, SHARD_ACQUIRE_BOUND);
        assert_eq!(ack, ACK_QUERY_BOUND);
    }

    #[test]
    #[cfg(feature = "db")]
    fn split_reserve_shrinks_both_bounds_proportionally() {
        let reserve = std::time::Duration::from_secs(1);
        let (checkout, ack) = split_reserve(reserve);

        assert_eq!(
            checkout + ack,
            reserve,
            "the split must account for every reserved nanosecond"
        );
        assert_eq!(checkout, std::time::Duration::from_nanos(714_285_714));
        assert_eq!(ack, std::time::Duration::from_nanos(285_714_286));
        assert!(
            checkout > ack,
            "the 5:2 ratio between SHARD_ACQUIRE_BOUND and ACK_QUERY_BOUND must survive the \
             split, or a request storm during the reacquire could still starve the query"
        );
    }

    #[test]
    #[cfg(feature = "db")]
    fn split_reserve_of_zero_is_zero() {
        assert_eq!(
            split_reserve(std::time::Duration::ZERO),
            (std::time::Duration::ZERO, std::time::Duration::ZERO)
        );
    }

    // ── The liveness budget sums every stage of a tick, not just the max ────

    #[test]
    #[cfg(feature = "db")]
    fn liveness_interval_is_the_bare_poll_interval_when_unconfigured() {
        let interval = Duration::from_secs(30);
        assert_eq!(audit_export_liveness_interval(interval, None), interval);
    }

    #[test]
    #[cfg(feature = "db")]
    fn liveness_interval_sums_sleep_checkout_and_lease() {
        struct UnusedSink;
        impl AuditSink for UnusedSink {
            fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
                unreachable!("this test never delivers a batch")
            }
        }

        let interval = Duration::from_secs(60);
        let lease = Duration::from_secs(60);
        let config = AuditExportRuntimeConfig {
            sink: std::sync::Arc::new(UnusedSink),
            secret: CallbackSecret::new(Vec::new()),
            batch_size: 100,
            backoff: ExportBackoff::default(),
            lease,
            chain_key: None,
        };

        let budget = audit_export_liveness_interval(interval, Some(&config));

        assert_eq!(
            budget,
            interval + SHARD_ACQUIRE_BOUND + lease,
            "a poll interval close to the lease must not collapse to just their max: the \
             sleep, the initial checkout, and the lease-bounded cycle are three sequential \
             stages of one tick, so their worst cases add"
        );
    }

    // ── The HMAC secret is a required control for the webhook path ──────────

    #[test]
    fn a_webhook_without_a_secret_is_reported_as_misconfigured() {
        let webhook_only = AuditExportBuilderConfig {
            webhook_url: Some("https://siem.example.com/audit".to_string()),
            ..AuditExportBuilderConfig::default()
        };
        assert!(
            webhook_only.webhook_is_missing_a_secret(),
            "HMAC-SHA256 accepts an empty key and emits a well-formed signature \
             anyone can recompute, so an unconfigured secret is worse than no \
             signature at all -- it must fail the build, not warn"
        );

        let with_secret = AuditExportBuilderConfig {
            secret: Some(CallbackSecret::new(b"k".to_vec())),
            ..webhook_only.clone()
        };
        assert!(!with_secret.webhook_is_missing_a_secret());

        // An embedder-supplied sink may authenticate some other way (IAM,
        // mTLS, a local file), so a missing HMAC key there is legitimate.
        let custom_sink = AuditExportBuilderConfig {
            sink: Some(std::sync::Arc::new(NoopSink)),
            ..webhook_only
        };
        assert!(!custom_sink.webhook_is_missing_a_secret());

        assert!(
            !AuditExportBuilderConfig::default().webhook_is_missing_a_secret(),
            "an unconfigured feature is not a misconfiguration"
        );
    }

    #[test]
    fn the_builder_config_debug_never_prints_the_secret() {
        let config = AuditExportBuilderConfig {
            secret: Some(CallbackSecret::new(b"SUPERSECRET".to_vec())),
            webhook_url: Some("https://siem.example.com/audit".to_string()),
            ..AuditExportBuilderConfig::default()
        };
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("SUPERSECRET"),
            "the HMAC key must never reach a log line: {rendered}"
        );
        assert!(rendered.contains("redacted"));
    }

    // ── Trait shape ─────────────────────────────────────────────────────────

    struct NoopSink;

    impl AuditSink for NoopSink {
        fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
            Box::pin(async { SinkAttempt::success(200) })
        }
    }

    /// Delivery is bounded by what REMAINS of the claim's lease, not the full
    /// lease (issue #953, Codex review round 19 P1). A sink that answers just
    /// inside the full lease but outside the remainder must be timed out here,
    /// because the database lease has expired and another exporter may already
    /// have reclaimed the shard and bumped the epoch — an acknowledgement from
    /// this attempt would be refused, and at consistently near-lease latency
    /// the cursor would never advance.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn delivery_is_bounded_by_the_remaining_lease_not_the_whole_one() {
        struct SlowSink;
        impl AuditSink for SlowSink {
            fn deliver<'a>(&'a self, _batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
                Box::pin(async {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    SinkAttempt::success(200)
                })
            }
        }

        let config = AuditExportRuntimeConfig {
            sink: std::sync::Arc::new(SlowSink),
            secret: CallbackSecret::new(b"k".to_vec()),
            batch_size: 10,
            backoff: ExportBackoff::default(),
            // A generous lease: bounding by THIS would let the 300ms sink win.
            lease: std::time::Duration::from_secs(60),
            chain_key: None,
        };
        let batch = AuditBatch {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
            body: b"{}",
            headers: &[],
            records: &[],
        };

        // Most of the lease is already spent: only 50ms remain.
        let nearly_expired = Utc::now() + chrono::Duration::milliseconds(50);
        let attempt = deliver_within_lease(&config, &batch, nearly_expired).await;
        assert!(
            attempt.status.is_none(),
            "a sink answering after the lease remainder must be a transport \
             failure, not a success: the claim is gone by then"
        );

        // Ample remainder: the same sink succeeds.
        let fresh = Utc::now() + chrono::Duration::seconds(5);
        let attempt = deliver_within_lease(&config, &batch, fresh).await;
        assert_eq!(attempt.status, Some(200));

        // An already-elapsed lease is an immediate timeout, never a skip.
        let past = Utc::now() - chrono::Duration::seconds(1);
        let attempt = deliver_within_lease(&config, &batch, past).await;
        assert!(attempt.status.is_none());
    }

    /// A SIEM ingest URL routinely carries its credential in the path or query,
    /// so `Debug` must not echo it (issue #953, Codex review round 23 P1). The
    /// hand-written impl already redacted the HMAC key; the URL is a secret in
    /// the same way.
    #[test]
    fn debug_never_prints_a_webhook_urls_path_or_query() {
        let config = AuditExportBuilderConfig {
            webhook_url: Some(
                "https://http-inputs.example.splunkcloud.com/services/collector/\
                 B5A79AAD-D822-46CC-80D1-819F80D7BFB0?index=audit"
                    .to_string(),
            ),
            secret: Some(CallbackSecret::new(b"hmac".to_vec())),
            ..AuditExportBuilderConfig::default()
        };

        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("B5A79AAD"),
            "the collector token must never reach a log line: {rendered}"
        );
        assert!(
            !rendered.contains("index=audit"),
            "nor the query string, which can carry one too: {rendered}"
        );
        assert!(
            rendered.contains("https://http-inputs.example.splunkcloud.com/<redacted>"),
            "the origin is the useful, non-secret part and should survive: {rendered}"
        );
        assert!(
            !rendered.contains("hmac"),
            "and the HMAC key stays redacted: {rendered}"
        );
    }

    #[test]
    fn redact_webhook_url_strips_userinfo_and_refuses_to_echo_junk() {
        // Userinfo is itself a credential.
        assert_eq!(
            redact_webhook_url("https://user:s3cret@siem.example.com/ingest?k=v"),
            "https://siem.example.com/<redacted>"
        );
        // A bare origin still renders.
        assert_eq!(
            redact_webhook_url("https://siem.example.com"),
            "https://siem.example.com/<redacted>"
        );
        // Anything unparseable is NOT echoed back on the assumption it is safe.
        for junk in ["not-a-url", "https://", ""] {
            let rendered = redact_webhook_url(junk);
            assert_eq!(rendered, "<unparseable webhook url redacted>");
        }
    }

    #[test]
    fn redact_webhook_url_stops_the_authority_at_a_backslash() {
        // Issue #1274: `url::Url::parse` treats a backslash as a path
        // separator for `https`/`http`, the same as a forward slash.
        // `redact_webhook_url` split only on `/`, `?`, and `#`, so a
        // backslash-delimited secret rode along as part of the authority.
        assert_eq!(
            redact_webhook_url("https://evil.com\\bearer-secret"),
            "https://evil.com/<redacted>"
        );
    }

    #[test]
    fn redact_webhook_url_withholds_a_malformed_authority() {
        // Issue #1274: `user:s3cr3t` with no `@` is `host:port`, not
        // userinfo. `s3cr3t` is not a valid port, so `url::Url::parse`
        // rejects the whole string. A naive split on `/` still finds an
        // "authority" here and renders the secret through it.
        assert_eq!(
            redact_webhook_url("https://user:s3cr3t/api"),
            "<unparseable webhook url redacted>"
        );
    }

    #[test]
    fn redact_webhook_url_keeps_a_non_default_port() {
        // Issue #1274: a port is part of the origin, not the credential.
        // Two targets on the same host at different ports are different
        // endpoints. Dropping the port made them indistinguishable in a
        // startup error or a runtime log line.
        assert_eq!(
            redact_webhook_url("https://siem.example.com:8443/token"),
            "https://siem.example.com:8443/<redacted>"
        );
        // The default port for the scheme adds no information; omit it,
        // matching the pre-existing behavior for a bare origin.
        assert_eq!(
            redact_webhook_url("https://siem.example.com:443/token"),
            "https://siem.example.com/<redacted>"
        );
    }

    /// A delivery whose sink was swapped mid-flight must not advance the
    /// cursor: the records went to the OLD destination, and marking them
    /// delivered would leave the newly configured SIEM without them (issue
    /// #953, Codex review round 25 P1).
    ///
    /// Fully deterministic — the decision is a pure function of its arguments,
    /// so this touches no process-wide state and cannot race the 33 other tests
    /// in this binary that write `GLOBAL_AUDIT_EXPORT_CONFIG`.
    #[cfg(feature = "db")]
    #[test]
    fn a_sink_swapped_mid_flight_holds_the_cursor() {
        fn config(batch_size: i64) -> std::sync::Arc<AuditExportRuntimeConfig> {
            std::sync::Arc::new(AuditExportRuntimeConfig {
                sink: std::sync::Arc::new(NoopSink),
                secret: CallbackSecret::new(b"k".to_vec()),
                batch_size,
                backoff: ExportBackoff::default(),
                lease: std::time::Duration::from_secs(30),
                chain_key: None,
            })
        }

        let delivered_with = config(10);

        // Same Arc still installed: the outcome is untouched.
        let kept = fence_against_sink_swap(
            SinkAttempt::success(200),
            &delivered_with,
            Some(&delivered_with),
        );
        assert_eq!(
            kept.status,
            Some(200),
            "an unchanged sink must leave the outcome alone"
        );

        // A different runtime's sink is now installed: the 2xx must not count.
        let replacement = config(99);
        let fenced = fence_against_sink_swap(
            SinkAttempt::success(200),
            &delivered_with,
            Some(&replacement),
        );
        assert!(
            fenced.status.is_none(),
            "a 2xx from the sink that was replaced mid-flight must be held, or \
             the cursor advances past records the new SIEM never received"
        );

        // An equal-but-distinct config is still a different sink: identity,
        // not value, is what decides where the bytes actually went.
        let twin = config(10);
        let fenced =
            fence_against_sink_swap(SinkAttempt::success(200), &delivered_with, Some(&twin));
        assert!(fenced.status.is_none());

        // Export switched off mid-flight counts as swapped too.
        let fenced = fence_against_sink_swap(SinkAttempt::success(200), &delivered_with, None);
        assert!(fenced.status.is_none());
    }

    #[test]
    fn an_explicitly_empty_webhook_secret_is_missing() {
        // The realistic path: `audit_export_secret(std::env::var("SIEM_HMAC")
        // .unwrap_or_default())` with the variable unset. An `is_none()` check
        // waves this through and the build ships a signature anyone can
        // recompute -- the exact outcome the check exists to prevent.
        let mut config = AuditExportBuilderConfig {
            webhook_url: Some("https://siem.example.com/ingest".to_string()),
            secret: Some(CallbackSecret::new(Vec::new())),
            ..AuditExportBuilderConfig::default()
        };
        assert!(
            config.webhook_is_missing_a_secret(),
            "an empty secret must fail closed exactly like an absent one"
        );

        config.secret = Some(CallbackSecret::new(b"real".to_vec()));
        assert!(!config.webhook_is_missing_a_secret());

        // Still scoped to the webhook path: an embedder sink may authenticate
        // however it likes, so an empty secret there is only a warning.
        config.secret = Some(CallbackSecret::new(Vec::new()));
        config.sink = Some(std::sync::Arc::new(NoopSink));
        assert!(!config.webhook_is_missing_a_secret());
    }

    #[test]
    fn audit_sink_is_object_safe_and_send_sync() {
        fn assert_bounds<T: AuditSink>() {}
        assert_bounds::<NoopSink>();
        let boxed: Box<dyn AuditSink> = Box::new(NoopSink);
        let _arc: std::sync::Arc<dyn AuditSink> = std::sync::Arc::from(boxed);
    }

    // ── Disabled-after-enabled signal (issue #1506) ─────────────────────────
    //
    // These tests use a private slot and flag. Builder tests in this binary
    // write the process-wide statics at the same time.

    #[cfg(feature = "db")]
    #[derive(Default)]
    struct ObservedLog(std::sync::Mutex<Vec<(u16, bool)>>);

    #[cfg(feature = "db")]
    impl crate::telemetry::MetricsRecorder for ObservedLog {
        fn record_audit_export_observed(&self, shard: u16, observed: bool) {
            self.0.lock().expect("log lock").push((shard, observed));
        }
    }

    #[cfg(feature = "db")]
    fn live_config() -> std::sync::Arc<AuditExportRuntimeConfig> {
        std::sync::Arc::new(AuditExportRuntimeConfig {
            sink: std::sync::Arc::new(NoopSink),
            secret: CallbackSecret::new(b"k".to_vec()),
            batch_size: 10,
            backoff: ExportBackoff::default(),
            lease: Duration::from_secs(30),
            chain_key: None,
        })
    }

    /// A private config slot and flag.
    #[cfg(feature = "db")]
    #[derive(Default)]
    struct Edge {
        slot: Option<std::sync::Arc<AuditExportRuntimeConfig>>,
        disabled: std::sync::atomic::AtomicBool,
    }

    #[cfg(feature = "db")]
    impl Edge {
        fn set(&mut self, live: bool) {
            apply_config_edge(&mut self.slot, &self.disabled, live.then(live_config));
        }

        fn report(&self, shard: u16, log: &ObservedLog) {
            report_if_disabled(
                self.disabled.load(std::sync::atomic::Ordering::Relaxed),
                log,
                shard,
            );
        }

        fn observed(&self) -> Vec<(u16, bool)> {
            let log = ObservedLog::default();
            self.report(3, &log);
            log.0.lock().expect("log lock").clone()
        }
    }

    #[cfg(feature = "db")]
    #[test]
    fn disabling_a_live_export_marks_the_shard_unobserved() {
        let mut edge = Edge::default();
        edge.set(true);
        edge.set(false);
        assert_eq!(edge.observed(), vec![(3, false)]);
    }

    #[cfg(feature = "db")]
    #[test]
    fn a_never_configured_process_reports_nothing() {
        let mut edge = Edge::default();
        edge.set(false);
        assert_eq!(edge.observed(), [] as [(u16, bool); 0]);
    }

    #[cfg(feature = "db")]
    #[test]
    fn reenabling_export_clears_the_signal() {
        let mut edge = Edge::default();
        for live in [true, false, true] {
            edge.set(live);
        }
        assert_eq!(edge.observed(), [] as [(u16, bool); 0]);
    }

    #[cfg(feature = "db")]
    #[test]
    fn clearing_twice_keeps_the_signal() {
        let mut edge = Edge::default();
        for live in [true, false, false] {
            edge.set(live);
        }
        assert_eq!(edge.observed(), vec![(3, false)]);
    }

    #[cfg(feature = "db")]
    #[test]
    fn the_signal_repeats_on_every_tick() {
        let mut edge = Edge::default();
        edge.set(true);
        edge.set(false);
        let log = ObservedLog::default();
        edge.report(1, &log);
        edge.report(2, &log);
        assert_eq!(
            *log.0.lock().expect("log lock"),
            vec![(1, false), (2, false)]
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn disabled_ticks_report_assigned_shards_or_the_default() {
        use crate::types::ShardId;
        let assigned = [ShardId::new(4), ShardId::new(7)];
        assert_eq!(disabled_tick_shards(Some(0), &assigned), vec![4, 7]);
        assert_eq!(disabled_tick_shards(Some(2), &[]), vec![2]);
        assert_eq!(disabled_tick_shards(None, &assigned), vec![0]);
    }

    /// Issue #1667: only a confirmed valid index opens the build gate. A lost
    /// lock race keeps the retry wait. A refused build waits far longer.
    #[cfg(feature = "db")]
    #[test]
    fn the_index_build_gate_opens_only_on_a_ready_index() {
        let key: BuildKey = (
            9_001,
            dsn_fingerprint("postgres://gate-test/lock-busy"),
            "public".to_owned(),
        );
        assert!(index_build_due(&key), "a fresh key is due");
        assert!(!index_build_due(&key), "an in-flight build is not due");

        index_build_finished(&key, BuildEnd::Retry);
        assert!(
            !index_build_due(&key),
            "a lost lock race keeps the gate closed"
        );

        index_build_finished(&key, BuildEnd::Ready);
        assert!(index_build_due(&key), "a valid index opens the gate");

        index_build_finished(&key, BuildEnd::Refused);
        let not_before = INDEX_BUILD_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .copied()
            .expect("a refused build leaves a gate entry");
        let remaining = not_before.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining > INDEX_BUILD_RETRY,
            "a refused build waits longer than an ordinary retry: {remaining:?}"
        );
        index_build_finished(&key, BuildEnd::Ready);
    }

    /// Issue #1667: the notice gate is per database. Two databases can share a
    /// shard number inside one process. One must not hide the other's notice.
    #[cfg(feature = "db")]
    #[test]
    fn the_index_notice_gate_is_per_database() {
        let first: NoticeKey = (9_002, "notice-test-db-a".to_owned());
        let second: NoticeKey = (9_002, "notice-test-db-b".to_owned());
        assert!(index_notice_due(&first), "a fresh key is due");
        assert!(
            !index_notice_due(&first),
            "a repeat inside the interval waits"
        );
        assert!(
            index_notice_due(&second),
            "another database with the same shard number is due"
        );
    }

    /// Issue #1667: a host that accepts the socket but never answers must not
    /// hold the build task. The connect timeout turns the stall into an error.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn a_stalled_connect_ends_the_build_attempt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let _holder = tokio::spawn(async move {
            let Ok((_socket, _)) = listener.accept().await else {
                return;
            };
            // Hold the socket open and never answer.
            std::future::pending::<()>().await;
        });
        let dsn = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let started = std::time::Instant::now();
        let result = build_unexported_index_on_dedicated_connection(
            &dsn,
            "public",
            std::time::Duration::from_millis(200),
        )
        .await;
        assert!(result.is_err(), "a stalled connect must be an error");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    /// Issue #1667: the operator statements name the schema the builder
    /// resolved. A migration role with another default path then acts on the
    /// right table. `CREATE INDEX` takes an unqualified index name.
    #[test]
    fn the_operator_statements_name_the_resolved_schema() {
        let build = unexported_index_ddl_in("tenant");
        assert!(build.contains("ON tenant.harvest_audit_log"), "{build}");
        assert!(build.contains("CONCURRENTLY"), "{build}");
        let cleanup = unexported_index_drop_ddl_in("tenant");
        assert!(
            cleanup.contains("tenant.harvest_audit_log_unexported_idx"),
            "{cleanup}"
        );
    }

    /// Issue #1667: two tenant schemas in one database share a shard number
    /// and a database identity. The schema must tell their notices apart.
    #[cfg(feature = "db")]
    #[test]
    fn the_notice_key_names_the_schema() {
        let first = notice_key(0, "db@host:5432", "tenant_a");
        let second = notice_key(0, "db@host:5432", "tenant_b");
        assert_ne!(first, second);
        assert_eq!(first, notice_key(0, "db@host:5432", "tenant_a"));
    }

    /// Issue #1667: a task that is still alive keeps its gate closed. A task
    /// can stall after it connects, so the retry wait must not reopen the gate
    /// while the task runs. Only the end of the task sets the next wait.
    #[cfg(feature = "db")]
    #[test]
    fn an_in_flight_build_keeps_the_gate_closed_past_the_retry_wait() {
        let key: BuildKey = (
            9_003,
            dsn_fingerprint("postgres://gate-test/in-flight"),
            "public".to_owned(),
        );
        assert!(index_build_due(&key));
        let not_before = INDEX_BUILD_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .copied()
            .expect("an in-flight build leaves a gate entry");
        let remaining = not_before.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining > INDEX_BUILD_REFUSED_RETRY,
            "an in-flight build must outlast every retry wait: {remaining:?}"
        );
        index_build_finished(&key, BuildEnd::Ready);
    }

    /// Issue #1667: two tenant schemas can share a shard number and a build
    /// URL. A refused build in one must not close the gate of the other.
    #[cfg(feature = "db")]
    #[test]
    fn the_build_gate_is_per_schema() {
        let dsn = dsn_fingerprint("postgres://gate-test/schemas");
        let first: BuildKey = (9_004, dsn, "tenant_a".to_owned());
        let second: BuildKey = (9_004, dsn, "tenant_b".to_owned());
        assert!(index_build_due(&first));
        index_build_finished(&first, BuildEnd::Refused);
        assert!(!index_build_due(&first), "the refused schema waits");
        assert!(index_build_due(&second), "the other schema is not blocked");
        index_build_finished(&first, BuildEnd::Ready);
        index_build_finished(&second, BuildEnd::Ready);
    }

    /// Issue #1667: Unix-socket connections report no address and no port. Two
    /// clusters with the same database name must still differ, so the start
    /// time of the postmaster is part of the identity.
    #[cfg(feature = "db")]
    #[test]
    fn the_database_identity_tells_unix_socket_clusters_apart() {
        let first = database_identity_of("harvest", "", "", "2026-10-02 08:00:00+00");
        let second = database_identity_of("harvest", "", "", "2026-10-02 08:05:00+00");
        assert_ne!(first, second);
        assert_eq!(
            first,
            database_identity_of("harvest", "", "", "2026-10-02 08:00:00+00")
        );
    }

    /// Issue #1667: a probe on a stalled connection must not hold shutdown. The
    /// race against the cancel token ends it.
    #[cfg(feature = "db")]
    #[tokio::test]
    async fn a_stalled_probe_ends_when_shutdown_is_requested() {
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let stalled = std::future::pending::<u8>();
        assert_eq!(until_cancelled(&cancel, stalled).await, None);
        let live = tokio_util::sync::CancellationToken::new();
        assert_eq!(until_cancelled(&live, async { 7_u8 }).await, Some(7));
    }

    /// Issue #1667: a dropped build future must not leave the gate closed for
    /// the in-flight period. Shutdown can drop it mid-probe. The guard turns
    /// the gate into an ordinary retry wait. A disarmed guard changes nothing.
    #[cfg(feature = "db")]
    #[test]
    fn a_dropped_in_flight_guard_leaves_a_retry_wait() {
        let key: BuildKey = (
            9_005,
            dsn_fingerprint("postgres://gate-test/guard"),
            "public".to_owned(),
        );
        let remaining = |key: &BuildKey| {
            INDEX_BUILD_GATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .copied()
                .map(|not_before| not_before.saturating_duration_since(std::time::Instant::now()))
        };
        assert!(index_build_due(&key));
        drop(InFlightGuard::new(key.clone()));
        let after_drop = remaining(&key).expect("a dropped guard leaves an entry");
        assert!(after_drop <= INDEX_BUILD_RETRY, "{after_drop:?}");
        assert!(!index_build_due(&key), "the retry wait still holds");
        index_build_finished(&key, BuildEnd::Ready);

        assert!(index_build_due(&key));
        InFlightGuard::new(key.clone()).disarm();
        let after_disarm = remaining(&key).expect("a disarmed guard keeps the entry");
        assert!(after_disarm > INDEX_BUILD_REFUSED_RETRY, "{after_disarm:?}");
        index_build_finished(&key, BuildEnd::Ready);
    }

    /// Issue #1667: a healthy exporter must not probe the catalogs on every
    /// tick. The probe gate admits one probe per interval for each shard, build
    /// URL and pool. A second pool with the same shard and URL has its own gate,
    /// so two tenants cannot starve each other.
    #[cfg(feature = "db")]
    #[test]
    fn the_index_probe_is_throttled_per_pool() {
        let dsn = dsn_fingerprint("postgres://probe-test/db");
        let key: ProbeKey = (9_006, dsn, 1);
        let other_pool: ProbeKey = (9_006, dsn, 2);
        assert!(index_probe_due(&key), "the first tick probes");
        assert!(
            !index_probe_due(&key),
            "the next tick inside the interval waits"
        );
        assert!(
            index_probe_due(&other_pool),
            "another pool has its own gate"
        );
    }

    /// Issue #1667: the gates are process-wide statics. A build URL can carry a
    /// password, so no key may hold its text. A fingerprint stands in for it.
    #[cfg(feature = "db")]
    #[test]
    fn the_gate_keys_hold_no_dsn_text() {
        let dsn = "postgres://user:s3cret-pass@host/db";
        let fingerprint = dsn_fingerprint(dsn);
        assert_eq!(fingerprint, dsn_fingerprint(dsn), "stable inside a process");
        assert_ne!(
            fingerprint,
            dsn_fingerprint("postgres://user:other@host/db")
        );
        let key: ProbeKey = (1, fingerprint, 0);
        assert!(!format!("{key:?}").contains("s3cret"));
    }

    /// Issue #1667: a gate entry that has expired means the same as no entry.
    /// A long-lived process that churns workers must not keep it. Each new
    /// admission evicts the expired entries.
    #[cfg(feature = "db")]
    #[test]
    fn expired_gate_entries_are_evicted() {
        let expired = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("an instant one second ago");
        let stale_probe: ProbeKey = (9_008, dsn_fingerprint("postgres://evict/probe"), 1);
        INDEX_PROBE_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(stale_probe, expired);
        assert!(index_probe_due(&(
            9_008,
            dsn_fingerprint("postgres://evict/probe"),
            2
        )));
        assert!(
            !INDEX_PROBE_GATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&stale_probe),
            "an expired probe entry must be evicted"
        );

        let stale_build: BuildKey = (
            9_008,
            dsn_fingerprint("postgres://evict/build"),
            "s".to_owned(),
        );
        INDEX_BUILD_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(stale_build.clone(), expired);
        let fresh: BuildKey = (
            9_008,
            dsn_fingerprint("postgres://evict/build"),
            "t".to_owned(),
        );
        assert!(index_build_due(&fresh));
        assert!(
            !INDEX_BUILD_GATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&stale_build),
            "an expired build entry must be evicted"
        );
        index_build_finished(&fresh, BuildEnd::Ready);
    }

    /// Issue #1667: a privilege failure is the one error a retry cannot fix.
    #[cfg(feature = "db")]
    #[test]
    fn a_privilege_error_needs_an_owner() {
        use crate::error::HarvestError;
        for message in [
            "must be owner of table harvest_audit_log",
            "permission denied for table harvest_audit_log",
            "SQLSTATE 42501",
        ] {
            assert!(
                index_build_needs_owner(&HarvestError::Database(message.to_owned())),
                "{message}"
            );
        }
        assert!(!index_build_needs_owner(&HarvestError::Database(
            "canceling statement due to statement timeout".to_owned()
        )));
        assert!(!index_build_needs_owner(&HarvestError::Config(
            "must be owner".to_owned()
        )));
    }
}
