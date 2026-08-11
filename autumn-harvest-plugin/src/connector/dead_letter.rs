//! Durable dead-letter destination for poison broker messages (issue #944).
//!
//! # Why a plugin-owned table, not `harvest_dead_letters`
//!
//! The engine's `harvest_dead_letters` table is keyed to a *task*:
//! `original_task_id` is `NOT NULL` and `task_type` carries a
//! `CHECK (task_type IN ('workflow','activity'))`. A poison broker message has
//! neither — it never became a task, and by definition never produced an
//! execution. Reusing that table would mean relaxing a **core** schema
//! constraint; issue #944 explicitly forbids a core migration for the trigger
//! path and explicitly sanctions a plugin-owned table instead.
//!
//! `harvest_connector_dead_letters` therefore lives in the plugin's own
//! migrations directory, alongside `harvest_workflow_outbox` — both are
//! app-side ingestion/egress tables rather than engine state. **No
//! `WorkflowEvent` variant, no core migration.**

use async_trait::async_trait;
use autumn_harvest::telemetry::PoisonReason;
use chrono::{DateTime, Utc};
use diesel_async::AsyncPgConnection;
use uuid::Uuid;

use super::source::ConnectorError;

/// One poison message, captured with everything needed to diagnose and replay
/// it by hand.
#[derive(Debug, Clone)]
pub struct ConnectorDeadLetter {
    /// The binding the message arrived on.
    pub binding: String,
    /// The logical stream (topic or queue).
    pub stream: String,
    /// Rendered broker coordinates, e.g. `orders:0:91`.
    pub coordinates: String,
    /// The derived idempotency key, so an operator can correlate this record
    /// with a redelivery.
    pub idempotency_key: String,
    /// The target workflow type.
    pub workflow_name: String,
    /// Bounded poison classification.
    pub reason: PoisonReason,
    /// Human-readable detail (the decode error, the rejection, the refusal).
    pub detail: String,
    /// Consecutive strikes at the point of dead-lettering.
    pub attempts: i32,
    /// The raw message body, so the message can be replayed after a fix.
    pub payload: Vec<u8>,
    /// When the message was dead-lettered.
    pub failed_at: DateTime<Utc>,
}

/// Whether a [`DeadLetterSink::write`] created the record or found it already
/// there.
///
/// Dead-lettering is idempotent by idempotency key, so the *same physical
/// message* can reach a sink more than once — the runtime acks only after the
/// write succeeds and `ack` is deliberately best-effort, so a lost commit (or a
/// process death between the two) brings the message back. The in-process
/// poison tracker dedupes that within one process; this is what still knows
/// across a **restart**, which is precisely when the redelivery is most likely.
///
/// The runtime counts `harvest.connector.poisoned` only on
/// [`Self::Recorded`], so one poison message contributes one sample however
/// many times it is redelivered or however often the process restarts.
///
/// A sink that genuinely cannot tell — because its store has no conflict
/// signal — returns `Recorded`, which is the pre-existing behaviour: at worst
/// the metric over-counts a redelivery, exactly as it did before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterOutcome {
    /// This write created the record: a new poison message.
    Recorded,
    /// A record for this idempotency key already existed, so the write was a
    /// no-op and this is a redelivery of an already-quarantined message.
    AlreadyRecorded,
}

/// Where a binding's poison messages are written.
#[async_trait]
pub trait DeadLetterSink: Send + Sync {
    /// Durably record a poison message.
    ///
    /// The runtime acknowledges the broker message **only after** this
    /// returns `Ok`, so a sink failure leaves the message for redelivery
    /// rather than losing it.
    ///
    /// Must be **idempotent by [`ConnectorDeadLetter::idempotency_key`]**: the
    /// same physical message can arrive more than once (see
    /// [`DeadLetterOutcome`]), and a second record for it would double-count
    /// every dashboard built on the table. Report which of the two happened —
    /// a sink whose store cannot tell returns [`DeadLetterOutcome::Recorded`].
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Broker`] (or [`ConnectorError::Config`]) when
    /// the record could not be written.
    async fn write(&self, entry: &ConnectorDeadLetter)
    -> Result<DeadLetterOutcome, ConnectorError>;
}

/// A sink that writes to `harvest_connector_dead_letters`.
pub struct PostgresDeadLetterSink {
    pool: autumn_harvest::worker::DbPool,
}

impl std::fmt::Debug for PostgresDeadLetterSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresDeadLetterSink")
            .finish_non_exhaustive()
    }
}

impl PostgresDeadLetterSink {
    /// Write dead letters to `pool`'s database.
    #[must_use]
    pub const fn new(pool: autumn_harvest::worker::DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DeadLetterSink for PostgresDeadLetterSink {
    async fn write(
        &self,
        entry: &ConnectorDeadLetter,
    ) -> Result<DeadLetterOutcome, ConnectorError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| ConnectorError::Broker(format!("dead-letter pool: {e}")))?;

        insert_dead_letter(&mut conn, entry).await
    }
}

/// Insert one dead-letter row, reporting whether it created it.
///
/// Split out from the sink so integration tests can drive it against a
/// caller-owned connection.
///
/// # Errors
///
/// Returns [`ConnectorError::Broker`] when the insert fails.
pub async fn insert_dead_letter(
    conn: &mut AsyncPgConnection,
    entry: &ConnectorDeadLetter,
) -> Result<DeadLetterOutcome, ConnectorError> {
    use diesel::sql_types::{Binary, Int4, Text, Timestamptz, Uuid as SqlUuid};
    use diesel_async::RunQueryDsl;

    // `ON CONFLICT (idempotency_key) DO NOTHING` makes dead-lettering itself
    // idempotent: a redelivery that dead-letters again (e.g. after a
    // connector restart reset the strike counter) must not create a second
    // record for the same message.
    //
    // The affected-row count is that conflict made observable: zero rows means
    // the record was already there, which is the only durable answer to "is
    // this a NEW poison message" once a restart has emptied the in-process
    // tracker. Discarding it left `harvest.connector.poisoned` counting one
    // sample per restart-redelivery of a message already quarantined.
    let inserted = diesel::sql_query(
        "INSERT INTO harvest_connector_dead_letters \
         (id, binding, stream, coordinates, idempotency_key, workflow_name, reason, detail, \
          attempts, payload, failed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind::<SqlUuid, _>(Uuid::new_v4())
    .bind::<Text, _>(&entry.binding)
    .bind::<Text, _>(&entry.stream)
    .bind::<Text, _>(&entry.coordinates)
    .bind::<Text, _>(&entry.idempotency_key)
    .bind::<Text, _>(&entry.workflow_name)
    .bind::<Text, _>(entry.reason.as_str())
    .bind::<Text, _>(&entry.detail)
    .bind::<Int4, _>(entry.attempts)
    .bind::<Binary, _>(&entry.payload)
    .bind::<Timestamptz, _>(entry.failed_at)
    .execute(conn)
    .await
    .map_err(|e| ConnectorError::Broker(format!("dead-letter insert: {e}")))?;

    if inserted == 0 {
        Ok(DeadLetterOutcome::AlreadyRecorded)
    } else {
        Ok(DeadLetterOutcome::Recorded)
    }
}

/// A sink that records nothing.
///
/// Used with [`super::disposition::DeadLetterMode::BrokerNative`], where the
/// broker's own redrive policy owns the dead-letter destination and the
/// runtime abandons the message rather than acknowledging it. Also useful in
/// tests.
///
/// Deliberately **not** the runtime's default — see
/// [`UnconfiguredDeadLetterSink`]. Installing this one under
/// [`DeadLetterMode::HarvestSink`][hs] acknowledges every poison message with
/// no record anywhere, so reach for it only when something else owns the
/// dead-letter destination.
///
/// [hs]: super::disposition::DeadLetterMode::HarvestSink
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopDeadLetterSink;

#[async_trait]
impl DeadLetterSink for NoopDeadLetterSink {
    async fn write(
        &self,
        _entry: &ConnectorDeadLetter,
    ) -> Result<DeadLetterOutcome, ConnectorError> {
        // Records nothing, so it can never observe a conflict. `Recorded` is
        // the honest answer for a sink that cannot tell — see
        // [`DeadLetterOutcome`].
        Ok(DeadLetterOutcome::Recorded)
    }
}

/// The default sink for a runtime nobody has given one to.
///
/// A binding defaults to
/// [`DeadLetterMode::HarvestSink`][hs], so a runtime built directly — via
/// [`ConnectorRuntime::new`][new] or [`for_binding`][fb], both public — would
/// otherwise pair "record this poison message" with a sink that records
/// nothing, acknowledge the message, and lose it permanently with no trace in
/// any table or broker.
///
/// Failing is the safe default: the runtime only acknowledges a dead letter
/// *after* the sink write succeeds, so an error here leaves the message on the
/// broker for redelivery and logs loudly, turning silent data loss into a
/// visible misconfiguration. Install a real sink with
/// [`with_dead_letter_sink`][wds], or let the broker own dead-lettering with
/// [`SourceBinding::broker_native_dead_letter`][bn] — that mode abandons
/// rather than writing, so it never reaches a sink at all.
///
/// [hs]: super::disposition::DeadLetterMode::HarvestSink
/// [new]: super::runtime::ConnectorRuntime::new
/// [fb]: super::runtime::ConnectorRuntime::for_binding
/// [wds]: super::runtime::ConnectorRuntime::with_dead_letter_sink
/// [bn]: super::binding::SourceBinding::broker_native_dead_letter
#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredDeadLetterSink;

#[async_trait]
impl DeadLetterSink for UnconfiguredDeadLetterSink {
    async fn write(
        &self,
        _entry: &ConnectorDeadLetter,
    ) -> Result<DeadLetterOutcome, ConnectorError> {
        Err(ConnectorError::Config(
            "no dead-letter sink installed: this binding dead-letters into harvest, but the \
             runtime has no sink to write to. Call `with_dead_letter_sink(...)`, or switch the \
             binding to `broker_native_dead_letter()` so the broker's redrive policy owns it. \
             The message was left on the broker rather than acknowledged."
                .to_string(),
        ))
    }
}

/// A sink that records into memory, exported for tests and local development.
///
/// Lets an embedder assert *what* would have been dead-lettered — reason,
/// coordinates, raw payload — without provisioning a database, and is what the
/// connector's own poison-isolation suite runs against.
#[derive(Debug, Default, Clone)]
pub struct RecordingDeadLetterSink {
    entries: std::sync::Arc<std::sync::Mutex<Vec<ConnectorDeadLetter>>>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RecordingDeadLetterSink {
    /// An empty recording sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Make every subsequent [`DeadLetterSink::write`] fail, so a caller can
    /// assert the runtime leaves the message for redelivery rather than
    /// acknowledging a dead letter it never durably recorded.
    pub fn fail_writes(&self, fail: bool) {
        self.fail.store(fail, std::sync::atomic::Ordering::SeqCst);
    }

    /// Everything written so far, in order.
    #[must_use]
    pub fn entries(&self) -> Vec<ConnectorDeadLetter> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl DeadLetterSink for RecordingDeadLetterSink {
    async fn write(
        &self,
        entry: &ConnectorDeadLetter,
    ) -> Result<DeadLetterOutcome, ConnectorError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ConnectorError::Broker("recording sink failure".to_string()));
        }
        // Idempotent by key, exactly like the Postgres sink's
        // `ON CONFLICT (idempotency_key) DO NOTHING`. This sink stands in for
        // that one in the poison-isolation suite, so diverging here would let a
        // double-record bug pass every no-database test. The check and the push
        // share one guard so the pair is atomic, as the SQL statement is.
        let outcome = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if entries
                .iter()
                .any(|e| e.idempotency_key == entry.idempotency_key)
            {
                DeadLetterOutcome::AlreadyRecorded
            } else {
                entries.push(entry.clone());
                DeadLetterOutcome::Recorded
            }
        };
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_sink_accepts_everything() {
        let sink = NoopDeadLetterSink;
        let entry = ConnectorDeadLetter {
            binding: "orders".to_string(),
            stream: "orders".to_string(),
            coordinates: "orders:0:1".to_string(),
            idempotency_key: "conn:L6:orders:L0::L11:orders:0:1".to_string(),
            workflow_name: "order_flow".to_string(),
            reason: PoisonReason::Malformed,
            detail: "bad json".to_string(),
            attempts: 1,
            payload: b"{".to_vec(),
            failed_at: Utc::now(),
        };
        assert!(sink.write(&entry).await.is_ok());
    }

    #[tokio::test]
    async fn the_unconfigured_sink_refuses_and_says_how_to_fix_it() {
        let entry = ConnectorDeadLetter {
            binding: "orders".to_string(),
            stream: "orders".to_string(),
            coordinates: "orders:0:1".to_string(),
            idempotency_key: "conn:L6:orders:L0::L11:orders:0:1".to_string(),
            workflow_name: "order_flow".to_string(),
            reason: PoisonReason::Malformed,
            detail: "bad json".to_string(),
            attempts: 1,
            payload: b"{".to_vec(),
            failed_at: Utc::now(),
        };

        let err = UnconfiguredDeadLetterSink
            .write(&entry)
            .await
            .expect_err("an unconfigured sink must refuse, so the runtime never acks");

        // The error is the operator's only signal here, so it has to name both
        // remedies rather than just complaining.
        let message = err.to_string();
        assert!(
            message.contains("with_dead_letter_sink"),
            "must name the sink remedy: {message}"
        );
        assert!(
            message.contains("broker_native_dead_letter"),
            "must name the broker-native remedy: {message}"
        );
    }

    fn entry() -> ConnectorDeadLetter {
        ConnectorDeadLetter {
            binding: "orders".to_string(),
            stream: "orders".to_string(),
            coordinates: "orders:0:1".to_string(),
            idempotency_key: "k".to_string(),
            workflow_name: "order_flow".to_string(),
            reason: PoisonReason::Malformed,
            detail: "bad json".to_string(),
            attempts: 1,
            payload: b"{".to_vec(),
            failed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn recording_sink_captures_entries_in_order() {
        let sink = RecordingDeadLetterSink::new();
        sink.write(&entry()).await.unwrap();
        let mut second = entry();
        second.coordinates = "orders:0:2".to_string();
        // Distinct messages carry distinct keys -- the key is derived from the
        // coordinates, so sharing one here would model two deliveries of the
        // SAME message rather than two different ones.
        second.idempotency_key = "k2".to_string();
        sink.write(&second).await.unwrap();

        let entries = sink.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].coordinates, "orders:0:1");
        assert_eq!(entries[1].coordinates, "orders:0:2");
    }

    #[tokio::test]
    async fn recording_sink_is_idempotent_by_key_and_says_so() {
        // Mirrors the Postgres sink's `ON CONFLICT (idempotency_key) DO
        // NOTHING`. The no-database poison suite runs against this sink, so a
        // divergence here would let a double-record bug pass every test that
        // does not need Docker.
        let sink = RecordingDeadLetterSink::new();

        assert_eq!(
            sink.write(&entry()).await.unwrap(),
            DeadLetterOutcome::Recorded
        );

        // The same message comes back -- a lost ack, or a restart between the
        // write and the commit.
        let mut redelivered = entry();
        redelivered.detail = "bad json (again)".to_string();
        assert_eq!(
            sink.write(&redelivered).await.unwrap(),
            DeadLetterOutcome::AlreadyRecorded,
            "the key already has a record, and the caller needs to know that to \
             avoid counting the same poison message twice"
        );

        assert_eq!(sink.entries().len(), 1, "one record per poison message");
        assert_eq!(
            sink.entries()[0].detail,
            "bad json",
            "the first record wins, exactly as DO NOTHING leaves the original row"
        );
    }

    #[tokio::test]
    async fn recording_sink_can_be_made_to_fail() {
        let sink = RecordingDeadLetterSink::new();
        sink.fail_writes(true);
        assert!(sink.write(&entry()).await.is_err());
        assert!(sink.entries().is_empty(), "a failed write records nothing");

        sink.fail_writes(false);
        assert!(sink.write(&entry()).await.is_ok());
        assert_eq!(sink.entries().len(), 1);
    }
}
