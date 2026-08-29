//! Payload-codec key rotation and the lazy re-encryption sweep (issue #948).
//!
//! ## Why
//!
//! The [`crate::payload_codec`] boundary lets an embedder encrypt every
//! payload-bearing field before it touches `harvest_events`. Without rotation
//! that is compliance theater: `harvest_events` is append-only, so after a key
//! compromise every byte of stored history stays encrypted under the
//! compromised key forever. This module closes that: envelopes carry a key id,
//! the registry holds many keyed codecs with exactly one active, and a
//! shard-local background sweep lazily re-encrypts stored rows onto the active
//! key until the old key can be retired with proof that nothing depends on it.
//!
//! ## ⚠️ Sanctioned in-place mutation exception #3
//!
//! Re-encryption **mutates stored `harvest_events.event_data` bytes in place**.
//! Exactly two code paths write `event_data` after insert: PII erasure
//! ([`crate::erase`], issue #495, historically "exception #2") and this
//! ("#3"). See the "Engine Invariants" section of `CLAUDE.md`, including why
//! the heartbeat checkpoint issue #948 named as the first exception is not one
//! (it mutates `harvest_task_queue`, not the event log).
//!
//! The scope guarantee that makes it safe: **only the ciphertext bytes inside
//! payload fields change.** The decoded plaintext is byte-identical before and
//! after, and the event `type`, variant structure, event ids, ordering, and
//! timestamps are never touched — so replay determinism is unaffected *by
//! construction*, not by convention. `replay_fidelity` in
//! `tests/integration/codec_rotation_db_tests.rs` proves it end to end.
//!
//! ## What the sweep will not touch
//!
//! - **Offload reference envelopes** (`_harvest_offload_envelope`, issue #524).
//!   Offload composes *after* codec encode, so the field holds a reference, not
//!   ciphertext; re-encoding it would encrypt the reference and orphan the
//!   blob. Re-encrypting the offloaded blob itself is embedder-owned storage
//!   and explicitly out of scope.
//! - **Erasure tombstones** (`_harvest_erased`, issue #495) — no ciphertext.
//! - **Plaintext fields.** A field that is not an envelope carries no key id,
//!   so it is not "a row carrying a non-active key id". The sweep migrates keys;
//!   it never newly encrypts history that was written in the clear.
//! - **Envelopes already on the active key** — which is what makes a re-run a
//!   no-op and the sweep idempotent.

use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};
use crate::payload_codec::{PayloadCodecs, codec_envelope_key_id};
use crate::payload_store::{PAYLOAD_FIELD_KEYS, is_offload_envelope};

/// What one call to [`reencrypt_event_payload_fields`] did to a single event.
///
/// Counts only — never payload content — so it is safe to thread into logs and
/// operator reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReencryptOutcome {
    /// Payload fields decoded with a retired key and re-encoded under the
    /// active one.
    pub fields_reencrypted: usize,
    /// Offload reference envelopes (issue #524) passed through untouched.
    pub fields_skipped_offloaded: usize,
    /// Erasure tombstones (issue #495) passed through untouched.
    pub fields_skipped_erased: usize,
    /// Fields already encoded under the active key — the idempotence path.
    pub fields_already_active: usize,
}

impl ReencryptOutcome {
    /// Whether the event's stored bytes actually changed, i.e. whether the
    /// caller has anything to write back.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.fields_reencrypted > 0
    }

    /// Saturating merge for multi-row accumulation.
    #[must_use]
    pub const fn merged(self, other: Self) -> Self {
        Self {
            fields_reencrypted: self
                .fields_reencrypted
                .saturating_add(other.fields_reencrypted),
            fields_skipped_offloaded: self
                .fields_skipped_offloaded
                .saturating_add(other.fields_skipped_offloaded),
            fields_skipped_erased: self
                .fields_skipped_erased
                .saturating_add(other.fields_skipped_erased),
            fields_already_active: self
                .fields_already_active
                .saturating_add(other.fields_already_active),
        }
    }
}

/// `true` when `codecs` is in a state where sweeping could only *remove*
/// encryption.
///
/// Registering [`crate::payload_codec::IdentityCodec`] under a key id and
/// activating it is a request to store payloads in the clear. Honouring that on
/// the *write* path is the embedder's business; honouring it on the sweep would
/// mean walking stored history and replacing ciphertext with plaintext, which
/// is a crypto-shredding-in-reverse operation nobody asked for. The sweep
/// refuses to do it and reports zero work instead.
fn active_key_would_decrypt(codecs: &PayloadCodecs) -> bool {
    codecs.active_codec_id().is_none_or(|id| id == "identity")
}

/// Re-encode every payload-bearing field of one serialized event that carries a
/// **non-active** codec key id, leaving everything else byte-identical.
///
/// `event_value` must be the adjacently-tagged form stored in
/// `harvest_events.event_data`: `{"type": "...", "data": {...}}`. Only values
/// under [`PAYLOAD_FIELD_KEYS`] inside `data` are considered — the same
/// allowlist the codec, erasure and export paths already share — so the event
/// `type`, event ids, ordering and timestamps are structurally out of reach.
///
/// **All-or-nothing.** Every field is decoded and re-encoded into a staging
/// buffer first; `event_value` is mutated only once every field has succeeded.
/// A field that cannot be decoded (an unregistered key id, corrupt ciphertext)
/// returns `Err` with `event_value` **completely unmodified**, so the caller can
/// never persist a half-rotated row and never persists plaintext where
/// ciphertext used to be.
///
/// Returns a zero outcome — touching nothing — when no keyed codec is
/// registered, which is every deployment that has not adopted rotation.
///
/// # Errors
///
/// [`HarvestError::UnknownCodecKey`] when a field names a key id this registry
/// cannot resolve; any other [`HarvestError`] the codec's own decode/encode
/// raises.
pub fn reencrypt_event_payload_fields(
    codecs: &PayloadCodecs,
    event_value: &mut Value,
) -> HarvestResult<ReencryptOutcome> {
    reencrypt_event_payload_fields_under(codecs, &codecs.active_key_id(), event_value)
}

/// [`reencrypt_event_payload_fields`], pinned to an explicit target key id.
///
/// [`sweep_codec_reencryption_once`] resolves the active key once per batch and
/// calls THIS function (not the wrapper) for every row, so a concurrent
/// `set_active_key` cannot straddle a single row and leave half its
/// fields on the old key and half on the new one. It also cannot cause a
/// plaintext write: [`PayloadCodecs::encode_payload_under`] refuses an identity
/// codec at the point of use rather than relying on a check made earlier.
///
/// # Errors
///
/// As [`reencrypt_event_payload_fields`], plus [`HarvestError::Config`] when
/// `target_key_id` names the identity codec and
/// [`HarvestError::UnknownCodecKey`] when it is not registered at all.
pub fn reencrypt_event_payload_fields_under(
    codecs: &PayloadCodecs,
    target_key_id: &str,
    event_value: &mut Value,
) -> HarvestResult<ReencryptOutcome> {
    let mut outcome = ReencryptOutcome::default();
    if !codecs.has_keyed_codecs() {
        return Ok(outcome);
    }
    let Some(data) = event_value.get("data").and_then(Value::as_object) else {
        return Ok(outcome);
    };

    let mut staged: Vec<(&'static str, Value)> = Vec::with_capacity(PAYLOAD_FIELD_KEYS.len());
    for key in PAYLOAD_FIELD_KEYS {
        let Some(field) = data.get(key) else {
            continue;
        };
        if crate::erase::is_erasure_tombstone(field) {
            outcome.fields_skipped_erased += 1;
            continue;
        }
        // Discriminator-only, NOT the strict `extract_offload_ref` parser: a
        // field bearing the offload marker is passed through whether or not its
        // reference parses, because there is no ciphertext here to rotate and
        // rewriting it would orphan the blob either way.
        if is_offload_envelope(field) {
            outcome.fields_skipped_offloaded += 1;
            continue;
        }
        let Some(key_id) = codec_envelope_key_id(field) else {
            // Plaintext: no key id, so nothing to rotate. The sweep never
            // newly encrypts history written in the clear.
            continue;
        };
        if key_id == target_key_id {
            outcome.fields_already_active += 1;
            continue;
        }
        let plaintext = codecs.decode_payload(field)?;
        staged.push((key, codecs.encode_payload_under(target_key_id, &plaintext)?));
    }

    if staged.is_empty() {
        return Ok(outcome);
    }
    // The immutable borrow above established that `data` is an object, so this
    // cannot fail; treating it as an error rather than an `if let` keeps a
    // future refactor from silently discarding `staged` and reporting no work.
    let data_mut = event_value
        .get_mut("data")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            HarvestError::Config(
                "event data object vanished between read and write during re-encryption"
                    .to_string(),
            )
        })?;
    for (key, value) in staged {
        data_mut.insert(key.to_string(), value);
        outcome.fields_reencrypted += 1;
    }
    Ok(outcome)
}

/// Whether any payload-bearing field of a serialized event carries a codec key
/// id other than `active` — i.e. whether this row has anything for the sweep to
/// do at all.
///
/// A few map lookups and no allocation, so the sweep can skip the deep clone of
/// an event that is already fully converted. That is the steady state on a
/// swept shard, and cloning multi-kilobyte event JSON to discover it is pure
/// waste. Deliberately NOT built on [`event_key_ids`], which allocates a
/// `BTreeSet<String>` — right for the error-reporting path, wrong for a
/// per-row pre-check.
#[must_use]
pub fn has_non_active_key(event_value: &Value, active: &str) -> bool {
    event_value
        .get("data")
        .and_then(Value::as_object)
        .is_some_and(|data| {
            PAYLOAD_FIELD_KEYS.iter().any(|key| {
                data.get(*key)
                    .and_then(codec_envelope_key_id)
                    .is_some_and(|key_id| key_id != active)
            })
        })
}

/// Every codec key id referenced by one serialized event's payload fields
/// (issue #948).
///
/// A kid-less envelope resolves to
/// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID). Fields
/// that carry no ciphertext at all — plaintext, offload reference envelopes,
/// erasure tombstones — contribute nothing, which is exactly what makes the
/// retirement gate's census agree with what the sweep is able to convert.
#[must_use]
pub fn event_key_ids(event_value: &Value) -> std::collections::BTreeSet<String> {
    let mut ids = std::collections::BTreeSet::new();
    let Some(data) = event_value.get("data").and_then(Value::as_object) else {
        return ids;
    };
    for key in PAYLOAD_FIELD_KEYS {
        if let Some(field) = data.get(key)
            && let Some(key_id) = codec_envelope_key_id(field)
        {
            ids.insert(key_id.to_string());
        }
    }
    ids
}

// ── DB-gated sweep, census, and retirement gate ──────────────────────────────

/// Rows examined per shard per scanner tick when the caller does not override
/// it.
///
/// The sweep's rate limiter: raising it converts history faster and holds the
/// scanner connection longer; lowering it (or passing `0`) throttles or stops
/// the sweep without a deploy. Unconditional (not `db`-gated) so a no-`db`
/// build can still name the default in a `WorkerConfig`.
pub const CODEC_ROTATION_DEFAULT_BATCH: i64 = 200;

#[cfg(feature = "db")]
pub use db::{
    CodecRotationCursor, ShardRotationProgress, compare_and_swap_event, count_rows_by_key_id,
    load_shard_rotation_progress, retire_codec_key, sweep_codec_reencryption,
    sweep_codec_reencryption_once,
};

#[cfg(feature = "db")]
mod db {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use diesel::sql_types::{Array, BigInt, Integer, Jsonb, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use serde::Serialize;
    use serde_json::Value;

    use crate::error::{CodecKeyShardRemainder, HarvestError, HarvestResult, database_error};
    use crate::payload_codec::PayloadCodecs;
    use crate::payload_store::PAYLOAD_FIELD_KEYS;
    use crate::telemetry::MetricsRecorder;

    use super::{
        active_key_would_decrypt, has_non_active_key, reencrypt_event_payload_fields_under,
    };

    /// The exact SQL mirror of
    /// [`codec_envelope_parts`](crate::payload_codec) — an object with
    /// `_harvest_codec_envelope == 1`, string `codec_id`, string `data`, and
    /// either exactly those three keys or those three plus a string `kid`.
    ///
    /// Kept byte-for-byte in step with the Rust shape check so the census can
    /// never count a row the sweep is unable to convert (which would make the
    /// retirement gate block forever) nor miss one it can (which would let the
    /// gate open early). `a_near_envelope_is_neither_counted_nor_swept` in
    /// `tests/integration/codec_rotation_db_tests.rs` pins the two together.
    const ENVELOPE_PREDICATE: &str = "
              jsonb_typeof(f.value) = 'object'
          AND f.value -> '_harvest_codec_envelope' = '1'::jsonb
          -- jsonb compares numbers as `numeric`, so the line above alone also
          -- accepts 1.0 -- which serde_json's `as_i64` rejects. Pin the text
          -- form too, so Postgres and Rust classify byte-identically.
          AND f.value ->> '_harvest_codec_envelope' = '1'
          AND jsonb_typeof(f.value -> 'codec_id') = 'string'
          AND jsonb_typeof(f.value -> 'data') = 'string'
          -- Exactly 3 keys, or exactly 4 with a string `kid` that satisfies the
          -- same charset/length rule `validate_key_id` applies in Rust (an
          -- out-of-charset `kid` is not an envelope on either side, so crafted
          -- workflow input cannot inject census rows).
          AND (SELECT COUNT(*) FROM jsonb_object_keys(f.value))
              = CASE WHEN jsonb_typeof(f.value -> 'kid') = 'string'
                          AND f.value ->> 'kid' ~ '^[A-Za-z0-9._:-]{1,64}$'
                     THEN 4 ELSE 3 END
          AND (
                  jsonb_typeof(f.value -> 'kid') IS NULL
               OR (jsonb_typeof(f.value -> 'kid') = 'string'
                   AND f.value ->> 'kid' ~ '^[A-Za-z0-9._:-]{1,64}$')
              )";

    #[derive(diesel::QueryableByName)]
    struct KeyCountRow {
        #[diesel(sql_type = Text)]
        key_id: String,
        #[diesel(sql_type = BigInt)]
        row_count: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct EventRow {
        #[diesel(sql_type = BigInt)]
        id: i64,
        #[diesel(sql_type = Jsonb)]
        event_data: Value,
    }

    #[derive(diesel::QueryableByName)]
    struct CursorRow {
        #[diesel(sql_type = Text)]
        active_key_id: String,
        #[diesel(sql_type = BigInt)]
        last_event_id: i64,
        #[diesel(sql_type = BigInt)]
        rows_reencrypted: i64,
        #[diesel(sql_type = BigInt)]
        unresolved_rows: i64,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        completed_at: Option<DateTime<Utc>>,
        #[diesel(sql_type = Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }

    /// The durable per-shard resume cursor for one rotation pass (issue #948).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct CodecRotationCursor {
        /// The key id this pass is converting ONTO. When it differs from the
        /// process's current active key the pass is stale and the next sweep
        /// restarts from the beginning of the shard.
        pub active_key_id: String,
        /// Highest `harvest_events.id` this pass has examined.
        pub last_event_id: i64,
        /// Rows this pass has actually rewritten.
        pub rows_reencrypted: i64,
        /// Rows this pass examined but could not convert — an unregistered key
        /// id, corrupt ciphertext, or a compare-and-swap lost to a concurrent
        /// erasure. Non-zero at the end of a pass forces another pass instead
        /// of a completion stamp, so a transient failure always gets another
        /// chance.
        pub unresolved_rows: i64,
        /// Set only when a pass reached the end of the shard having converted
        /// everything it saw — "this key is safe to gate on", not merely "the
        /// scan ran off the end".
        pub completed_at: Option<DateTime<Utc>>,
        /// Last time the cursor advanced.
        pub updated_at: DateTime<Utc>,
    }

    /// One shard's answer to "how far along is the rotation?" (issue #948, AC7).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ShardRotationProgress {
        /// The key id this process is currently encoding new writes under.
        pub active_key_id: String,
        /// Rows carrying each observed key id, including the active one. A
        /// kid-less (pre-rotation) envelope counts under
        /// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID).
        pub rows_by_key_id: BTreeMap<String, i64>,
        /// The resume cursor for the active key's pass, when one exists.
        pub cursor: Option<CodecRotationCursor>,
    }

    impl ShardRotationProgress {
        /// Rows still carrying a **non-active** key id — the number that must
        /// reach zero before the outgoing key can be retired.
        #[must_use]
        pub fn rows_remaining(&self) -> i64 {
            self.rows_by_key_id
                .iter()
                .filter(|(key_id, _)| *key_id != &self.active_key_id)
                .map(|(_, count)| *count)
                .sum()
        }
    }

    /// Whether `harvest_codec_rotation_cursor` exists yet.
    ///
    /// A deployment mid-migration must not fail its whole scanner tick because
    /// the sweep's bookkeeping table has not landed.
    async fn cursor_table_present(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
        let probe: Present = diesel::sql_query(
            "SELECT to_regclass('harvest_codec_rotation_cursor') IS NOT NULL AS present",
        )
        .get_result(conn)
        .await
        .map_err(database_error)?;
        Ok(probe.present)
    }

    /// Count `harvest_events` rows per codec key id on this connection's shard
    /// (issue #948).
    ///
    /// A row is counted once per distinct key id it references, so an event
    /// whose `input` and `output` sit under different keys contributes to both.
    /// Fields with no ciphertext — plaintext, offload reference envelopes
    /// (#524), erasure tombstones (#495) — contribute nothing, which is what
    /// keeps this census in agreement with what the sweep can actually convert.
    ///
    /// This is a sequential scan of the largest table in the schema. It backs an
    /// admin-gated, operator-invoked read and the retirement gate — both
    /// rotation-scoped, run a handful of times per rotation — not anything on a
    /// request or dispatch path.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    pub async fn count_rows_by_key_id(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<BTreeMap<String, i64>> {
        // Targeted `->` lookups over the compile-time-constant field list,
        // rather than `jsonb_each` materialising a tuple for every key of
        // `data` (timestamps, ids, everything) only to discard most of them.
        let sql = format!(
            "SELECT key_id, COUNT(*)::BIGINT AS row_count \
             FROM ( \
                 SELECT e.id AS event_row_id, \
                        COALESCE(f.value ->> 'kid', $2) AS key_id \
                 FROM harvest_events e \
                 CROSS JOIN LATERAL unnest($1::TEXT[]) AS k(field) \
                 CROSS JOIN LATERAL (SELECT e.event_data -> 'data' -> k.field) AS f(value) \
                 WHERE f.value IS NOT NULL AND {ENVELOPE_PREDICATE} \
                 GROUP BY e.id, COALESCE(f.value ->> 'kid', $2) \
             ) s \
             GROUP BY key_id"
        );
        let field_keys: Vec<String> = PAYLOAD_FIELD_KEYS
            .iter()
            .map(|k| (*k).to_string())
            .collect();
        let rows: Vec<KeyCountRow> = diesel::sql_query(sql)
            .bind::<Array<Text>, _>(field_keys)
            .bind::<Text, _>(crate::payload_codec::CODEC_LEGACY_KEY_ID)
            .load(conn)
            .await
            .map_err(database_error)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.key_id, row.row_count))
            .collect())
    }

    /// Read one shard's rotation progress: the per-key census plus the active
    /// key's resume cursor (issue #948, AC7).
    ///
    /// Read-only — it never mutates a row.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    pub async fn load_shard_rotation_progress(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<ShardRotationProgress> {
        let active_key_id = codecs.active_key_id();
        // With no keyed codec registered -- every deployment that has not
        // adopted rotation -- the answer is trivially "nothing to rotate", and
        // running a sequential scan of the largest table to say so would let an
        // unauthenticated-to-the-DB dashboard poll turn an admin GET into
        // repeated full-table reads on every shard.
        if !codecs.has_keyed_codecs() {
            return Ok(ShardRotationProgress {
                active_key_id,
                rows_by_key_id: BTreeMap::new(),
                cursor: None,
            });
        }
        let rows_by_key_id = count_rows_by_key_id(conn).await?;
        let cursor = if cursor_table_present(conn).await? {
            load_cursor(conn, shard_id).await?
        } else {
            None
        };
        Ok(ShardRotationProgress {
            active_key_id,
            rows_by_key_id,
            cursor,
        })
    }

    async fn load_cursor(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
    ) -> HarvestResult<Option<CodecRotationCursor>> {
        let rows: Vec<CursorRow> = diesel::sql_query(
            "SELECT active_key_id, last_event_id, rows_reencrypted, unresolved_rows, \
                    completed_at, updated_at \
             FROM harvest_codec_rotation_cursor WHERE shard_id = $1",
        )
        .bind::<Integer, _>(shard_id)
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next().map(|row| CodecRotationCursor {
            active_key_id: row.active_key_id,
            last_event_id: row.last_event_id,
            rows_reencrypted: row.rows_reencrypted,
            unresolved_rows: row.unresolved_rows,
            completed_at: row.completed_at,
            updated_at: row.updated_at,
        }))
    }

    /// Run ONE bounded batch of the lazy re-encryption sweep on this
    /// connection's shard (issue #948, AC4).
    ///
    /// Returns the number of rows whose stored bytes were rewritten.
    ///
    /// **Zero cost when unused.** With no keyed codec registered — every
    /// deployment that has not adopted rotation — this returns without issuing
    /// a single statement.
    ///
    /// **Resumable.** Progress is a durable `(shard_id, active_key_id)` cursor,
    /// so a restart continues where the last batch stopped and a key flip starts
    /// a fresh pass automatically.
    ///
    /// **Idempotent.** A row already on the active key is skipped, so re-running
    /// over swept ground rewrites nothing.
    ///
    /// **Rate-limitable.** `batch_limit` bounds rows examined per call; `<= 0`
    /// disables the sweep entirely without a redeploy.
    ///
    /// ## Racing writers
    ///
    /// The write is a **compare-and-swap**:
    /// `UPDATE harvest_events SET event_data = $new WHERE id = $1 AND event_data = $old`.
    /// If anything changed the row between the read and the write — PII erasure
    /// (#495) tombstoning it, or a second sweeper — the update matches zero
    /// rows and this sweep counts it unresolved and skips it. The sweep always
    /// loses such a race, which is the only safe direction: re-writing
    /// ciphertext over a tombstone would resurrect payload data an erasure had
    /// just destroyed.
    ///
    /// A row that cannot be decoded is logged (by id only, never content) and
    /// left byte-identical, so one unreadable row cannot wedge the whole pass —
    /// and it keeps the retirement gate correctly blocked.
    ///
    /// # Errors
    ///
    /// Propagates database failures. A per-row codec failure is not an error.
    pub async fn sweep_codec_reencryption_once(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        codecs: &PayloadCodecs,
        batch_limit: i64,
        metrics: &(dyn MetricsRecorder + Send + Sync),
    ) -> HarvestResult<usize> {
        if batch_limit <= 0 || !codecs.has_keyed_codecs() || active_key_would_decrypt(codecs) {
            return Ok(0);
        }
        if !cursor_table_present(conn).await? {
            return Ok(0);
        }
        // Resolve the target key ONCE for the whole batch. Every row is
        // re-encoded under this exact key id, so a concurrent `set_active_key`
        // cannot straddle a row, and the progress we file below is attributed to
        // the key we actually converted onto.
        let active_key_id = codecs.active_key_id();
        let previous = load_cursor(conn, shard_id).await?;
        // A cursor recorded against a DIFFERENT key is stale: rotating, rotating
        // again, and ROLLING BACK all mean "rescan this shard from the start".
        // Rollback is the case that makes this load-bearing — the key being
        // rolled back to may have completed a pass of its own long ago, and
        // resuming that pass would skip every row written under the key being
        // rolled back from.
        let resumed = previous
            .as_ref()
            .filter(|cursor| cursor.active_key_id == active_key_id);
        let resume_from = resumed.map_or(0, |cursor| cursor.last_event_id);

        let rows: Vec<EventRow> = diesel::sql_query(
            "SELECT id, event_data FROM harvest_events \
             WHERE id > $1 ORDER BY id LIMIT $2",
        )
        .bind::<BigInt, _>(resume_from)
        .bind::<BigInt, _>(batch_limit)
        .load(conn)
        .await
        .map_err(database_error)?;

        let reached_end = i64::try_from(rows.len()).unwrap_or(i64::MAX) < batch_limit;
        let highest_id = rows.last().map_or(resume_from, |row| row.id);

        let mut rewritten = 0usize;
        let mut unresolved = 0i64;
        for row in rows {
            // Cheap pre-check before the deep clone: the steady state is a
            // fully-converted shard where every fetched row is already on the
            // active key, and cloning a multi-kilobyte event JSON to discover
            // that is pure waste.
            if !has_non_active_key(&row.event_data, &active_key_id) {
                continue;
            }
            let original = row.event_data;
            let mut candidate = original.clone();
            match reencrypt_event_payload_fields_under(codecs, &active_key_id, &mut candidate) {
                Ok(outcome) if outcome.changed() => {
                    if compare_and_swap_event(conn, row.id, &original, &candidate).await? {
                        rewritten += 1;
                    } else {
                        // The row changed under us — a PII erasure, or another
                        // sweeper. We lose, by design. Count it so the pass does
                        // not report itself complete over a row it never
                        // converted.
                        unresolved += 1;
                        tracing::debug!(
                            event_row_id = row.id,
                            shard_id,
                            "codec re-encryption skipped: the event row changed under the sweep"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    unresolved += 1;
                    // Bounded and content-free: the row id, the operator-chosen
                    // key ids the row references, and the typed error's own
                    // rendering. Never a payload, never ciphertext. Naming the
                    // key ids is what makes the log actionable — the usual
                    // cause is a key dropped from the registry before its rows
                    // were converted, and this says which one to put back.
                    tracing::warn!(
                        event_row_id = row.id,
                        shard_id,
                        key_ids = ?super::event_key_ids(&original),
                        error = %error,
                        "codec re-encryption skipped: the event row could not be decoded"
                    );
                }
            }
        }

        let carried = resumed.map_or((0, 0), |cursor| {
            (cursor.rows_reencrypted, cursor.unresolved_rows)
        });
        let rows_reencrypted_total = carried
            .0
            .saturating_add(i64::try_from(rewritten).unwrap_or(i64::MAX));
        let unresolved_total = carried.1.saturating_add(unresolved);

        // A pass that ran off the end of the shard having left something
        // unconverted must NOT be marked complete and must NOT leave those rows
        // behind the cursor forever: reset to 0 so they get another attempt.
        // That is what makes a transient failure — a key registered late, a race
        // lost to an erasure — eventually consistent rather than permanent.
        let (next_last_event_id, next_unresolved, next_completed_at) = if reached_end {
            if unresolved_total > 0 {
                (0, 0, None)
            } else {
                (
                    highest_id,
                    0,
                    resumed
                        .and_then(|cursor| cursor.completed_at)
                        .or_else(|| Some(Utc::now())),
                )
            }
        } else {
            (highest_id, unresolved_total, None)
        };

        write_cursor(
            conn,
            shard_id,
            &active_key_id,
            next_last_event_id,
            rows_reencrypted_total,
            next_unresolved,
            next_completed_at,
        )
        .await?;

        if rewritten > 0 {
            metrics.record_codec_reencrypted(
                &shard_id.to_string(),
                u64::try_from(rewritten).unwrap_or(u64::MAX),
            );
        }
        Ok(rewritten)
    }

    /// Write `candidate` over `original` only if the row still holds
    /// `original`. Returns whether the swap took effect.
    ///
    /// This is the single guard that keeps sanctioned exception #3 from
    /// undoing exception #2: if anything changed the row between the sweep's
    /// read and this write — a PII erasure (#495) tombstoning it, or a second
    /// sweeper — the `WHERE` clause matches nothing and the sweep loses. That
    /// is the only safe direction, and the caller counts the loss so the pass
    /// re-runs rather than reporting itself complete over that row.
    ///
    /// `#[doc(hidden)] pub` purely so that race semantics can be exercised
    /// directly by an integration test (a stale `original` must not win), which
    /// is not reachable through the batch-oriented public sweep entry point.
    /// Not part of the engine's semver-stable surface.
    ///
    /// # Errors
    ///
    /// Propagates database failures.
    #[doc(hidden)]
    pub async fn compare_and_swap_event(
        conn: &mut AsyncPgConnection,
        event_row_id: i64,
        original: &Value,
        candidate: &Value,
    ) -> HarvestResult<bool> {
        let updated = diesel::sql_query(
            "UPDATE harvest_events SET event_data = $1 WHERE id = $2 AND event_data = $3",
        )
        .bind::<Jsonb, _>(candidate)
        .bind::<BigInt, _>(event_row_id)
        .bind::<Jsonb, _>(original)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(updated > 0)
    }

    /// Persist the pass's absolute state.
    ///
    /// Absolute rather than incremental (`SET x = x + n`) because the values are
    /// computed from a read this same call made: only one scanner per shard runs
    /// this, and a second one racing it simply overwrites with its own
    /// consistent view rather than compounding two partial increments onto a
    /// row whose meaning it never read.
    async fn write_cursor(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        active_key_id: &str,
        last_event_id: i64,
        rows_reencrypted: i64,
        unresolved_rows: i64,
        completed_at: Option<DateTime<Utc>>,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "INSERT INTO harvest_codec_rotation_cursor \
                 (shard_id, active_key_id, last_event_id, rows_reencrypted, unresolved_rows, \
                  completed_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
             ON CONFLICT (shard_id) DO UPDATE SET \
                 active_key_id = EXCLUDED.active_key_id, \
                 last_event_id = EXCLUDED.last_event_id, \
                 rows_reencrypted = EXCLUDED.rows_reencrypted, \
                 unresolved_rows = EXCLUDED.unresolved_rows, \
                 completed_at = EXCLUDED.completed_at, \
                 updated_at = NOW()",
        )
        .bind::<Integer, _>(shard_id)
        .bind::<Text, _>(active_key_id)
        .bind::<BigInt, _>(last_event_id)
        .bind::<BigInt, _>(rows_reencrypted)
        .bind::<BigInt, _>(unresolved_rows)
        .bind::<Nullable<Timestamptz>, _>(completed_at)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// Run one sweep batch on every assigned shard (issue #948).
    ///
    /// Folded into [`crate::timeout::enforce_timeouts_once`], mirroring the
    /// shard fan-out shape of the throttle / debounce / start-idempotency
    /// sweeps: a shard whose pool cannot be reached is logged and skipped, never
    /// fatal to the tick.
    ///
    /// # Errors
    ///
    /// Propagates database failures from a reachable shard.
    pub async fn sweep_codec_reencryption(
        conn: &mut AsyncPgConnection,
        sharded_pool: &Option<crate::shard::ShardedDbPool>,
        shard_assignments: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        batch_limit: i64,
        metrics: &(dyn MetricsRecorder + Send + Sync),
    ) -> HarvestResult<usize> {
        if batch_limit <= 0 || !codecs.has_keyed_codecs() || active_key_would_decrypt(codecs) {
            return Ok(0);
        }
        let mut total = 0usize;
        match sharded_pool {
            Some(sp) if !shard_assignments.is_empty() => {
                for shard in shard_assignments {
                    let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                        // Assigned but with no pool in this process (mid a
                        // shard-add rollout). Logged rather than skipped
                        // silently: its rows stay on the retired key, and an
                        // operator watching rows_remaining refuse to fall needs
                        // to know why.
                        tracing::warn!(
                            "[codec_rotation] shard {shard:?} is assigned but has no connection \
                             pool in this process; its rows are not being swept"
                        );
                        continue;
                    };
                    let mut shard_conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(
                                "[codec_rotation] failed to get connection to shard {shard:?}: {e:?}"
                            );
                            continue;
                        }
                    };
                    // Best-effort per shard. Propagating would abort the WHOLE
                    // timeout tick -- timeout enforcement, broken-session
                    // reclaim, mutex-lease reclaim, for every shard -- because
                    // one shard's sweep failed. The rotation simply does not
                    // advance there, which the admin read makes visible.
                    match sweep_codec_reencryption_once(
                        &mut shard_conn,
                        shard.as_i32(),
                        codecs,
                        batch_limit,
                        metrics,
                    )
                    .await
                    {
                        Ok(n) => total += n,
                        Err(e) => {
                            tracing::error!("[codec_rotation] sweep failed on shard {shard:?}: {e}")
                        }
                    }
                }
            }
            _ => {
                // Unsharded (or no explicit assignment): attribute progress to
                // the shard this connection actually serves when we know it,
                // rather than assuming 0 -- otherwise the admin read fans out
                // over the router's shards and finds no cursor row.
                let shard_id = shard_assignments.first().map_or(0, |s| s.as_i32());
                match sweep_codec_reencryption_once(conn, shard_id, codecs, batch_limit, metrics)
                    .await
                {
                    Ok(n) => total += n,
                    Err(e) => {
                        tracing::error!("[codec_rotation] sweep failed on shard {shard_id}: {e}");
                    }
                }
            }
        }
        Ok(total)
    }

    /// Retire a codec key, refusing unless **every** expected shard proves it
    /// holds zero rows referencing it (issue #948, AC6).
    ///
    /// Fail-closed by construction: a shard whose pool is missing or whose
    /// census errors is recorded as `reachable = false` and blocks the
    /// retirement on its own. An uncounted shard is never counted as a zero —
    /// that is the difference between a compliance control and a compliance
    /// incident. An empty `expected_shards` proves nothing and is refused for
    /// the same reason.
    ///
    /// On success the key is dropped from this process's in-memory registry via
    /// [`PayloadCodecs::retire_key_local`]; disposing of the key *material* is
    /// the embedder's business (this crate never holds it).
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Config`] when `key_id` is the active key, or when
    ///   `expected_shards` is empty.
    /// - [`HarvestError::CodecKeyRetirementBlocked`] naming the per-shard
    ///   remaining count (or unreadability) of every blocking shard.
    pub async fn retire_codec_key(
        sharded_pool: &crate::shard::ShardedDbPool,
        expected_shards: &[crate::types::ShardId],
        codecs: &PayloadCodecs,
        key_id: &str,
    ) -> HarvestResult<()> {
        if codecs.active_key_id() == key_id {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is the active key and cannot be retired; \
                 activate a different key first"
            )));
        }
        if expected_shards.is_empty() {
            return Err(HarvestError::Config(format!(
                "cannot retire codec key id {key_id:?}: no shards were supplied to inspect, so \
                 zero remaining rows cannot be established"
            )));
        }
        if codecs.codec_for_key(key_id).is_none() {
            return Err(HarvestError::Config(format!(
                "codec key id {key_id:?} is not registered; retiring it would report success \
                 without having proved anything about the rows that reference it"
            )));
        }
        // Fail closed on an INCOMPLETE list, not just an unreachable shard. A
        // caller that passes a stale or process-local subset would otherwise get
        // a vacuous `Ok` while whole shards were never censused -- and the
        // runbook tells the operator that `Ok` is when key material may be
        // destroyed.
        let missing: Vec<CodecKeyShardRemainder> = sharded_pool
            .iter_shards()
            .map(|(shard, _)| shard)
            .filter(|shard| !expected_shards.contains(shard))
            .map(|shard| CodecKeyShardRemainder {
                shard_id: shard.as_i32(),
                rows: 0,
                reachable: false,
                reason: Some(
                    "this process has a pool for this shard but it was omitted from the \
                     supplied shard list"
                        .to_string(),
                ),
            })
            .collect();
        if !missing.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining: missing,
            });
        }

        let mut remaining: Vec<CodecKeyShardRemainder> = Vec::new();
        for shard in expected_shards {
            let shard_id = shard.as_i32();
            let Some(pool) = sharded_pool.exact_pool_for(*shard) else {
                remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: false,
                    reason: Some("no connection pool for this shard in this process".to_string()),
                });
                continue;
            };
            let mut conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    remaining.push(CodecKeyShardRemainder {
                        shard_id,
                        rows: 0,
                        reachable: false,
                        reason: Some(format!("connection unavailable: {e}")),
                    });
                    continue;
                }
            };
            match count_rows_by_key_id(&mut conn).await {
                Ok(counts) => {
                    let rows = counts.get(key_id).copied().unwrap_or(0);
                    if rows > 0 {
                        remaining.push(CodecKeyShardRemainder {
                            shard_id,
                            rows,
                            reachable: true,
                            reason: None,
                        });
                    }
                }
                Err(e) => remaining.push(CodecKeyShardRemainder {
                    shard_id,
                    rows: 0,
                    reachable: false,
                    reason: Some(format!("census failed: {e}")),
                }),
            }
        }

        if !remaining.is_empty() {
            return Err(HarvestError::CodecKeyRetirementBlocked {
                key_id: key_id.to_string(),
                remaining,
            });
        }
        codecs.retire_key_local(key_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload_codec::{
        CODEC_ENVELOPE_KID_KEY, CODEC_LEGACY_KEY_ID, CodecError, PayloadCodec,
    };
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Debug)]
    struct XorCodec(u8);

    impl PayloadCodec for XorCodec {
        fn codec_id(&self) -> &'static str {
            "xor"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(raw.iter().map(|b| b ^ self.0).collect())
        }
        fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(encoded.iter().map(|b| b ^ self.0).collect())
        }
    }

    /// A registry holding `k1` (retired) and `k2` (active).
    fn rotated_registry() -> PayloadCodecs {
        let codecs = PayloadCodecs::default();
        codecs
            .register_key("k1", Arc::new(XorCodec(0x11)))
            .expect("register k1");
        codecs
            .register_key("k2", Arc::new(XorCodec(0x22)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");
        codecs
    }

    /// A serialized `WorkflowStarted` whose `input` is encoded under `key_id`.
    fn event_under(codecs: &PayloadCodecs, key_id: &str, input: Value) -> Value {
        let restore = codecs.active_key_id();
        codecs.set_active_key(key_id).expect("activate for fixture");
        let event = codecs
            .encode_event(&crate::event::WorkflowEvent::WorkflowStarted {
                input,
                timestamp: chrono::DateTime::parse_from_rfc3339("2026-08-29T12:00:00Z")
                    .expect("fixed timestamp")
                    .with_timezone(&chrono::Utc),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            })
            .expect("encode");
        codecs.set_active_key(&restore).expect("restore active key");
        event
    }

    #[test]
    fn reencrypts_a_field_carrying_a_non_active_key_id() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k1");

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 1);
        assert!(outcome.changed());
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn decoded_plaintext_is_byte_identical_and_structure_is_untouched() {
        // The scope guarantee behind sanctioned exception #3, at unit level.
        let codecs = rotated_registry();
        let plaintext = json!({"user": "alice", "amounts": [1, 2, 3], "nested": {"k": null}});
        let mut event = event_under(&codecs, "k1", plaintext.clone());
        let before = event.clone();

        reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(event["type"], before["type"], "event type is never touched");
        assert_eq!(
            event["data"]["timestamp"], before["data"]["timestamp"],
            "timestamps are never touched"
        );
        assert_ne!(
            event["data"]["input"]["data"], before["data"]["input"]["data"],
            "the ciphertext bytes did change"
        );
        let decoded = codecs.decode_event(event).expect("decode after sweep");
        match decoded {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, plaintext, "decoded plaintext is byte-identical");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn is_idempotent() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));

        let first = reencrypt_event_payload_fields(&codecs, &mut event).expect("first");
        let after_first = event.clone();
        let second = reencrypt_event_payload_fields(&codecs, &mut event).expect("second");

        assert_eq!(first.fields_reencrypted, 1);
        assert_eq!(second.fields_reencrypted, 0);
        assert!(!second.changed(), "a re-run is a no-op");
        assert_eq!(event, after_first, "the row is byte-identical on a re-run");
    }

    #[test]
    fn skips_offload_reference_envelopes() {
        // AC8: offload composes AFTER encode, so the field holds a reference,
        // not ciphertext. Re-encoding it would orphan the blob.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        let offload_envelope = json!({
            "_harvest_offload_envelope": 1,
            "store_id": "mem",
            "key": "blob/abc",
            "len": 4096,
            "checksum": "deadbeef",
        });
        event["data"]["input"] = offload_envelope.clone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert_eq!(outcome.fields_skipped_offloaded, 1);
        assert_eq!(
            event["data"]["input"], offload_envelope,
            "passed through verbatim"
        );
    }

    #[test]
    fn skips_erasure_tombstones() {
        // AC8: a tombstone has no ciphertext to rotate.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["input"] = crate::erase::erasure_tombstone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert_eq!(outcome.fields_skipped_erased, 1);
        assert_eq!(event["data"]["input"], crate::erase::erasure_tombstone());
    }

    #[test]
    fn skips_plaintext_fields() {
        // The sweep migrates keys; it never newly encrypts cleartext history.
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["input"] = json!({"plain": "text"});

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 0);
        assert!(!outcome.changed());
        assert_eq!(event["data"]["input"], json!({"plain": "text"}));
    }

    #[test]
    fn a_kidless_envelope_is_swept_as_the_legacy_key() {
        // AC1 + AC4: pre-upgrade rows carry no `kid` and must be swept onto the
        // active key like any other non-active key id.
        let codecs = PayloadCodecs::default();
        codecs
            .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
            .expect("register legacy");
        let mut event = event_under(&codecs, CODEC_LEGACY_KEY_ID, json!({"old": true}));
        assert!(
            event["data"]["input"].get(CODEC_ENVELOPE_KID_KEY).is_none(),
            "the fixture really is a pre-upgrade, kid-less envelope"
        );

        codecs
            .register_key("k2", Arc::new(XorCodec(0x22)))
            .expect("register k2");
        codecs.set_active_key("k2").expect("activate k2");

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, 1);
        assert_eq!(event["data"]["input"][CODEC_ENVELOPE_KID_KEY], "k2");
    }

    #[test]
    fn a_row_that_cannot_be_decoded_is_left_completely_untouched() {
        // Never write a half-rotated row, and never write plaintext: a field we
        // cannot decode aborts the whole row with the ORIGINAL bytes intact.
        let writer = PayloadCodecs::default();
        writer
            .register_key("k0", Arc::new(XorCodec(0x00)))
            .expect("register k0");
        let mut event = event_under(&writer, "k0", json!({"user": "alice"}));
        let before = event.clone();

        // A registry that has never heard of `k0`.
        let codecs = rotated_registry();
        let err = reencrypt_event_payload_fields(&codecs, &mut event)
            .expect_err("an unresolvable key must fail the row");
        assert!(
            matches!(err, HarvestError::UnknownCodecKey { .. }),
            "{err:?}"
        );
        assert_eq!(
            event, before,
            "the row is byte-identical after a failed sweep"
        );
    }

    #[test]
    fn every_payload_bearing_field_is_swept() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        // Plant an encoded value in every payload-bearing key, not just `input`.
        let encoded = event["data"]["input"].clone();
        for key in PAYLOAD_FIELD_KEYS {
            event["data"][key] = encoded.clone();
        }

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert_eq!(outcome.fields_reencrypted, PAYLOAD_FIELD_KEYS.len());
        for key in PAYLOAD_FIELD_KEYS {
            assert_eq!(
                event["data"][key][CODEC_ENVELOPE_KID_KEY], "k2",
                "field {key}"
            );
        }
    }

    #[test]
    fn census_counts_key_ids_referenced_by_an_event() {
        let codecs = rotated_registry();
        let mut event = event_under(&codecs, "k1", json!({"user": "alice"}));
        event["data"]["output"] =
            event_under(&codecs, "k2", json!({"done": true}))["data"]["input"].clone();
        event["data"]["details"] = crate::erase::erasure_tombstone();

        let ids = event_key_ids(&event);

        assert_eq!(
            ids,
            ["k1".to_string(), "k2".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn a_registry_with_no_keyed_codecs_sweeps_nothing() {
        // AC: zero overhead on the ~all deployments that never rotate.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(XorCodec(0x11)));
        let mut event = codecs
            .encode_event(&crate::event::WorkflowEvent::SideEffectRecorded {
                kind: crate::event::SideEffectKind::Custom,
                name: Some("x".to_string()),
                value: json!({"a": 1}),
            })
            .expect("encode");
        let before = event.clone();

        let outcome = reencrypt_event_payload_fields(&codecs, &mut event).expect("reencrypt");

        assert!(!outcome.changed());
        assert_eq!(event, before);
    }
}
