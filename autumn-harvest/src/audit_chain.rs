//! Keyed hash chain over exported audit rows (issue #1838).
//!
//! The chain is optional. It runs only when audit export runs and a chain key
//! is set with `HarvestBuilder::audit_export_chain_key`.
//!
//! # Where the chain is stamped
//!
//! The exporter already gives each audit row a dense per-shard `export_seq`
//! under the cursor row lock. The chain is stamped at that same point. So it
//! adds no lock and no work to the audit insert path.
//!
//! A row is unchained from its insert until the next export tick stamps it.
//! The SIEM copy covers that window. See `docs/audit-export.md`.
//!
//! # What one link holds
//!
//! `chain_hash = HMAC-SHA256(key, CHAIN_DOMAIN || chain_prev || canonical(row))`.
//! `chain_prev` is the hash of the row with the previous `export_seq`. The
//! first row uses [`GENESIS`]. The canonical row includes `shard` and `seq`,
//! so a row cannot move to another position.
//!
//! Each row stores its own `chain_prev`. So a row stays verifiable after
//! retention deletes its predecessor. A missing row shows as a sequence gap.
//! A changed row shows as a hash break.
//!
//! # The keyed checkpoint
//!
//! The cursor holds a [`ChainCheckpoint`]: the first chained `seq`, the
//! newest chained `seq`, its link and its `occurred_at`. A MAC under the chain
//! key covers all four. A writer without the key cannot move the checkpoint.
//! So the verifier finds a stripped row and a deleted tail.
//!
//! The exporter extends only a checkpoint that its key accepts. Otherwise a
//! writer could move the head and have the exporter sign it. A missing or
//! invalid checkpoint stops the chain until [`reanchor_shard_chain`] runs.
//!
//! A writer who
//! removes every chain value and the whole checkpoint leaves a table that
//! looks unchained. Only the SIEM copy detects that.
//!
//! # Retention
//!
//! Retention deletes old rows, so it leaves gaps. It keeps some old rows, such
//! as export decommission records. Pass the retention cutoff to
//! [`ChainVerifyOptions`]. A gap then counts as retention while every row
//! before it is older than the cutoff. After the first newer row, a gap is a
//! finding. A writer can therefore hide a deletion only at the old end of the
//! chain, near the cutoff.
//!
//! # Why a key
//!
//! An unkeyed hash gives no protection against a database writer. That writer
//! can recompute every later link. The key lives outside the database. So a
//! writer without the key cannot forge a link. A process that holds the key
//! can. This control protects against database-level tampering, not against
//! a compromised Harvest process.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::audit_export::AuditExportRecord;
use crate::completion_callback::CallbackSecret;

/// Domain separator for the chain MAC.
pub const CHAIN_DOMAIN: &[u8] = b"harvest-audit-chain-v1";

/// Shortest chain key the exporter accepts, in bytes.
pub const MIN_CHAIN_KEY_BYTES: usize = 32;

/// A chain key of at least [`MIN_CHAIN_KEY_BYTES`] bytes.
///
/// The exporter stamps links only with this type. So a short or empty key
/// cannot reach the stamp path, whichever way a caller builds the config.
///
/// The key can also hold accepted keys. The exporter accepts a stored
/// checkpoint under any of them, but it signs only with the active key.
#[derive(Clone)]
pub struct AuditChainKey {
    active: CallbackSecret,
    accepted: Vec<CallbackSecret>,
}

/// The rejected key was shorter than [`MIN_CHAIN_KEY_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an audit chain key is {len} bytes; it needs at least {MIN_CHAIN_KEY_BYTES} bytes")]
pub struct ChainKeyTooShort {
    /// The rejected key length.
    pub len: usize,
}

impl AuditChainKey {
    /// Wrap `key` as the active key.
    ///
    /// # Errors
    /// Returns [`ChainKeyTooShort`] for a key shorter than
    /// [`MIN_CHAIN_KEY_BYTES`].
    pub fn new(key: impl Into<Vec<u8>>) -> Result<Self, ChainKeyTooShort> {
        let key = key.into();
        if key.len() < MIN_CHAIN_KEY_BYTES {
            return Err(ChainKeyTooShort { len: key.len() });
        }
        Ok(Self {
            active: CallbackSecret::new(key),
            accepted: Vec::new(),
        })
    }

    /// Also accept the keys of `other` on a stored checkpoint.
    ///
    /// Use it for a key rotation. The exporter never signs with an accepted
    /// key. See `docs/audit-export.md`.
    #[must_use]
    pub fn with_accepted_key(mut self, other: Self) -> Self {
        self.accepted.push(other.active);
        self.accepted.extend(other.accepted);
        self
    }

    /// The active key as the HMAC secret.
    #[must_use]
    pub const fn secret(&self) -> &CallbackSecret {
        &self.active
    }

    /// `true` when `mac` is the MAC of `checkpoint` on `shard` under the
    /// active key or an accepted key.
    #[must_use]
    pub fn accepts(&self, checkpoint: &ChainCheckpoint, shard: i32, mac: &ChainHash) -> bool {
        std::iter::once(&self.active)
            .chain(&self.accepted)
            .any(|key| checkpoint.mac(key, shard) == *mac)
    }
}

impl std::fmt::Debug for AuditChainKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuditChainKey(<redacted>)")
    }
}

/// One chain link: an HMAC-SHA256 output.
pub type ChainHash = [u8; 32];

/// The predecessor of the first chained row.
pub const GENESIS: ChainHash = [0; 32];

/// Domain separator for the checkpoint MAC.
pub const CHECKPOINT_DOMAIN: &[u8] = b"harvest-audit-chain-checkpoint-v1";

/// Clock slack for the retention test.
///
/// A late commit can carry an `occurred_at` older than the rows before it. A
/// row counts as old when it is older than the cutoff plus this slack.
pub const RETENTION_SLACK: chrono::TimeDelta = chrono::TimeDelta::hours(1);

/// Length marker for an absent field in [`canonical_record`].
const ABSENT: u32 = u32::MAX;

/// Encode `record` as the length-prefixed bytes the chain MAC covers.
///
/// Each field is a big-endian `u32` length, then its UTF-8 bytes. An absent
/// optional field is the length `u32::MAX` with no bytes. So `None` and an
/// empty string differ. The field order is fixed and documented in
/// `docs/audit-export.md`.
#[must_use]
pub fn canonical_record(record: &AuditExportRecord) -> Vec<u8> {
    let shard = record.shard.to_string();
    let seq = record.seq.to_string();
    let id = record.id.hyphenated().to_string();
    let shard_id = record.shard_id.map(|id| id.to_string());
    let occurred_at = record
        .occurred_at
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let fields: [Option<&str>; 15] = [
        Some(&shard),
        Some(&seq),
        Some(&id),
        shard_id.as_deref(),
        Some(&occurred_at),
        Some(&record.actor),
        Some(&record.operation),
        Some(&record.target_type),
        record.target_id.as_deref(),
        Some(&record.route_or_command),
        record.request_id.as_deref(),
        record.idempotency_key.as_deref(),
        Some(&record.status),
        record.error_summary.as_deref(),
        Some(&record.source),
    ];
    let mut out = Vec::with_capacity(256);
    for field in fields {
        match field {
            Some(text) => {
                // An audit field longer than 4 GiB cannot reach the table.
                let len = u32::try_from(text.len()).unwrap_or(ABSENT - 1);
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            None => out.extend_from_slice(&ABSENT.to_be_bytes()),
        }
    }
    out
}

/// Compute the link for `record` after `prev`.
///
/// # Panics
/// Never: HMAC-SHA256 accepts a key of any length.
#[must_use]
pub fn link(key: &CallbackSecret, prev: &ChainHash, record: &AuditExportRecord) -> ChainHash {
    #[expect(clippy::expect_used, reason = "HMAC accepts a key of any length")]
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(CHAIN_DOMAIN);
    mac.update(prev);
    mac.update(&canonical_record(record));
    mac.finalize().into_bytes().into()
}

/// Lowercase hex of a chain hash.
#[must_use]
pub fn to_hex(hash: &ChainHash) -> String {
    use std::fmt::Write as _;

    hash.iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Parse 32 bytes, as read from a `BYTEA` column, into a chain hash.
#[must_use]
pub fn from_bytes(bytes: &[u8]) -> Option<ChainHash> {
    bytes.try_into().ok()
}

/// The keyed checkpoint the cursor holds for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainCheckpoint {
    /// The first chained `export_seq`.
    pub start_seq: i64,
    /// The `export_seq` of the newest chained row.
    pub head_seq: i64,
    /// The link of the newest chained row.
    pub head: ChainHash,
    /// The `occurred_at` of the newest chained row.
    pub head_occurred_at: DateTime<Utc>,
}

impl ChainCheckpoint {
    /// The MAC that binds this checkpoint to `shard` under `key`.
    ///
    /// # Panics
    /// Never: HMAC-SHA256 accepts a key of any length.
    #[must_use]
    pub fn mac(&self, key: &CallbackSecret, shard: i32) -> ChainHash {
        #[expect(clippy::expect_used, reason = "HMAC accepts a key of any length")]
        let mut mac =
            Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
        mac.update(CHECKPOINT_DOMAIN);
        mac.update(&shard.to_be_bytes());
        mac.update(&self.start_seq.to_be_bytes());
        mac.update(&self.head_seq.to_be_bytes());
        mac.update(&self.head);
        mac.update(&self.head_occurred_at.timestamp_micros().to_be_bytes());
        mac.finalize().into_bytes().into()
    }
}

/// One stored row, as the verifier reads it.
#[derive(Debug, Clone)]
pub struct ChainRow {
    /// The row content, as the exporter would ship it.
    pub record: AuditExportRecord,
    /// The stored `chain_prev`, if the row is chained.
    pub prev: Option<ChainHash>,
    /// The stored `chain_hash`, if the row is chained.
    pub hash: Option<ChainHash>,
}

/// One problem the verifier found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainFinding {
    /// The stored hash does not match the row content and stored `chain_prev`.
    Tampered {
        /// The row's `export_seq`.
        seq: i64,
    },
    /// The stored `chain_prev` does not match the hash of the row before it.
    LinkMismatch {
        /// The row's `export_seq`.
        seq: i64,
    },
    /// A row at or after the chain start has no hash.
    Unchained {
        /// The row's `export_seq`.
        seq: i64,
    },
    /// Sequence numbers are missing between two rows of the chain.
    Gap {
        /// The last `export_seq` before the gap.
        after_seq: i64,
        /// The first `export_seq` after the gap.
        before_seq: i64,
    },
    /// The newest chained row does not match the checkpoint head.
    HeadMismatch {
        /// The head `export_seq` the checkpoint names.
        expected_seq: i64,
        /// The newest chained `export_seq` found, if any.
        found_seq: Option<i64>,
    },
    /// Chained rows exist, but the cursor holds no complete checkpoint.
    CheckpointMissing,
    /// The checkpoint MAC does not verify under any key.
    CheckpointInvalid,
}

/// The result of a chain verification.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainReport {
    /// Chained rows checked.
    pub checked: u64,
    /// Rows before the chain start. The chain does not cover them.
    pub unchained_prefix: u64,
    /// The first chained `export_seq` the checkpoint names.
    pub start_seq: Option<i64>,
    /// The `export_seq` of the first chained row found.
    pub anchor_seq: Option<i64>,
    /// The `export_seq` of the last chained row found.
    pub last_seq: Option<i64>,
    /// The link of the last chained row found. Compare it with the SIEM copy.
    pub last_hash: Option<ChainHash>,
    /// Every problem found, in sequence order.
    pub findings: Vec<ChainFinding>,
    /// Gaps that retention explains. They are not findings.
    pub retention_gaps: Vec<ChainFinding>,
}

impl ChainReport {
    /// `true` when the verifier found no problem.
    #[must_use]
    pub const fn is_intact(&self) -> bool {
        self.findings.is_empty()
    }
}

/// An incremental verifier. Push rows in `export_seq` order.
pub struct ChainVerifier<'k> {
    keys: &'k [CallbackSecret],
    retention_cutoff: Option<DateTime<Utc>>,
    report: ChainReport,
    /// The `seq` and stored hash of the previous row, once the chain starts.
    previous: Option<(i64, Option<ChainHash>)>,
    /// `true` after a row of the chain that retention cannot have reached.
    seen_recent: bool,
}

impl<'k> ChainVerifier<'k> {
    /// Start a verification with `key`.
    #[must_use]
    pub fn new(key: &'k CallbackSecret) -> Self {
        Self::with_keys(std::slice::from_ref(key))
    }

    /// Start a verification that accepts a link made with any of `keys`.
    ///
    /// Pass the old and the new key after a key rotation.
    #[must_use]
    pub fn with_keys(keys: &'k [CallbackSecret]) -> Self {
        Self {
            keys,
            retention_cutoff: None,
            report: ChainReport::default(),
            previous: None,
            seen_recent: false,
        }
    }

    /// Treat a gap at the old end of the chain as retention. `cutoff` is
    /// `now - audit_retention_days`.
    #[must_use]
    pub const fn with_retention_cutoff(mut self, cutoff: Option<DateTime<Utc>>) -> Self {
        self.retention_cutoff = cutoff;
        self
    }

    /// Require a link on every row at or after `start_seq`.
    ///
    /// Without a start, unchained rows before the first chained row count as
    /// the unchained prefix.
    #[must_use]
    pub const fn with_start_seq(mut self, start_seq: Option<i64>) -> Self {
        self.report.start_seq = start_seq;
        self
    }

    /// `true` when retention may have deleted a row that occurred at `at`.
    fn is_old(&self, at: DateTime<Utc>) -> bool {
        self.retention_cutoff
            .is_some_and(|cutoff| at < cutoff + RETENTION_SLACK)
    }

    /// Record a gap as a finding, or as retention.
    ///
    /// Retention explains a gap only while every row before it is old.
    fn gap(&mut self, after_seq: i64, before_seq: i64) {
        let gap = ChainFinding::Gap {
            after_seq,
            before_seq,
        };
        if self.retention_cutoff.is_some() && !self.seen_recent {
            self.report.retention_gaps.push(gap);
        } else {
            self.report.findings.push(gap);
        }
    }

    /// Check one row against its own hash and the row before it.
    pub fn push(&mut self, row: &ChainRow) {
        let seq = row.record.seq;
        let in_chain = self.previous.is_some()
            || row.hash.is_some()
            || self.report.start_seq.is_some_and(|start| seq >= start);
        if !in_chain {
            self.report.unchained_prefix += 1;
            return;
        }

        match self.previous {
            Some((previous_seq, _)) if seq != previous_seq + 1 => {
                self.gap(previous_seq, seq);
            }
            None => {
                // Rows between the checkpoint start and the first row found
                // are missing.
                if let Some(start) = self.report.start_seq
                    && seq > start
                {
                    self.gap(start - 1, seq);
                }
            }
            Some(_) => {}
        }

        if !self.is_old(row.record.occurred_at) {
            self.seen_recent = true;
        }
        if row.hash.is_none() {
            self.report.findings.push(ChainFinding::Unchained { seq });
            self.previous = Some((seq, None));
            return;
        }
        // A link check needs both neighbours present and chained.
        if let Some((previous_seq, Some(previous_hash))) = self.previous
            && seq == previous_seq + 1
            && row.prev != Some(previous_hash)
        {
            self.report
                .findings
                .push(ChainFinding::LinkMismatch { seq });
        }
        if self.report.anchor_seq.is_none() {
            self.report.anchor_seq = Some(seq);
        }
        self.check_own_hash(row);
    }

    /// Check a chained row against its stored `chain_prev` and record it.
    fn check_own_hash(&mut self, row: &ChainRow) {
        let seq = row.record.seq;
        let matches = row.prev.is_some_and(|prev| {
            self.keys
                .iter()
                .any(|key| row.hash == Some(link(key, &prev, &row.record)))
        });
        if !matches {
            self.report.findings.push(ChainFinding::Tampered { seq });
        }
        self.report.checked += 1;
        self.report.last_seq = Some(seq);
        self.report.last_hash = row.hash;
        self.previous = Some((seq, row.hash));
    }

    /// End the verification against the cursor's verified `checkpoint`.
    ///
    /// The newest chained row must be the checkpoint head. Rows missing after
    /// it count as retention only when the head and every row found are old.
    #[must_use]
    pub fn finish(mut self, checkpoint: Option<&ChainCheckpoint>) -> ChainReport {
        let Some(checkpoint) = checkpoint else {
            return self.report;
        };
        let found = self.report.last_seq.zip(self.report.last_hash);
        if found == Some((checkpoint.head_seq, checkpoint.head)) {
            return self.report;
        }
        let tail_purged = found.is_none_or(|(seq, _)| seq < checkpoint.head_seq)
            && !self.seen_recent
            && self.is_old(checkpoint.head_occurred_at);
        if tail_purged {
            let after_seq = self.report.last_seq.unwrap_or(checkpoint.start_seq - 1);
            self.report.retention_gaps.push(ChainFinding::Gap {
                after_seq,
                before_seq: checkpoint.head_seq + 1,
            });
        } else {
            self.report.findings.push(ChainFinding::HeadMismatch {
                expected_seq: checkpoint.head_seq,
                found_seq: self.report.last_seq,
            });
        }
        self.report
    }
}

/// What [`stamp_chain`] wrote.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Stamped {
    /// The first `export_seq` it chained.
    pub(crate) first_seq: i64,
    /// The newest `export_seq` it chained.
    pub(crate) head_seq: i64,
    /// The link of the newest row.
    pub(crate) head: ChainHash,
    /// The `occurred_at` of the newest row.
    pub(crate) head_occurred_at: DateTime<Utc>,
}

/// Rows the verifier reads per query.
#[cfg(feature = "db")]
const VERIFY_PAGE_ROWS: i64 = 1_000;

/// The chain state the stored rows hold.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoredChainState {
    /// The link of the newest chained row.
    pub(crate) head: ChainHash,
    /// The `export_seq` of the newest chained row.
    pub(crate) head_seq: i64,
    /// The `occurred_at` of the newest chained row.
    pub(crate) head_occurred_at: DateTime<Utc>,
    /// The first chained `export_seq`.
    pub(crate) start_seq: i64,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct StoredChainRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Bytea>)]
    head: Option<Vec<u8>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    head_seq: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    head_occurred_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    start_seq: Option<i64>,
}

/// Read the newest link and the chain start from rows up to `through_seq`.
///
/// Returns `None` when no row is chained. It scans the unchained rows, so
/// the exporter calls it only for a cursor with no checkpoint.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub(crate) async fn stored_chain_state(
    conn: &mut diesel_async::AsyncPgConnection,
    through_seq: i64,
) -> crate::error::HarvestResult<Option<StoredChainState>> {
    use diesel_async::RunQueryDsl;

    let row: StoredChainRow = diesel::sql_query(
        "SELECT h.chain_hash AS head, h.export_seq AS head_seq, \
             h.occurred_at AS head_occurred_at, \
             (SELECT MIN(export_seq) FROM harvest_audit_log \
              WHERE chain_hash IS NOT NULL AND export_seq <= $1) AS start_seq \
         FROM (SELECT 1) AS one \
         LEFT JOIN ( \
             SELECT chain_hash, export_seq, occurred_at FROM harvest_audit_log \
             WHERE chain_hash IS NOT NULL AND export_seq <= $1 \
             ORDER BY export_seq DESC LIMIT 1 \
         ) AS h ON true",
    )
    .bind::<diesel::sql_types::BigInt, _>(through_seq)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.head.as_deref().and_then(from_bytes).and_then(|head| {
        Some(StoredChainState {
            head,
            head_seq: row.head_seq?,
            head_occurred_at: row.head_occurred_at?,
            start_seq: row.start_seq?,
        })
    }))
}

/// The point the exporter extends the chain from.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChainAnchor {
    /// The link to extend, or `None` for a new chain.
    pub(crate) head: Option<ChainHash>,
    /// The first chained `export_seq`, or `None` for a new chain.
    pub(crate) start_seq: Option<i64>,
}

/// The anchor for the next stamp on `cursor`, or `None` to refuse the stamp.
///
/// The exporter signs a new checkpoint over each stamp. So it must not trust
/// a head that a database writer can set. It extends a checkpoint only when
/// `key` accepts its MAC. It starts a new chain only when the cursor has no
/// checkpoint and no row is chained. Any other state needs
/// [`reanchor_shard_chain`].
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub(crate) async fn chain_anchor(
    conn: &mut diesel_async::AsyncPgConnection,
    cursor: &crate::models::AuditExportCursor,
    key: &AuditChainKey,
) -> crate::error::HarvestResult<Option<ChainAnchor>> {
    if let Some((checkpoint, mac)) = stored_checkpoint(cursor) {
        let valid = key.accepts(&checkpoint, cursor.shard_id, &mac)
            && checkpoint.head_seq <= cursor.last_assigned_seq;
        return Ok(valid.then_some(ChainAnchor {
            head: Some(checkpoint.head),
            start_seq: Some(checkpoint.start_seq),
        }));
    }
    let partial = cursor.chain_head.is_some()
        || cursor.chain_start_seq.is_some()
        || cursor.chain_head_seq.is_some()
        || cursor.chain_head_occurred_at.is_some()
        || cursor.chain_mac.is_some();
    if partial {
        return Ok(None);
    }
    let stored = stored_chain_state(conn, cursor.last_assigned_seq).await?;
    Ok(stored.is_none().then_some(ChainAnchor {
        head: None,
        start_seq: None,
    }))
}

/// Write `checkpoint` and its MAC under the active key to the cursor.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub(crate) async fn write_checkpoint(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    checkpoint: &ChainCheckpoint,
    key: &AuditChainKey,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    let mac = checkpoint.mac(key.secret(), shard_id);
    diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
        .set((
            cur::chain_start_seq.eq(checkpoint.start_seq),
            cur::chain_head_seq.eq(checkpoint.head_seq),
            cur::chain_head.eq(checkpoint.head.to_vec()),
            cur::chain_head_occurred_at.eq(checkpoint.head_occurred_at),
            cur::chain_mac.eq(mac.to_vec()),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Sign a new checkpoint over the stored chain on `shard_id`.
///
/// The exporter does not extend a chain whose checkpoint is missing or does
/// not verify. New rows then stay unchained. An operator calls this to
/// recover, for example after a cursor rebuild.
///
/// It accepts the stored rows as they are. So compare the chain with the
/// SIEM copy first. It chains the unchained rows after the newest link, up to
/// `last_assigned_seq`. It then signs the checkpoint with the active key.
///
/// Returns the new checkpoint. Returns `None` when the cursor is missing or
/// no row is chained. In the second case it clears the checkpoint, and the
/// next export tick starts a new chain.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn reanchor_shard_chain(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    key: &AuditChainKey,
) -> crate::error::HarvestResult<Option<ChainCheckpoint>> {
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    use crate::schema::harvest_audit_export_cursor::dsl as cur;

    Box::pin(
        conn.transaction::<_, crate::error::HarvestError, _>(async |conn| {
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
            let Some(seed) = stored_chain_state(conn, cursor.last_assigned_seq).await? else {
                diesel::update(cur::harvest_audit_export_cursor.find(shard_id))
                    .set((
                        cur::chain_start_seq.eq(None::<i64>),
                        cur::chain_head_seq.eq(None::<i64>),
                        cur::chain_head.eq(None::<Vec<u8>>),
                        cur::chain_head_occurred_at.eq(None::<DateTime<Utc>>),
                        cur::chain_mac.eq(None::<Vec<u8>>),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                return Ok(None);
            };
            let stamped = stamp_chain(
                conn,
                shard_id,
                key.secret(),
                seed.head_seq,
                cursor.last_assigned_seq,
                Some(seed.head.as_slice()),
            )
            .await?;
            let checkpoint = ChainCheckpoint {
                start_seq: seed.start_seq,
                head_seq: stamped.map_or(seed.head_seq, |s| s.head_seq),
                head: stamped.map_or(seed.head, |s| s.head),
                head_occurred_at: stamped.map_or(seed.head_occurred_at, |s| s.head_occurred_at),
            };
            write_checkpoint(conn, shard_id, &checkpoint, key).await?;
            Ok(Some(checkpoint))
        }),
    )
    .await
}

/// Stamp the chain over the rows with `after_seq < export_seq <= through_seq`.
///
/// `head` is the cursor's `chain_head`, or `None` for the first link. Returns
/// `None` when no row is in the range. The caller holds the cursor row lock,
/// so no other exporter stamps the same rows.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub(crate) async fn stamp_chain(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    key: &CallbackSecret,
    after_seq: i64,
    through_seq: i64,
    head: Option<&[u8]>,
) -> crate::error::HarvestResult<Option<Stamped>> {
    use diesel::prelude::*;
    use diesel::sql_types::{Array, Bytea};
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_log::dsl as log;

    let rows: Vec<crate::models::AuditExportRow> = log::harvest_audit_log
        .filter(log::export_seq.gt(after_seq))
        .filter(log::export_seq.le(through_seq))
        .order(log::export_seq.asc())
        .select(crate::models::AuditExportRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut prev = head.and_then(from_bytes).unwrap_or(GENESIS);
    let mut ids = Vec::with_capacity(rows.len());
    let mut prevs = Vec::with_capacity(rows.len());
    let mut hashes = Vec::with_capacity(rows.len());
    let mut stamped: Option<Stamped> = None;
    for row in rows {
        let id = row.id;
        let Some(record) = AuditExportRecord::from_row(shard_id, row) else {
            continue;
        };
        let hash = link(key, &prev, &record);
        ids.push(id);
        prevs.push(prev.to_vec());
        hashes.push(hash.to_vec());
        prev = hash;
        stamped = Some(Stamped {
            first_seq: stamped.map_or(record.seq, |s| s.first_seq),
            head_seq: record.seq,
            head: hash,
            head_occurred_at: record.occurred_at,
        });
    }
    if ids.is_empty() {
        return Ok(None);
    }

    diesel::sql_query(
        "UPDATE harvest_audit_log a \
         SET chain_prev = v.prev, chain_hash = v.hash \
         FROM unnest($1, $2, $3) AS v(id, prev, hash) \
         WHERE a.id = v.id",
    )
    .bind::<Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<Array<Bytea>, _>(&prevs)
    .bind::<Array<Bytea>, _>(&hashes)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(stamped)
}

/// How to verify a chain.
#[derive(Debug, Clone, Copy)]
pub struct ChainVerifyOptions<'a> {
    /// Every key that may have made a link: the current key, and the old key
    /// after a rotation.
    pub keys: &'a [CallbackSecret],
    /// The audit retention cutoff, `now - audit_retention_days`. `None` when
    /// retention does not run. Rows missing before it count as retention.
    pub retention_cutoff: Option<DateTime<Utc>>,
}

/// Verify the chain on `shard_id` with `key`, with no retention cutoff.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn verify_shard_chain(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    key: &CallbackSecret,
) -> crate::error::HarvestResult<ChainReport> {
    let options = ChainVerifyOptions {
        keys: std::slice::from_ref(key),
        retention_cutoff: None,
    };
    verify_shard_chain_with(conn, shard_id, &options).await
}

/// Verify the chain on `shard_id` with `options`.
///
/// It reads the cursor first, then pages the rows up to the cursor's
/// `last_assigned_seq` in short reads. A long scan therefore holds no
/// snapshot. Rows at or below that bound change only by retention or by
/// tampering.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn verify_shard_chain_with(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    options: &ChainVerifyOptions<'_>,
) -> crate::error::HarvestResult<ChainReport> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    let cursor: Option<crate::models::AuditExportCursor> = cur::harvest_audit_export_cursor
        .find(shard_id)
        .select(crate::models::AuditExportCursor::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    let bound = cursor.as_ref().map_or(i64::MAX, |c| c.last_assigned_seq);
    let (checkpoint, checkpoint_finding) = match cursor.as_ref().map(stored_checkpoint) {
        Some(Some((checkpoint, mac))) => {
            if options
                .keys
                .iter()
                .any(|key| checkpoint.mac(key, shard_id) == mac)
            {
                (Some(checkpoint), None)
            } else {
                (None, Some(ChainFinding::CheckpointInvalid))
            }
        }
        _ => (None, None),
    };

    let mut verifier = ChainVerifier::with_keys(options.keys)
        .with_retention_cutoff(options.retention_cutoff)
        .with_start_seq(checkpoint.map(|c| c.start_seq));
    let mut after_seq = 0_i64;
    loop {
        let rows: Vec<crate::models::AuditExportRow> = log::harvest_audit_log
            .filter(log::export_seq.gt(after_seq))
            .filter(log::export_seq.le(bound))
            .order(log::export_seq.asc())
            .limit(VERIFY_PAGE_ROWS)
            .select(crate::models::AuditExportRow::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
        let Some(last_seq) = rows.last().and_then(|row| row.export_seq) else {
            break;
        };
        for row in rows {
            let prev = row.chain_prev.as_deref().and_then(from_bytes);
            let hash = row.chain_hash.as_deref().and_then(from_bytes);
            if let Some(record) = AuditExportRecord::from_row(shard_id, row) {
                verifier.push(&ChainRow { record, prev, hash });
            }
        }
        after_seq = last_seq;
    }

    let mut report = verifier.finish(checkpoint.as_ref());
    if let Some(finding) = checkpoint_finding {
        report.findings.insert(0, finding);
    } else if checkpoint.is_none() && report.checked > 0 {
        report.findings.insert(0, ChainFinding::CheckpointMissing);
    }
    Ok(report)
}

/// The checkpoint and its stored MAC, when every column is set.
#[cfg(feature = "db")]
pub(crate) fn stored_checkpoint(
    cursor: &crate::models::AuditExportCursor,
) -> Option<(ChainCheckpoint, ChainHash)> {
    Some((
        ChainCheckpoint {
            start_seq: cursor.chain_start_seq?,
            head_seq: cursor.chain_head_seq?,
            head: cursor.chain_head.as_deref().and_then(from_bytes)?,
            head_occurred_at: cursor.chain_head_occurred_at?,
        },
        cursor.chain_mac.as_deref().and_then(from_bytes)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> CallbackSecret {
        CallbackSecret::new(vec![7_u8; MIN_CHAIN_KEY_BYTES])
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 123_456_000).unwrap_or_default()
    }

    fn rec(seq: i64) -> AuditExportRecord {
        AuditExportRecord {
            shard: 2,
            seq,
            id: uuid::Uuid::from_u128(u128::try_from(seq).unwrap_or_default()),
            shard_id: Some(2),
            occurred_at: at(1_700_000_000 + seq),
            actor: "alice".into(),
            operation: "workflow.cancel".into(),
            target_type: "execution".into(),
            target_id: Some(format!("exec-{seq}")),
            route_or_command: "POST /workflows/{id}/cancel".into(),
            request_id: None,
            idempotency_key: None,
            status: "succeeded".into(),
            error_summary: None,
            source: "api".into(),
            chain_prev: None,
            chain_hash: None,
        }
    }

    /// A correctly chained run of rows for `seqs`, starting at [`GENESIS`].
    fn chain(seqs: &[i64]) -> Vec<ChainRow> {
        let key = key();
        let mut prev = GENESIS;
        seqs.iter()
            .map(|&seq| {
                let record = rec(seq);
                let hash = link(&key, &prev, &record);
                let row = ChainRow {
                    record,
                    prev: Some(prev),
                    hash: Some(hash),
                };
                prev = hash;
                row
            })
            .collect()
    }

    /// The checkpoint the exporter writes after stamping `rows`.
    fn checkpoint(rows: &[ChainRow]) -> Option<ChainCheckpoint> {
        let first = rows.first()?;
        let last = rows.last()?;
        Some(ChainCheckpoint {
            start_seq: first.record.seq,
            head_seq: last.record.seq,
            head: last.hash?,
            head_occurred_at: last.record.occurred_at,
        })
    }

    fn verify_with(
        rows: &[ChainRow],
        checkpoint: Option<ChainCheckpoint>,
        cutoff: Option<DateTime<Utc>>,
    ) -> ChainReport {
        let key = key();
        let mut verifier = ChainVerifier::new(&key)
            .with_retention_cutoff(cutoff)
            .with_start_seq(checkpoint.map(|c| c.start_seq));
        for row in rows {
            verifier.push(row);
        }
        verifier.finish(checkpoint.as_ref())
    }

    fn verify(rows: &[ChainRow], checkpoint: Option<ChainCheckpoint>) -> ChainReport {
        verify_with(rows, checkpoint, None)
    }

    #[test]
    fn canonical_record_separates_absent_from_empty() {
        let absent = rec(1);
        let mut empty = rec(1);
        empty.request_id = Some(String::new());
        assert_ne!(canonical_record(&absent), canonical_record(&empty));
    }

    #[test]
    fn canonical_record_cannot_be_shifted_across_fields() {
        let mut a = rec(1);
        a.actor = "ab".into();
        a.operation = "c".into();
        let mut b = rec(1);
        b.actor = "a".into();
        b.operation = "bc".into();
        assert_ne!(canonical_record(&a), canonical_record(&b));
    }

    #[test]
    fn canonical_record_ignores_the_chain_fields() {
        let mut a = rec(1);
        a.chain_prev = Some("00".into());
        a.chain_hash = Some("ff".into());
        assert_eq!(canonical_record(&a), canonical_record(&rec(1)));
    }

    /// A fixed vector. A change here breaks every stored chain and every SIEM
    /// verifier, so it must be a deliberate, versioned change.
    #[test]
    fn the_encoding_and_the_link_match_a_fixed_vector() {
        let mut expected = Vec::new();
        for field in [
            Some("2"),
            Some("1"),
            Some("00000000-0000-0000-0000-000000000001"),
            Some("2"),
            Some("2023-11-14T22:13:21.123456Z"),
            Some("alice"),
            Some("workflow.cancel"),
            Some("execution"),
            Some("exec-1"),
            Some("POST /workflows/{id}/cancel"),
            None,
            None,
            Some("succeeded"),
            None,
            Some("api"),
        ] {
            match field {
                Some(text) => {
                    let len = u32::try_from(text.len()).unwrap_or_default();
                    expected.extend_from_slice(&len.to_be_bytes());
                    expected.extend_from_slice(text.as_bytes());
                }
                None => expected.extend_from_slice(&u32::MAX.to_be_bytes()),
            }
        }
        assert_eq!(canonical_record(&rec(1)), expected);
        assert_eq!(
            to_hex(&link(&key(), &GENESIS, &rec(1))),
            "53a9a78bc5cefd88056afff7bb0bc0c7b33ac96b245a352685cd6e43529b2fe2"
        );
    }

    #[test]
    fn link_depends_on_key_prev_and_every_field() {
        let key = key();
        let base = link(&key, &GENESIS, &rec(1));
        assert_ne!(base, GENESIS);
        assert_eq!(base, link(&key, &GENESIS, &rec(1)));
        assert_ne!(
            base,
            link(&CallbackSecret::new(vec![8_u8; 32]), &GENESIS, &rec(1))
        );
        assert_ne!(base, link(&key, &[1; 32], &rec(1)));
        let mut changed = rec(1);
        changed.status = "failed".into();
        assert_ne!(base, link(&key, &GENESIS, &changed));
        let mut moved = rec(1);
        moved.seq = 2;
        assert_ne!(base, link(&key, &GENESIS, &moved));
        let mut other_shard = rec(1);
        other_shard.shard = 3;
        assert_ne!(base, link(&key, &GENESIS, &other_shard));
    }

    #[test]
    fn a_short_chain_key_cannot_be_built() {
        assert_eq!(
            AuditChainKey::new(vec![1_u8; MIN_CHAIN_KEY_BYTES - 1]).err(),
            Some(ChainKeyTooShort {
                len: MIN_CHAIN_KEY_BYTES - 1
            })
        );
        assert_eq!(
            AuditChainKey::new(Vec::new()).err(),
            Some(ChainKeyTooShort { len: 0 })
        );
        let key = AuditChainKey::new(vec![1_u8; MIN_CHAIN_KEY_BYTES]).unwrap_or_else(|e| {
            panic!("a full-length key builds: {e}");
        });
        assert_eq!(key.secret().as_bytes().len(), MIN_CHAIN_KEY_BYTES);
        assert_eq!(format!("{key:?}"), "AuditChainKey(<redacted>)");
    }

    #[test]
    fn a_key_accepts_its_own_and_its_accepted_checkpoints_only() {
        let full = |byte: u8| {
            AuditChainKey::new(vec![byte; MIN_CHAIN_KEY_BYTES])
                .unwrap_or_else(|e| panic!("a full-length key builds: {e}"))
        };
        let rows = chain(&[1]);
        let Some(checkpoint) = checkpoint(&rows) else {
            panic!("checkpoint");
        };
        let mac = |byte: u8| checkpoint.mac(full(byte).secret(), 2);
        let rotated = full(1).with_accepted_key(full(2).with_accepted_key(full(3)));
        assert_eq!(
            rotated.secret().as_bytes(),
            &[1_u8; MIN_CHAIN_KEY_BYTES][..]
        );
        for byte in [1, 2, 3] {
            assert!(rotated.accepts(&checkpoint, 2, &mac(byte)));
        }
        assert!(!rotated.accepts(&checkpoint, 2, &mac(4)));
        assert!(!rotated.accepts(&checkpoint, 3, &mac(1)));
        assert!(!full(1).accepts(&checkpoint, 2, &mac(2)));
    }

    #[test]
    fn hex_and_bytes_round_trip() {
        let hash = link(&key(), &GENESIS, &rec(1));
        let hex = to_hex(&hash);
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(from_bytes(&hash), Some(hash));
        assert_eq!(from_bytes(&hash[..31]), None);
    }

    #[test]
    fn the_checkpoint_mac_binds_every_field_and_the_shard() {
        let rows = chain(&[1, 2]);
        let Some(base) = checkpoint(&rows) else {
            panic!("checkpoint");
        };
        let mac = base.mac(&key(), 2);
        assert_eq!(mac, base.mac(&key(), 2));
        assert_ne!(mac, base.mac(&key(), 3));
        assert_ne!(mac, base.mac(&CallbackSecret::new(vec![8_u8; 32]), 2));
        for changed in [
            ChainCheckpoint {
                start_seq: 2,
                ..base
            },
            ChainCheckpoint {
                head_seq: 1,
                ..base
            },
            ChainCheckpoint {
                head: GENESIS,
                ..base
            },
            ChainCheckpoint {
                head_occurred_at: at(0),
                ..base
            },
        ] {
            assert_ne!(mac, changed.mac(&key(), 2));
        }
    }

    #[test]
    fn an_intact_chain_verifies() {
        let rows = chain(&[1, 2, 3]);
        let report = verify(&rows, checkpoint(&rows));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.checked, 3);
        assert_eq!(report.anchor_seq, Some(1));
        assert_eq!(report.last_seq, Some(3));
        assert_eq!(report.last_hash, rows[2].hash);
    }

    #[test]
    fn a_changed_row_is_reported_as_tampered() {
        let mut rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        rows[1].record.actor = "mallory".into();
        let report = verify(&rows, cp);
        assert_eq!(report.findings, vec![ChainFinding::Tampered { seq: 2 }]);
    }

    #[test]
    fn a_rehashed_row_without_the_key_breaks_the_next_link() {
        let mut rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        // A writer without the key cannot recompute row 2 correctly.
        rows[1].hash = Some([9; 32]);
        let report = verify(&rows, cp);
        assert_eq!(
            report.findings,
            vec![
                ChainFinding::Tampered { seq: 2 },
                ChainFinding::LinkMismatch { seq: 3 },
            ]
        );
    }

    #[test]
    fn a_deleted_row_is_reported_as_a_gap() {
        let mut rows = chain(&[1, 2, 3, 4]);
        let cp = checkpoint(&rows);
        rows.remove(1);
        let report = verify(&rows, cp);
        assert_eq!(
            report.findings,
            vec![ChainFinding::Gap {
                after_seq: 1,
                before_seq: 3,
            }]
        );
    }

    #[test]
    fn a_deleted_oldest_row_is_a_gap_from_the_checkpoint_start() {
        let rows = chain(&[1, 2, 3]);
        let report = verify(&rows[1..], checkpoint(&rows));
        assert_eq!(
            report.findings,
            vec![ChainFinding::Gap {
                after_seq: 0,
                before_seq: 2,
            }]
        );
    }

    #[test]
    fn without_a_checkpoint_a_purged_prefix_verifies_from_the_first_row() {
        let rows = chain(&[1, 2, 3, 4]);
        let report = verify(&rows[2..], None);
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.anchor_seq, Some(3));
    }

    /// Rows at `seconds` after a base time, chained from [`GENESIS`].
    fn chain_at(times: &[(i64, i64)]) -> Vec<ChainRow> {
        let key = key();
        let mut prev = GENESIS;
        times
            .iter()
            .map(|&(seq, seconds)| {
                let mut record = rec(seq);
                record.occurred_at = at(seconds);
                let hash = link(&key, &prev, &record);
                let row = ChainRow {
                    record,
                    prev: Some(prev),
                    hash: Some(hash),
                };
                prev = hash;
                row
            })
            .collect()
    }

    #[test]
    fn retention_explains_gaps_at_the_old_end_of_the_chain() {
        let day = 86_400;
        let rows = chain_at(&[
            (1, 0),
            (2, day),
            (3, 2 * day),
            (4, 100 * day),
            (5, 101 * day),
        ]);
        let cp = checkpoint(&rows);
        // Retention kept row 2, a lifecycle record, and purged rows 1 and 3.
        let survivors = [rows[1].clone(), rows[3].clone(), rows[4].clone()];
        let report = verify_with(&survivors, cp, Some(at(10 * day)));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(
            report.retention_gaps,
            vec![
                ChainFinding::Gap {
                    after_seq: 0,
                    before_seq: 2,
                },
                ChainFinding::Gap {
                    after_seq: 2,
                    before_seq: 4,
                },
            ]
        );

        // A gap after a recent row is a finding, whatever the cutoff.
        let deleted = [rows[1].clone(), rows[3].clone()];
        let report = verify_with(&deleted, cp, Some(at(10 * day)));
        assert_eq!(
            report.findings,
            vec![ChainFinding::HeadMismatch {
                expected_seq: 5,
                found_seq: Some(4),
            }]
        );
    }

    #[test]
    fn old_style_retention_cutoff_explains_both_gaps() {
        let rows = chain(&[1, 2, 3, 4, 5]);
        let cp = checkpoint(&rows);
        // Retention kept row 3, a lifecycle record, and purged rows 1, 2 and 4.
        let survivors = [rows[2].clone(), rows[4].clone()];
        let cutoff = at(1_700_000_000 + 5) + RETENTION_SLACK + chrono::TimeDelta::seconds(1);
        let report = verify_with(&survivors, cp, Some(cutoff));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(
            report.retention_gaps,
            vec![
                ChainFinding::Gap {
                    after_seq: 0,
                    before_seq: 3,
                },
                ChainFinding::Gap {
                    after_seq: 3,
                    before_seq: 5,
                },
            ]
        );
    }

    #[test]
    fn retention_does_not_explain_a_gap_newer_than_the_cutoff() {
        let rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        let survivors = [rows[0].clone(), rows[2].clone()];
        // Row 1 is newer than the cutoff, so retention did not purge row 2.
        let cutoff = at(1_700_000_000) - RETENTION_SLACK - chrono::TimeDelta::hours(1);
        let report = verify_with(&survivors, cp, Some(cutoff));
        assert_eq!(
            report.findings,
            vec![ChainFinding::Gap {
                after_seq: 1,
                before_seq: 3,
            }]
        );
    }

    #[test]
    fn retention_explains_a_purged_tail_only_when_the_head_is_old() {
        let rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        let old = at(1_700_000_000 + 3) + RETENTION_SLACK + chrono::TimeDelta::seconds(1);
        let purged = verify_with(&rows[..1], cp, Some(old));
        assert!(purged.is_intact(), "{purged:?}");
        assert_eq!(
            purged.retention_gaps,
            vec![ChainFinding::Gap {
                after_seq: 1,
                before_seq: 4,
            }]
        );

        let early = at(1_700_000_000) - RETENTION_SLACK - chrono::TimeDelta::hours(1);
        let deleted = verify_with(&rows[..1], cp, Some(early));
        assert_eq!(
            deleted.findings,
            vec![ChainFinding::HeadMismatch {
                expected_seq: 3,
                found_seq: Some(1),
            }]
        );
    }

    #[test]
    fn unchained_rows_before_the_chain_start_are_counted_not_flagged() {
        let mut rows: Vec<ChainRow> = (1..=2)
            .map(|seq| ChainRow {
                record: rec(seq),
                prev: None,
                hash: None,
            })
            .collect();
        let key = key();
        let record = rec(3);
        let hash = link(&key, &GENESIS, &record);
        rows.push(ChainRow {
            record,
            prev: Some(GENESIS),
            hash: Some(hash),
        });
        let report = verify(&rows, checkpoint(&rows[2..]));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.unchained_prefix, 2);
        assert_eq!(report.anchor_seq, Some(3));
    }

    #[test]
    fn an_unchained_row_after_the_chain_start_is_flagged() {
        let mut rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        rows[1].prev = None;
        rows[1].hash = None;
        let report = verify(&rows, cp);
        assert_eq!(report.findings, vec![ChainFinding::Unchained { seq: 2 }]);
    }

    #[test]
    fn stripping_every_link_cannot_hide_from_the_checkpoint() {
        let mut rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        for row in &mut rows {
            row.prev = None;
            row.hash = None;
        }
        rows[1].record.actor = "mallory".into();
        let report = verify(&rows, cp);
        assert_eq!(
            report.findings,
            vec![
                ChainFinding::Unchained { seq: 1 },
                ChainFinding::Unchained { seq: 2 },
                ChainFinding::Unchained { seq: 3 },
                ChainFinding::HeadMismatch {
                    expected_seq: 3,
                    found_seq: None,
                },
            ]
        );
    }

    #[test]
    fn a_truncated_tail_does_not_match_the_checkpoint_head() {
        let rows = chain(&[1, 2, 3]);
        let report = verify(&rows[..2], checkpoint(&rows));
        assert_eq!(
            report.findings,
            vec![ChainFinding::HeadMismatch {
                expected_seq: 3,
                found_seq: Some(2),
            }]
        );
    }

    #[test]
    fn a_key_ring_verifies_a_chain_across_a_key_rotation() {
        let old = key();
        let new = CallbackSecret::new(vec![8_u8; 32]);
        let first = rec(1);
        let h1 = link(&old, &GENESIS, &first);
        let second = rec(2);
        let h2 = link(&new, &h1, &second);
        let rows = [
            ChainRow {
                record: first,
                prev: Some(GENESIS),
                hash: Some(h1),
            },
            ChainRow {
                record: second,
                prev: Some(h1),
                hash: Some(h2),
            },
        ];
        let cp = checkpoint(&rows);
        let ring = [old, new.clone()];
        let mut verifier = ChainVerifier::with_keys(&ring).with_start_seq(Some(1));
        for row in &rows {
            verifier.push(row);
        }
        assert!(verifier.finish(cp.as_ref()).is_intact());

        let mut only_new = ChainVerifier::new(&new).with_start_seq(Some(1));
        for row in &rows {
            only_new.push(row);
        }
        assert_eq!(
            only_new.finish(cp.as_ref()).findings,
            vec![ChainFinding::Tampered { seq: 1 }]
        );
    }

    #[test]
    fn a_wrong_key_flags_every_row() {
        let rows = chain(&[1, 2]);
        let other = CallbackSecret::new(vec![1_u8; 32]);
        let mut verifier = ChainVerifier::new(&other);
        for row in &rows {
            verifier.push(row);
        }
        let report = verifier.finish(None);
        assert_eq!(
            report.findings,
            vec![
                ChainFinding::Tampered { seq: 1 },
                ChainFinding::Tampered { seq: 2 },
            ]
        );
    }
}
