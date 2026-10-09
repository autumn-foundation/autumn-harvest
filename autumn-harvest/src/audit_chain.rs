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
//! A row is unprotected from its insert until the next export tick stamps it.
//! A database writer can change or delete it in that window, and nothing
//! detects that. The SIEM has no copy yet. See `docs/audit-export.md`.
//!
//! # What one link holds
//!
//! `chain_hash = HMAC-SHA256(key, CHAIN_DOMAIN || chain_prev ||
//! newest_before || canonical(row))`. `chain_prev` is the hash of the row
//! with the previous `export_seq`. The first row uses [`GENESIS`].
//! `newest_before` is the newest `occurred_at` of every row chained before
//! this one. The canonical row includes `shard` and `seq`, so a row cannot
//! move to another position.
//!
//! Each row stores its own `chain_prev`. So a row stays verifiable after
//! retention deletes its predecessor. A missing row shows as a sequence gap.
//! A changed row shows as a hash break.
//!
//! # The keyed checkpoint
//!
//! The cursor holds a [`ChainCheckpoint`]: the first chained `seq`, the
//! newest chained `seq`, its link and the newest `occurred_at` of the chain.
//! A MAC under the chain key covers all four. A writer without the key cannot move the checkpoint.
//! So the verifier finds a stripped row and a deleted tail.
//!
//! A valid MAC does not prove that a checkpoint is the newest one. A writer
//! can restore an older table and cursor. Pass the newest link that an
//! earlier check saw as [`ChainVerifyOptions::known_head`] to detect that.
//!
//! The exporter extends only a checkpoint that its key accepts. Otherwise a
//! writer could move the head and have the exporter sign it. A missing or
//! invalid checkpoint stops the chain until [`reanchor_shard_chain`] starts a
//! new one after the sequenced rows.
//!
//! A writer who
//! removes every chain value and the whole checkpoint leaves a table that
//! looks unchained. Only the SIEM copy detects that.
//!
//! # Retention
//!
//! Retention deletes old rows, so it leaves gaps. It keeps some old rows, such
//! as export decommission records. Pass the retention cutoff to
//! [`ChainVerifyOptions`]. A gap then counts as retention only when the row
//! after it proves that every row before it is old. The proof is its
//! `newest_before`, under the MAC. The ages of the surviving rows are no
//! proof, because a late commit can carry an old `occurred_at`. A writer can
//! therefore delete only rows that retention deletes within
//! [`RETENTION_SLACK`].
//!
//! # Why a key
//!
//! An unkeyed hash gives no protection against a database writer. That writer
//! can recompute every later link. The key lives outside the database. So a
//! writer without the key cannot forge a link. A process that holds the key
//! can. This control protects against database-level tampering, not against
//! a compromised Harvest process.

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
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
/// A time counts as old when it is older than the cutoff plus this slack. The
/// slack absorbs late commits near the cutoff.
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
    let mut out = Vec::with_capacity(256);
    for_each_canonical_chunk(record, |chunk| out.extend_from_slice(chunk));
    out
}

/// Feed the bytes of [`canonical_record`] to `sink`, in order, in pieces.
///
/// The pieces concatenate to exactly the bytes [`canonical_record`] returns.
/// The MAC path uses this to skip the buffer that holds the whole record.
fn for_each_canonical_chunk(record: &AuditExportRecord, mut sink: impl FnMut(&[u8])) {
    let mut shard = DecimalBuf::default();
    let mut seq = DecimalBuf::default();
    let mut shard_id = DecimalBuf::default();
    if let Some(value) = record.shard_id {
        shard_id.write(value);
    }
    let mut id = uuid::Uuid::encode_buffer();
    let occurred_at = canonical_time(record.occurred_at);
    let fields: [Option<&str>; 15] = [
        Some(shard.write(record.shard)),
        Some(seq.write(record.seq)),
        Some(record.id.hyphenated().encode_lower(&mut id)),
        record.shard_id.map(|_| shard_id.as_str()),
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
    for field in fields {
        push_field(&mut sink, field);
    }
}

/// A stack buffer that holds the decimal text of one integer.
#[derive(Default)]
struct DecimalBuf {
    bytes: [u8; 20],
    len: usize,
}

impl DecimalBuf {
    /// Write `value` in decimal and return the text.
    fn write(&mut self, value: impl std::fmt::Display) -> &str {
        use std::io::Write as _;

        let mut cursor = &mut self.bytes[..];
        // An `i64` has at most 20 characters, so the write cannot fail.
        let _ = write!(cursor, "{value}");
        let left = cursor.len();
        self.len = self.bytes.len() - left;
        self.as_str()
    }

    /// The text written last.
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).unwrap_or_default()
    }
}

/// Compute the link for `record` after `prev`.
///
/// `newest_before` is the newest `occurred_at` of every row chained before
/// `record`, or `None` for the first link. It is encoded as a canonical
/// field. The MAC covers it, so the verifier can trust it for retention.
///
/// # Panics
/// Never: HMAC-SHA256 accepts a key of any length.
#[must_use]
pub fn link(
    key: &CallbackSecret,
    prev: &ChainHash,
    newest_before: Option<DateTime<Utc>>,
    record: &AuditExportRecord,
) -> ChainHash {
    link_with(&keyed_mac(key), prev, newest_before, record)
}

/// The HMAC state after the key setup.
///
/// Clone it per link. The clone skips the two key-block compressions that
/// `Hmac::new_from_slice` runs on every call.
fn keyed_mac(key: &CallbackSecret) -> Hmac<Sha256> {
    #[expect(clippy::expect_used, reason = "HMAC accepts a key of any length")]
    Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length")
}

/// [`link`] with a key that [`keyed_mac`] already set up.
fn link_with(
    keyed: &Hmac<Sha256>,
    prev: &ChainHash,
    newest_before: Option<DateTime<Utc>>,
    record: &AuditExportRecord,
) -> ChainHash {
    let mut mac = keyed.clone();
    mac.update(CHAIN_DOMAIN);
    mac.update(prev);
    let newest = newest_before.map(canonical_time);
    push_field(&mut |chunk| mac.update(chunk), newest.as_deref());
    for_each_canonical_chunk(record, |chunk| mac.update(chunk));
    mac.finalize().into_bytes().into()
}

/// A time as the canonical encoding writes it: RFC 3339, UTC, six digits.
fn canonical_time(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Append one length-prefixed field, or the absent marker.
fn push_field(sink: &mut impl FnMut(&[u8]), field: Option<&str>) {
    match field {
        Some(text) => {
            // An audit field longer than 4 GiB cannot reach the table.
            let len = u32::try_from(text.len()).unwrap_or(ABSENT - 1);
            sink(&len.to_be_bytes());
            sink(text.as_bytes());
        }
        None => sink(&ABSENT.to_be_bytes()),
    }
}

/// The newest of `newest_before` and `at`.
fn newest_through(newest_before: Option<DateTime<Utc>>, at: DateTime<Utc>) -> DateTime<Utc> {
    newest_before.map_or(at, |newest| newest.max(at))
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
    /// The newest `occurred_at` of every chained row, through the head.
    pub newest_at: DateTime<Utc>,
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
        mac.update(&self.newest_at.timestamp_micros().to_be_bytes());
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
    /// The stored `chain_newest_before`. It is `None` for the first link.
    pub newest_before: Option<DateTime<Utc>>,
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
    /// The checkpoint MAC does not verify under any key, or the checkpoint
    /// is partly missing.
    CheckpointInvalid,
    /// The chain ends before the known head. Someone restored an older
    /// state of the table and the cursor.
    RolledBack {
        /// The `seq` of the known head.
        known_seq: i64,
        /// The newest chained `seq` the database holds, if any.
        head_seq: Option<i64>,
    },
    /// The row at the known head's `seq` has another link. Or retention
    /// purged that row, and its successor names another link. Someone
    /// replaced the chain after that point.
    KnownLinkMismatch {
        /// The `seq` of the known head.
        seq: i64,
    },
    /// Retention purged the known head's row and its successor. Nothing can
    /// prove the link. Refresh the known head on every run.
    KnownLinkUnverifiable {
        /// The `seq` of the known head.
        seq: i64,
    },
    /// Another shard's live export cursor is in this database. Two cursors
    /// share the `export_seq` space, so the exporter does not stamp the
    /// chain here.
    SharedDatabase,
    /// The checkpoint head is behind the cursor's `last_assigned_seq`. Rows
    /// were sequenced without a link. The exporter does not extend the chain
    /// until a re-anchor.
    CheckpointBehindCursor {
        /// The checkpoint head `seq`.
        head_seq: i64,
        /// The cursor's `last_assigned_seq`.
        last_assigned_seq: i64,
    },
    /// The checkpoint head is past the cursor's `last_assigned_seq`. Someone
    /// lowered the cursor. Retention never does.
    CursorBehindCheckpoint {
        /// The checkpoint head `seq`.
        head_seq: i64,
        /// The cursor's `last_assigned_seq`.
        last_assigned_seq: i64,
    },
}

/// A link that a check outside the database saw: an earlier verification or
/// the SIEM copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownLink {
    /// The link's `export_seq`.
    pub seq: i64,
    /// The link's `chain_hash`.
    pub hash: ChainHash,
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
    /// The keyed HMAC state of each accepted key.
    macs: Vec<Hmac<Sha256>>,
    keys: std::marker::PhantomData<&'k [CallbackSecret]>,
    retention_cutoff: Option<DateTime<Utc>>,
    report: ChainReport,
    /// The previous row, once the chain starts. See [`Previous`].
    previous: Option<Previous>,
    known_head: Option<KnownLink>,
    /// `true` after the row at the known head's `seq`.
    known_seen: bool,
    /// `true` after the row just after the known head's `seq`.
    successor_seen: bool,
}

/// The verifier state for the previous row.
#[derive(Debug, Clone, Copy)]
struct Previous {
    seq: i64,
    /// The stored hash. `None` for an unchained row.
    hash: Option<ChainHash>,
    /// The newest `occurred_at` through this row. `None` for an unchained row.
    newest: Option<DateTime<Utc>>,
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
            keys: std::marker::PhantomData,
            macs: keys.iter().map(keyed_mac).collect(),
            retention_cutoff: None,
            report: ChainReport::default(),
            previous: None,
            known_head: None,
            known_seen: false,
            successor_seen: false,
        }
    }

    /// Require the chain to reach `known` and to hold it.
    ///
    /// `known` is the newest link that a check outside the database saw. A
    /// signed checkpoint cannot prove that it is the newest one. So only this
    /// check detects a restored older state.
    #[must_use]
    pub const fn with_known_head(mut self, known: Option<KnownLink>) -> Self {
        self.known_head = known;
        self
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
    /// Retention explains a gap only when `newest_before`, the verified
    /// newest time before the row after the gap, is old.
    fn gap(&mut self, after_seq: i64, before_seq: i64, newest_before: Option<DateTime<Utc>>) {
        let gap = ChainFinding::Gap {
            after_seq,
            before_seq,
        };
        if newest_before.is_some_and(|newest| self.is_old(newest)) {
            self.report.retention_gaps.push(gap);
        } else {
            self.report.findings.push(gap);
        }
    }

    /// Check one row against its own hash and the row before it.
    pub fn push(&mut self, row: &ChainRow) {
        let seq = row.record.seq;
        // The known link holds wherever it is, before the start too. When
        // retention purged it, its successor's `chain_prev` must name it.
        if let Some(known) = self.known_head {
            let mismatch = if seq == known.seq {
                self.known_seen = true;
                row.hash != Some(known.hash)
            } else if seq == known.seq + 1 {
                self.successor_seen = true;
                !self.known_seen && row.prev != Some(known.hash)
            } else {
                false
            };
            if mismatch {
                self.report
                    .findings
                    .push(ChainFinding::KnownLinkMismatch { seq: known.seq });
            }
        }
        // A re-anchor starts a new chain after chained rows. The checkpoint
        // start therefore decides, when there is one.
        let in_chain = match self.report.start_seq {
            Some(start) => seq >= start,
            None => self.previous.is_some() || row.hash.is_some(),
        };
        if !in_chain {
            self.report.unchained_prefix += 1;
            return;
        }

        // Only a link that verifies can explain a gap.
        let valid = self.own_hash_matches(row);
        let newest_before = row.newest_before.filter(|_| valid);
        match self.previous {
            Some(previous) if seq != previous.seq + 1 => {
                self.gap(previous.seq, seq, newest_before);
            }
            None => {
                // Rows between the checkpoint start and the first row found
                // are missing.
                if let Some(start) = self.report.start_seq
                    && seq > start
                {
                    self.gap(start - 1, seq, newest_before);
                }
            }
            Some(_) => {}
        }

        if row.hash.is_none() {
            self.report.findings.push(ChainFinding::Unchained { seq });
            self.previous = Some(Previous {
                seq,
                hash: None,
                newest: None,
            });
            return;
        }
        // A link check needs both neighbours present and chained.
        if let Some(Previous {
            seq: previous_seq,
            hash: Some(previous_hash),
            newest,
        }) = self.previous
            && seq == previous_seq + 1
            && (row.prev != Some(previous_hash) || row.newest_before != newest)
        {
            self.report
                .findings
                .push(ChainFinding::LinkMismatch { seq });
        }
        if self.report.anchor_seq.is_none() {
            self.report.anchor_seq = Some(seq);
        }
        if !valid {
            self.report.findings.push(ChainFinding::Tampered { seq });
        }
        self.report.checked += 1;
        self.report.last_seq = Some(seq);
        self.report.last_hash = row.hash;
        self.previous = Some(Previous {
            seq,
            hash: row.hash,
            newest: Some(newest_through(row.newest_before, row.record.occurred_at)),
        });
    }

    /// `true` when the row's stored hash is its link under some key.
    fn own_hash_matches(&self, row: &ChainRow) -> bool {
        row.prev.is_some_and(|prev| {
            self.macs.iter().any(|keyed| {
                row.hash == Some(link_with(keyed, &prev, row.newest_before, &row.record))
            })
        })
    }

    /// End the verification against the cursor's verified `checkpoint`.
    ///
    /// The newest chained row must be the checkpoint head. Rows missing after
    /// it count as retention only when the checkpoint's newest time is old.
    #[must_use]
    pub fn finish(mut self, checkpoint: Option<&ChainCheckpoint>) -> ChainReport {
        if let Some(checkpoint) = checkpoint {
            self.check_head(checkpoint);
        }
        let head_seq = checkpoint.map(|c| c.head_seq).or(self.report.last_seq);
        if let Some(known) = self.known_head {
            if head_seq.is_none_or(|head| head < known.seq) {
                self.report.findings.push(ChainFinding::RolledBack {
                    known_seq: known.seq,
                    head_seq,
                });
            } else if !self.known_seen && !self.successor_seen {
                // Retention purged both rows that could prove the link.
                self.report
                    .findings
                    .push(ChainFinding::KnownLinkUnverifiable { seq: known.seq });
            }
        }
        self.report
    }

    /// Check that the newest chained row is the checkpoint head.
    fn check_head(&mut self, checkpoint: &ChainCheckpoint) {
        let found = self.report.last_seq.zip(self.report.last_hash);
        if found == Some((checkpoint.head_seq, checkpoint.head)) {
            return;
        }
        // A re-anchored chain with no row yet.
        if found.is_none() && checkpoint.head_seq < checkpoint.start_seq {
            return;
        }
        let tail_purged = found.is_none_or(|(seq, _)| seq < checkpoint.head_seq)
            && self.is_old(checkpoint.newest_at);
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
    /// The newest `occurred_at` of the chain, through the newest row.
    pub(crate) newest_at: DateTime<Utc>,
}

/// Rows the verifier reads per query.
#[cfg(feature = "db")]
const VERIFY_PAGE_ROWS: i64 = 1_000;

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct AnyChained {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    present: bool,
}

/// `true` when a row up to `through_seq` is chained.
///
/// It can scan the unchained rows, so the exporter calls it only for a
/// cursor with no checkpoint.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
async fn any_chained_row(
    conn: &mut diesel_async::AsyncPgConnection,
    through_seq: i64,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let row: AnyChained = diesel::sql_query(
        "SELECT EXISTS ( \
             SELECT 1 FROM harvest_audit_log \
             WHERE chain_hash IS NOT NULL AND export_seq <= $1 \
         ) AS present",
    )
    .bind::<diesel::sql_types::BigInt, _>(through_seq)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.present)
}

/// The point the exporter extends the chain from.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ChainAnchor {
    /// The link to extend, or `None` for a new chain.
    pub(crate) head: Option<ChainHash>,
    /// The first chained `export_seq`, or `None` for a new chain.
    pub(crate) start_seq: Option<i64>,
    /// The newest time of the chain so far, or `None` for a new chain.
    pub(crate) newest_at: Option<DateTime<Utc>>,
}

/// The anchor for the next stamp on `cursor`, or `None` to refuse the stamp.
///
/// The exporter signs a new checkpoint over each stamp. So it must not trust
/// a head that a database writer can set. It extends a checkpoint only when
/// `key` accepts its MAC and the head is `last_assigned_seq`. A head behind
/// it means rows were sequenced without a link. Extending would skip them.
/// It starts a new chain only when the cursor has no
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
) -> crate::error::HarvestResult<Result<ChainAnchor, ChainRefusal>> {
    if other_live_cursor(conn, cursor.shard_id).await? {
        return Ok(Err(ChainRefusal::SharedDatabase));
    }
    if let Some((checkpoint, mac)) = stored_checkpoint(cursor) {
        let valid = key.accepts(&checkpoint, cursor.shard_id, &mac)
            && checkpoint.head_seq == cursor.last_assigned_seq;
        // An empty re-anchored chain starts like a new one: genesis, and no
        // newest time before the first link.
        let empty = checkpoint.head_seq < checkpoint.start_seq;
        return Ok(valid
            .then_some(ChainAnchor {
                head: (!empty).then_some(checkpoint.head),
                start_seq: Some(checkpoint.start_seq),
                newest_at: (!empty).then_some(checkpoint.newest_at),
            })
            .ok_or(ChainRefusal::Checkpoint));
    }
    if has_checkpoint_column(cursor) || any_chained_row(conn, cursor.last_assigned_seq).await? {
        return Ok(Err(ChainRefusal::Checkpoint));
    }
    Ok(Ok(ChainAnchor::default()))
}

/// Why the exporter does not stamp the chain on this tick.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChainRefusal {
    /// The checkpoint is missing, does not verify or lags.
    Checkpoint,
    /// Another shard's live cursor shares this database.
    SharedDatabase,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct OtherCursor {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    present: bool,
}

/// `true` when another shard's live export cursor is in this database.
///
/// Two logical shards on one database share the `export_seq` space. A
/// per-shard chain cannot be correct there.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
async fn other_live_cursor(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let row: OtherCursor = diesel::sql_query(
        "SELECT EXISTS ( \
             SELECT 1 FROM harvest_audit_export_cursor \
             WHERE shard_id <> $1 AND retired_at IS NULL \
         ) AS present",
    )
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.present)
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
            cur::chain_newest_at.eq(checkpoint.newest_at),
            cur::chain_mac.eq(mac.to_vec()),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Start a new chain on `shard_id` after its sequenced rows.
///
/// The exporter does not extend a chain whose checkpoint is missing, does not
/// verify or lags. New rows then stay unchained. An operator calls this to
/// recover, for example after a cursor rebuild.
///
/// It never changes a sequenced row. Those rows may already be exported, and
/// a redrive must send the same bytes. It signs an empty checkpoint that
/// starts at `last_assigned_seq + 1`. The next export tick chains from there.
/// The verifier then skips the rows before the new start, so compare them
/// with the SIEM copy first.
///
/// Returns the new checkpoint, or `None` when the cursor is missing.
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
            let last_assigned_seq: Option<i64> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(cur::last_assigned_seq)
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let Some(last_assigned_seq) = last_assigned_seq else {
                return Ok(None);
            };
            // No row in the new chain yet. The head is the genesis link, one
            // before the start. The newest time is now, a safe upper bound.
            let checkpoint = ChainCheckpoint {
                start_seq: last_assigned_seq + 1,
                head_seq: last_assigned_seq,
                head: GENESIS,
                newest_at: Utc::now(),
            };
            write_checkpoint(conn, shard_id, &checkpoint, key).await?;
            Ok(Some(checkpoint))
        }),
    )
    .await
}

/// Stamp the chain over the rows with `after_seq < export_seq <= through_seq`.
///
/// It continues from `anchor`. Returns `None` when no row is in the range.
/// The caller holds the cursor row lock, so no other exporter stamps the same
/// rows.
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
    anchor: &ChainAnchor,
) -> crate::error::HarvestResult<Option<Stamped>> {
    use diesel::prelude::*;
    use diesel::sql_types::{Array, Bytea, Nullable, Timestamptz};
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

    let mut prev = anchor.head.unwrap_or(GENESIS);
    let mut newest = anchor.newest_at;
    let mut ids = Vec::with_capacity(rows.len());
    let mut prevs = Vec::with_capacity(rows.len());
    let mut newests = Vec::with_capacity(rows.len());
    let mut hashes = Vec::with_capacity(rows.len());
    let mut stamped: Option<Stamped> = None;
    for row in rows {
        let id = row.id;
        let Some(record) = AuditExportRecord::from_row(shard_id, row) else {
            continue;
        };
        let hash = link(key, &prev, newest, &record);
        ids.push(id);
        prevs.push(prev.to_vec());
        newests.push(newest);
        hashes.push(hash.to_vec());
        prev = hash;
        let newest_at = newest_through(newest, record.occurred_at);
        newest = Some(newest_at);
        stamped = Some(Stamped {
            first_seq: stamped.map_or(record.seq, |s| s.first_seq),
            head_seq: record.seq,
            head: hash,
            newest_at,
        });
    }
    if ids.is_empty() {
        return Ok(None);
    }

    diesel::sql_query(
        "UPDATE harvest_audit_log a \
         SET chain_prev = v.prev, chain_newest_before = v.newest, chain_hash = v.hash, \
             chain_shard = $5 \
         FROM unnest($1, $2, $3, $4) AS v(id, prev, newest, hash) \
         WHERE a.id = v.id",
    )
    .bind::<Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<Array<Bytea>, _>(&prevs)
    .bind::<Array<Nullable<Timestamptz>>, _>(&newests)
    .bind::<Array<Bytea>, _>(&hashes)
    .bind::<diesel::sql_types::Integer, _>(shard_id)
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
    /// The newest link a check outside the database saw. A signed checkpoint
    /// cannot prove that it is the newest one. So only this link detects a
    /// restored older state.
    pub known_head: Option<KnownLink>,
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
        known_head: None,
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
    let (checkpoint, checkpoint_finding) = cursor.as_ref().map_or((None, None), |cursor| {
        verified_checkpoint(cursor, options.keys)
    });

    let mut verifier = ChainVerifier::with_keys(options.keys)
        .with_retention_cutoff(options.retention_cutoff)
        .with_start_seq(checkpoint.map(|c| c.start_seq))
        .with_known_head(options.known_head);
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
            // Links another shard made do not belong to this chain.
            let own = row.chain_shard == Some(shard_id);
            let prev = row
                .chain_prev
                .as_deref()
                .filter(|_| own)
                .and_then(from_bytes);
            let hash = row
                .chain_hash
                .as_deref()
                .filter(|_| own)
                .and_then(from_bytes);
            let newest_before = row.chain_newest_before.filter(|_| own);
            if let Some(record) = AuditExportRecord::from_row(shard_id, row) {
                verifier.push(&ChainRow {
                    record,
                    prev,
                    hash,
                    newest_before,
                });
            }
        }
        after_seq = last_seq;
    }

    let mut report = verifier.finish(checkpoint.as_ref());
    if other_live_cursor(conn, shard_id).await? {
        report.findings.insert(0, ChainFinding::SharedDatabase);
    }
    // Retention never lowers the cursor. So a head past it is an edit, not a
    // purged tail. A head behind it means rows were sequenced without a link.
    if let Some(checkpoint) = checkpoint {
        let finding = match checkpoint.head_seq.cmp(&bound) {
            std::cmp::Ordering::Greater => Some(ChainFinding::CursorBehindCheckpoint {
                head_seq: checkpoint.head_seq,
                last_assigned_seq: bound,
            }),
            std::cmp::Ordering::Less => Some(ChainFinding::CheckpointBehindCursor {
                head_seq: checkpoint.head_seq,
                last_assigned_seq: bound,
            }),
            std::cmp::Ordering::Equal => None,
        };
        if let Some(finding) = finding {
            report.findings.insert(0, finding);
        }
    }
    if let Some(finding) = checkpoint_finding {
        report.findings.insert(0, finding);
    } else if checkpoint.is_none() && report.checked > 0 {
        report.findings.insert(0, ChainFinding::CheckpointMissing);
    }
    Ok(report)
}

/// The checkpoint on `cursor` that one of `keys` signed, or the finding.
#[cfg(feature = "db")]
fn verified_checkpoint(
    cursor: &crate::models::AuditExportCursor,
    keys: &[CallbackSecret],
) -> (Option<ChainCheckpoint>, Option<ChainFinding>) {
    match stored_checkpoint(cursor) {
        Some((checkpoint, mac))
            if keys
                .iter()
                .any(|key| checkpoint.mac(key, cursor.shard_id) == mac) =>
        {
            (Some(checkpoint), None)
        }
        Some(_) => (None, Some(ChainFinding::CheckpointInvalid)),
        // A partial checkpoint is an edit, not an absent checkpoint.
        None if has_checkpoint_column(cursor) => (None, Some(ChainFinding::CheckpointInvalid)),
        None => (None, None),
    }
}

/// `true` when any checkpoint column is set.
#[cfg(feature = "db")]
const fn has_checkpoint_column(cursor: &crate::models::AuditExportCursor) -> bool {
    cursor.chain_head.is_some()
        || cursor.chain_start_seq.is_some()
        || cursor.chain_head_seq.is_some()
        || cursor.chain_newest_at.is_some()
        || cursor.chain_mac.is_some()
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
            newest_at: cursor.chain_newest_at?,
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
            chain_newest_before: None,
            chain_hash: None,
        }
    }

    /// A correctly chained run of rows for `seqs`, starting at [`GENESIS`].
    fn chain(seqs: &[i64]) -> Vec<ChainRow> {
        chain_records(seqs.iter().map(|&seq| rec(seq)).collect())
    }

    /// `records`, chained in order from [`GENESIS`], as the exporter does.
    fn chain_records(records: Vec<AuditExportRecord>) -> Vec<ChainRow> {
        let key = key();
        let mut prev = GENESIS;
        let mut newest = None;
        records
            .into_iter()
            .map(|record| {
                let hash = link(&key, &prev, newest, &record);
                let row = ChainRow {
                    prev: Some(prev),
                    hash: Some(hash),
                    newest_before: newest,
                    record,
                };
                prev = hash;
                newest = Some(newest_through(newest, row.record.occurred_at));
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
            newest_at: rows.iter().map(|row| row.record.occurred_at).max()?,
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

    /// The integer fields use a stack buffer. The widest values must still
    /// match the text that `to_string` gives.
    #[test]
    fn the_widest_integers_encode_as_decimal_text() {
        let mut wide = rec(1);
        wide.shard = i32::MIN;
        wide.seq = i64::MIN;
        wide.shard_id = Some(i32::MAX);
        let bytes = canonical_record(&wide);
        let mut expected = Vec::new();
        for text in [i32::MIN.to_string(), i64::MIN.to_string()] {
            expected
                .extend_from_slice(&u32::try_from(text.len()).unwrap_or_default().to_be_bytes());
            expected.extend_from_slice(text.as_bytes());
        }
        assert!(bytes.starts_with(&expected));
        let shard_id = i32::MAX.to_string();
        let mut tail = u32::try_from(shard_id.len())
            .unwrap_or_default()
            .to_be_bytes()
            .to_vec();
        tail.extend_from_slice(shard_id.as_bytes());
        assert!(bytes.windows(tail.len()).any(|w| w == tail));
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
            to_hex(&link(&key(), &GENESIS, None, &rec(1))),
            "5ee3864c95187c4f68b36457c90ea9a06943ff4509e7949866cd9abe0e271aa8"
        );
        assert_eq!(
            to_hex(&link(&key(), &[1; 32], Some(at(1_700_000_000)), &rec(1))),
            "b911ece96316f2fa5b264dea4d4c86605e469aa43f2c7132b0405b1b64b21365"
        );
    }

    #[test]
    fn link_depends_on_key_prev_and_every_field() {
        let key = key();
        let base = link(&key, &GENESIS, None, &rec(1));
        assert_ne!(base, GENESIS);
        assert_eq!(base, link(&key, &GENESIS, None, &rec(1)));
        assert_ne!(
            base,
            link(
                &CallbackSecret::new(vec![8_u8; 32]),
                &GENESIS,
                None,
                &rec(1)
            )
        );
        assert_ne!(base, link(&key, &[1; 32], None, &rec(1)));
        assert_ne!(base, link(&key, &GENESIS, Some(at(0)), &rec(1)));
        assert_ne!(
            link(&key, &GENESIS, Some(at(0)), &rec(1)),
            link(&key, &GENESIS, Some(at(1)), &rec(1))
        );
        let mut changed = rec(1);
        changed.status = "failed".into();
        assert_ne!(base, link(&key, &GENESIS, None, &changed));
        let mut moved = rec(1);
        moved.seq = 2;
        assert_ne!(base, link(&key, &GENESIS, None, &moved));
        let mut other_shard = rec(1);
        other_shard.shard = 3;
        assert_ne!(base, link(&key, &GENESIS, None, &other_shard));
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
    fn an_old_row_after_a_deleted_recent_row_does_not_make_the_gap_retention() {
        let day = 86_400;
        // Row 3 is a late commit: it is older than row 2.
        let rows = chain_at(&[(1, 0), (2, 100 * day), (3, day)]);
        let cp = checkpoint(&rows);
        let survivors = [rows[0].clone(), rows[2].clone()];
        let report = verify_with(&survivors, cp, Some(at(10 * day)));
        assert_eq!(
            report.findings,
            vec![ChainFinding::Gap {
                after_seq: 1,
                before_seq: 3,
            }]
        );
        assert!(report.retention_gaps.is_empty(), "{report:?}");

        // The tail goes too. The head is old, but the chain's newest time is not.
        let report = verify_with(&rows[..1], cp, Some(at(10 * day)));
        assert_eq!(
            report.findings,
            vec![ChainFinding::HeadMismatch {
                expected_seq: 3,
                found_seq: Some(1),
            }]
        );
    }

    #[test]
    fn a_changed_newest_before_is_reported() {
        let mut rows = chain(&[1, 2, 3]);
        let cp = checkpoint(&rows);
        rows[2].newest_before = Some(at(0));
        let report = verify(&rows, cp);
        assert_eq!(
            report.findings,
            vec![
                ChainFinding::LinkMismatch { seq: 3 },
                ChainFinding::Tampered { seq: 3 },
            ]
        );
    }

    #[test]
    fn a_known_head_detects_a_rolled_back_or_replaced_chain() {
        let rows = chain(&[1, 2, 3]);
        let known = KnownLink {
            seq: 3,
            hash: rows[2].hash.unwrap_or(GENESIS),
        };
        let key = key();
        let run = |rows: &[ChainRow], cp: Option<ChainCheckpoint>| {
            let mut verifier = ChainVerifier::new(&key)
                .with_start_seq(Some(1))
                .with_known_head(Some(known));
            for row in rows {
                verifier.push(row);
            }
            verifier.finish(cp.as_ref()).findings
        };
        assert_eq!(run(&rows, checkpoint(&rows)), Vec::new());
        assert_eq!(
            run(&rows[..2], checkpoint(&rows[..2])),
            vec![ChainFinding::RolledBack {
                known_seq: 3,
                head_seq: Some(2),
            }]
        );
        let mut records: Vec<AuditExportRecord> = rows.into_iter().map(|row| row.record).collect();
        records[2].actor = "mallory".into();
        let replaced = chain_records(records);
        assert_eq!(
            run(&replaced, checkpoint(&replaced)),
            vec![ChainFinding::KnownLinkMismatch { seq: 3 }]
        );
    }

    #[test]
    fn a_reanchored_chain_skips_the_rows_before_its_start() {
        let old = chain(&[1, 2]);
        // An empty new chain that starts at seq 4. Seq 3 is unchained.
        let mut rows = old.clone();
        rows.push(ChainRow {
            record: rec(3),
            prev: None,
            hash: None,
            newest_before: None,
        });
        let empty = ChainCheckpoint {
            start_seq: 4,
            head_seq: 3,
            head: GENESIS,
            newest_at: at(0),
        };
        let report = verify(&rows, Some(empty));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!((report.checked, report.unchained_prefix), (0, 3));

        let new = chain_records(vec![rec(4)]);
        rows.extend(new.iter().cloned());
        let report = verify(
            &rows,
            Some(ChainCheckpoint {
                start_seq: 4,
                ..checkpoint(&new).unwrap_or(empty)
            }),
        );
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.anchor_seq, Some(4));

        // The empty checkpoint still finds a new row deleted after it.
        let report = verify(
            &old,
            Some(ChainCheckpoint {
                head_seq: 5,
                ..empty
            }),
        );
        assert!(!report.is_intact(), "{report:?}");
    }

    #[test]
    fn a_purged_known_head_is_checked_through_its_successor() {
        let day = 86_400;
        let original = chain_at(&[(1, 0), (2, day), (3, 2 * day)]);
        let known = KnownLink {
            seq: 2,
            hash: original[1].hash.unwrap_or(GENESIS),
        };
        // A restored seq 1, then a replacement branch from seq 2 on.
        let mut records: Vec<AuditExportRecord> =
            original.iter().map(|row| row.record.clone()).collect();
        records[1].actor = "mallory".into();
        let branch = chain_records(records);
        let cp = checkpoint(&branch);
        let key = key();
        let run = |rows: &[ChainRow]| {
            let mut verifier = ChainVerifier::new(&key)
                .with_start_seq(Some(1))
                .with_retention_cutoff(Some(at(10 * day)))
                .with_known_head(Some(known));
            for row in rows {
                verifier.push(row);
            }
            verifier.finish(cp.as_ref())
        };
        // Retention purged the replacement seq 2. Its successor still names it.
        let survivors = [branch[0].clone(), branch[2].clone()];
        assert_eq!(
            run(&survivors).findings,
            vec![ChainFinding::KnownLinkMismatch { seq: 2 }]
        );
        // The original chain with the same purge passes.
        let cp_ok = checkpoint(&original);
        let mut verifier = ChainVerifier::new(&key)
            .with_start_seq(Some(1))
            .with_retention_cutoff(Some(at(10 * day)))
            .with_known_head(Some(known));
        for row in [&original[0], &original[2]] {
            verifier.push(row);
        }
        assert_eq!(verifier.finish(cp_ok.as_ref()).findings, Vec::new());
    }

    #[test]
    fn a_known_head_with_no_surviving_neighbour_is_unverifiable() {
        let day = 86_400;
        let original = chain_at(&[(1, 0), (2, day), (3, 2 * day), (4, 3 * day)]);
        let known = KnownLink {
            seq: 2,
            hash: original[1].hash.unwrap_or(GENESIS),
        };
        let mut records: Vec<AuditExportRecord> =
            original.iter().map(|row| row.record.clone()).collect();
        records[1].actor = "mallory".into();
        let branch = chain_records(records);
        let key = key();
        let run = |rows: &[&ChainRow], cp: Option<ChainCheckpoint>| {
            let mut verifier = ChainVerifier::new(&key)
                .with_start_seq(Some(1))
                .with_retention_cutoff(Some(at(10 * day)))
                .with_known_head(Some(known));
            for row in rows {
                verifier.push(row);
            }
            verifier.finish(cp.as_ref()).findings
        };
        // Retention purged seq 2 and 3. Nothing proves or refutes the link.
        assert_eq!(
            run(&[&branch[0], &branch[3]], checkpoint(&branch)),
            vec![ChainFinding::KnownLinkUnverifiable { seq: 2 }]
        );
        assert_eq!(
            run(&[&original[0], &original[3]], checkpoint(&original)),
            vec![ChainFinding::KnownLinkUnverifiable { seq: 2 }]
        );
    }

    #[test]
    fn hex_and_bytes_round_trip() {
        let hash = link(&key(), &GENESIS, None, &rec(1));
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
                newest_at: at(0),
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
        chain_records(
            times
                .iter()
                .map(|&(seq, seconds)| {
                    let mut record = rec(seq);
                    record.occurred_at = at(seconds);
                    record
                })
                .collect(),
        )
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
                newest_before: None,
            })
            .collect();
        let key = key();
        let record = rec(3);
        let hash = link(&key, &GENESIS, None, &record);
        rows.push(ChainRow {
            record,
            prev: Some(GENESIS),
            hash: Some(hash),
            newest_before: None,
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
        let h1 = link(&old, &GENESIS, None, &first);
        let second = rec(2);
        let newest = Some(first.occurred_at);
        let h2 = link(&new, &h1, newest, &second);
        let rows = [
            ChainRow {
                record: first,
                prev: Some(GENESIS),
                hash: Some(h1),
                newest_before: None,
            },
            ChainRow {
                record: second,
                prev: Some(h1),
                hash: Some(h2),
                newest_before: newest,
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
