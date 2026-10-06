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
//! # Why a key
//!
//! An unkeyed hash gives no protection against a database writer. That writer
//! can recompute every later link. The key lives outside the database. So a
//! writer without the key cannot forge a link. A process that holds the key
//! can. This control protects against database-level tampering, not against
//! a compromised Harvest process.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::audit_export::AuditExportRecord;
use crate::completion_callback::CallbackSecret;

/// Domain separator for the chain MAC.
pub const CHAIN_DOMAIN: &[u8] = b"harvest-audit-chain-v1";

/// Shortest chain key the builder accepts, in bytes.
pub const MIN_CHAIN_KEY_BYTES: usize = 32;

/// One chain link: an HMAC-SHA256 output.
pub type ChainHash = [u8; 32];

/// The predecessor of the first chained row.
pub const GENESIS: ChainHash = [0; 32];

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
    /// A row after the chain start has no hash.
    Unchained {
        /// The row's `export_seq`.
        seq: i64,
    },
    /// One or more sequence numbers are missing between two chained rows.
    Gap {
        /// The last `export_seq` before the gap.
        after_seq: i64,
        /// The first `export_seq` after the gap.
        before_seq: i64,
    },
    /// The newest chained row does not match the cursor's `chain_head`.
    HeadMismatch {
        /// The `export_seq` of the newest chained row.
        seq: i64,
    },
}

/// The result of a chain verification.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainReport {
    /// Chained rows checked.
    pub checked: u64,
    /// Rows before the first chained row. The chain does not cover them.
    pub unchained_prefix: u64,
    /// The `export_seq` of the first chained row.
    pub anchor_seq: Option<i64>,
    /// The `export_seq` of the last chained row.
    pub last_seq: Option<i64>,
    /// Every problem found, in sequence order.
    pub findings: Vec<ChainFinding>,
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
    report: ChainReport,
    /// The `seq` and stored hash of the previous row, once the chain starts.
    previous: Option<(i64, Option<ChainHash>)>,
    /// The stored hash of the newest chained row.
    newest: Option<ChainHash>,
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
            report: ChainReport::default(),
            previous: None,
            newest: None,
        }
    }

    /// Check one row against its own hash and the row before it.
    pub fn push(&mut self, row: &ChainRow) {
        let seq = row.record.seq;
        let Some((previous_seq, previous_hash)) = self.previous else {
            // The chain has not started. An unchained row here is outside it.
            if row.hash.is_none() {
                self.report.unchained_prefix += 1;
                return;
            }
            self.report.anchor_seq = Some(seq);
            self.check_own_hash(row);
            return;
        };

        if seq != previous_seq + 1 {
            self.report.findings.push(ChainFinding::Gap {
                after_seq: previous_seq,
                before_seq: seq,
            });
        }
        if row.hash.is_none() {
            self.report.findings.push(ChainFinding::Unchained { seq });
            self.previous = Some((seq, None));
            return;
        }
        // A link check needs both neighbours present and chained.
        if seq == previous_seq + 1
            && let Some(previous_hash) = previous_hash
            && row.prev != Some(previous_hash)
        {
            self.report
                .findings
                .push(ChainFinding::LinkMismatch { seq });
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
        self.previous = Some((seq, row.hash));
        self.newest = row.hash;
    }

    /// End the verification. `head` is the cursor's `chain_head`.
    ///
    /// The head check runs only when a chained row survives. An empty table
    /// proves nothing either way.
    #[must_use]
    pub fn finish(mut self, head: Option<ChainHash>) -> ChainReport {
        if let (Some(head), Some(newest), Some(seq)) = (head, self.newest, self.report.last_seq)
            && head != newest
        {
            self.report
                .findings
                .push(ChainFinding::HeadMismatch { seq });
        }
        self.report
    }
}

/// Rows the verifier reads per query.
#[cfg(feature = "db")]
const VERIFY_PAGE_ROWS: i64 = 1_000;

/// Stamp the chain over the rows with `after_seq < export_seq <= through_seq`.
///
/// `head` is the cursor's `chain_head`, or `None` for the first link. Returns
/// the new head, or `None` when no row is in the range. The caller holds the
/// cursor row lock, so no other exporter stamps the same rows.
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
) -> crate::error::HarvestResult<Option<ChainHash>> {
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
    Ok(Some(prev))
}

/// Verify the chain on `shard_id` with `key`.
///
/// Reads the cursor head and every sequenced row in one repeatable-read
/// snapshot, so a concurrent export tick cannot cause a false head mismatch.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn verify_shard_chain(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    key: &CallbackSecret,
) -> crate::error::HarvestResult<ChainReport> {
    verify_shard_chain_with_keys(conn, shard_id, std::slice::from_ref(key)).await
}

/// [`verify_shard_chain`] that accepts a link made with any of `keys`.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn verify_shard_chain_with_keys(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: i32,
    keys: &[CallbackSecret],
) -> crate::error::HarvestResult<ChainReport> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_audit_export_cursor::dsl as cur;
    use crate::schema::harvest_audit_log::dsl as log;

    let mut tx = conn.build_transaction().repeatable_read().read_only();
    Box::pin(
        tx.run::<ChainReport, crate::error::HarvestError, _>(async |conn| {
            let head: Option<Vec<u8>> = cur::harvest_audit_export_cursor
                .find(shard_id)
                .select(cur::chain_head)
                .first::<Option<Vec<u8>>>(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .flatten();

            let mut verifier = ChainVerifier::with_keys(keys);
            let mut after_seq = 0_i64;
            loop {
                let rows: Vec<crate::models::AuditExportRow> = log::harvest_audit_log
                    .filter(log::export_seq.gt(after_seq))
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
            Ok(verifier.finish(head.as_deref().and_then(from_bytes)))
        }),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> CallbackSecret {
        CallbackSecret::new(vec![7_u8; MIN_CHAIN_KEY_BYTES])
    }

    fn rec(seq: i64) -> AuditExportRecord {
        AuditExportRecord {
            shard: 2,
            seq,
            id: uuid::Uuid::from_u128(u128::try_from(seq).unwrap()),
            shard_id: Some(2),
            occurred_at: chrono::DateTime::from_timestamp(1_700_000_000 + seq, 123_456_000)
                .unwrap(),
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

    fn verify(rows: &[ChainRow], head: Option<ChainHash>) -> ChainReport {
        let key = key();
        let mut verifier = ChainVerifier::new(&key);
        for row in rows {
            verifier.push(row);
        }
        verifier.finish(head)
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
    fn canonical_record_ignores_the_chain_hash_field() {
        let mut a = rec(1);
        a.chain_hash = Some("ff".into());
        assert_eq!(canonical_record(&a), canonical_record(&rec(1)));
    }

    #[test]
    fn canonical_record_starts_with_the_shard_and_seq() {
        let bytes = canonical_record(&rec(42));
        assert_eq!(&bytes[..5], &[0, 0, 0, 1, b'2']);
        assert_eq!(&bytes[5..11], &[0, 0, 0, 2, b'4', b'2']);
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
    fn an_intact_chain_verifies() {
        let rows = chain(&[1, 2, 3]);
        let head = rows[2].hash;
        let report = verify(&rows, head);
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.checked, 3);
        assert_eq!(report.anchor_seq, Some(1));
        assert_eq!(report.last_seq, Some(3));
    }

    #[test]
    fn a_changed_row_is_reported_as_tampered() {
        let mut rows = chain(&[1, 2, 3]);
        rows[1].record.actor = "mallory".into();
        let report = verify(&rows, None);
        assert_eq!(report.findings, vec![ChainFinding::Tampered { seq: 2 }]);
    }

    #[test]
    fn a_rehashed_row_without_the_key_breaks_the_next_link() {
        let mut rows = chain(&[1, 2, 3]);
        // A writer without the key cannot recompute row 2 correctly.
        rows[1].hash = Some([9; 32]);
        let report = verify(&rows, None);
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
        rows.remove(1);
        let report = verify(&rows, rows.last().and_then(|r| r.hash));
        assert_eq!(
            report.findings,
            vec![ChainFinding::Gap {
                after_seq: 1,
                before_seq: 3,
            }]
        );
    }

    #[test]
    fn a_purged_prefix_still_verifies_from_the_first_surviving_row() {
        let rows = chain(&[1, 2, 3, 4]);
        let report = verify(&rows[2..], rows[3].hash);
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.anchor_seq, Some(3));
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
        let report = verify(&rows, Some(hash));
        assert!(report.is_intact(), "{report:?}");
        assert_eq!(report.unchained_prefix, 2);
        assert_eq!(report.anchor_seq, Some(3));
    }

    #[test]
    fn an_unchained_row_after_the_chain_start_is_flagged() {
        let mut rows = chain(&[1, 2, 3]);
        rows[1].prev = None;
        rows[1].hash = None;
        let report = verify(&rows, rows[2].hash);
        assert_eq!(report.findings, vec![ChainFinding::Unchained { seq: 2 }]);
    }

    #[test]
    fn a_truncated_tail_does_not_match_the_cursor_head() {
        let rows = chain(&[1, 2, 3]);
        let report = verify(&rows[..2], rows[2].hash);
        assert_eq!(report.findings, vec![ChainFinding::HeadMismatch { seq: 2 }]);
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
        let ring = [old, new.clone()];
        let mut verifier = ChainVerifier::with_keys(&ring);
        for row in &rows {
            verifier.push(row);
        }
        assert!(verifier.finish(Some(h2)).is_intact());

        let mut only_new = ChainVerifier::new(&new);
        for row in &rows {
            only_new.push(row);
        }
        assert_eq!(
            only_new.finish(Some(h2)).findings,
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
