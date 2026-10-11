# Design — Issue #2009: export aged partitions before the drop

Retention drops an aged `harvest_events` partition. This change exports
the partition to object storage first. The sweep then reads the export
back, checks it, and drops the partition only when the check passes.

The issue also asks about delta encoding of agent context windows. §5
gives the measurement and the decision.

**One small migration** (`20261011024658_harvest_partition_export_marker`,
§3.1). **No new `WorkflowEvent` variant. No new route.** The two sanctioned
`event_data` writers do not change. The export reads rows and the drop is
DDL, so the append-only trigger does not apply.

---

## 0. Planning record

### 0.1 Brainstorm — where does the export go?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add a partition method to `HistoryArchiver`, with a default body. | Rejected. A default body lets an old archiver "succeed" with no upload. The unit is also different: one run, or many runs. |
| B2 | A new `PartitionArchiver` trait with two calls: `put(key, bytes)` and `get(key)`. Core owns the layout and the checks. | **Adopted.** The trait is a plain object store. The S3 and GCS backends of #1983 map to it in a few lines. |
| B3 | Upload the partition as one object. | Rejected. The legacy partition can hold the whole pre-conversion history. One object means one large buffer. |
| B4 | Upload numbered segments, then a manifest that lists each segment with its row count and SHA-256. | **Adopted.** Memory is bounded by one segment and one page of 256 rows. The manifest goes last, so a manifest always points at complete segments. |
| B5 | Encode each row as `to_jsonb(e)::text`. | **Adopted.** A new column is exported by default, like the append-only trigger guards a new column by default. |
| B6 | Hold the partition lock during the upload. | Rejected. The lock would stay open across network I/O. Retention already releases its connection before an archive call. |
| B7 | Upload without a lock. Then, under the `SHARE` lock of the drop, check the partition again against the manifest. | **Adopted.** `SHARE` blocks every row change. A match proves that the export is the content the drop removes. §0.4 gives the form of the check. |
| B8 | Read each segment back after the upload and compare its hash. | **Adopted.** This is the "verified" step of the issue. |
| B9 | A `DirectoryPartitionArchiver` in core. | **Adopted.** It is a working backend until #1983 merges, and the integration test uses it. |
| B10 | A read-back API that checks every hash and returns typed rows. | **Adopted.** It also groups rows into a run history for replay debugging. |
| B11 | Delta-encode context windows in the event log. | Measured, then declined. See §5. |

### 0.2 Reverse brainstorm — how can the export lose data?

| # | How to lose data | Mitigation |
|---|------------------|------------|
| R1 | Drop the partition when the upload failed. | An upload error, a read-back error or a timeout blocks the drop. The reason goes into `SweepOutcome::blocked`. |
| R2 | A row changes after the upload and before the drop. The codec rotation sweep (exception #3) can rewrite a row in a closed partition. | The drop computes the row checksum under `SHARE`. A different checksum or row count blocks the drop. The sweep exports again. |
| R3 | A backend returns other bytes than it stored. | The read-back compares each segment hash, the whole-partition hash and the row checksum, and parses every row. |
| R4 | A later partition with the same name overwrites an old export. The legacy partition name comes back after a disable and an enable. | The key holds the shard and the cohort bounds. A new enable gets a new conversion instant, so the bounds differ. |
| R5 | The straggler `DELETE` removes orphan rows that no export holds. | `try_build` refuses `straggler_grace_secs` with a partition archiver. The sweep skips straggler deletes when it exports or the shard holds the marker. The runtime warns when both are set. |
| R6 | A shard ID is missing from the key. Two shards then share one key. | The key starts with `shard-<id>`. |
| R7 | The export hash depends on the session time zone. `to_jsonb` formats `timestamptz` in the session zone. | Each read sets `TimeZone` to `UTC` for its transaction. |
| R8 | A key escapes the archive root of the directory backend. | The backend accepts only `[A-Za-z0-9._-]` path parts and refuses `.` and `..`. |
| R9 | An export of a large partition makes the liveness scanner flag the loop as stale. | The sweep calls the progress hook after each uploaded segment, after the manifest, and after each segment it reads back. |
| R10 | A dry run or `harvest partition status` uploads data. | Only an applying sweep with an archiver exports. A dry run has a zero drop budget. `evaluate` passes no archiver. |
| R11 | An offloaded payload is gone when the partition is exported. Retention collects the run's blobs when it deletes the run. | Documented. The per-run `HistoryArchiver` inflates offloaded payloads. Use it when a run needs its offloaded payloads in cold storage. |
| R12 | An erasure request arrives after the export. | Documented. Erasure does not reach an archive object. `docs/archival.md` already states this for per-run archives. |

### 0.3 Six thinking hats (first pass)

| Hat | Notes |
|-----|-------|
| White | A partition is dropped only when no live run owns a row. Its rows are orphans. Retention already sent each run to `HistoryArchiver` when one is set. `sha2` is a core dependency. #1983 (PR #2063) is open and adds S3 and GCS backends. |
| Red | Operators fear a silent loss more than a slow sweep. A blocked partition with a clear reason feels safe. |
| Black | The export reads the partition once, downloads it once and scans it once under `SHARE`. `SHARE` stalls an `UPDATE` with no `cohort` filter (erasure, codec rotation, rebalance) for the length of the scan. One statement under `exact_scan_timeout` bounds that, the same bound tier 3 has. |
| Yellow | Cold history costs much less in object storage. The archive keeps ciphertext, so the codec keys still protect it. No append path changes. |
| Green | Keep the trait minimal, so any blob store fits. A compressing backend can compress in `put` and expand in `get`. The hashes still match, because they cover the bytes core gives and gets back. |
| Blue | Red phase: unit tests for keys, manifest, segments and the directory backend; DB tests for export, verify, drop and read-back; failure tests. Green phase: the module, the sweep hook, the builder and the runtime. Refactor phase: docs, gates, review. |

### 0.4 Corrections after the review

Four review agents read the first version: data loss, Postgres locking,
API and tests, and docs. These changes followed. Each has a test.

| # | Finding | Fix | Test |
|---|---------|-----|------|
| C1 | Any sweep with no archiver dropped with no export: `harvest partition maintain`, `RetentionRuntime::spawn`, a process started without the archiver. | The marker table. A sweep with an archiver writes the row. A sweep with no archiver on a marked shard drops nothing. | `a_sweep_without_an_archiver_drops_nothing_once_a_shard_exported` |
| C2 | Every runner sweeps every shard. Two processes could export one partition, and a late upload could replace a segment after the drop. | A session advisory lock lets one process export a shard. Segment keys hold their SHA-256, so a late segment writes its own key. | `another_exporting_process_makes_the_shard_busy` |
| C3 | The hash scan under `SHARE` ran page by page, with no bound on its total time. | One statement computes the row count and a row checksum, a sum of 64-bit row hashes. `exact_scan_timeout` bounds it. A timeout has its own reason. | `the_row_hash_is_the_first_eight_digest_bytes_as_a_signed_integer`; every drop test |
| C4 | A failed drop exported the whole partition again, with no budget. | A clean earlier export is used again. A pass makes at most 4 new exports. | `a_failed_drop_reuses_the_export_on_the_next_pass`, `the_export_budget_spreads_a_backlog_over_passes` |
| C5 | The directory backend did not sync the directory after the rename. | It syncs the directory. The trait says an object must be durable when `put` returns. | Unit tests of the backend |
| C6 | Verify checked bytes only, so an unparseable row could drop and then fail `read_back`. | Verify runs the read-back path, which parses every row. `read_back` also checks the manifest against its key. | `read_back_refuses_a_manifest_under_another_key` |
| C7 | Pages of 1,000 large rows could use a lot of memory. | Pages hold 256 rows. | — |
| C8 | Internal types were public, and core-built types were exhaustive. | `SegmentWriter`, `RowDigest` and `sha256_hex` are crate-private. The manifest, segment entry, archived partition and `RetentionHooks` are `non_exhaustive`. | Builds |
| C9 | (Codex) The trait lets a timed-out `put` land later. A late manifest at the one fixed key could replace the manifest that the drop checked. | Manifest keys name their content. A reuse hint and a drop record replace the fixed key. | `exports_of_different_rows_have_different_manifest_keys`, `find_dropped_reads_the_drop_record` |
| C10 | (Codex) The directory backend synced only the deepest new directory. | It syncs the parent of each directory that the write creates. | Unit tests of the backend |
| C11 | (Codex) A sweep with no archiver could read "no marker" just before the first exporter wrote it, then drop. | Every applying sweep takes the export lock. An exporter takes it exclusive and writes the marker under it. A sweep with no archiver takes it shared and reads the marker under it. | `a_sweep_without_an_archiver_drops_nothing_while_an_exporter_holds_the_lock`, `an_exporter_writes_no_marker_while_a_sweep_without_an_archiver_holds_the_lock` |
| C12 | (Codex) A least-privilege runtime role had no grant on the marker table. | The preflight probe requires `SELECT` and `INSERT` on it. The upgrade guide and `docs/archival.md` name the grant. | `the_partition_export_marker_is_covered_by_the_privilege_probe` |
| C13 | (Codex) The drop record was written after the drop. A failed write left a dropped partition with no record. | The record goes before each drop attempt. A failed write keeps the partition. Two verified exports of one sealed, orphaned partition differ only in ciphertext, so a late record write still names the same plaintext. | `a_failed_drop_record_keeps_the_partition` |
| C14 | (Codex) `harvest partition status` reported a marked shard's partitions as droppable, but a sweep with no archiver drops nothing there. | The read-only pass reads the marker and reports `export required`. | `a_sweep_without_an_archiver_drops_nothing_once_a_shard_exported` |
| C15 | (Codex) A cancelled pass can keep the session lock on a pooled connection. | The cost is liveness, not data: other sweeps report busy. The retention runtime never cancels mid-shard, and it closes the connection when the fence is lost. The public sweep and maintain calls document that a cancelled pass must close its connection. | Docs |

The row checksum detects a change. It is not a security boundary, the same
as the append-only guard.

---

## 1. Trait

```rust
pub trait PartitionArchiver: Send + Sync + 'static {
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()>;
    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>>;
}
```

`get` returns `Ok(None)` for a missing key. The builder takes one with
`HarvestBuilder::partition_archiver`.

## 2. Layout

```
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001-<sha256:16>.jsonl
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest-<sha256:16>.json
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/latest.json
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/dropped.json
```

A segment or manifest key names its content, so a late upload writes its
own key. `latest.json` is a reuse hint. `dropped.json` names the checked
manifest. The sweep writes it before each drop attempt, so every dropped
partition has one.

`<lower>` is `min` for a `MINVALUE` bound. Bounds use `%Y%m%dT%H%M%SZ`. A
bound with a fraction, such as the legacy cutover, keeps its microseconds.

A segment holds one row per line, in `(id, cohort)` order. A segment closes
at 10,000 rows or 8 MiB. The manifest holds these fields:

- the format version, the shard, and the partition name and bounds;
- the row count and the row checksum;
- a SHA-256 of all segment bytes in order;
- one entry for each segment.

## 3. Sweep order

At the start of an applying pass with an archiver, the sweep writes the
marker row and tries the export lock. For each partition that the gate
marks as droppable:

1. **Reuse.** Read back an earlier export at the same key. Use it when it
   checks clean.
2. **Export.** Otherwise read the rows in pages. Upload each segment.
   Upload the manifest last. This counts against the pass budget.
3. **Verify.** Read back each segment and the manifest. Compare the bytes,
   the hashes, the row count and the row checksum.
4. **Drop.** Take `SHARE`, run the ownership check again, and compute the
   row checksum. Drop only when the row count and the checksum match the
   manifest. A reused export that fails here gets one new export.

A failure at any step blocks the drop and records the reason. Each
backend call has the `archival_timeout_secs` limit.
`SweepOutcome::exported` lists the manifest key of each dropped
partition.

### 3.1 The marker

`harvest_partition_export` holds one row at most. Deleting the row ends the
requirement. A binary older than this change ignores the row, so the
archiver goes to every process before an operator relies on it.

## 4. Read-back

`partition_archive::read_back(archiver, manifest_key)` reads the
manifest and each segment. It checks every hash and returns an
`ArchivedPartition`. `ArchivedPartition::history(exec_id)` returns the
events of one run in `event_id` order. Payload fields stay as stored, so
codec envelopes stay ciphertext.

## 5. Delta encoding — measured, declined

`autumn-harvest-agent/tests/history_delta_measure.rs` builds a 40-turn
agent loop from the real `ModelTurnRequest` and `ModelTurn` types. Each
model turn records the whole transcript, so history grows with the square
of the turn count. `docs/rnd/2026-10-11-agent-history-delta-encoding.md`
records the numbers:

| Form | Vocabulary text | Random text |
|---|---:|---:|
| Plain, delta | 11.3% | 11.4% |
| Plain, gzip | 12.6% | 47.9% |
| AES-GCM codec | 134.2% | 134.2% |
| AES-GCM, gzip | 101.0% | 101.0% |

The decision is to decline delta encoding in the event log:

- The codec encrypts each payload field with a fresh nonce. Two equal
  context windows give different ciphertext, so a delta over stored bytes
  saves nothing. A delta must run before the codec, on every write, read
  and replay.
- The repeat comes from the agent layer, which sends the whole transcript
  in each activity input. The agent layer can record only the new messages
  of each turn, and rebuild the window from history it already has. That
  gets the same reduction with no engine change, and it works under the
  codec.
- gzip matches the delta only on text that compresses well. Its 32 KiB
  window misses repeats in a long transcript. A compressing archive backend
  needs a long window, and it does not help under the codec.
