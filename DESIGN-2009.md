# Design — Issue #2009: export aged partitions before the drop

Retention drops an aged `harvest_events` partition. This change exports
the partition to object storage first. The sweep then reads the export
back, checks it, and drops the partition only when the check passes.

The issue also asks about delta encoding of agent context windows. §5
gives the measurement and the decision.

**No migration. No new `WorkflowEvent` variant. No new route.** The two
sanctioned `event_data` writers do not change. The export reads rows and
the drop is DDL, so the append-only trigger does not apply.

---

## 0. Planning record

### 0.1 Brainstorm — where does the export go?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add a partition method to `HistoryArchiver`, with a default body. | Rejected. A default body lets an old archiver "succeed" with no upload. The unit is also different: one run, or many runs. |
| B2 | A new `PartitionArchiver` trait with two calls: `put(key, bytes)` and `get(key)`. Core owns the layout and the checks. | **Adopted.** The trait is a plain object store. The S3 and GCS backends of #1983 map to it in a few lines. |
| B3 | Upload the partition as one object. | Rejected. The legacy partition can hold the whole pre-conversion history. One object means one large buffer. |
| B4 | Upload numbered segments, then a manifest that lists each segment with its row count and SHA-256. | **Adopted.** Memory is bounded by one segment. The manifest goes last, so a manifest always points at complete segments. |
| B5 | Encode each row as `to_jsonb(e)::text`. | **Adopted.** A new column is exported by default, like the append-only trigger guards a new column by default. |
| B6 | Hold the partition lock during the upload. | Rejected. The lock would stay open across network I/O. Retention already releases its connection before an archive call. |
| B7 | Upload without a lock. Then, under the `SHARE` lock of the drop, hash the partition again and compare with the manifest. | **Adopted.** `SHARE` blocks every row change. A match proves that the export is the content the drop removes. |
| B8 | Read each segment back after the upload and compare its hash. | **Adopted.** This is the "verified" step of the issue. |
| B9 | A `DirectoryPartitionArchiver` in core. | **Adopted.** It is a working backend until #1983 merges, and the integration test uses it. |
| B10 | A read-back API that checks every hash and returns typed rows. | **Adopted.** It also groups rows into a run history for replay debugging. |
| B11 | Delta-encode context windows in the event log. | Measured, then declined. See §5. |

### 0.2 Reverse brainstorm — how can the export lose data?

| # | How to lose data | Mitigation |
|---|------------------|------------|
| R1 | Drop the partition when the upload failed. | An upload error, a read-back error or a timeout blocks the drop. The reason goes into `SweepOutcome::blocked`. |
| R2 | A row changes after the upload and before the drop. The codec rotation sweep (exception #3) can rewrite a row in a closed partition. | The drop hashes the partition again under `SHARE`. A different hash blocks the drop. The next tick exports again. |
| R3 | A backend returns other bytes than it stored. | The read-back compares each segment hash. The drop compares the whole-partition hash. |
| R4 | A later partition with the same name overwrites an old export. The legacy partition name comes back after a disable and an enable. | The key holds the shard and the cohort bounds. A new enable gets a new conversion instant, so the bounds differ. |
| R5 | The straggler `DELETE` removes orphan rows that no export holds. | `try_build` refuses `straggler_grace_secs` with a partition archiver. The sweep also skips straggler deletes when it exports. |
| R6 | A shard ID is missing from the key. Two shards then share one key. | The key starts with `shard-<id>`. |
| R7 | The export hash depends on the session time zone. `to_jsonb` formats `timestamptz` in the session zone. | Each read sets `TimeZone` to `UTC` for its transaction. |
| R8 | A key escapes the archive root of the directory backend. | The backend accepts only `[A-Za-z0-9._-]` path parts and refuses `.` and `..`. |
| R9 | An export of a large partition makes the liveness scanner flag the loop as stale. | The sweep calls the progress hook after each segment. |
| R10 | A dry run or `harvest partition status` uploads data. | Only an applying sweep exports. `evaluate` never does. |
| R11 | An offloaded payload is gone when the partition is exported. Retention collects the run's blobs when it deletes the run. | Documented. The per-run `HistoryArchiver` inflates offloaded payloads. Use it when a run needs its offloaded payloads in cold storage. |
| R12 | An erasure request arrives after the export. | Documented. Erasure does not reach an archive object. `docs/archival.md` already states this for per-run archives. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | A partition is dropped only when no live run owns a row. Its rows are orphans. Retention already sent each run to `HistoryArchiver` when one is set. `sha2` is a core dependency. #1983 (PR #2063) is open and adds S3 and GCS backends. |
| Red | Operators fear a silent loss more than a slow sweep. A blocked partition with a clear reason feels safe. |
| Black | The export reads the partition twice and downloads it once. A large partition costs time on each tick until it drops. The hash scan under `SHARE` holds the vacuum horizon for its length. Each scan page has a statement timeout, so the scan cannot run without a limit. |
| Yellow | Cold history costs much less in object storage. The archive keeps ciphertext, so the codec keys still protect it. No append path changes. |
| Green | Keep the trait minimal, so any blob store fits. A compressing backend can compress in `put` and expand in `get`. The hashes still match, because they cover the bytes core gives and gets back. |
| Blue | Red phase: unit tests for keys, manifest, segments and the directory backend; DB tests for export, verify, drop and read-back; failure tests. Green phase: the module, the sweep hook, the builder and the runtime. Refactor phase: docs, gates, review. |

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
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/segment-000001.jsonl
harvest-partitions/shard-<id>/<partition>/<lower>_<upper>/manifest.json
```

`<lower>` is `min` for a `MINVALUE` bound. Bounds use `%Y%m%dT%H%M%SZ`.

A segment holds one row per line, in `id` order. A segment closes at
10,000 rows or 8 MiB. The manifest holds the format version, the shard,
the partition name and bounds, the row count, a SHA-256 of all segment
bytes in order, and one entry for each segment.

## 3. Sweep order

For each partition that the gate marks as droppable, when an archiver is
set:

1. **Export.** Read the rows in pages. Upload each segment. Upload the
   manifest last.
2. **Verify.** Read back each segment and the manifest. Compare the
   bytes and the hashes.
3. **Drop.** Take `SHARE`, run the ownership check again, and hash the
   partition again. Drop only when the row count and the hash match the
   manifest.

A failure at any step blocks the drop and records the reason. Each
backend call has the `archival_timeout_secs` limit.
`SweepOutcome::exported` lists the manifest key of each dropped
partition.

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
