# Design: issue #2258, the owner-local disk cache

Stage 2 of epic #2247. A decision owner keeps the histories of its runs in a
local file. A cold decision then reads the file and the Postgres delta, not
the full Postgres history. [ADR 0012](docs/adr/0012-owner-disk-cache.md)
records the decision.

**No migration. No new `WorkflowEvent` variant. Off by default.** The cache
needs the `owner-disk-cache` feature, a directory and owner mode. The stage 2
gate (#2257) is still undecided, so the cache ships as an opt-in.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The memory cache (`cache.rs`) holds decoded events and `next_event_id`.
  A warm hit loads only the delta from `next_event_id`. Event ids start at
  0 and have no gap, so event `i` is at index `i`.
- `store_cache_entry` runs after each commit. `Some(true)` is a suspension.
  `Some(false)` is a terminal run.
- A tenure ends at five sites: a fence (`drop_fenced_tenure`), a scan drop
  and a new acquisition (`evict_owner_keys`), a release and a shutdown
  (`close_workflow_cache`).
- A foreign state change sends an evict wake to the owner (#2253). The
  owner drops the memory entry. A lost `NOTIFY` loses that eviction.
- Erasure (`erase.rs`) runs on terminal runs only, from any node, with no
  delay after the terminal write.
- Codec rotation keeps the decoded plaintext. `KeyEpoch::generation`
  counts each change of the key set or the active key.
- A DR bump expires every lease, so each owner changes epoch.
- The core crate cannot depend on `autumn-harvest-sqlite`. The SQLite
  crate depends on the core crate.
- The gate bench: a local SQLite read is 2.2 to 3.3 times faster than a
  Postgres read. 100k live runs take 6.6 s to warm on 8 Postgres
  connections.

### 0.2 Brainstorm: what can the disk cache hold, and when?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Store the stored rows, codec ciphertext included. | Rejected. Retired-key ciphertext stays on disk after a rotation. |
| B2 | Store the decoded events, sealed under a node key. | **Adopted.** No codec ciphertext reaches the disk. |
| B3 | Use SQLCipher for the whole file. | Rejected. It is a new C dependency. |
| B4 | Seal each row with AES-256-GCM. The associated data binds the partition, run and sequence. | **Adopted.** A changed, moved or swapped row fails to open. |
| B5 | Write each suspension through to disk, as an append of new events. | **Adopted.** One small write per decision. An eviction from memory costs nothing. |
| B6 | Write an entry to disk only when memory evicts it. | Rejected. Each thrash writes the full history. |
| B7 | Take the disk entry at the start of a decision, as memory does. | Rejected. A disk entry holds no live future. It is a prefix of an append-only log, so it stays valid. |
| B8 | Check the last cached event against Postgres before a disk hit. | **Adopted.** A shorter history after a failover, or a rewrite, fails the check. |
| B9 | Keep the stamp only in memory. | Rejected. The issue asks for a stamp per file. The sealed stamp also detects a damaged file. |
| B10 | Put the cache in `autumn-harvest-sqlite`. | Rejected. The worker cannot call it. |
| B11 | A `StorageBackend` trait. | Rejected by the issue. |
| B12 | Purge at each terminal commit, each evict wake and each tenure end. | **Adopted.** |
| B13 | A reconcile pass that purges each cached run that is not `RUNNING`. | **Adopted.** It is the backstop for a lost evict wake. |
| B14 | Wipe the directory at open and at shutdown. | **Adopted.** The issue asks for the open wipe. |
| B15 | An exclusive `flock` on a sidecar file, as in `autumn-harvest-sqlite/src/lock.rs`. | **Adopted.** Two processes cannot wipe each other. |
| B16 | A counter `harvest.workflow.disk_cache{workflow, queue, outcome}`. | **Adopted.** An operator sees the hit rate and each purge. |
| B17 | Rebuild the file for all runs at takeover. | Rejected. The issue asks for a lazy rebuild. |

### 0.3 Reverse brainstorm: how can the disk cache cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | Keep erased PII on disk. | Purge at the terminal commit and at the evict wake. Reconcile purges a missed run. `secure_delete` zeroes freed pages. |
| H2 | Crash between the terminal commit and the purge. | The key dies with the process. The next open wipes the directory. |
| H3 | Keep retired-key ciphertext. | The cache stores decoded events only. A key change purges each file. |
| H4 | Keep plaintext on disk. | Each row is sealed. The journal and temp files stay in memory. |
| H5 | Replay a damaged file. | The seal fails. The decision purges the entry and replays cold from Postgres. |
| H6 | Replay a stale file after a takeover. | Each tenure end drops the file. The stamp holds the epoch. A new epoch wipes the file. |
| H7 | Replay a stale entry after a failover lost the tail. | The anchor check compares the last cached event with Postgres. |
| H8 | A late write lands after a purge. | Each write is awaited and holds the partition lock. A write checks the epoch under that lock. |
| H9 | Two processes share one directory. | The sidecar lock refuses the second open. |
| H10 | A nonce repeats. | A random prefix and a counter build each nonce. The key lives for one process. |
| H11 | Disk I/O blocks the runtime. | Reads and writes run in `spawn_blocking`. |
| H12 | A disk error fails a run. | Every disk error reads as a miss. The decision loads from Postgres. |
| H13 | A disk hit counts as a memory hit. | The memory counter records a miss. The disk counter records the hit. |
| H14 | A disk hit keeps a resident future. | A disk hit has no future. Its resident outcome is `cold`. |
| H15 | Reconcile loads the database. | It runs every 30 s, in chunks of 1,000 ids. |
| H16 | Turn the cache on by default. | It needs a feature, a directory and owner mode. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue asks for four red tests and a
  warm-up gain over the gate baseline.
- **Red (gut).** A disk copy of PII feels risky. The node key, the wipe and
  the purge points answer that, and each one has a test.
- **Black (risks).** Section 0.3. The main risks are erased PII that
  survives and a stale entry that replays. H1, H2, H6 and H7 answer them.
- **Yellow (value).** Runs past the memory cap and runs after a takeover
  read a local file. The bench gives the gain.
- **Green (ideas).** Keep the file across a restart with a sealed key in a
  KMS. Deferred: the issue asks for a disposable cache.
- **Blue (process).** Red tests first: the module tests, then the worker
  tests on Postgres. Then the module and the worker hooks until green. Then
  the bench, the docs and the gates. Then a review from several angles.

## 1. Change

1. `owner_disk_cache.rs`: the directory lock, the wipe, the node key, the
   sealed rows, the stamp, load, store, purge, drop and reconcile.
2. `store.rs`: `load_event_inflated` reads one event for the anchor check.
3. `worker.rs`: the disk read on a memory miss and the write after a
   suspension. Also the purge points, the reconcile in the owner scan and
   the drop at shutdown.
4. `WorkerConfig::with_owner_disk_cache`.
5. The counter `harvest.workflow.disk_cache` and its dashboard panel.
6. `examples/history-load-gate`: the warm-up with the disk cache.
7. Docs: ADR 0012, `docs/sticky-routing.md`, the telemetry table, the
   record doc and a changelog fragment.

## 2. Tests

| Test | Proves |
|------|--------|
| `an_erased_run_leaves_no_local_bytes` | After the terminal commit and an erasure, the directory holds no byte of the run. |
| `a_killed_owner_leaves_no_readable_bytes` | A `SIGKILL` between the commit and the purge leaves only sealed rows. The next open wipes them. |
| `a_retired_codec_key_leaves_no_ciphertext` | The disk never holds codec ciphertext. A key change purges each file. |
| `a_corrupt_file_replays_cold` | A damaged file purges the entry. The run completes with no non-determinism block. |
| module unit tests | Seal, stamp, wipe, lock, epoch, anchor and reconcile rules. |

## 3. Review findings

Four review agents read the change: correctness, security, tests and docs.
The fixes:

- The default build did not compile. One `cfg` covered a `let` but not the
  `if let` after it.
- An evict wake could lose a race with a store in flight, which then wrote
  a terminal run back. A purge from an evict wake now holds back stores of
  that run for 30 s.
- A run whose purge was lost stayed on disk until the reconcile pass. An
  erasure now sends the owner an evict wake.
- An owner cut off from Postgres kept the files of expired tenures. The
  owner scan now deletes them when it cannot reach the database.
- A late close of an old partition could delete the file of a new one. A
  close now locks the current partition first, and a closed one deletes
  nothing.
- A failed delete left a partition half closed. It now closes the
  partition, which deletes the whole file.
- One worker's shutdown deleted the files of every worker on the registry.
  A worker now deletes only the files that it wrote.
- The open set the mode of any directory to `0700`. It now creates a new
  directory with `0700` and refuses an existing one that other users can
  reach.
- The tests raced the purge after a commit. They now wait for it. New
  tests cover the evict hold, the erasure wake, a stale partition close,
  the directory check and a codec rotation in a real worker.
- The docs said that a lost tenure deletes every file. They now say what
  the code does.

Accepted as is:

- A core dump or swap can hold the key. ADR 0012 says so.
- `ghash` 0.5 does not clear its key on drop. That predates this change.
- The restart warm-up is not faster: the open wipes the cache. The gain is
  on the next decision of each run past the memory cache. Section 4 gives
  the numbers.

## 4. Out of scope

- A cache that survives a restart.
- The go or no-go of the stage 2 gate.
- The two cheaper changes of the gate record: one history read on the
  cold path, and a memory bound on `WorkflowCache`.
