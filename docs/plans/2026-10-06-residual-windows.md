# Close the documented residual windows (issue #1839)

**Status**: implemented. Written before the code. Section 6 records the
review changes.

**Scope**: three items from issue #1839.

1. A scanner settles a shard rebalance that stalls after its cutover.
2. A recorded decision for the partitioned-events duplicate-append window.
3. The sharding limits stay documented. No code change.

---

## 1. Brainstorming

Ideas for item 1 (the run is claimable on neither shard):

| # | Idea |
|---|---|
| B1 | A worker loop calls `resume_incomplete_migrations` for every phase. |
| B2 | A worker loop settles only `COMMITTED` rows that are older than a grace period. |
| B3 | The cutover transaction also schedules a timer that activates the target. |
| B4 | Activate the target before the seal (two shards claimable for a moment). |
| B5 | A two-phase commit across the source and target databases. |
| B6 | Alert on a stalled row, and keep the manual `rebalance-resume`. |

Ideas for item 2 (two in-flight appends of one `event_id` in two cohorts):

| # | Idea |
|---|---|
| D1 | An advisory lock per execution on every append. |
| D2 | The advisory lock only near a cohort boundary. |
| D3 | A `backup verify` probe that finds duplicate `(workflow_exec_id, event_id)` pairs. |
| D4 | Accept the window. Keep the documentation only. |

**Choice.** B2 and D3.

- B1 drives pre-cutover rows too. Before the cutover the source is
  authoritative and claimable, so there is no liveness gap to close. An
  automatic cutover of a rebalance that an operator stopped is a policy
  decision. The scanner must not make it.
- B3 needs a cross-database timer. B4 makes the run live on two shards. B5 is
  out of scope in `docs/sharding.md`. B6 does not close the gap.
- D1 and D2 put a lock on the append hot path. That lock can deadlock against
  the advisory locks of the admission and mutex paths. D2 also needs a clock
  window, and clock skew makes it unsafe. D3 adds no cost to the hot path.

## 2. Reverse brainstorming

"How can the scanner cause harm?" Each answer names a guard.

| Harm | Guard |
|---|---|
| It discards the staged copy of a live `harvest shard rebalance` run. | It reads `COMMITTED` rows only. Before the cutover it does nothing. |
| It races a live CLI between cutover and activation. | A grace period (`rebalance_stall_after`, 30 s, floor 5 s). Activation is idempotent if the race still occurs. |
| Many replicas audit one row many times. | One `UPDATE ... FOR UPDATE SKIP LOCKED` claims one row and moves `updated_at`. Other replicas skip it for one grace period. |
| A target outage makes it spin. | The claim moves `updated_at`, so a retry waits one grace period. A failed step counts in `attempts`. |
| It blocks shutdown on a full pool. | The pass is selected against the cancel token. An open transaction marks the connection broken, so the pool discards it. |
| It hides a stall from operators. | It writes a `shard.rebalance.auto_resume` audit row and a `warn` log. |
| It wedges and nobody sees it. | It registers as scanner `rebalance_resume` for `scanner_liveness`. |

"How can the duplicate check mislead?"

| Harm | Guard |
|---|---|
| It reports a pass on a layout it did not check. | It runs on the partitioned layout only. The flat layout has a unique constraint. An unknown layout is a soft error, so the shard is `undetermined`. |
| It hides a broken history. | The class is `incoherent`. The drill fails. |
| It loads a live shard. | `backup verify` runs on a restored copy, in a read-only session. |

## 3. Six thinking hats

- **White (facts).** The gap is the time between the `COMMITTED` and `DONE`
  phases. `activate_target` is idempotent. `resume_incomplete_migrations`
  already drives that step. The audit table accepts the sources `api`, `cli`
  and `ui` only.
- **Red (feelings).** Operators do not want a manual step after a crash. They
  also do not trust a background task that moves runs on its own.
- **Black (risks).** See section 2. One more: the audit `source` must be one of
  the three values. The load-shed sampler sets the same precedent: `actor`
  `system` and a `background.*` route mark the row as automatic.
- **Yellow (benefits).** The liveness gap closes within one grace period plus
  one interval. No migration and no new `WorkflowEvent` variant.
- **Green (alternatives).** A row claim replaces a scanner lease. The claim
  also fences the audit row.
- **Blue (process).** Red, green, refactor. Each test names the issue.

## 4. Design

### Item 1: the rebalance-resume scanner

- `shard_rebalance::resume_stalled_cutovers(pool, shard, stall_after, limit)`
  claims stalled `COMMITTED` rows on `shard` and settles them. It shares the
  per-record step loop with `resume_incomplete_migrations`.
- Each settled row writes one audit row: operation
  `shard.rebalance.auto_resume`, actor `system`, source `cli`, route
  `background.rebalance_resume_scanner <from> -> <to>`.
- `rebalance_resume::spawn_rebalance_resume_scanner` runs the pass on a fixed
  interval for one shard.
- The worker starts one scanner per assigned shard when a `ShardedDbPool` is
  configured.
- `ScannerConfig` gets `rebalance_resume_interval` (5 s) and
  `rebalance_stall_after` (30 s).

### Item 2: decision

Detection only. See
[ADR 0004](../adr/0004-partitioned-duplicate-append-detection.md). New
`backup verify` class `duplicate_event_id`, severity `incoherent`.

### Item 3

No change. The limits stay in `docs/sharding.md`. Child placement stays a
separate decision.

## 5. Tests (red first)

| Test | Proves |
|---|---|
| `the_scanner_resumes_a_rebalance_interrupted_after_cutover_within_its_interval` | Issue AC 1. |
| `a_worker_resumes_a_stalled_cutover_without_an_operator` | The worker starts the scanner. |
| `the_scanner_leaves_a_fresh_cutover_to_its_operator` | The grace period. |
| `the_scanner_never_drives_a_pre_cutover_migration` | Pre-cutover rows stay with the operator. |
| `concurrent_scanner_passes_settle_a_stalled_cutover_once` | The claim fences the audit row. |
| `detects_a_duplicate_event_id_on_the_partitioned_layout` | Item 2 detection. |

## 6. Review changes

A review from three angles (correctness, tests, operations) found no defect
of high severity in the claim or the refactor. These changes came from it:

- A pass claims one record per statement. A batch claim could expire while
  earlier records of the batch still ran.
- `rebalance_stall_after` has a 5 s floor. A zero grace let two passes claim
  one record.
- `activate_target` returns whether it settled the record. A record that
  another driver settled first gets no outcome and no audit row.
- A replica claims only records whose target is in its pool.
- The claim and the activation check the DR fence when it is enabled.
- `rebalance_resume_enabled` turns the scanner off. The sleep uses the
  scanner jitter.
- Deferred: a metric for a record that never settles. Today the record's
  `attempts` and `last_error`, and an error log per try, show it.

