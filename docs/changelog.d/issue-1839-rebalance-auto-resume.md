## Sharding — Automatic resume after a stalled cutover; duplicate-append detection (issue #1839)

**Behavior change.** A worker now settles a shard migration that stalled
after its cutover. Before, only a manual `harvest shard rebalance-resume`
did this. Set `ScannerConfig::rebalance_resume_enabled = false` to keep the
old behavior. See [`sharding.md`](../sharding.md#automatic-resume-after-a-stalled-cutover-issue-1839).

**Source-visible change.** `ScannerConfig` has three new public fields:
`rebalance_resume_enabled`, `rebalance_resume_interval` and
`rebalance_stall_after`. Code that builds it as a struct literal without
`..ScannerConfig::default()` no longer compiles. `activate_target` now
returns `bool`: `true` when the call settled the record.

**Rebalance-resume scanner.** A crash between the cutover and the target's
activation leaves the run claimable on neither shard.

- Each worker runs one scanner per assigned shard when a `ShardedDbPool` is
  configured. New module `rebalance_resume`.
- A pass calls the new `shard_rebalance::resume_stalled_cutovers`. It settles
  each `COMMITTED` record older than `ScannerConfig::rebalance_stall_after`
  (default 30 s, floor 5 s). Passes run every
  `ScannerConfig::rebalance_resume_interval` (default 5 s), with the scanner
  jitter.
- Records before the cutover are not touched. There the source is still
  claimable, and a cutover is the operator's decision.
- One `UPDATE ... FOR UPDATE SKIP LOCKED` claims one record and sets its
  `updated_at`. Many replicas can run the pass. One settlement writes one
  audit row, and a down target is retried one grace period apart. No scanner
  lease is used.
- A replica claims only records whose target shard is in its pool. A record
  that another driver settled first gets no outcome and no audit row.
- When the DR fence is enabled, the claim and the activation check it.
- Audit: new operation `shard.rebalance.auto_resume`
  (`audit::OP_SHARD_REBALANCE_AUTO_RESUME`), actor `system`, source `cli`,
  route `background.rebalance_resume_scanner <from> -> <to>`. The table
  accepts only `api`, `cli` and `ui`, so `cli` is the nearest value. The
  operator's `shard.rebalance.migrate` is now `audit::OP_SHARD_REBALANCE_MIGRATE`.
  Both are in `AUDITED_OPERATIONS`.
- Liveness: new `scanner_health::Scanner::RebalanceResume`, label
  `rebalance_resume`. GET /admin/config reports `rebalance_resume_enabled`,
  `rebalance_resume_interval_ms` and `rebalance_stall_after_ms`.
- `resume_incomplete_migrations` and the new pass share one per-record step
  loop, `drive_migration`. One CLI change: `rebalance-resume` no longer
  reports `migrated` or writes an audit row for a record that another driver
  settled first.

**Partitioned-events duplicate append: detection only.** Two in-flight
appends of one `event_id` can still commit in two cohorts. ADR 0004 records
the decision not to lock the append hot path.

- New `backup_verify::FindingClass::DuplicateEventId`
  (`duplicate_event_id`), severity `incoherent`.
- The probe runs on the partitioned layout only. An unknown layout is a soft
  error, so the shard is `undetermined`.

**Sharding limits.** No change. They stay in `docs/sharding.md`. Child
placement stays a separate decision.

**Invariants.** No migration. No new `WorkflowEvent` variant. No write to
`harvest_events`.

**Tests.** `shard_rebalance_db_tests`:
`the_scanner_resumes_a_rebalance_interrupted_after_cutover_within_its_interval`,
`a_worker_resumes_a_stalled_cutover_without_an_operator`,
`the_scanner_leaves_a_fresh_cutover_to_its_operator`,
`the_scanner_never_drives_a_pre_cutover_migration`,
`concurrent_scanner_passes_settle_a_stalled_cutover_once`,
`a_worker_with_the_scanner_off_leaves_a_stalled_cutover_alone`,
`a_record_settled_by_an_operator_is_not_reported_by_the_scanner`,
`a_down_target_is_retried_one_grace_period_later`,
`a_replica_without_the_target_pool_does_not_claim_the_record`.
`backup_verify_tests::detects_a_duplicate_event_id_on_the_partitioned_layout`.
