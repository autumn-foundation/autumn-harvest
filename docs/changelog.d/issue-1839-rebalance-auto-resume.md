## Sharding — Automatic resume after a stalled cutover; duplicate-append detection (issue #1839)

**Behavior change.** A worker now settles a shard migration that stalled
after its cutover. Before, only a manual `harvest shard rebalance-resume`
did this. See [`sharding.md`](../sharding.md#automatic-resume-after-a-stalled-cutover-issue-1839).

**Rebalance-resume scanner.** A crash between the cutover and the target's
activation leaves the run claimable on neither shard.

- Each worker runs one scanner per assigned shard when a `ShardedDbPool` is
  configured. New module `rebalance_resume`.
- A pass calls the new `shard_rebalance::resume_stalled_cutovers`. It settles
  each `COMMITTED` record older than `ScannerConfig::rebalance_stall_after`
  (default 30 s). Passes run every `ScannerConfig::rebalance_resume_interval`
  (default 5 s).
- Records before the cutover are not touched. There the source is still
  claimable, and a cutover is the operator's decision.
- One `UPDATE ... FOR UPDATE SKIP LOCKED` claims each record and sets its
  `updated_at`. Many replicas can run the pass. Each settlement writes one
  audit row, and a down target is retried one grace period apart. No scanner
  lease is used.
- Audit: new operation `shard.rebalance.auto_resume`
  (`audit::OP_SHARD_REBALANCE_AUTO_RESUME`), actor `system`, source `cli`,
  route `background.rebalance_resume_scanner <from> -> <to>`.
- Liveness: new `scanner_health::Scanner::RebalanceResume`, label
  `rebalance_resume`. GET /admin/config reports
  `rebalance_resume_interval_ms` and `rebalance_stall_after_ms`.
- `resume_incomplete_migrations` and the new pass share one per-record step
  loop, `drive_migration`. The CLI's behavior does not change.

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
`concurrent_scanner_passes_settle_a_stalled_cutover_once`.
`backup_verify_tests::detects_a_duplicate_event_id_on_the_partitioned_layout`.
