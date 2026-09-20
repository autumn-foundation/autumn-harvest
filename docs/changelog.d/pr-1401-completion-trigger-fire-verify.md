## Fix — backup verify adjudicates cross-shard completion-trigger fires (issue #1401)

`backup_verify.rs` had zero references to `harvest_completion_trigger_fires`
or `harvest_completion_trigger_outbox`. `completion_trigger.rs`'s cross-shard
relay (`relay_gate_checked_start`) commits the source shard's outbox delete
and the target shard's start in two independent transactions; a restore that
skews the two shards can leave the source confirming a delivery the target
never received, with no scanner and, until now, no drill check to catch it.

**The fix.** Two new `FindingClass` variants:

- `completion_trigger_fire_lost` (`incoherent`, exit 1): the target execution
  is absent AND the target shard's restore point (its newest event) predates
  the fire's `fired_at`. The relay can only start the target at or after
  `fired_at`, so this is proof, not inference.
- `completion_trigger_fire_unproven` (`undetermined`, exit 2): the target is
  absent, no `harvest_execution_summaries` row proves retention, and the
  target shard's restore point does not rule it out either. The same
  evidentiary gap `retention_unproven` names, resolved here from the fire's
  own `fired_at` as a fallback rather than requiring a summary.

**Residual limitation, summary-free retention only** (Codex follow-up). A
target that completed and was retention-collected can itself have been its
shard's newest event. With no other shard traffic since, deleting it pulls
the visible restore point back to before `fired_at`, misreading a coherent
restore as `completion_trigger_fire_lost`. `harvest_execution_summaries`
closes this when enabled, since it is checked before the timestamp
heuristic runs. Closing it with summaries disabled needs a genuine durable
restore-point marker — exactly the durable-marker work issue #1401 chose
not to require. Documented in `docs/runbooks/backup-restore.md` §4.2(d)
and in `absence_is_decisive_loss`'s doc comment.

A same-shard fire is never adjudicated: `evaluate_triggers_for_execution`'s
inline path inserts the fires row and starts the target in one transaction,
so it is atomic and immune by construction (scope: cross-shard only, per the
issue).

**Confirmed delivered, precisely.** `outcome IS NULL` alone is set at
trigger-evaluation time, before any relay attempt — not proof of delivery.
The scan additionally requires the matching `harvest_completion_trigger_outbox`
row to be gone (the relay's actual delivery signal), so a fire still queued
or backed off behind a quota retry is never adjudicated. A permanently
rejected relay (oversized input payload) now resolves the fires row the same
way an admission-gate block already did
(`enforce_completion_triggers_outbox_with_codecs`, `payload_too_large`), so
it is likewise excluded rather than misread as a lost delivery.

**Migration `20260920215812`.** `harvest_completion_trigger_fires` gains two
nullable columns, `target_shard` and `target_workflow_name`, populated by
`completion_trigger.rs` at relay time. Reading the historical values a fire
actually used, instead of reconstructing them during verification, closes
two real gaps a Codex review found:

- The target shard was previously re-derived via
  `ShardRouter::pick_for_new_workflow` against the CURRENT shard topology,
  which cannot see a shard that was drained (readable, not writable) at the
  historical fire time and would compute a different pick than the live
  relay did.
- The target name was previously read via a join to
  `harvest_completion_triggers.target_workflow_name`, which
  `sync_completion_triggers` can update in place for an existing trigger id,
  so a fire predating that update would be checked against the wrong name.

A fire predating this migration (`target_shard`/`target_workflow_name` both
NULL) falls back to the old reconstruction, so both gaps remain a residual
limitation for pre-migration data only. `route_trigger_fires` narrows the
blast radius of the shard gap: a re-derived pick that lands on the fire's
own source shard is not trusted as proof of an atomic same-shard commit.
It is reported as `completion_trigger_fire_unproven` instead of being
dropped silently, so a historical drain can no longer hide a lost relay
behind a coincidental same-shard pick. This is documented in
`docs/runbooks/backup-restore.md` §4.2(d) and in the relevant doc comments.

**Design.** `route_trigger_fires` (target-shard routing, now preferring the
persisted shard) and `absence_is_decisive_loss` (the `fired_at`-vs-
restore-point classification) are pure functions, unit-tested without
Postgres, matching this module's existing pure/db split. The DB-gated scan
(`scan_completion_trigger_fires`) pages the fires table exhaustively on its
own primary key, mirroring the existing cross-shard event scan's "never
silently truncate" guarantee. `retention_summary_exists_by_key` checks
`harvest_execution_summaries` by business key (the only handle available —
the target execution id does not exist until start time) before the
timestamp heuristic runs, so proven retention always wins.

**No new `WorkflowEvent` variant, no engine-runtime behavior change beyond
the two additive write-path fixes above.** Read-only in `backup_verify.rs`,
reusing `execution::execution_exists_by_key` — the exact any-state
existence check the relay itself runs for its own idempotent retry.

**Tests.** New DB integration tests in
`autumn-harvest/tests/integration/backup_verify_tests.rs`:
`detects_a_lost_cross_shard_completion_trigger_fire`,
`an_absent_completion_trigger_target_past_the_fire_is_unproven`,
`a_delivered_completion_trigger_fire_stays_silent`,
`a_same_shard_completion_trigger_fire_is_not_probed`,
`a_reconstructed_pre_migration_same_shard_pick_is_unproven`,
`a_resolved_completion_trigger_fire_is_not_probed`,
`a_fire_still_pending_relay_is_not_probed`,
`an_absent_completion_trigger_target_with_a_summary_stays_silent`,
`a_payload_too_large_completion_trigger_fire_is_not_probed`,
`a_persisted_target_shard_from_a_historical_drain_is_honored`, and
`a_persisted_target_name_survives_a_later_trigger_update`. Plus pure unit
tests in `backup_verify.rs` for the routing and classification functions.

**Also fixed in passing.** `queue::requeue_workflow_task_for_quota_retry`
(issue #1391, PR #1658) did not compile: an unrelated concurrent fix
(issue #1389, PR #1659) removed `PendingRequeueChangeset`'s `next_run`
parameter in favor of a DB-computed `scheduled_at`, and the two PRs merged
without reconciling this call site. Updated it to the DB-computed
`clock_timestamp() + make_interval(...)` shape `requeue_for_retry` and
`requeue_workflow_task_nd_blocked` already use, and fixed its paired shape
test. This was blocking the crate from compiling at all.
