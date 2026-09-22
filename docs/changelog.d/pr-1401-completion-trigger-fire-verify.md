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
  the fire's `fired_at` by more than the cross-shard clock-skew tolerance
  (`max_skew_secs`). The relay can only start the target at or after
  `fired_at`, so this is proof, not inference.
- `completion_trigger_fire_unproven` (`undetermined`, exit 2): the target is
  absent, no `harvest_execution_summaries` row proves retention, and the
  target shard's restore point does not rule it out either. The same
  evidentiary gap `retention_unproven` names, resolved here from the fire's
  own `fired_at` as a fallback rather than requiring a summary.

**Residual limitation** (Codex follow-up x2). A target that completed and
was retention-collected can itself have been its shard's newest event.
With no other shard traffic since, deleting it pulls the visible restore
point back to before `fired_at`, misreading a coherent restore as
`completion_trigger_fire_lost`. `harvest_execution_summaries` closes this
when checked — but only within the summary's own `--summary-age` horizon.
`harvest_completion_trigger_fires` has no cleanup path, so an old fire
outlives its target's summary once that summary is GC'd
(`retention::gc_execution_summaries`). This is not just a "summaries
disabled" gap: it recurs for aged fires under any finite summary horizon.
Closing it unconditionally needs a genuine durable restore-point marker —
exactly the durable-marker work issue #1401 chose not to require.
Documented in `docs/runbooks/backup-restore.md` §4.2(d) and in
`absence_is_decisive_loss`'s doc comment. Tying fires-table retention to
the summary horizon is tracked as a possible follow-up rather than
built here — it is a new engine-retention capability, not a
`backup_verify` change.

A same-shard fire is never adjudicated: `evaluate_triggers_for_execution`'s
inline path inserts the fires row and starts the target in one transaction,
so it is atomic and immune by construction (scope: cross-shard only, per the
issue).

`route_trigger_fires`'s `uncertain` sample list (below) is bounded the same
way as `TriggerFireBuckets`'s (Codex follow-up x3): capped at
`MAX_FINDING_SAMPLES`, with a separate exact `uncertain_count`. Added
`route_trigger_fires_bounds_uncertain_samples_but_keeps_an_exact_count`.

`resolve_trigger_fires`'s `UninspectedShardReference` diagnostic for a
`pending` fire whose target shard was not supplied gets the same
bounded-count treatment (Codex follow-up x4). No new test: this one is
only reachable through the full `verify_restore` path, and the fix is
the same push-bounded/count-separately shape already pinned by three
other tests on this PR.

`absence_is_decisive_loss` compared `fired_at` (the SOURCE shard's clock)
against `latest_event_at` (the TARGET shard's clock) with a raw `<`,
treating ordinary cross-shard clock skew as proof of loss (Codex follow-up
x5). It now requires the gap to exceed `max_skew_secs` — the same
operator-configured tolerance `restore_point_skew` already uses for
exactly this kind of cross-host timestamp comparison — before trusting it
as proof; a smaller gap stays `completion_trigger_fire_unproven`. Threaded
`max_skew_secs` through `resolve_trigger_fires`/`adjudicate_trigger_fires`/
`adjudicate_trigger_fire_chunk` from `VerifyOptions`. Added
`absence_is_decisive_loss_requires_the_gap_to_exceed_the_skew_tolerance`.

`scan_completion_trigger_fires` reported a false truncation when the
qualifying fire count landed exactly on a page-size multiple at the
`MAX_TRIGGER_FIRE_SCAN_PAGES` ceiling (Codex follow-up x6): every page
came back full, including the true last one, so `len < page` never fired
and the loop's fixed iteration budget ran out with no page left to
confirm exhaustion. A completely scanned shard would then read as
truncated, forcing `ProbeFailed`/exit 2 on a healthy restore. Fixed by
fetching one more page, outside the nominal budget, before declaring
truncation — an empty result confirms the scan was actually complete. No
dedicated test: reproducing the exact boundary needs
`MAX_TRIGGER_FIRE_SCAN_PAGES` (1,000) full pages of seeded fires, and the
fix is a straightforward extra confirming fetch, verified by inspection
plus the full existing suite.

That extra confirming page had its own off-by-one (Codex follow-up x7): it
checked only whether the page was EMPTY, not whether it was merely SHORT
(1..page rows). A qualifying count landing 1 through `probe_limit` rows
past the nominal ceiling is still fully scanned, but the code
unconditionally reported truncation for it anyway. Now checks
`len < page`, matching the loop's own exhaustion check.

**Payload-rejection resolution, closed the last race** (Codex follow-up
x8). The existence recheck still ran BEFORE claiming (deleting) the
outbox row, leaving a window: another attempt could deliver the target
and roll back only its own outbox delete in between our check and our
delete, and our check would never see that delivery. Reordered to claim
first, check last — once our delete commits, no other attempt can touch
the row again, so the existence check immediately after is the last
possible look. Also applies `stamp_outbox_relay_backoff` on any failure in
this path, matching the generic error arm; previously a failure here left
the outbox row's backoff untouched, so it retried at full poll cadence
regardless of the failure.

**Payload-rejection resolution, made fully atomic** (Codex follow-up x9).
The claim (delete), the cross-shard existence check, and the fires update
were three separate steps: if the check or the update failed after the
delete had already committed, the outbox row was permanently gone while
`fires.outcome` stayed `NULL` — a fire no scanner would ever revisit, and
the backoff call on that failure path was a no-op with nothing left to
stamp. All three now run inside one open transaction: the cross-shard
check is awaited in the middle of the transaction closure, and a failure
at any step rolls the delete back too, restoring the outbox row for the
next scan tick to retry. The backoff call is no longer a conditional
no-op — every failure in this path now leaves a real row to back off.

**Payload-rejection resolution, retention-aware** (Codex follow-up x10).
The existence check was LIVE-TABLE-only: retention can remove a
`harvest_workflow_executions` row within `--summary-age` seconds of
completion, as low as one second. A target genuinely delivered by an
earlier, differently-configured attempt and retention-collected before
this attempt's check ran would read as absent, and this path would mark
it `payload_too_large` — a wrong, permanent rejection of a real delivery.
Added `execution::execution_summary_exists_by_key` (new, not
testing-gated, since this runs in the live engine) checking
`harvest_execution_summaries` by business key, and the resolution now
treats a live row OR a summary as proof of delivery. Summaries are
opt-in, so this narrows the gap rather than closing it outright when
they are disabled — the same residual limitation `backup_verify`'s own
retention checks already carry and document.

**Confirmed delivered, precisely.** `outcome IS NULL` alone is set at
trigger-evaluation time, before any relay attempt — not proof of delivery.
The scan additionally requires the matching `harvest_completion_trigger_outbox`
row to be gone (the relay's actual delivery signal), so a fire still queued
or backed off behind a quota retry is never adjudicated. A permanently
rejected relay (oversized input payload) now resolves the fires row the same
way an admission-gate block already did
(`enforce_completion_triggers_outbox_with_codecs`, `payload_too_large`), so
it is likewise excluded rather than misread as a lost delivery.

A fire an older build rejected the same way, before this write existed
(Codex follow-up x11), was left `outcome IS NULL` with no outbox row —
the same shape the scan reads as a candidate for adjudication. Nothing
durable records which case applies, so verify cannot tell that fire from
a genuinely lost relay after the fact. This is the same category of
residual, unfixable-after-the-fact gap as the pre-migration
`target_shard`/`target_workflow_name` reconstruction below, scoped to
fires rejected before this deploy rather than fires written before the
migration. See `docs/runbooks/backup-restore.md` §4.2(d).

That resolution now also checks its own outbox delete affected a row
before marking the fire (Codex follow-up). A rolled-back claim followed
by a concurrent, successful delivery of the SAME row — plausible during a
rolling deployment with a differently configured payload cap — left the
delete matching zero rows while the update ran unconditionally, silently
overwriting a genuine successful delivery with a permanent, wrong
`payload_too_large` outcome. No dedicated race test: no existing harness
exercises this function's write path at all, and reproducing the race
deterministically would need new concurrency-test infrastructure
disproportionate to a one-line guard. Verified by inspection and the full
existing suite.

A deleted outbox row is still not proof no delivery happened (Codex
follow-up x2). An earlier attempt could have started the target and then
failed at its OWN outbox-delete step, leaving the row for a later,
differently-configured attempt to reject as oversized. The rejection path
now re-checks the target's any-state existence on the target shard FIRST,
mirroring the existence check `relay_gate_checked_start` itself already
runs before claiming a delivery. A target that already exists is treated
as delivered (drop the stale outbox row, leave `fires.outcome` `NULL`)
instead of rejected.

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

`collect_trigger_fires`'s missing-trigger-definition diagnostic (a fire
whose `trigger_id` no longer joins to a `harvest_completion_triggers` row)
now bounds its sample text the same way `fold_reference_events` already
bounds `undecodable_samples`: an exact count, plus a joined sample list
capped at `MAX_FINDING_SAMPLES` (Codex follow-up). It previously joined
every matching row into one unbounded string.

`matching_workflow_keys` now chunks its `names`/`ids` arrays at
`WORKFLOW_KEY_LOOKUP_CHUNK` (1,000) keys per query instead of sending a
whole shard's batch in one `UNNEST` call (Codex follow-up). A shard whose
confirmed-delivered fires all land in one batch could otherwise turn into
a single hundreds-of-megabytes request. The lookup stays set-based, just
spread across bounded queries.

Migration `20260920215812` also adds
`idx_harvest_completion_trigger_outbox_source_trigger` on
`harvest_completion_trigger_outbox(source_exec_id, trigger_id)` (Codex
follow-up). The scan's `NOT EXISTS` outbox-pending check
(`cf06f27`, above) had no supporting index — the table's only other index
starts with `target_shard` — so each of up to 1,000 scan pages ran that
anti-join against the whole outbox table.

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
`missing_trigger_definitions_are_reported_with_a_bounded_sample`,
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
