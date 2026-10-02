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
Issue #1676 later closed this for a finite summary horizon.
`retention::purge_expired_trigger_fires` deletes a fire at the same horizon
as its target's summary. The gap remains only when summaries are disabled.
`docs/runbooks/backup-restore.md` §4.2(d) and the `absence_is_decisive_loss`
doc comment describe the current state.
