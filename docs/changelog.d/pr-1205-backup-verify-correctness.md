## Phase — backup-verify post-budget correctness fixes (issue #1205)

Four correctness findings from the Codex review of #1204's `e3b089f`, filed
after that PR's review-round budget was exhausted. All four were verified
against source before filing; none is a regression from #1204 — each is a
gap in the verifier #1204 shipped.

**1. `replay_verified: true` while some sampled histories were never read
(P1).** `ReplaySummary::verified()` ignored `unreadable`, so a report could
say `replay_verified: true` — the field the runbook says to check before
trusting any `clean` verdict — while some registered-workflow histories in
the sample were never read at all. `verified()` now requires
`unreadable == 0`. `HistoryUnreadable` stays advisory-severity (a single
unreadable history among many replayed must not fail the drill — a
deliberate, tested decision this PR does not touch); only the reported
*coverage* field changes. The CLI text renderer gained a third branch,
`replay: PARTIALLY VERIFIED`, for "some replayed, but coverage has gaps" —
distinct from both "fully verified" and "nothing replayed", so the existing
"register the workflow handlers" advice is not shown when handlers plainly
are registered.

**2. Unencoded (pre-sharding) target ids checked on the observing shard, not
the fleet's default shard (P2).** `owning_shard` fell back to the shard that
happened to be *observing* the reference for `ShardId::UNENCODED`, while
every runtime routing path (`ShardRouter::shard_for_execution`,
`ShardedDbPool::pool_for_execution`/`exact_pool_for_execution`) falls back to
the fleet's *configured default shard*. On a fleet migrated from pre-sharding
ids, this produced a false `child_execution_missing`/`external_target_missing`
(`Incoherent`, exit 1) on a healthy restore. Added `VerifyOptions::default_shard`
(default `0`) and a `harvest backup verify --default-shard <N>` flag, used by
`owning_shard` in place of the observing shard.

**3. Retention and a pre-creation rollback are indistinguishable, and both
passed silently (P1).** A recorded child terminal, or a delivered external
effect, whose target execution row is gone entirely has two causes: ordinary
retention (benign), or the target shard restored to a point *before* the
target ever existed (a genuine cross-shard break). The verifier could not
tell them apart and folded both into a silent pass. New finding class
`RetentionUnproven` (`Undetermined`, exit 2): when the target is absent, the
verifier now checks `harvest_execution_summaries` for a row proving
retention (issue #752's tiered-summary table); if none exists, the absence is
reported rather than assumed benign. Summaries are opt-in, so a fleet that
does not write them sees this on any restore where such a reference exists —
the honest answer ("we cannot tell") rather than a false `clean`.

**4. `Finding::new` hard-coded `truncated: false` before clipping samples
(P2).** Every cross-shard and replay finding class (nine of them, plus the
four replay classes) built its `Finding` straight from the constructor with
no `.with_truncated(...)` override, so a finding with more than
`MAX_FINDING_SAMPLES` matches reported `truncated: false` — asserting the
enumeration was complete when it was not. `Finding::new` now derives
`truncated` from `samples.len() > MAX_FINDING_SAMPLES` before clipping;
`with_truncated` ORs into that derived value instead of overwriting it, so
the one caller that already computes a smarter value (the bounded per-class
probe path, which also knows about a `LIMIT`ed `total`) keeps winning.

**Test evidence.**

- `autumn-harvest/src/backup_verify.rs` (unit, no DB): `truncated_is_derived_not_hardcoded`,
  `with_truncated_ors_in_rather_than_overwrites`,
  `replay_summary_is_not_verified_while_a_history_went_unread`, plus the
  existing `severity_truth_table_is_pinned_for_every_class` extended to pin
  `RetentionUnproven`.
- `autumn-harvest/tests/integration/backup_verify_tests.rs` (DB, two-shard
  fixtures): `an_unencoded_child_target_is_checked_on_the_default_shard`,
  `default_shard_option_changes_which_shard_is_checked` (proves the option is
  live, not coincidentally correct at the `0` default),
  `an_absent_child_terminal_target_without_a_summary_is_undetermined` +
  its retention-summary control, and the external-effect analogue pair.
- `autumn-harvest-cli`: `backup_verify_default_shard_defaults_to_zero_and_is_overridable`.
- `docs/runbooks/backup-restore.md` §4.2/§4.3/§5 updated to document
  `retention_unproven`, `--default-shard`, and the `PARTIALLY VERIFIED`
  replay state.
