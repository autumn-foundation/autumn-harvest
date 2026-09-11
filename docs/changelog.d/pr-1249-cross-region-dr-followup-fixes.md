## Phase — cross-region DR follow-up correctness fixes (issue #1249)

Thirteen correctness findings from rounds 6–9 of the automated review of
#1246's cross-region DR work (issue #954), filed after that PR's review-round
budget was exhausted. None is a regression against `trunk-dev` — each is a
gap in the DR code #1246 shipped. Grouped by root cause, as the issue itself
recommends.

**One change: the unmeasurable-RPO signal cluster (findings 1, 2, 9, 12).**
Four call sites collapsed a distinct "this cannot be measured" condition into
a value that reads as healthy:

- `WatermarkReading` gained two variants. `PartiallyMeasured` (finding 1):
  `measure_rpo`'s position query used to fold "one slot has no position" into
  the same `Unknown` state as "nothing has been consumed anywhere", which let
  an abandoned or never-connected DR slot hide behind a healthy peer's small
  lag. The query now returns the measured watermark for slots that DO have a
  position, plus a count of the ones that do not (`unmeasurable_slots`,
  exposed as `ReplicationStatus::unmeasurable_slot_count()`), and this state
  is never eligible for the `replay_lag` fallback. `Failed` (finding 12): a
  watermark-read error used to become `Unknown` (fallback-eligible) instead
  of a state that reports the RPO as unknown — precisely when `replay_lag` is
  least trustworthy, since it freezes for a stuck logical apply worker.
- `ReplicationStatus::max_replay_lag_seconds()` (finding 9) used to
  `filter_map` out any standby with no `replay_lag`, so a healthy standby's
  small lag masked an unmeasurable one from the worst-case reduction. It now
  treats any connected standby with an unknown `replay_lag` as making the
  whole reduction unknown.
- A new `harvest.replication.rpo_known{shard}` gauge (finding 2), emitted on
  every sampler tick the replication views are readable, `0` included. A
  Prometheus gauge keeps exporting its last value, so simply skipping the RPO
  gauge when it is unknown does not make the panel stale — it freezes it at
  the last healthy reading. The existing `harvest.replication.observable`
  gauge already covers the views-unreadable case; this covers "readable but
  the RPO itself has no source" (a physical standby attached with no DR slot,
  before its first `replay_lag`). New alert `harvest_replication_rpo_unknown`
  in the starter pack and `docs/runbooks/harvest-alerts.md`, gated on
  `observable == 1` and `standbys > 0` so the three replication-health rules
  stay mutually exclusive.

**One change: write-once `FenceRegistry` (findings 3, 13).** `register` and
`set_default_shard` were the two public entry points that bypassed
`publish`'s conflict check with an unconditional write. Both are now
conflict-checked identically to `publish`: a differing re-pin is refused
(`PinConflict` / new `DefaultShardConflict`), an identical re-pin is a no-op.
`publish`'s own default-shard write is now validated in the same pre-flight
pass as its generations, via a new `PublishConflict` enum wrapping both
conflict shapes.

**Independent hardening (findings 4, 5, 6, 7, 8, 10, 11):**

- **Finding 4** — `run_multi_shard`'s startup registered this worker in the
  fleet and mutated rate-limit buckets BEFORE `pin_dr_generations`, opposite
  the single-shard path and the "fence FIRST" comment beside it. Reordered.
- **Finding 5** — the sequence-advance catalog query filtered on the
  SEQUENCE's own schema (`sn.nspname = current_schema()`) instead of the
  OWNING TABLE's (`tn.nspname`), silently skipping a sequence created in a
  different schema than the table it belongs to.
- **Finding 6** — `WorkerRuntimeConfig` carried only `dr_fencing: bool` on the
  struct, racing the cadence, retention and slot-prefix knobs through a
  last-writer-wins process global. `slot_prefix` decides which walsenders
  count as this deployment's DR replication at all, so racing it can change
  *what* is sampled, not merely *when*. The field is now the whole
  `DrConfig`, built directly from the `WorkerConfig` conversion; `Worker::new`
  reads it from the runtime config it was given rather than a fresh global
  read.
- **Finding 7** — every slot/standby prefix match used
  `LIKE $1 || '%'`, and `LIKE` treats `_` as a wildcard. The shipped default
  prefix `harvest_dr` contains one, so an unrelated slot such as
  `harvestXdr_shard0` counted as a DR standby on the DEFAULT configuration.
  Switched to `starts_with`/literal comparison at all four call sites.
- **Finding 8** — `ensure_generation_row`'s fallback read shared its `INSERT`
  statement's snapshot via `UNION ALL`, so under concurrent first starts the
  loser could see zero rows even after its `ON CONFLICT DO NOTHING` finished
  waiting on the winner's commit — a spurious refusal to start, worst on a
  fleet-wide rollout. The fallback is now a separate statement with a fresh
  snapshot.
- **Finding 10** — `harvest_replication_heartbeat` is replicated by the
  `FOR ALL TABLES` publication the topology doc prescribes, so a standby can
  carry beats from the OLD primary whose LSNs belong to a different WAL
  stream than the new primary's. New migration column `fence_generation`
  stamps each beat with the epoch in force when it was written;
  `measure_rpo` now reads only beats from the shard's CURRENT generation.
- **Finding 11** — promotion's sequence-advance used `GREATEST` unconditionally,
  which assumes ascending issuance; a descending sequence's "furthest issued"
  value is its MINIMUM, so `GREATEST` could reset it backward into a value it
  had already issued past. Now branches on `pg_sequences.increment_by`
  (`GREATEST`/ascending vs `LEAST`/descending), bounded by the sequence's own
  `min_value`/`max_value` rather than a hardcoded `1`.

**Test evidence.**

- `autumn-harvest/src/replication.rs` (unit, no DB): new tests for the
  `PartiallyMeasured`/`Failed` fallback behavior, the `max_replay_lag_seconds`
  masking fix, and `FenceRegistry`'s conflict checks on `register`,
  `publish`'s default shard, and `set_default_shard`.
- `autumn-harvest/src/telemetry.rs`: `replication_gauges_have_default_noop_impls_and_stable_names`
  extended to cover `METRIC_REPLICATION_RPO_KNOWN`.
- `autumn-harvest/tests/integration/cross_region_dr_tests.rs` (DB, live
  Postgres): `one_unmeasurable_slot_beside_a_measurable_one_is_a_partial_reading`,
  `a_slot_matching_the_prefix_only_under_like_wildcards_is_not_counted`,
  `promotion_advances_a_sequence_owned_by_a_table_in_a_different_schema`,
  `promotion_never_rewinds_a_descending_sequence`,
  `concurrent_first_starts_always_agree_on_the_provisioned_generation`,
  `measure_rpo_ignores_heartbeats_from_a_superseded_generation`.
- `docs/cross-region-dr.md` and `docs/runbooks/cross-region-failover.md`
  updated with the generation-scoped watermark trail; `docs/runbooks/harvest-alerts.md`
  and `docs/alerts/starter-pack-v0.1.0.json` gained the new alert.
