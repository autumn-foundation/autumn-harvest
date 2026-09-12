## Phase — audit-export retention bootstrap-window guard (issue #1266)

`purge_old_audit_records` already refused to delete an unexported audit row
when a live cursor existed for the shard, or when the sweeping process had a
sink configured. Issue #1266 (a Codex review round 6 follow-up on PR #1261)
found both signals could be absent at once in a split web/worker deployment:
before the worker's first successful tick on a shard, the process running
retention has no sink and no cursor row to read. A retention sweep landing in
that window deleted every retention-aged audit row, including rows the
exporter had not shipped yet.

Adds a third, explicit signal: `RetentionConfig::protect_unexported_audit`
(default `None`, disabled). Set the same way on every process in a split
deployment, it closes the window from the moment export is configured rather
than from the moment the exporter's first tick succeeds — the two cheaper
fixes the issue considered and rejected (seeding the cursor row in a
migration; letting the retention process create it) both fail for the same
reason: a shard's own database cannot know whether an exporter is coming,
and the retention process does not know either.

`purge_old_audit_records` gained a third parameter, `protect_unexported_audit:
bool`, OR'd flatly with the existing `is_configured()` signal — no schema
change. A first draft instead scoped the flag to `NOT EXISTS(any cursor row)`,
so a retired cursor always overrode it; a Codex review (round 1 P1) caught
that this reopened the exact bootstrap window for a shard being **re-enabled**
after decommission, since its cursor stays retired until the worker's first
new tick, and is indistinguishable from a shard meant to stay decommissioned.
The flag now shares `is_configured`'s existing "both steps required" trade
instead: decommissioning a shard does not resume purging there while either
signal stays `true` on the sweeping process, an operational cost documented
next to the pre-existing one for `is_configured`.

The same review round found a second, independent defect in the per-row
pending check: a row already stamped with an `export_seq` was treated as
already acknowledged whenever no cursor row existed at all — exactly backward
from `ensure_cursor_row`'s own handling of a cursor lost to a manual `DELETE`
or a partial restore, which rebuilds it at `last_acked_seq = 0` so every
stamped row is redelivered. The pending check now also treats "no cursor row
for the shard" as pending, matching that rebuild.

A further review round (P1) found that a single process-wide boolean cannot
represent a fleet with more than one shard: decommissioning shard A requires
disabling the flag, which simultaneously strips bootstrap protection from
shard B if B is mid-bootstrap on the same sweep. `RetentionConfig` changed
`protect_unexported_audit` from `bool` to `Option<BTreeSet<ShardId>>` —
`None` disables it, `Some(exempt)` protects every shard not in `exempt`. The
retention sweep now computes the per-shard bool itself
(`RetentionConfig::protects_unexported_audit`) instead of passing one flag
to every shard; `purge_old_audit_records`'s own signature is unaffected,
since it already took a plain `bool` per call. `with_protect_unexported_audit`
keeps its `bool` shape for the common case; a new
`excluding_shard_from_protect_unexported_audit` adds the exemption.

A fourth review round (P1) found that the per-shard fix above was still
unsafe under a supported topology: two logical shards can alias one
physical pool (`ShardedDbPool::from_map`, a pre-split staging shape).
`purge_old_audit_records` issues one unscoped `DELETE` per call, so calling
it once per logical shard let two aliased shards apply two different
decisions to the same physical audit table within one tick — a less
protective decision could commit before a more protective one ever ran.
The sweep now groups shards by pool identity first (`Pool::manager()`
returns a reference into the pool's shared `Arc` allocation, so `ptr::eq`
on it detects aliasing safely, with no private field or unsafe code), and
combines each group's decision with `any`: protect the shared pool
whenever any aliased shard wants protection. This also purges each
physical pool exactly once per tick instead of once per logical shard.

A fifth review round (P1) found the fourth round's fix incomplete:
`ShardedDbPool::from_dsns` builds a separate `Pool` object per shard entry
even when two entries carry the same DSN, so `ptr::eq` on `Pool::manager()`
never sees the alias. `ShardedDbPool` now records each shard's pool-group
number at construction time and exposes it via `pool_groups()`.
`from_map` still groups by `Pool::manager()` identity. `from_dsns` groups
by the DSN string instead, compared before each string is consumed into a
manager. The retention sweep's own grouping logic moved into
`ShardedDbPool::pool_groups()`, so both callers share one grouping.

New tests:
- `retention_protects_unexported_audit_when_configured_with_no_cursor_and_no_local_sink`
  reproduces the exact bootstrap window (no cursor row anywhere, no sink in
  the sweeping process) and asserts the flag alone keeps every row.
- `retention_decommission_alone_does_not_resume_purging_while_the_flag_is_true`
  and `retention_protects_a_shard_being_re_enabled_after_decommission` pin the
  round 1 P1 fix: the flag survives a retired cursor, and a shard coming back
  from decommission stays protected until the worker ticks it again.
- `retention_protects_a_stamped_row_when_its_cursor_row_is_gone` pins the
  second round 1 P1 fix: a stamped row with no cursor row to check against
  is never treated as acknowledged.
- `retention_still_purges_acknowledged_rows_when_protect_unexported_audit_is_true`
  confirms the flag never blocks purging of rows the exporter already
  shipped, even while a live cursor exists.
- `excluding_a_shard_leaves_every_other_shard_protected` and
  `excluding_a_shard_while_disabled_changes_nothing` pin the per-shard
  exemption: excluding shard 0 never touches shard 1's protection, and
  excluding a shard while the flag is off entirely is a no-op.
- `from_map_groups_cloned_pools_together` pins the fourth round's fix at
  its new home in `shard.rs`. `from_dsns_groups_shards_sharing_one_dsn`
  pins the fifth round's fix: two shards built from one DSN string
  through `from_dsns` still collapse to one group, even though `from_dsns`
  built them as two distinct `Pool` objects.
  `from_dsns_keeps_distinct_dsns_separate` confirms two different DSNs
  never collapse.

**Zero migration, zero engine impact beyond the new parameter.** No new
`WorkflowEvent` variant, no schema change, no change to any existing call
site's behavior when the new flag is left at its default (disabled).
