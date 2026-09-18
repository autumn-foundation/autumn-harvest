## Phase — cross-shard business-key race: decision recorded, not re-fought (issue #1313)

Issue #1313 followed up on PR #1306 (issue #1146, Codex round 4): a by-id
fan-out reads every expected shard sequentially, on separate connections,
with no shared snapshot. A run of `(workflow_name, workflow_id)` that
starts on an already-read shard, mid fan-out, is invisible to that pass. A
cancel can then report `ExternalCancelDelivered` while that run is live. A
signal can deliver to an older run than the one that just started. Neither
`uninspected` nor `other_live` catches it: the shard in question answered
correctly, before the race, so there is nothing to disagree with.

The issue laid out three options and recorded a lean toward accepting the
limitation rather than paying for it: cross-shard uniqueness for the
business key (a coordination primitive the sharding design has so far
declined to add), verify-after-act for cancel (a latency tax on every
correct deployment to narrow a window that only opens in an
already-inconsistent one), or documenting the bound precisely and moving
on. This PR chooses the third, matching the issue's own inclination, and
closes the loop that left as an omission rather than a decision.

**What shipped.**

- A regression test, `external_target_location::tests
  ::issue_1313_a_start_racing_the_fanout_is_invisible_and_the_answer_is_
  still_authoritative`, narrates the exact race from the issue at the
  `merge_locations` level and pins today's answer: `is_authoritative_for_
  key()` is `true`, `uninspected` is empty, `other_live` is empty. A
  future change that tries to narrow this window without updating the
  documented decision now has a named test to explain.
- A stale doc comment on `TargetLocation` is fixed. It described "several
  live runs across shards" as a hypothetical fourth enum variant "one bug
  report away," when `Found`'s `other_live` field already covers exactly
  that case (issue #1146) — both landed in the same historical commit,
  and the comment was never updated once the field shipped. It now names
  the actual still-open gap: this issue's race, which cannot become an
  enum variant because no single fan-out can observe it.
- A new counter, `harvest.external_signal.by_id_other_live_observed`
  (`MetricsRecorder::record_external_by_id_other_live_observed`), fires
  whenever a **complete** by-id fan-out still finds more than one live
  run of a business key. It cannot catch the race this issue names — a
  run that starts mid fan-out is invisible to it by construction, same
  as `other_live`/`uninspected` are — but it is the observable proxy for
  the precondition that makes the race possible at all: a key pinned to
  one shard while an unpinned start of it hashed to another. An operator
  now has a signal for "this deployment is not keeping the pinning
  discipline `docs/sharding.md` asks for," distinct from the existing
  `by_id_indeterminate_shard`/`by_id_found_over_incomplete_fanout`
  counters, which are both about an *unreachable* shard rather than an
  *observed* second live run.

**Preconditions, corrected during review.** An earlier draft of this
fragment named only one precondition for two live runs of one business
key: a deployment mixing pinned and unpinned starts of the same
`workflow_id` — the discipline `docs/sharding.md`'s *Caveats* section
already asks operators to keep. Review (Codex) pointed out that is not
the only path. Pinning is not required at all: `ShardRouter::pick_writable`
re-hashes over the *current* `writable_shards` when the readable-set hash
falls outside it, so draining a shard moves where a fresh start of the
same key resolves — while an existing live run of it stays put on the
drained shard. The same two-live-runs state, reached by draining a shard
during a topology change, with every start left unpinned throughout.
`external_target_location.rs`'s module doc and `docs/sharding.md`'s by-id
addressing section already name
this drift as the second of the two ways a hash-derived shard can diverge
from where a run actually lives; this fragment now names it as a second
precondition too, so operators do not read "never pin" as sufficient.
Today's behavior is still strictly better than pre-#1146 either way: a
by-id resolution then consulted exactly one hash-derived shard and missed
a second live run unconditionally, not only under a race.

**Runtime behavior is unchanged; observability is not.**
`is_authoritative_for_key()`, `merge_locations`, and every delivery
outcome are untouched — no signal or cancel resolves, delivers, or
reports any differently than before. The only new runtime effect is the
counter above, recorded at the same point `timeout.rs` already records
`by_id_found_over_incomplete_fanout`, and gated by a pure predicate,
`should_record_other_live_observed`, so a partial fan-out that also saw
`other_live` is never double-counted against the other counter (review
finding). `cargo test -p autumn-harvest --lib --all-features` on
`external_target_location::`, `timeout::`, `telemetry::`, and
`metrics_rs_adapter::` (210 tests across the four modules) and `python3
docs/audits/comment-hygiene.py --base origin/trunk-dev` both pass.
