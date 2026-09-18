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
  that case (issue #1146, Codex round 3) — both landed in the same
  historical commit, and the comment was never updated once the field
  shipped. It now names the actual still-open gap: this issue's race,
  which cannot become an enum variant because no single fan-out can
  observe it.

**Preconditions, unchanged.** Two live runs of one business key require a
deployment to mix pinned and unpinned starts of the same `workflow_id` —
the discipline `docs/sharding.md`'s *Caveats* section already asks
operators to keep, for exactly this reason. Today's behavior is still
strictly better than pre-#1146: a by-id resolution then consulted exactly
one hash-derived shard and missed a second live run unconditionally,
not only under a race.

**No runtime behavior change.** `is_authoritative_for_key()`,
`merge_locations`, and every call site are untouched. `cargo test -p
autumn-harvest --lib external_target_location::` (33 tests, was 32) and
`python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` both
pass.
