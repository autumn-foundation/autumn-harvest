## Phase replay perf — hash replay event positions without SipHash (PR #2303)

`HistoryMatcher::is_consumed` runs once per scanned event, and it probed
three sets of event positions with the default `SipHash`. Those sets now use
`EventIndexHasher`, a multiplicative hasher in `replay.rs`. An event position
is not data that an attacker chooses, and no code iterates the sets, so
behavior is unchanged. No event, migration or append-only change.

Evidence: the new `match_activity_profile` callgrind harness replays a
fan-out through `HistoryMatcher::match_activity`. At N=1000, instructions
fall 61.6% for `fanout` and 56.4% for `fanout_rev`. A sequential history
moves +0.2%. The scan stayed quadratic until issue #2330. See
`docs/performance-match-activity-fanout.md`.
