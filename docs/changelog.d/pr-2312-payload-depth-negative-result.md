## Docs — depth walk profile, negative result (issue #2312)

`payload_depth::nests_past_cap` runs on every successful activity result. A
callgrind profile shows it at 10.6% of a harness that only clones and checks
values. A real completion adds serialization, an append and a database round
trip, so its share there is far below the 5% floor. No code changes.

The profile harness is now in the tree as `benches/payload_depth_profile.rs`.
`docs/performance-payload-depth.md` holds the numbers and the reproduce steps,
so nobody repeats the profile.

No migration, no route change, and no `harvest_events` change.
