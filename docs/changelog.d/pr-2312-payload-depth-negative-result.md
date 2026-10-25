## Docs — depth walk profile, negative result (issue #2312)

`payload_depth::nests_past_cap` runs on every successful activity result. A
callgrind profile shows it at 10.6% of a harness that only clones and checks
values. The one idea that the profile suggests saves at most 18.5% of the walk.
No profile of a real completion exists, so the production impact is
undetermined. No code changes.

The profile harness is now in the tree as `benches/payload_depth_profile.rs`.
`docs/performance-payload-depth.md` holds the numbers and the reproduce steps,
so nobody repeats the profile.

No migration, no route change, and no `harvest_events` change.
