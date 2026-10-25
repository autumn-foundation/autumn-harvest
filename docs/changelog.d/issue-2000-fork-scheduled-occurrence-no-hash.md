## Phase fork perf — count activity occurrences without hashing (issue #2000)

`fork::scheduled_occurrence` found the occurrence number of a scheduled
activity with a `HashMap` that hashed every `ActivityScheduled` name before the
match. It now finds the activity by id first, then counts earlier events with
the same name. The result is unchanged. No event, migration or append-only
change.

Evidence: the new `fork_resolve_profile` callgrind harness resolves a K-way
fan-out through `resolve_activity` and `settle_holds`. Instructions fall from
22.68M to 15.29M at K=250 (-32.6%), from 69.23M to 39.93M at K=500 (-42.3%),
and from 237.10M to 120.45M at K=1000 (-49.2%). The 34 `fork::tests` pass
unchanged. See `docs/performance-fork-scheduled-occurrence.md`.
