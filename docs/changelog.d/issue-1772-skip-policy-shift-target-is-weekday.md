## Phase X.Y — Skip-policy shift target is a weekday (issue #1772)

`run_next_business_day` and `run_prev_business_day` could land on Saturday or Sunday for calendars such as `us-federal-holidays` and `nyse`. The scheduler passes `exclude_weekends = false` for every calendar except `weekends-off`.

`apply_skip_policy_with` now requires a shift target to be a weekday and not excluded. The check on the original slot date is unchanged, so a weekend slot that is not excluded keeps its date. No migration. No `harvest_events` change.

Tests: four new `apply_skip_policy_*` unit tests in `calendar.rs` cover both directions, an unshifted weekend slot, and both `exclude_weekends` values. Two existing tests pinned the weekend result and now expect Monday.
