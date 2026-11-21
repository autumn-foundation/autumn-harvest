## Phase X.Y — Schedule stagger planner

`schedule_stagger::plan` turns the schedule herd forecast into a fix. It
gives each cron schedule a fixed delay, so the largest herd gets small.

- The planner visits the schedules in offender order. For each one, it
  tries each delay from zero to `max` in steps of `step`, and keeps the
  delay with the smallest peak. A tie goes to the lower crowding total, and
  then to the smaller delay.
- A zero delay is always a candidate, so the peak after the plan is never
  above the peak before it. A schedule that is in no herd never moves.
- Interval and manual schedules have no fixed phase. They never move.
- `MAX_CANDIDATE_DELAYS` bounds the delays for one schedule. A fine step
  over a long `max` is widened to that bound.
- The module is pure and sends no SQL. No migration, and no new
  `WorkflowEvent` variant.
