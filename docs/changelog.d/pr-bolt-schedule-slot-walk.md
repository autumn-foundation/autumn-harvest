## Phase X.Y — Schedule catch-up walk parses the cron once

A catch-up walk parsed the cron expression again for each missed slot.

- `schedule_plan::SlotWalker` is new. It parses the schedule once and
  returns slots with `next_after`.
- `next_fire_after` keeps its signature and result.
- The backfill planner and the catch-up planners use one walker per walk.
