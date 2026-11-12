## Phase X.Y — Schedule herd forecast

`schedule_herd::forecast` finds the moments when many schedules fire
together. Twenty schedules on `0 * * * *` each pass validation, but the
scheduler then starts twenty runs at the top of each hour.

- The forecast walks each cron schedule over a window with `SlotWalker`. It
  groups the fires into buckets of fixed width.
- `spots` lists each bucket with at least `min_herd` schedules, the largest
  first. `peak` gives the largest one.
- `offenders` ranks the schedules by the number of spots that include them.
  Move the top one to another minute first.
- Interval and manual schedules have no fixed phase. The forecast lists them
  as `unanchored` and leaves them out of the buckets.
- The module is pure and sends no SQL.
