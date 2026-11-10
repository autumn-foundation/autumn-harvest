## Phase X.Y — DAG worker-demand profile

`dag_concurrency` turns a slack report into a step function of running tasks.
It answers how many worker slots a run needs to meet its makespan.

- `profile` builds the demand curve for the earliest or the latest schedule.
  `ConcurrencyProfile::peak` and `peak_at` give the peak and when it starts.
- `peak_bounds` gives the smaller and larger peak of the two schedules. The
  gap shows how much slack can level the load.
- Back-to-back tasks do not count as concurrent. Zero-duration tasks add no
  demand.
- The module is pure and sends no SQL.
