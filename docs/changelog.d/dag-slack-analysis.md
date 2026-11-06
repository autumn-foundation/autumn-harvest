## Phase X.Y — DAG slack analysis

`DagSlackAnalyzer` extends the critical path with the critical path method.
It reports, for each task, the earliest and latest start and finish, and the
slack: how long the task can slip before it delays the run.

- A task with zero slack is on a critical path. A task with slack is a
  candidate for slower or cheaper capacity.
- `DagSlackReport::critical_indices` lists every zero-slack task. Unlike
  `CriticalPathAnalyzer`, it reports all tied paths.
- The module is pure and sends no SQL.
