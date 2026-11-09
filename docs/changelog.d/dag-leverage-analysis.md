## Phase X.Y — DAG leverage analysis

`DagLeverage` is a what-if companion to the slack analysis. It answers which
activity to speed up first: how much faster the whole run gets when one task
gets faster.

- `rank` orders every task by the time saved if it took no time. A task off
  the critical path saves nothing. A critical task saves up to the gap to the
  next longest path.
- `makespan_with` gives the run time for any replacement duration of one task.
- The module is pure and sends no SQL.
