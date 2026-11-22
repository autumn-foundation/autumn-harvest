## Phase — DagLeverage::rank skips tasks that lie on no longest path
`DagLeverage::rank` re-ran a full DAG pass for every task. It now computes the
longest path through each task once. A task on no longest path keeps the run time
when it takes no time, so `rank` reuses the base run time for it. Only tasks on a
longest path run the full pass. The result is identical. Callgrind on the new
`dag_leverage_profile` harness: 500 tasks 30.3M to 5.93M instructions, 1000 tasks
114.5M to 15.7M. dhat bytes at 500 tasks: 8.88M to 1.55M.
