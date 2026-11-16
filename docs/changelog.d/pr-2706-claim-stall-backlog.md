## Phase fix — claim stall test backlog scales with the rate (issue #2706)

`claim_throughput_recovers_after_a_stall` no longer asserts on a fixed backlog.
The run measures its rate before the stall. It then adds the tasks that the
recovery window needs, with 1.5x headroom. A fast runner no longer fails the
check. `backlog_shortfall` has unit tests. Test-only change.
