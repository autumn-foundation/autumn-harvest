## Claim stall test backlog scales with the rate (issue #2706)

`claim_throughput_recovers_after_a_stall` no longer asserts on a fixed backlog.
The run measures its rate before the stall. It then adds the tasks that the
recovery window needs, with 1.5x headroom. A fast runner no longer fails the
check. Each round re-reads the claims made during the seed. A round refills to the headroom target but passes at the bare need. Test-only change.

Test evidence: eight `shortfall_*` unit tests cover `backlog_shortfall`, among
them the failing run of the issue (4688 left at 400/s).
