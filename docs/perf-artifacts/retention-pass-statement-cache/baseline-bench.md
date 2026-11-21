seeding unpartitioned arm: 20000 executions x 10 events…
measuring…
seeding partitioned arm: 20000 executions x 10 events…
measuring…

Scale: 20000 executions x 10 events = 200000 event rows, across 20 daily cohorts; 50% expired.

| layout | pass total | event reclamation | events reclaimed | event-row DELETEs | dead-tuple ratio after | append p99 (quiet -> during) | claim p99 (quiet -> during) | load ops/s |
|---|---:|---:|---:|---:|---:|---|---|---:|
| unpartitioned | 64.65s | (inside the pass) | 100000 | 99210 | 99.71% | 2.10 -> 2.10 ms (+0.3%) | 1.66 -> 1.71 ms (+3.4%) | 498 |
| partitioned | 60.88s | 0.169s | 100000 | 0 | 0.00% | 3.82 -> 3.80 ms (-0.4%) | 2.35 -> 2.15 ms (-8.5%) | 449 |

`pass total` is the whole retention pass. `event reclamation` splits out the part issue #958 changes: on the partitioned layout it is the partition drop; on the unpartitioned layout the cascade fires inside the candidate loop and cannot be separated from it, so it is reported as part of the pass. The remainder of the pass is the per-execution candidate loop (archive hook, hold re-check, summary demotion, auxiliary cleanup, execution-row delete), which issue #958 does not change and which the `HistoryArchiver` contract requires to visit each execution individually.

wrote /home/user/autumn-harvest/docs/perf-artifacts/event-partition-retention/measured.md
