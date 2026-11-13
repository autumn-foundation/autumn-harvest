
Scale: 20000 executions x 10 events = 200000 event rows, across 20 daily cohorts; 50% expired.

| layout | pass total | event reclamation | events reclaimed | event-row DELETEs | dead-tuple ratio after | append p99 (quiet -> during) | claim p99 (quiet -> during) | load ops/s |
|---|---:|---:|---:|---:|---:|---|---|---:|
| unpartitioned | 22.52s | (inside the pass) | 100000 | 97510 | 45.45% | 2.07 -> 2.78 ms (+34.6%) | 1.46 -> 1.77 ms (+21.4%) | 474 |
| partitioned | 22.63s | 0.197s | 100000 | 0 | 0.00% | 3.79 -> 3.30 ms (-12.7%) | 2.42 -> 1.75 ms (-27.6%) | 398 |

`pass total` is the whole retention pass. `event reclamation` splits out the part issue #958 changes: on the partitioned layout it is the partition drop; on the unpartitioned layout the cascade fires inside the candidate loop and cannot be separated from it, so it is reported as part of the pass. The remainder of the pass is the per-execution candidate loop (archive hook, hold re-check, summary demotion, auxiliary cleanup, execution-row delete), which issue #958 does not change and which the `HistoryArchiver` contract requires to visit each execution individually.

wrote /home/user/autumn-harvest/docs/perf-artifacts/event-partition-retention/measured.md
