Scale: 20000 executions x 10 events = 200000 event rows, across 20 daily cohorts; 50% expired.

| layout | pass total | event reclamation | events reclaimed | event-row DELETEs | dead-tuple ratio after | append p99 (quiet -> during) | claim p99 (quiet -> during) | load ops/s |
|---|---:|---:|---:|---:|---:|---|---|---:|
| unpartitioned | 62.73s | (inside the pass) | 100000 | 98950 | 15.82% | 2.19 -> 2.13 ms (-2.4%) | 6.78 -> 5.99 ms (-11.7%) | 190 |
| partitioned | 60.99s | 0.154s | 100000 | 0 | 0.00% | 3.56 -> 2.93 ms (-17.7%) | 7.30 -> 6.02 ms (-17.6%) | 176 |

`pass total` is the whole retention pass. `event reclamation` splits out the part issue #958 changes: on the partitioned layout it is the partition drop; on the unpartitioned layout the cascade fires inside the candidate loop and cannot be separated from it, so it is reported as part of the pass. The remainder of the pass is the per-execution candidate loop (archive hook, hold re-check, summary demotion, auxiliary cleanup, execution-row delete), which issue #958 does not change and which the `HistoryArchiver` contract requires to visit each execution individually.
