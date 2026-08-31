Scale: 20000 executions x 10 events = 200000 event rows, across 20 daily cohorts; 50% expired.

| layout | pass total | event reclamation | events reclaimed | event-row DELETEs | dead-tuple ratio after | append p99 (quiet -> during) | claim p99 (quiet -> during) |
|---|---:|---:|---:|---:|---:|---|---|
| unpartitioned | 62.28s | (inside the pass) | 100000 | 99740 | 15.41% | 2.24 -> 2.03 ms (-9.4%) | 6.23 -> 5.88 ms (-5.7%) |
| partitioned | 62.06s | 0.099s | 100000 | 0 | 0.00% | 2.22 -> 2.16 ms (-2.4%) | 6.22 -> 5.90 ms (-5.2%) |

`pass total` is the whole retention pass. `event reclamation` splits out the part issue #958 changes: on the partitioned layout it is the partition drop; on the unpartitioned layout the cascade fires inside the candidate loop and cannot be separated from it, so it is reported as part of the pass. The remainder of the pass is the per-execution candidate loop (archive hook, hold re-check, summary demotion, auxiliary cleanup, execution-row delete), which issue #958 does not change and which the `HistoryArchiver` contract requires to visit each execution individually.
