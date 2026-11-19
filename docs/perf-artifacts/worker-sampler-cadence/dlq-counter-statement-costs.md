-- dlq-counter: buffers per call, 20 ticks x 2 workers, one claim per tick
| size | statement | calls | buffers | buffers per call | share of buffers |
|:--|:--|--:|--:|--:|--:|
| small | depth (bounded_queue_depths) | 22 | 22961 | 1044 | 6.4% |
| small | age (oldest_pending_ages) | 23 | 61435 | 2671 | 17.0% |
| small | concurrency (concurrency_key_stats) | 23 | 15456 | 672 | 4.3% |
| small | rate limit (sample_rate_limit_buckets) | 23 | 46 | 2 | 0.0% |
| small | dlq (dead_letter_count) | 23 | 23 | 1 | 0.0% |
| small | queue pause (paused_queue_names) | 23 | 0 | 0 | 0.0% |
| small | stranded work (claimable_pending_demand_by_queue) | 23 | 222456 | 9672 | 61.6% |
| small | workload (claim, complete, other) | 319 | 38643 | 121 | 10.7% |
| small | **all seven samplers** | | 322377 | | **89.3%** |
| medium | depth (bounded_queue_depths) | 40 | 50019 | 1250 | 1.0% |
| medium | age (oldest_pending_ages) | 40 | 35040 | 876 | 0.7% |
| medium | concurrency (concurrency_key_stats) | 40 | 271770 | 6794 | 5.4% |
| medium | rate limit (sample_rate_limit_buckets) | 40 | 560 | 14 | 0.0% |
| medium | dlq (dead_letter_count) | 40 | 40 | 1 | 0.0% |
| medium | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| medium | stranded work (claimable_pending_demand_by_queue) | 40 | 4597704 | 114943 | 91.4% |
| medium | workload (claim, complete, other) | 560 | 75577 | 135 | 1.5% |
| medium | **all seven samplers** | | 4955133 | | **98.5%** |
| large | depth (bounded_queue_depths) | 40 | 80498 | 2012 | 0.4% |
| large | age (oldest_pending_ages) | 40 | 86482 | 2162 | 0.5% |
| large | concurrency (concurrency_key_stats) | 40 | 809200 | 20230 | 4.3% |
| large | rate limit (sample_rate_limit_buckets) | 40 | 5280 | 132 | 0.0% |
| large | dlq (dead_letter_count) | 40 | 40 | 1 | 0.0% |
| large | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| large | stranded work (claimable_pending_demand_by_queue) | 40 | 17932568 | 448314 | 94.3% |
| large | workload (claim, complete, other) | 560 | 106614 | 190 | 0.6% |
| large | **all seven samplers** | | 18914068 | | **99.4%** |
