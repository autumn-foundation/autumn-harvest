-- live-index: buffers per call, 20 ticks x 2 workers, one claim per tick
| size | statement | calls | buffers | buffers per call | share of buffers |
|:--|:--|--:|--:|--:|--:|
| small | depth (bounded_queue_depths) | 40 | 30499 | 762 | 5.1% |
| small | age (oldest_pending_ages) | 40 | 106942 | 2674 | 18.0% |
| small | concurrency (concurrency_key_stats) | 40 | 1760 | 44 | 0.3% |
| small | rate limit (sample_rate_limit_buckets) | 40 | 80 | 2 | 0.0% |
| small | dlq (dead_letter_count) | 40 | 1560 | 39 | 0.3% |
| small | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| small | stranded work (claimable_pending_demand_by_queue) | 40 | 387222 | 9681 | 65.0% |
| small | workload (claim, complete, other) | 560 | 67327 | 120 | 11.3% |
| small | **all seven samplers** | | 528063 | | **88.7%** |
| medium | depth (bounded_queue_depths) | 40 | 50059 | 1251 | 1.0% |
| medium | age (oldest_pending_ages) | 40 | 35000 | 875 | 0.7% |
| medium | concurrency (concurrency_key_stats) | 40 | 19960 | 499 | 0.4% |
| medium | rate limit (sample_rate_limit_buckets) | 40 | 560 | 14 | 0.0% |
| medium | dlq (dead_letter_count) | 40 | 12920 | 323 | 0.3% |
| medium | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| medium | stranded work (claimable_pending_demand_by_queue) | 40 | 4597704 | 114943 | 95.9% |
| medium | workload (claim, complete, other) | 560 | 75583 | 135 | 1.6% |
| medium | **all seven samplers** | | 4716203 | | **98.4%** |
| large | depth (bounded_queue_depths) | 40 | 80419 | 2010 | 0.2% |
| large | age (oldest_pending_ages) | 40 | 86483 | 2162 | 0.2% |
| large | concurrency (concurrency_key_stats) | 40 | 58923 | 1473 | 0.1% |
| large | rate limit (sample_rate_limit_buckets) | 40 | 5280 | 132 | 0.0% |
| large | dlq (dead_letter_count) | 40 | 29766207 | 744155 | 62.0% |
| large | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| large | stranded work (claimable_pending_demand_by_queue) | 40 | 17932578 | 448314 | 37.3% |
| large | workload (claim, complete, other) | 560 | 106663 | 190 | 0.2% |
| large | **all seven samplers** | | 47929890 | | **99.8%** |
