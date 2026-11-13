-- baseline: buffers per call, 20 ticks x 2 workers, one claim per tick
| size | statement | calls | buffers | buffers per call | share of buffers |
|:--|:--|--:|--:|--:|--:|
| small | depth (bounded_queue_depths) | 40 | 30499 | 762 | 13.1% |
| small | age (oldest_pending_ages) | 40 | 106942 | 2674 | 45.8% |
| small | concurrency (concurrency_key_stats) | 40 | 26880 | 672 | 11.5% |
| small | rate limit (sample_rate_limit_buckets) | 40 | 80 | 2 | 0.0% |
| small | dlq (dead_letter_count) | 40 | 1560 | 39 | 0.7% |
| small | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| small | stranded work (claimable_pending_demand_by_queue) | 0 | 0 | 0 | 0.0% |
| small | workload (claim, complete, other) | 560 | 67407 | 120 | 28.9% |
| small | **all seven samplers** | | 165961 | | **71.1%** |
| medium | depth (bounded_queue_depths) | 40 | 50059 | 1251 | 10.5% |
| medium | age (oldest_pending_ages) | 40 | 64556 | 1614 | 13.6% |
| medium | concurrency (concurrency_key_stats) | 40 | 271770 | 6794 | 57.2% |
| medium | rate limit (sample_rate_limit_buckets) | 40 | 560 | 14 | 0.1% |
| medium | dlq (dead_letter_count) | 40 | 12920 | 323 | 2.7% |
| medium | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| medium | stranded work (claimable_pending_demand_by_queue) | 0 | 0 | 0 | 0.0% |
| medium | workload (claim, complete, other) | 560 | 75657 | 135 | 15.9% |
| medium | **all seven samplers** | | 399865 | | **84.1%** |
| large | depth (bounded_queue_depths) | 40 | 80538 | 2013 | 0.3% |
| large | age (oldest_pending_ages) | 40 | 86442 | 2161 | 0.3% |
| large | concurrency (concurrency_key_stats) | 40 | 809210 | 20230 | 2.6% |
| large | rate limit (sample_rate_limit_buckets) | 40 | 5280 | 132 | 0.0% |
| large | dlq (dead_letter_count) | 40 | 29766301 | 744158 | 96.5% |
| large | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| large | stranded work (claimable_pending_demand_by_queue) | 0 | 0 | 0 | 0.0% |
| large | workload (claim, complete, other) | 560 | 106694 | 191 | 0.3% |
| large | **all seven samplers** | | 30747771 | | **99.7%** |
