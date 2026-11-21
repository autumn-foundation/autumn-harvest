-- after: buffers per call, 20 ticks x 2 workers, one claim per tick
| size | statement | calls | buffers | buffers per call | share of buffers |
|:--|:--|--:|--:|--:|--:|
| small | depth (bounded_queue_depths) | 40 | 30499 | 762 | 4.9% |
| small | age (oldest_pending_ages) | 40 | 106942 | 2674 | 17.2% |
| small | concurrency (concurrency_key_stats) | 40 | 26880 | 672 | 4.3% |
| small | rate limit (sample_rate_limit_buckets) | 40 | 80 | 2 | 0.0% |
| small | dlq (dead_letter_count) | 40 | 1560 | 39 | 0.3% |
| small | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| small | stranded work (claimable_pending_demand_by_queue) | 40 | 387222 | 9681 | 62.4% |
| small | workload (claim, complete, other) | 560 | 67328 | 120 | 10.9% |
| small | **all seven samplers** | | 553183 | | **89.1%** |
| medium | depth (bounded_queue_depths) | 40 | 50059 | 1251 | 1.0% |
| medium | age (oldest_pending_ages) | 40 | 64556 | 1614 | 1.3% |
| medium | concurrency (concurrency_key_stats) | 40 | 271770 | 6794 | 5.4% |
| medium | rate limit (sample_rate_limit_buckets) | 40 | 560 | 14 | 0.0% |
| medium | dlq (dead_letter_count) | 40 | 12920 | 323 | 0.3% |
| medium | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| medium | stranded work (claimable_pending_demand_by_queue) | 40 | 4597704 | 114943 | 90.6% |
| medium | workload (claim, complete, other) | 560 | 75577 | 135 | 1.5% |
| medium | **all seven samplers** | | 4997569 | | **98.5%** |
| large | depth (bounded_queue_depths) | 40 | 80378 | 2009 | 0.2% |
| large | age (oldest_pending_ages) | 40 | 86522 | 2163 | 0.2% |
| large | concurrency (concurrency_key_stats) | 40 | 809200 | 20230 | 1.7% |
| large | rate limit (sample_rate_limit_buckets) | 40 | 5280 | 132 | 0.0% |
| large | dlq (dead_letter_count) | 40 | 29766340 | 744158 | 61.0% |
| large | queue pause (paused_queue_names) | 40 | 0 | 0 | 0.0% |
| large | stranded work (claimable_pending_demand_by_queue) | 40 | 17932568 | 448314 | 36.8% |
| large | workload (claim, complete, other) | 560 | 106620 | 190 | 0.2% |
| large | **all seven samplers** | | 48680288 | | **99.8%** |
