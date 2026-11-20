-- per call, 2000 tasks per phase
| index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |
|:--|:--|:--|--:|--:|--:|--:|--:|--:|
| without | keyed | enqueue (insert) | 2000 | 13.2 | 0.34 | 8.05 | 0.284 | 2313 |
| without | keyed | claim | 2001 | 112.2 | 0.01 | 10.17 | 0.003 | 750 |
| without | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | keyed | complete | 2000 | 19.1 | 0.01 | 6.06 | 0.002 | 515 |
| without | keyed | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | enqueue (insert) | 2000 | 13.2 | 0.33 | 8.04 | 0.283 | 2357 |
| without | plain | claim | 2001 | 97.2 | 0.01 | 9.08 | 0.003 | 681 |
| without | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | complete | 2000 | 19.0 | 0.01 | 6.04 | 0.002 | 504 |
| without | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | enqueue (insert) | 2000 | 15.2 | 0.32 | 9.07 | 0.270 | 2385 |
| with | keyed | claim | 2001 | 114.4 | 0.01 | 11.22 | 0.003 | 827 |
| with | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| with | keyed | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | enqueue (insert) | 2000 | 13.3 | 0.32 | 8.05 | 0.272 | 2364 |
| with | plain | claim | 2001 | 97.3 | 0.01 | 9.08 | 0.003 | 686 |
| with | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | complete | 2000 | 19.1 | 0.01 | 6.04 | 0.002 | 506 |
| with | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
