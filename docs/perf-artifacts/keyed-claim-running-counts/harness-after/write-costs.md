-- per call, 2000 tasks per phase
| index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |
|:--|:--|:--|--:|--:|--:|--:|--:|--:|
| without | keyed | enqueue (insert) | 2000 | 13.2 | 0.34 | 8.05 | 0.283 | 2312 |
| without | keyed | claim | 2001 | 112.3 | 0.01 | 10.17 | 0.003 | 751 |
| without | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| without | keyed | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | enqueue (insert) | 2000 | 13.2 | 0.33 | 8.04 | 0.284 | 2370 |
| without | plain | claim | 2001 | 97.3 | 0.01 | 9.08 | 0.002 | 678 |
| without | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| without | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | enqueue (insert) | 2000 | 15.2 | 0.33 | 9.07 | 0.276 | 2392 |
| with | keyed | claim | 2001 | 114.3 | 0.01 | 11.22 | 0.003 | 834 |
| with | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| with | keyed | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | enqueue (insert) | 2000 | 13.2 | 0.33 | 8.04 | 0.278 | 2376 |
| with | plain | claim | 2001 | 97.3 | 0.01 | 9.09 | 0.002 | 675 |
| with | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| with | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
