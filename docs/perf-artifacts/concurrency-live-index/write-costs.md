-- per call, 2000 tasks per phase
| index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |
|:--|:--|:--|--:|--:|--:|--:|--:|--:|
| without | keyed | enqueue (insert) | 2000 | 13.2 | 0.33 | 8.05 | 0.279 | 2309 |
| without | keyed | claim | 2001 | 1094.2 | 0.01 | 10.12 | 0.003 | 746 |
| without | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| without | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | enqueue (insert) | 2000 | 13.2 | 0.32 | 8.05 | 0.274 | 2334 |
| without | plain | claim | 2001 | 97.3 | 0.01 | 9.09 | 0.003 | 685 |
| without | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| without | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | enqueue (insert) | 2000 | 15.2 | 0.33 | 9.07 | 0.274 | 2401 |
| with | keyed | claim | 2001 | 1096.3 | 0.01 | 11.17 | 0.003 | 825 |
| with | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| with | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | enqueue (insert) | 2000 | 13.2 | 0.32 | 8.04 | 0.275 | 2381 |
| with | plain | claim | 2001 | 97.3 | 0.01 | 9.09 | 0.002 | 681 |
| with | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| with | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
