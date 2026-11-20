-- per call, 2000 tasks per phase
| index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |
|:--|:--|:--|--:|--:|--:|--:|--:|--:|
| without | keyed | enqueue (insert) | 2000 | 13.2 | 0.31 | 8.05 | 0.259 | 2310 |
| without | keyed | claim | 2001 | 1094.3 | 0.01 | 10.13 | 0.003 | 751 |
| without | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | keyed | complete | 2000 | 19.1 | 0.01 | 6.08 | 0.002 | 517 |
| without | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | enqueue (insert) | 2000 | 13.3 | 0.31 | 8.05 | 0.264 | 2382 |
| without | plain | claim | 2001 | 97.3 | 0.01 | 9.10 | 0.002 | 678 |
| without | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | complete | 2000 | 19.1 | 0.01 | 6.04 | 0.002 | 506 |
| without | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | enqueue (insert) | 2000 | 15.2 | 0.32 | 9.08 | 0.264 | 2393 |
| with | keyed | claim | 2001 | 1096.3 | 0.01 | 11.18 | 0.003 | 829 |
| with | keyed | release_claim_if_over_cap | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | complete | 2000 | 19.2 | 0.01 | 6.08 | 0.002 | 517 |
| with | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | enqueue (insert) | 2000 | 13.2 | 0.31 | 8.05 | 0.264 | 2337 |
| with | plain | claim | 2001 | 97.3 | 0.01 | 9.10 | 0.003 | 681 |
| with | plain | release_claim_if_over_cap | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| with | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
