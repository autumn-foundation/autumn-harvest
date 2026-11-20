-- per call, 2000 tasks per phase
| index | phase | statement | calls | buffers | dirtied | WAL records | WAL FPI | WAL bytes |
|:--|:--|:--|--:|--:|--:|--:|--:|--:|
| without | keyed | enqueue (insert) | 2000 | 13.2 | 0.32 | 8.05 | 0.271 | 2292 |
| without | keyed | claim | 2001 | 1094.2 | 0.01 | 10.12 | 0.003 | 748 |
| without | keyed | claim cap re-check | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | keyed | complete | 2000 | 19.1 | 0.01 | 6.07 | 0.002 | 516 |
| without | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | enqueue (insert) | 2000 | 13.2 | 0.32 | 8.04 | 0.272 | 2359 |
| without | plain | claim | 2001 | 97.2 | 0.01 | 9.08 | 0.002 | 679 |
| without | plain | claim cap re-check | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| without | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 505 |
| without | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | enqueue (insert) | 2000 | 15.2 | 0.32 | 9.08 | 0.264 | 2397 |
| with | keyed | claim | 2001 | 1096.3 | 0.01 | 11.18 | 0.003 | 835 |
| with | keyed | claim cap re-check | 2000 | 4.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | keyed | complete | 2000 | 19.1 | 0.01 | 6.08 | 0.002 | 517 |
| with | keyed | other | 22006 | 0.7 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | enqueue (insert) | 2000 | 13.2 | 0.32 | 8.05 | 0.270 | 2357 |
| with | plain | claim | 2001 | 97.2 | 0.01 | 9.09 | 0.002 | 679 |
| with | plain | claim cap re-check | 0 | 0.0 | 0.00 | 0.00 | 0.000 | 0 |
| with | plain | complete | 2000 | 19.1 | 0.01 | 6.03 | 0.002 | 506 |
| with | plain | other | 22006 | 0.8 | 0.00 | 0.00 | 0.000 | 0 |
