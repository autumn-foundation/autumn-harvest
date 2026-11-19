-- write-before: dead-letter writes, 200 single writes, 100-row batches, 8 x 50 concurrent inserts
| size | statement | calls | buffers | buffers per call | ms per call |
|:--|:--|--:|--:|--:|--:|
| small | insert, one row | 200 | 1870 | 9.3 | 0.029 |
| small | discard, one row | 200 | 1000 | 5.0 | 0.010 |
| small | discard, 100 rows | 4 | 1648 | 412.0 | 0.220 |
| small | insert, 8 connections (612 ms wall) | 400 | 4817 | 12.0 | 0.069 |
| medium | insert, one row | 200 | 2465 | 12.3 | 0.039 |
| medium | discard, one row | 200 | 1200 | 6.0 | 0.011 |
| medium | discard, 100 rows | 4 | 2048 | 512.0 | 0.314 |
| medium | insert, 8 connections (526 ms wall) | 400 | 5963 | 14.9 | 0.068 |
| large | insert, one row | 200 | 2313 | 11.6 | 0.041 |
| large | discard, one row | 200 | 1200 | 6.0 | 0.010 |
| large | discard, 100 rows | 4 | 2377 | 594.2 | 0.425 |
| large | insert, 8 connections (537 ms wall) | 400 | 5634 | 14.1 | 0.081 |
