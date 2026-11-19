-- write-after: dead-letter writes, 200 single writes, 100-row batches, 8 x 50 concurrent inserts
| size | statement | calls | buffers | buffers per call | ms per call |
|:--|:--|--:|--:|--:|--:|
| small | insert, one row | 200 | 3066 | 15.3 | 0.103 |
| small | insert, one row: nested counter update | 200 | 1200 | 6.0 | 0.018 |
| small | discard, one row | 200 | 2400 | 12.0 | 0.046 |
| small | discard, one row: nested counter update | 200 | 1200 | 6.0 | 0.011 |
| small | discard, 100 rows | 4 | 2072 | 518.0 | 0.237 |
| small | discard, 100 rows: nested counter update | 4 | 24 | 6.0 | 0.018 |
| small | insert, 8 connections (392 ms wall) | 400 | 9531 | 23.8 | 0.207 |
| medium | insert, one row | 200 | 3667 | 18.3 | 0.079 |
| medium | insert, one row: nested counter update | 200 | 1200 | 6.0 | 0.014 |
| medium | discard, one row | 200 | 2600 | 13.0 | 0.038 |
| medium | discard, one row: nested counter update | 200 | 1200 | 6.0 | 0.010 |
| medium | discard, 100 rows | 4 | 2472 | 618.0 | 0.335 |
| medium | discard, 100 rows: nested counter update | 4 | 24 | 6.0 | 0.017 |
| medium | insert, 8 connections (464 ms wall) | 400 | 10652 | 26.6 | 0.239 |
| large | insert, one row | 200 | 3507 | 17.5 | 0.105 |
| large | insert, one row: nested counter update | 200 | 1200 | 6.0 | 0.018 |
| large | discard, one row | 200 | 2600 | 13.0 | 0.053 |
| large | discard, one row: nested counter update | 200 | 1200 | 6.0 | 0.011 |
| large | discard, 100 rows | 4 | 2795 | 698.8 | 0.375 |
| large | discard, 100 rows: nested counter update | 4 | 24 | 6.0 | 0.015 |
| large | insert, 8 connections (390 ms wall) | 400 | 10431 | 26.1 | 0.217 |
