-- write-after: dead-letter writes, 200 single writes, 100-row batches, 2000 writes under a held snapshot, 8 x 50 concurrent inserts
| size | statement | calls | buffers | buffers per call | ms per call |
|:--|:--|--:|--:|--:|--:|
| small | insert, one row | 200 | 2269 | 11.3 | 0.051 |
| small | insert, one row: nested counter work | 200 | 401 | 2.0 | 0.005 |
| small | discard, one row | 200 | 1719 | 8.6 | 0.032 |
| small | discard, one row: nested counter work | 200 | 519 | 2.6 | 0.008 |
| small | discard, 100 rows | 4 | 2060 | 515.0 | 0.207 |
| small | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.012 |
| small | insert, one row, snapshot held (2000 writes) | 2000 | 24580 | 12.3 | 0.056 |
| small | gauge read after 2000 writes, snapshot held | 5 | 164 | 32.8 | 0.189 |
| small | counter after 2000 writes, snapshot held: 2808 rows, 18 pages | | | | |
| small | insert, 8 connections at once | 400 | 6753 | 16.9 | 0.144 |
| small | insert, 8 connections at once: nested counter work | 400 | 1344 | 3.4 | 0.017 |
| small | counter rows after the concurrent inserts: 3208 | | | | |
| medium | insert, one row | 200 | 2865 | 14.3 | 0.066 |
| medium | insert, one row: nested counter work | 200 | 401 | 2.0 | 0.006 |
| medium | discard, one row | 200 | 1919 | 9.6 | 0.054 |
| medium | discard, one row: nested counter work | 200 | 519 | 2.6 | 0.013 |
| medium | discard, 100 rows | 4 | 2460 | 615.0 | 0.347 |
| medium | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.015 |
| medium | insert, one row, snapshot held (2000 writes) | 2000 | 30546 | 15.3 | 0.061 |
| medium | gauge read after 2000 writes, snapshot held | 5 | 123 | 24.6 | 0.180 |
| medium | counter after 2000 writes, snapshot held: 2808 rows, 18 pages | | | | |
| medium | insert, 8 connections at once | 400 | 7997 | 20.0 | 0.141 |
| medium | insert, 8 connections at once: nested counter work | 400 | 1366 | 3.4 | 0.013 |
| medium | counter rows after the concurrent inserts: 3208 | | | | |
| large | insert, one row | 200 | 2716 | 13.6 | 0.055 |
| large | insert, one row: nested counter work | 200 | 401 | 2.0 | 0.005 |
| large | discard, one row | 200 | 1919 | 9.6 | 0.042 |
| large | discard, one row: nested counter work | 200 | 519 | 2.6 | 0.010 |
| large | discard, 100 rows | 4 | 2783 | 695.8 | 0.416 |
| large | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.013 |
| large | insert, one row, snapshot held (2000 writes) | 2000 | 28588 | 14.3 | 0.054 |
| large | gauge read after 2000 writes, snapshot held | 5 | 123 | 24.6 | 0.168 |
| large | counter after 2000 writes, snapshot held: 2808 rows, 18 pages | | | | |
| large | insert, 8 connections at once | 400 | 7620 | 19.1 | 0.130 |
| large | insert, 8 connections at once: nested counter work | 400 | 1353 | 3.4 | 0.016 |
| large | counter rows after the concurrent inserts: 3208 | | | | |
