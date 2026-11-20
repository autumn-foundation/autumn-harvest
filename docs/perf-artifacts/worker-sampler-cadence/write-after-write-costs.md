-- write-after: dead-letter writes, 200 single writes, 100-row batches, 2000 writes under a held snapshot, 8 x 50 concurrent inserts
| size | statement | calls | buffers | buffers per call | ms per call |
|:--|:--|--:|--:|--:|--:|
| small | insert, one row | 200 | 2946 | 14.7 | 0.055 |
| small | insert, one row: nested counter work | 209 | 1001 | 4.8 | 0.007 |
| small | discard, one row | 200 | 2427 | 12.1 | 0.038 |
| small | discard, one row: nested counter work | 224 | 1227 | 5.5 | 0.009 |
| small | discard, 100 rows | 4 | 2060 | 515.0 | 0.209 |
| small | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.014 |
| small | insert, one row, snapshot held (2000 writes) | 2000 | 32610 | 16.3 | 0.056 |
| small | gauge read after 2000 writes, snapshot held | 5 | 90 | 18.0 | 0.059 |
| small | counter after 2000 writes, snapshot held: 72 rows, 18 pages | | | | |
| small | insert, 8 connections at once | 400 | 13353 | 33.4 | 0.174 |
| small | insert, 8 connections at once: nested counter work | 427 | 3657 | 8.6 | 0.020 |
| small | counter rows after the concurrent inserts: 19 | | | | |
| medium | insert, one row | 200 | 3589 | 17.9 | 0.061 |
| medium | insert, one row: nested counter work | 218 | 1039 | 4.8 | 0.006 |
| medium | discard, one row | 200 | 2482 | 12.4 | 0.048 |
| medium | discard, one row: nested counter work | 209 | 1082 | 5.2 | 0.012 |
| medium | discard, 100 rows | 4 | 2460 | 615.0 | 0.389 |
| medium | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.021 |
| medium | insert, one row, snapshot held (2000 writes) | 2000 | 39341 | 19.7 | 0.063 |
| medium | gauge read after 2000 writes, snapshot held | 5 | 85 | 17.0 | 0.055 |
| medium | counter after 2000 writes, snapshot held: 38 rows, 17 pages | | | | |
| medium | insert, 8 connections at once | 400 | 13440 | 33.6 | 0.161 |
| medium | insert, 8 connections at once: nested counter work | 439 | 3391 | 7.7 | 0.018 |
| medium | counter rows after the concurrent inserts: 36 | | | | |
| large | insert, one row | 200 | 2974 | 14.9 | 0.074 |
| large | insert, one row: nested counter work | 206 | 579 | 2.8 | 0.006 |
| large | discard, one row | 200 | 3022 | 15.1 | 0.040 |
| large | discard, one row: nested counter work | 215 | 1622 | 7.5 | 0.011 |
| large | discard, 100 rows | 4 | 2788 | 697.0 | 0.414 |
| large | discard, 100 rows: nested counter work | 4 | 12 | 3.0 | 0.018 |
| large | insert, one row, snapshot held (2000 writes) | 2000 | 37387 | 18.7 | 0.063 |
| large | gauge read after 2000 writes, snapshot held | 5 | 90 | 18.0 | 0.056 |
| large | counter after 2000 writes, snapshot held: 2 rows, 18 pages | | | | |
| large | insert, 8 connections at once | 400 | 12950 | 32.4 | 0.160 |
| large | insert, 8 connections at once: nested counter work | 436 | 3441 | 7.9 | 0.016 |
| large | counter rows after the concurrent inserts: 3 | | | | |
