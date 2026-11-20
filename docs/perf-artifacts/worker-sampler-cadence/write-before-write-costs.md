-- write-before: dead-letter writes, 200 single writes, 100-row batches, 2000 writes under a held snapshot, 8 x 50 concurrent inserts
| size | statement | calls | buffers | buffers per call | ms per call |
|:--|:--|--:|--:|--:|--:|
| small | insert, one row | 200 | 1862 | 9.3 | 0.020 |
| small | insert, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| small | discard, one row | 200 | 1000 | 5.0 | 0.005 |
| small | discard, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| small | discard, 100 rows | 4 | 1648 | 412.0 | 0.115 |
| small | discard, 100 rows: nested counter work | 0 | 0 | 0.0 | 0.000 |
| small | insert, one row, snapshot held (2000 writes) | 2000 | 18669 | 9.3 | 0.020 |
| small | gauge read after 2000 writes, snapshot held | 5 | 1415 | 283.0 | 0.656 |
| small | insert, 8 connections at once | 400 | 4782 | 12.0 | 0.045 |
| small | insert, 8 connections at once: nested counter work | 0 | 0 | 0.0 | 0.000 |
| medium | insert, one row | 200 | 2463 | 12.3 | 0.022 |
| medium | insert, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| medium | discard, one row | 200 | 1200 | 6.0 | 0.006 |
| medium | discard, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| medium | discard, 100 rows | 4 | 2048 | 512.0 | 0.190 |
| medium | discard, 100 rows: nested counter work | 0 | 0 | 0.0 | 0.000 |
| medium | insert, one row, snapshot held (2000 writes) | 2000 | 24483 | 12.2 | 0.022 |
| medium | gauge read after 2000 writes, snapshot held | 5 | 2801 | 560.2 | 4.730 |
| medium | insert, 8 connections at once | 400 | 5985 | 15.0 | 0.059 |
| medium | insert, 8 connections at once: nested counter work | 0 | 0 | 0.0 | 0.000 |
| large | insert, one row | 200 | 2307 | 11.5 | 0.026 |
| large | insert, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| large | discard, one row | 200 | 1200 | 6.0 | 0.007 |
| large | discard, one row: nested counter work | 0 | 0 | 0.0 | 0.000 |
| large | discard, 100 rows | 4 | 2376 | 594.0 | 0.308 |
| large | discard, 100 rows: nested counter work | 0 | 0 | 0.0 | 0.000 |
| large | insert, one row, snapshot held (2000 writes) | 2000 | 22517 | 11.3 | 0.030 |
| large | gauge read after 2000 writes, snapshot held | 5 | 3718397 | 743679.4 | 84.206 |
| large | insert, 8 connections at once | 400 | 5660 | 14.2 | 0.059 |
| large | insert, 8 connections at once: nested counter work | 0 | 0 | 0.0 | 0.000 |
