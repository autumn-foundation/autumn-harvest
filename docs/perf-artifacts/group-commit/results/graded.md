## sync-on: 9 criterion rounds, 12 files

| writers | direct ms | solo ms | group ms | group / solo | solo p50 | group p50 | mean batch |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 3.824 | 4.146 | 3.684 | 0.89 | 3.554 | 3.340 | 1.00 |
| 4 | 1.565 | 1.587 | 1.450 | 0.91 | 5.059 | 4.521 | 1.00 |
| 16 | 1.530 | 1.516 | 1.392 | 0.92 | 20.869 | 18.556 | 2.12 |
| 64 | 1.533 | 1.469 | 1.327 | 0.90 | 64.837 | 59.770 | 5.65 |

Low-load A/B, one writer, modes in turn (p50 ms):

| mode | p50, median | p50 range | ratio to solo, median | ratio range |
|:--|--:|--:|--:|--:|
| direct | 3.498 | 3.048 to 5.520 | 0.999 | 0.991 to 1.026 |
| solo | 3.477 | 3.067 to 5.540 | 1.000 | 1.000 to 1.000 |
| group | 3.483 | 3.026 to 5.520 | 0.998 | 0.987 to 1.010 |

Depth A/B, alternating bursts (decisions per second):

| writers | pairs | solo, median | group, median | ratio, median | ratio range | pairs faster |
|--:|--:|--:|--:|--:|--:|--:|
| 16 | 30 | 807.6 | 956.9 | 1.164 | 0.823 to 1.386 | 24 of 30 |
| 64 | 30 | 620.8 | 785.1 | 1.327 | 1.039 to 1.511 | 30 of 30 |

Per-round ratios, group over solo (below 1 is faster):

| writers | wall ratio, median | wall ratio, range | rounds faster | p50 ratio, median |
|--:|--:|--:|--:|--:|
| 1 | 0.92 | 0.81 to 1.35 | 7 of 9 | 0.93 |
| 4 | 0.89 | 0.61 to 1.23 | 6 of 9 | 0.89 |
| 16 | 0.92 | 0.77 to 1.03 | 8 of 9 | 0.91 |
| 64 | 0.83 | 0.67 to 1.11 | 6 of 9 | 0.89 |

## sync-off: 3 criterion rounds, 5 files

| writers | direct ms | solo ms | group ms | group / solo | solo p50 | group p50 | mean batch |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 3.172 | 3.386 | 2.949 | 0.87 | 2.903 | 2.701 | 1.00 |
| 4 | 1.410 | 1.407 | 1.194 | 0.85 | 3.889 | 3.839 | 1.00 |
| 16 | 1.342 | 1.486 | 1.330 | 0.89 | 18.027 | 17.718 | 2.12 |
| 64 | 1.506 | 1.410 | 1.144 | 0.81 | 60.775 | 56.001 | 5.66 |

Low-load A/B, one writer, modes in turn (p50 ms):

| mode | p50, median | p50 range | ratio to solo, median | ratio range |
|:--|--:|--:|--:|--:|
| direct | 2.977 | 2.777 to 3.531 | 0.996 | 0.976 to 0.998 |
| solo | 2.987 | 2.844 to 3.555 | 1.000 | 1.000 to 1.000 |
| group | 2.974 | 2.802 to 3.513 | 0.995 | 0.985 to 0.998 |

Depth A/B, alternating bursts (decisions per second):

| writers | pairs | solo, median | group, median | ratio, median | ratio range | pairs faster |
|--:|--:|--:|--:|--:|--:|--:|
| 16 | 20 | 859.2 | 978.2 | 1.066 | 0.842 to 1.746 | 13 of 20 |
| 64 | 20 | 608.7 | 730.7 | 1.195 | 0.989 to 1.305 | 19 of 20 |

Per-round ratios, group over solo (below 1 is faster):

| writers | wall ratio, median | wall ratio, range | rounds faster | p50 ratio, median |
|--:|--:|--:|--:|--:|
| 1 | 0.90 | 0.87 to 0.97 | 3 of 3 | 0.93 |
| 4 | 0.98 | 0.74 to 1.01 | 2 of 3 | 0.98 |
| 16 | 0.88 | 0.82 to 1.09 | 2 of 3 | 0.89 |
| 64 | 0.76 | 0.72 to 1.00 | 3 of 3 | 0.91 |

