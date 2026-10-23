### Workflows/sec (mean of valid runs; run count in brackets)

| depth | claims1 | claims2 | claims4 |
|--:|--:|--:|--:|
| 250 | 31.40 (3) | 40.72 (3) | 43.56 (3) |
| 500 | 31.27 (3) | 41.65 (3) | 44.27 (3) |
| 1000 | 32.26 (3) | 40.33 (3) | 45.99 (3) |
| 2000 | 31.29 (3) | 41.21 (3) | 47.44 (3) |

### Claims in flight (claim count x mean claim time / elapsed)

| depth | claims1 | claims2 | claims4 |
|--:|--:|--:|--:|
| 250 | 0.89 | 1.80 | 3.16 |
| 500 | 0.92 | 1.84 | 3.18 |
| 1000 | 0.94 | 1.86 | 3.23 |
| 2000 | 0.94 | 1.87 | 3.39 |

### Claim split (mean over depths and runs)

| claim loops | workflow claims per workflow | workflow ms | activity claims per workflow | activity ms | empty claims per workflow | empty ms | workflow share of claim time |
|:--|--:|--:|--:|--:|--:|--:|--:|
| 1 | 4.00 | 4.33 | 3.00 | 4.07 | 0.01 | 1.66 | 58.7% |
| 2 | 4.00 | 6.61 | 3.00 | 6.28 | 0.02 | 3.01 | 58.4% |
| 4 | 4.00 | 10.01 | 3.00 | 9.60 | 0.65 | 4.15 | 55.8% |

### Claim time by kind and depth (ms, workflow / activity)

| depth | claims1 | claims2 | claims4 |
|--:|--:|--:|--:|
| 250 | 4.16 / 3.88 | 6.45 / 6.08 | 10.04 / 9.78 |
| 500 | 4.33 / 4.04 | 6.50 / 6.07 | 9.89 / 9.79 |
| 1000 | 4.25 / 4.01 | 6.71 / 6.40 | 9.81 / 9.32 |
| 2000 | 4.39 / 4.14 | 6.60 / 6.30 | 10.14 / 9.67 |

### Decision persist phases (mean ms per persist; share of the persist)

| claim loops | persists per workflow | persist ms | fence | events | tasks | notify | commit | other |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 4.00 | 4.14 | 0.00 (0%) | 0.57 (14%) | 0.58 (14%) | 0.00 (0%) | 0.24 (6%) | 2.76 (67%) |
| 2 | 4.00 | 6.35 | 0.00 (0%) | 0.82 (13%) | 0.84 (13%) | 0.00 (0%) | 0.35 (5%) | 4.33 (68%) |
| 4 | 4.00 | 10.27 | 0.00 (0%) | 1.33 (13%) | 1.29 (13%) | 0.01 (0%) | 0.57 (6%) | 7.08 (69%) |
