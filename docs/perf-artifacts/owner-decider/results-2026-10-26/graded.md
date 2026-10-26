### Workflows/sec (mean of valid runs; run count in brackets)

| depth | claim loops | legacy | owner | owner / legacy |
|--:|--:|--:|--:|--:|
| 500 | 1 | 28.57 (3) | 36.95 (3) | 1.29x |
| 1000 | 1 | 29.16 (3) | 37.02 (3) | 1.27x |
| 2000 | 1 | 28.56 (3) | 36.84 (3) | 1.29x |
| 500 | 2 | 38.83 (3) | 44.93 (3) | 1.16x |
| 1000 | 2 | 38.71 (3) | 45.01 (3) | 1.16x |
| 2000 | 2 | 39.32 (3) | 44.54 (3) | 1.13x |
| 500 | 4 | 41.95 (3) | 40.84 (3) | 0.97x |
| 1000 | 4 | 43.63 (3) | 42.00 (3) | 0.96x |
| 2000 | 4 | 43.86 (3) | 43.22 (3) | 0.99x |

### Owner / legacy over all depths

| claim loops | legacy wf/s | owner wf/s | ratio |
|--:|--:|--:|--:|
| 1 | 28.76 | 36.93 | 1.28x |
| 2 | 38.95 | 44.83 | 1.15x |
| 4 | 43.15 | 42.02 | 0.97x |

### Claims and takes per workflow, over all depths

| mode | claim loops | workflow claims | activity claims | empty claims | task claims | workflow takes | empty takes | claim ms | take ms |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| legacy | 1 | 4.00 | 3.00 | 0.00 | 7.00 | 0.00 | 0.00 | 4.66 | 0.00 |
| legacy | 2 | 4.00 | 3.00 | 0.01 | 7.00 | 0.00 | 0.00 | 6.80 | 0.00 |
| legacy | 4 | 4.00 | 3.00 | 0.60 | 7.00 | 0.00 | 0.00 | 10.23 | 0.00 |
| owner | 1 | 0.00 | 3.00 | 0.00 | 3.00 | 4.00 | 0.05 | 8.28 | 9.73 |
| owner | 2 | 0.00 | 3.00 | 0.01 | 3.00 | 4.00 | 0.06 | 13.54 | 13.84 |
| owner | 4 | 0.00 | 3.00 | 0.67 | 3.00 | 4.00 | 0.01 | 15.26 | 15.28 |
