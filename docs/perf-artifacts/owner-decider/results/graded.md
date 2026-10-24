### Workflows/sec (mean of valid runs; run count in brackets)

| depth | claim loops | legacy | owner | owner / legacy |
|--:|--:|--:|--:|--:|
| 500 | 1 | 35.64 (3) | 45.55 (3) | 1.28x |
| 1000 | 1 | 34.38 (3) | 43.59 (3) | 1.27x |
| 2000 | 1 | 32.65 (3) | 41.20 (3) | 1.26x |
| 500 | 2 | 46.55 (3) | 55.25 (3) | 1.19x |
| 1000 | 2 | 45.44 (3) | 54.84 (3) | 1.21x |
| 2000 | 2 | 43.84 (3) | 51.75 (3) | 1.18x |
| 500 | 4 | 49.61 (3) | 52.89 (3) | 1.07x |
| 1000 | 4 | 50.52 (3) | 53.79 (3) | 1.06x |
| 2000 | 4 | 50.04 (3) | 51.20 (3) | 1.02x |

### Owner / legacy over all depths

| claim loops | legacy wf/s | owner wf/s | ratio |
|--:|--:|--:|--:|
| 1 | 34.23 | 43.45 | 1.27x |
| 2 | 45.28 | 53.95 | 1.19x |
| 4 | 50.06 | 52.63 | 1.05x |

### Claims and takes per workflow, over all depths

| mode | claim loops | workflow claims | activity claims | empty claims | task claims | workflow takes | empty takes | claim ms | take ms |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| legacy | 1 | 4.00 | 3.00 | 0.00 | 7.00 | 0.00 | 0.00 | 4.01 | 0.00 |
| legacy | 2 | 4.00 | 3.00 | 0.01 | 7.00 | 0.00 | 0.00 | 5.96 | 0.00 |
| legacy | 4 | 4.00 | 3.00 | 0.55 | 7.00 | 0.00 | 0.00 | 8.96 | 0.00 |
| owner | 1 | 0.00 | 3.00 | 0.00 | 3.00 | 4.00 | 0.09 | 7.21 | 8.16 |
| owner | 2 | 0.00 | 3.00 | 0.01 | 3.00 | 4.00 | 0.05 | 11.41 | 11.84 |
| owner | 4 | 0.00 | 3.00 | 0.55 | 3.00 | 4.00 | 0.00 | 13.12 | 13.20 |
