### Mean decision persist latency, ms (runs in brackets)

| mode | writers 1 | writers 8-shared | writers 8-spread |
|:--|--:|--:|--:|
| `none` | 3.75 (3) | 6.19 (3) | 6.19 (3)* |
| `generation` | 4.28 (9) | 8.23 (3) | 8.23 (3)* |
| `owner` | 4.26 (3) | 9.86 (3) | 9.80 (3) |
| `generation_owner` | 5.15 (9) | 10.28 (3) | 11.75 (3) |
| `generation_plain_owner_row` | 4.78 (9) | 9.02 (3) | 9.65 (3) |
| `generation_owner_refence` | 5.99 (3) | 11.75 (3) | 11.05 (3) |

_A cell marked * reuses the shared case: the mode locks no owner row._

### Deltas of the pooled means

| delta | writers 1 | writers 8-shared | writers 8-spread |
|:--|--:|--:|--:|
| second row: `generation_owner` over `generation` | +20.4% | +24.9% | +42.7% |
| plain row lock: `generation_plain_owner_row` over `generation` | +11.8% | +9.6% | +17.2% |
| first row: `generation` over `none` | +14.1% | +32.9% | +32.9% |
| owner row alone: `owner` over `none` | +13.6% | +59.2% | +58.2% |
| the fix: `generation_owner` over `generation_owner_refence` | -14.1% | -12.5% | +6.3% |

### Deltas within each run, 1 writer

| run | `generation` ms | `generation_owner` ms | `generation_plain_owner_row` ms | second row | plain row lock |
|:--|--:|--:|--:|--:|--:|
| r0.txt | 4.12 | 5.18 | 5.08 | +25.9% | +23.3% |
| r1.txt | 4.33 | 5.44 | 4.64 | +25.6% | +7.1% |
| r2.txt | 4.34 | 4.81 | 4.80 | +10.8% | +10.5% |
| r3.txt | 4.28 | 5.08 | 5.10 | +18.6% | +19.1% |
| r4.txt | 4.65 | 5.01 | 4.63 | +7.7% | -0.5% |
| r5.txt | 3.69 | 5.53 | 4.82 | +49.9% | +30.7% |
| r6.txt | 4.51 | 5.26 | 4.67 | +16.6% | +3.4% |
| r7.txt | 4.11 | 4.84 | 4.85 | +17.8% | +18.0% |
| r8.txt | 4.47 | 5.22 | 4.46 | +16.6% | -0.4% |
| median | | | | +17.8% | +10.5% |

The second-row delta is 10% or more in 8 of 9 runs.
