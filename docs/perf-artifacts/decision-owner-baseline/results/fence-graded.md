### Mean decision persist latency, ms (runs in brackets)

| mode | writers 1 | writers 8-shared | writers 8-spread |
|:--|--:|--:|--:|
| `none` | 3.75 (3) | 6.19 (3) | 6.19 (3) |
| `generation` | 4.26 (3) | 8.23 (3) | 8.23 (3) |
| `owner` | 4.26 (3) | 9.86 (3) | 9.80 (3) |
| `generation_owner` | 5.14 (3) | 10.28 (3) | 11.75 (3) |
| `generation_plain_owner_row` | 4.84 (3) | 9.02 (3) | 9.65 (3) |
| `generation_owner_refence` | 5.99 (3) | 11.75 (3) | 11.05 (3) |

### Deltas

| delta | writers 1 | writers 8-shared | writers 8-spread |
|:--|--:|--:|--:|
| second row: `generation_owner` over `generation` | +20.7% | +24.9% | +42.7% |
| plain row lock: `generation_plain_owner_row` over `generation` | +13.5% | +9.6% | +17.2% |
| first row: `generation` over `none` | +13.7% | +32.9% | +32.9% |
| owner row alone: `owner` over `none` | +13.6% | +59.2% | +58.2% |
| the fix: `generation_owner` over `generation_owner_refence` | -14.2% | -12.5% | +6.3% |
