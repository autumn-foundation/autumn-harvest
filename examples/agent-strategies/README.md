# agent-strategies: one task under four inference strategies

This example runs one task under four strategies of
[epic #2438](https://github.com/autumn-foundation/autumn-harvest/issues/2438).
It uses SQLite and a scripted model, so it needs no API key and no network.

```sh
cargo run -p agent-strategies
```

| Strategy | What it does here |
|---|---|
| single draw | One cheap draw. It is wrong, so no answer passes. |
| best-of-3 | Three cheap draws. `ExactMatch` scores each, and the best one wins. |
| vote | Up to five cheap draws, keyed by the last number. It stops when the leader cannot lose. |
| cascade | The cheap model first, then the strong model when the cheap answer fails. |

The example prints the selected answer, each draw and the cost of each
run. The cheap model costs 5 µ$ a draw, and the strong model 300 µ$.

- Each draw is a durable child run, so a crash costs one draw.
- The sampling policy of the kind allows each strategy. By default a kind
  has none, and each run is one draw.
- `cargo test -p agent-strategies` checks each selected answer, and that
  the best-of-3 cost is the sum of its three draws.

Section 28 of [the agent guide](../../docs/agent-adapter.md#28-inference-strategies)
compares the strategies. [ADR 0017](../../docs/adr/0017-inference-strategies.md)
records the design choices.
