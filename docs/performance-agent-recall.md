# Long-horizon recall: what a durable agent forgets across ops

This page measures how much a durable `Agent` forgets across ops (issue
#2455). It gives one number for each memory config, so a change to memory
can show that it helps. The design is in
[DESIGN-2455](design/DESIGN-2455.md).

## 1. The method

- **Fixture.** `autumn-harvest-agent/src/recall/fixture.json`. Each item is
  original. No third-party dataset is in it. An item states a fact in an
  early op (`Remember: the <key> is <value>.`). It asks for the fact in its
  last op (`What is the <key>?`). The other ops are small talk. Each value
  is a number, so case cannot change a match.
- **One op per session.** Each item runs on its own durable agent. Each op
  is one user message.
- **Configs.** Each config keeps a window of 2 messages. The fixture
  assumes this window. With a wider window, most facts stay in the window.
  - `window`: the window only.
  - `notes`: progress notes (#2453), `ContextReset::new(1, 90)`.
  - `memory`: store memory. The model sees the `memory` tool.
  - `episodes`: store memory and the episode log, with a pass after each op
    (#2454), `EpisodeSettings::default().every(1)`. The model also sees the
    `memory` tool, so compare `episodes` with `memory`, not with `window`.
- **Score.** `ExactMatch` (#2441) on the answer of the last op. It trims
  spaces only. An answer of `7319.` does not match `7319`. A match is 1. A
  miss is 0. A failed item has no score and reads `error`. Each mean shows
  its scored items, for example `(6/6)`.
- **Driver.** `recall::sweep`.

The item `parcel-number` is the control. Its fact is one op before the
question, so the window keeps it. It must score 1 under `window`. Then a 0
on another item is a loss, not a broken harness.

## 2. The scripted check (CI)

`ReaderModel` answers only from its request. CI runs it in
`autumn-harvest-agent/tests/recall.rs`. It proves that the harness sees the
loss. `ReaderModel` saves each fact and never calls the `memory` tool. So
`notes` and `episodes` score 1 by construction, and `memory` scores as
`window`. A real model can score less.

```sh
cargo run -p recall-eval -- --scripted
```

### ReaderModel (scripted), 2026-11-04

- Model: `ReaderModel (scripted)`
- Date: 2026-11-04
- Config: window 2 messages. `notes` adds `ContextReset::new(1, 90)`. `memory` adds store memory and the `memory` tool. `episodes` adds store memory and `EpisodeSettings::default().every(1)`.
- Fixture: the built-in fixture, 6 items. Scorer: `ExactMatch`.

| item | window | notes | memory | episodes |
|---|---|---|---|---|
| vault-code | 0 | 1 | 0 | 1 |
| badge-number | 0 | 1 | 0 | 1 |
| server-port | 0 | 1 | 0 | 1 |
| meeting-room | 0 | 1 | 0 | 1 |
| parcel-number | 1 | 1 | 1 | 1 |
| wifi-pin | 0 | 1 | 0 | 1 |
| mean | 0.17 (6/6) | 1.00 (6/6) | 0.17 (6/6) | 1.00 (6/6) |

## 3. Real-model runs

A real-model run costs money, so it runs by hand, not in CI. Set
`ANTHROPIC_API_KEY`, then run:

```sh
cargo run -p recall-eval -- --model claude-opus-5-5 --db recall.db
```

The command prints one section with the model, the date, the config and the
table. Add the section below, newest first. Keep each older section.

On a rerun with the same `--db` file:

- The same item, profile and model reuse their agents. A finished item calls
  no model.
- A changed item, window or model gets new agents, so old rows never show
  under a new heading.
- A failed item stays failed. Use a new file to run it again.
- `memory` and `episodes` keep their stores in memory. After a crash, run
  them again on a new file.

No real-model run is recorded yet.
