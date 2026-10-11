# Delta encoding of agent context windows — measured, declined

Issue #2009, done-when item 3. Source tree: `trunk-dev` at `8a40df5`.

## Question

Each `agent_model_turn` activity records the whole transcript in its input.
A run's history then grows with the square of its turn count. Does a delta
on the previous transcript cut the stored size enough to put in the event
log?

## Method

`autumn-harvest-agent/tests/history_delta_measure.rs` builds a 40-turn loop
from the real `ModelTurnRequest` and `ModelTurn` types. Each turn has one
model call and one tool call. A tool result is 220 words. The test measures
five stored forms with two kinds of filler text:

- **Vocabulary text.** Words from a 16-word list. It compresses well, like
  repetitive logs.
- **Random text.** Random lowercase words. A compressor finds only the
  repeats.

"Delta" replaces each transcript with the messages it adds to the one before.
The test is deterministic. Run it with `-- --nocapture` to print the table.

## Result

| Form | Vocabulary text | Share | Random text | Share |
|---|---:|---:|---:|---:|
| Plain JSON | 1,687,724 | 100.0% | 1,630,292 | 100.0% |
| Plain, delta | 190,948 | 11.3% | 185,562 | 11.4% |
| Plain, gzip | 213,288 | 12.6% | 781,339 | 47.9% |
| AES-GCM codec | 2,265,124 | 134.2% | 2,188,516 | 134.2% |
| AES-GCM, gzip | 1,705,154 | 101.0% | 1,646,703 | 101.0% |

The last two encoded transcripts share 75 bytes of about 100 KB. That is the
envelope header.

## Reading

- On plaintext the delta removes 89%, whatever the text.
- gzip matches it only on text that compresses well. On random text it
  removes 52%. Deflate looks back 32 KiB, and a late transcript is about
  100 KB, so it misses most repeats.
- Under the codec the delta saves nothing. Each payload field gets a fresh
  nonce, so equal transcripts give different ciphertext. Compression saves
  nothing either.

## Decision

**Declined for the event log.**

1. A delta over stored bytes saves nothing when the codec is on. A delta
   that works must run before the codec, on every write, every read and
   every replay. That puts a new stored form on the replay path, which the
   determinism model guards most closely.
2. The repeat comes from the agent layer. `run_segment` sends
   `progress.messages.clone()` to each turn. The agent layer can record only
   the messages a turn adds, and rebuild the window from history it already
   holds. That gets the same 89% with no engine change, and it works under
   the codec because the delta runs before encryption.
3. For cold storage, a `PartitionArchiver` backend can compress. Use a long
   window, such as zstd with `--long`, to catch the repeats that gzip misses.
   It does not help under the codec.

Follow-up: the agent-layer change in item 2 belongs to the agent adapter
(#1973), not to this issue.
