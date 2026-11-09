# DESIGN-2484: a verdict and a key for an unanswered durable peer

Issue #2484. Related: #2269, #2483.

## 1. Problem

A `delegate` call to a durable peer sends a message, then waits for the
reply. When no reply comes, the record has no verdict and the model reads
a plain string. A caller cannot match the case in code. The receiver can
still apply the message, so a second ask can run the task twice. The
record gives no handle to see that two asks are the same.

## 2. Facts

- `await_reply` in `workflow.rs` returns `Err(String)` in three cases:
  the deadline passes, a caller rejects the promise, or a person
  interrupts the wait. Only a durable agent races an interrupt.
- `peer_outcome` records each of them as `stop: None, refusal: None`.
- Every refusal before the send uses `refuse`. The model reads
  `{"error": ..., "verdict": ...}`.
- The send uses the key `<from>:<seq>`. The engine and the receiver
  dedupe on it. A new ask takes a new `seq`, so a new key.
- `nested_units` counts a record with a `run_id`. A sent message counts
  one unit, so its record keeps the `run_id`.
- `note_serial` skips a record with a refusal. Today an unanswered
  delegation gets `serial: durable_peer`.
- A durable agent keeps no records. Only an `agent_loop` run reports
  them. Its wait has no interrupt branch.
- The anonymised evaluation clears each delegation `run_id` when the
  recording has none. An `agent_loop` sender uses its run id as `from`,
  so the key holds a run id too.
- The tool result enters the transcript. Strict replay compares each
  model input, so the repository gates each change of one with
  `ctx.patched`.
- Strict replay does not accept a patch marker right after a fired wait
  deadline. The worker path accepts it.
- The receiver keeps the applied numbers of each sender in
  `AgentState::inbound`. A `contains` there is sound.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the verdict goes | a new `NoReply` field · **new `Refusal` variants** |
| How many verdicts | one `no_reply` · **one per cause: `reply_timeout`, `reply_rejected`, `reply_interrupted`** |
| How a caller tells them apart | match each code · **`Refusal::unanswered()`, plus each code** |
| Where the key goes | the model content · **`DelegationRecord::idempotency_key`** |
| Which records hold a key | unanswered only · **each record of a message that went out** |
| A failed send | key it · **no key: nothing went out** |
| The model text | the old string · **the verdict text, the cause, and a warning that a new ask can run the task twice** |
| Old histories | change the text · **a patch gate** |
| Where the gate goes | after the wait, only with no reply · **after the send, before the wait** |
| How a caller uses the key | read the history of the peer · **`Inbound::applied_key` on the peer state** |
| A rejection reason | as it is · **cut to 256 characters for the model** |
| The record of an old history | keep it empty · **the verdict and the key: the report is not a model input** |

## 4. Reverse brainstorming

"How could this change make things worse?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A parent stops counting the unit of an unanswered child. | The record keeps its `run_id`. A unit test proves the count. |
| An unanswered delegation loses `serial: durable_peer`. | `note_serial` skips only a verdict that is not `unanswered()`. A test proves it. |
| A replay of an old run sends the model new text, and diverges. | The patch `agent_peer_reply_verdict` gates the text. A replay test proves both branches. |
| A run that waits across the upgrade gets the new text. | The gate comes before the wait. Such a run reads no marker there. |
| A huge rejection reason fills the request. | The model reads 256 characters of it. The legacy text keeps it whole, so replay holds. |
| An anonymised report leaks a run id through the key. | The scrub clears the key with the `run_id`. A unit test proves it. |
| An old worker cannot decode a report with a new verdict. | The guide says to roll out to every worker first, as for `SerialReason`. |
| A caller reads `reply_timeout` as "nothing ran" and asks again. | The text and the guide say that the peer can still apply the message. |
| The model sees an internal id. | The key stays out of the model content. |
| A new variant drifts from the guide. | A unit test reads the guide for each code, through an exhaustive match. |

## 5. Six thinking hats

- **White (facts).** Section 2.
- **Red (feelings).** A silent timeout feels like a lost request. An
  operator who sees a named verdict and a key trusts the report.
- **Black (risks).** Three new public variants grow the API. A report
  with them does not decode on an old worker. The record of an
  unanswered call now holds a refusal, so code that reads "refusal means
  not run" is wrong for it.
- **Yellow (benefits).** Every delegation outcome has a stable code. A
  caller can dedupe on the key. The model reads why a new ask is risky.
- **Green (ideas).** `Inbound::applied_key` makes the key useful at
  once. Reuse the key for a retry, so the receiver dedupes it. That needs
  a model-visible handle and a new op. It is out of scope.
- **Blue (process).** Write the failing tests first. Then add the
  variants, the key and the gate. Then update the guide.

## 6. Decision

- `Refusal` gets `ReplyTimeout`, `ReplyRejected` and
  `ReplyInterrupted`. `Refusal::unanswered()` is `true` for them only.
- `DelegationRecord::idempotency_key` holds `<from>:<seq>` for each
  message that went out.
- An unanswered call records its verdict, its `run_id`, its draws and
  its key. The model reads `{"error": ..., "verdict": ...}`.
- `Inbound::applied_key` finds a sent message at the receiver.
- The patch `agent_peer_reply_verdict` gates the new model text. A sent
  message asks it before its wait.

## 7. Acceptance

1. A durable-peer timeout is in `AgentReport::delegations` with the
   verdict `reply_timeout`.
2. A rejected reply has the verdict `reply_rejected`.
3. Each record of a sent message holds its key. A refusal before the
   send holds none.
4. The unit of an unanswered delegation still counts.
5. An old history replays with the old text.
6. A caller finds a late message at the receiver by its key.
