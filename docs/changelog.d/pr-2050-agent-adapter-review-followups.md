## Fix — Agent adapter: failed-edit keys, tick input cap, final-turn delivery, follow-up size (issue #2050)

Four items of the review rounds on autumn-foundation/autumn-harvest#2046.

- **Failed memory edit.** `InMemoryMemoryStore::apply` now keeps the first
  result of an `EditKey`, an error included. A retry returns that error. It
  cannot remove an entry that another run added after the crash. The
  `MemoryStore::apply` contract says the same for durable stores.
- **Tick input cap.** `HeartbeatTask::max_request_bytes` reaches
  `AgentTask`. A large tick request ends as `TranscriptFull`, not as
  `PayloadTooLarge`.
- **Final turn only.** Delivery uses the text of the turn that ended the
  segment. An empty final turn sends nothing. An empty `output_capped` turn
  sends the notice.
- **Follow-up size.** The 2 000 character limit counts the untrimmed prompt.

**No migration. No new `WorkflowEvent` variant.** `HeartbeatTask` gains an
optional field that is absent from JSON when unset.

**Tests.** `a_retried_failed_edit_returns_its_first_error`,
`a_tick_carries_the_worker_input_cap`,
`an_empty_final_turn_does_not_deliver_an_earlier_aside` and
`padding_counts_toward_the_prompt_limit`. Each failed before its fix.
Items 1 and 5 of the issue shipped in #2046.
