## Fix — callback 4xx and Retry-After, spread DLQ redrive, CLI timeouts (issue #1832)

Three fixes from the September 2026 resilience gap analysis (epic #1786).

**Completion callbacks: permanent 4xx and `Retry-After`.**
`classify_outcome` dead-letters a permanent 4xx response on the first
attempt. 408, 421, 425 and 429 are transient and still back off. The DLQ reason
stays `CallbackDeliveryExhausted`, with `attempts: 1`. An alert on that
reason now also fires on one 400.

A `Retry-After` hint is the minimum backoff for the next retry. All forms
are read: delta-seconds, IMF-fixdate, RFC 850 and asctime. The engine's
`Retry-After` ceiling (`DEFAULT_RETRY_AFTER_CEILING`, 15 minutes, issue
#744) caps it. `ReqwestCallbackDeliverer` reads the header.
`parse_retry_after` is public, so a custom deliverer can do the same.

**DLQ: bulk redrive spreads `scheduled_at`.** `POST /dlq/redrive` and
`POST /dead-letters/replay` no longer make every task due at once. Row `i`
of `n` gets the slot `[i, i+1) * window / n`. Its id jitters it inside the
slot. The base instant is the database clock (issue #1807).

- The new optional `spread_secs` field sets the window, up to 3600 s.
- The default is 60 s for a full 1000-row batch, scaled down for a smaller
  batch. `0` makes every task due at once.
- On a sharded cluster, every shard uses the window of the whole call.
- A completion-callback dead letter gets its next delivery attempt in the
  window (`redrive_delivery_at`).
- The CLI flag is `--spread-secs`.
- New core API: `redrive_spread_window`, `redrive_spread_offset`,
  `redrive_schedule`, `replay_dead_letter_at`, `redrive_dead_letter_at`,
  `replay_dead_letter_batch` and `redrive_delivery_at`.

No migration and no new `WorkflowEvent` variant.

**CLI and dev HTTP timeouts.** Every CLI request has a timeout:
`--http-timeout-secs` (env `HARVEST_HTTP_TIMEOUT_SECS`, default 30 s).

- The connect phase stops after 10 s at most.
- `workflow update --wait completed` gets its own `--timeout-secs` plus
  10 s. A bulk DLQ command that writes gets at least 300 s.
- `events tail` applies the timeout to the response headers. After that,
  it stops only when no data arrives for 60 s or the timeout, whichever is
  longer.
- The TUI limits one refresh to 5 s.
- The dev runtime's readiness probe stops each attempt after 2 s.

**Breaking for embedders.**

- `DeliveryAttempt` has a new public field, `retry_after`. A struct literal
  outside this repository must add `retry_after: None`. The constructors
  `success` and `transport_error` do not change.
- `CliError` has two new variants, `Timeout` and `ConnectTimeout`. An
  exhaustive `match` must handle them.

**Tests.** Each part has a failing test first.

- Core unit tests cover the permanent 4xx, the `Retry-After` floor and
  ceiling, all date forms, and the spread math.
- `completion_callback_tests` has three DB tests: a 400 is sent once, a 429
  with `Retry-After: 10` waits 10 s, and a bulk replay spreads callback
  dead letters.
- `redrive_tests` has four DB tests. A 1000-row redrive and a 1000-row
  replay spread over 60 s. The default window and `spread_secs: 0` are
  checked too.
- The plugin has a deliverer test with a loopback server, API body tests,
  fan-out window tests, three HTTP tests in `dlq_redrive_integration`, and
  a black-hole test for the dev readiness probe.
- The CLI has black-hole tests for `execute` and `events tail`, a live
  stream that outlasts the timeout, and the per-command timeouts.
