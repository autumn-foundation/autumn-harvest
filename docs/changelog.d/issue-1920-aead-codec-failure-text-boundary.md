## Phase 3.x — document that `AeadCodec` leaves failure text in clear (issue #1920)

`AeadCodec` encrypts the six payload fields only. Failure strings, such as
`WorkflowFailed.error` and `WorkflowStarted.last_error`, stay in clear. The
docs did not say so.

- **Decision.** Document the boundary. Do not encrypt error text. Erasure
  (issue #495) keeps operational error text on purpose, and the denormalized
  `error` columns stay in clear. Encrypting only the event copy would leave
  the PII readable and break the operator view.
- **Docs.** `docs/security-posture.md` now lists failure text under "What the
  codec does not cover". It tells operators to keep PII out of error strings
  and to use `details`, which the codec encrypts.
- **No code change.** No new `WorkflowEvent` variant. No migration.

**Tests.** `failure_text_stays_in_clear_but_payload_fields_do_not` pins the
boundary for four failure variants. `the_security_posture_doc_lists_failure_text_as_uncovered`
fails when the doc stops naming the uncovered fields.
