## Phase 3.x — document that `AeadCodec` leaves failure text in clear (issue #1920)

`AeadCodec` encrypts the six payload fields only. Failure strings, such as
`WorkflowFailed.error` and `WorkflowStarted.last_error`, stay in clear. The
docs did not name this boundary.

- **Decision.** Document the boundary. Do not encrypt error text.
- **Reason.** Erasure (issue #495) does not erase error text. The engine also
  writes the `error` columns as plain text. Encrypting only the event copy
  would leave the same text readable in those columns. It would also hide the
  error from operators.
- **Docs.** `docs/security-posture.md` lists the uncovered fields under "What
  the codec does not cover". It names the other clear copies. It tells
  operators to keep PII out of error strings. Four variants have a `details`
  field, which the codec encrypts. The other variants have none.
- **No code change.** The change adds no `WorkflowEvent` variant and no
  migration.

**Tests.** `failure_text_stays_in_clear_but_payload_fields_do_not` pins the
boundary for 14 failure variants. It also checks that each payload field is
encrypted and that the round trip is lossless.
`the_security_posture_doc_lists_failure_text_as_uncovered` fails when the doc
omits a variant and field pair.
