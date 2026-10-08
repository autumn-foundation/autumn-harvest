# ADR 0003: Event payload codec boundary

## Status
Accepted

## Decision
Workflow event payload fields (`input`, `output`, `payload`, `details`) are serialized through a pluggable `PayloadCodec` and persisted with a `{ codec_id, data }` envelope.

## Consequences
- Non-default codecs can be introduced without schema changes.
- Replay fails fast with `UnknownPayloadCodec` if required codec is not registered.
- Default compatibility remains with `IdentityCodec`.

## Addendum (issue #608): operator read-path tolerant decode

The management API / Vantage UI read path consumes the same envelope contract
via `PayloadCodecs::decode_value_lossy` / `decode_error_string_lossy` — an
envelope-driven recursive walk sharing the exact envelope shape check with the
strict `decode_event` path, so the two can never disagree about what an
envelope is. Unlike replay, the read path degrades per field instead of
failing fast: an undecodable envelope is replaced with
`{"_harvest_undecodable": {"codec_id", "reason"}}` (bounded reasons, never
ciphertext or codec error text) so one bad key never blanks an operator's
triage screen. Read-path decoding is opt-in (default off), admin-gated,
audited (`payload.decode_read`), and never mutates stored bytes. See
`docs/operations/read-path-decode.md`.
