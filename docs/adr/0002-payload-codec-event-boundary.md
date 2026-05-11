# ADR 0002: Event payload codec boundary

## Status
Accepted

## Decision
Workflow event payload fields (`input`, `output`, `payload`, `details`) are serialized through a pluggable `PayloadCodec` and persisted with a `{ codec_id, data }` envelope.

## Consequences
- Non-default codecs can be introduced without schema changes.
- Replay fails fast with `UnknownPayloadCodec` if required codec is not registered.
- Default compatibility remains with `IdentityCodec`.
