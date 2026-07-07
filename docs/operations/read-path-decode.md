# Read-path payload decoding (issue #608)

Issue #143 shipped `PayloadCodec` so embedders can encrypt workflow payloads
at rest, but the operator read surfaces (management API + Vantage UI) always
returned the stored bytes — on an encryption-at-rest deployment every triage
screen showed opaque ciphertext. Read-path decoding closes that gap: with an
explicit opt-in, admin reads are decoded server-side using the same in-process
codec registry the engine already holds. No sidecar codec server, no second
key distribution.

## Opt-in

```rust
let app = autumn_web::app().plugin(
    HarvestPlugin::new()
        .workflows(workflows![/* … */])
        .decode_payloads_on_read()          // issue #608 — default OFF
        .api("/api/harvest"),
);
```

Default **off**. With the flag off, no handler consults the codec registry and
responses are byte-for-byte identical to a deployment without the feature.

## Decode-only-when-admin (the AC6 gating decision)

The decoder is obtainable only when **both** hold:

1. the deployment-level opt-in above, and
2. the request passes `has_harvest_admin_access` — the exact predicate the
   `require_admin` route layer uses.

Ungated describe routes (`GET /workflows/{id}`, `/history`, `/result`,
`/stack`, `/history/export`) are deliberately **not** retro-gated behind
`require_admin` — that would break every existing non-encrypted deployment
whose dashboards call them without an admin session. Instead, a non-admin
caller on those routes receives exactly today's bytes (ciphertext when
encryption is on) — no new information is exposed. This means the **same URL
returns different bytes depending on the session**: plaintext for admins,
stored bytes for everyone else.

Note: `HarvestPlugin::api_with_auth(..)` sets the embedder auth boundary,
which makes every request that reaches the management API an admin — decode
then applies to all of them. That is the boundary's existing contract for the
whole management API, not a new widening.

## Decoded surfaces

| Surface | Notes |
|---|---|
| `GET /workflows/{id}/result` | `output` (JSONB) + `error` (TEXT) — both the zero-wait snapshot and the long-poll path |
| `GET /workflows/{id}/history/export` | **`payload_policy=full` only** — a Redacted export never decodes (decoding only to redact would be pointless plaintext exposure) and writes no decode audit row |
| `GET /admin/history/exports` | Same Full-only rule; one audit row per request across all export entries |
| `GET /workflows/{id}` (describe) | execution `input`/`output`/`memo`/`search_attrs`/`error`, embedded history page, `last_completion_result`/`last_error` |
| `GET /workflows/{id}/history` | each event's `data` payload |
| `GET /workflows/{id}/stack` | pending-activity heartbeat checkpoints, plus a decoded `input` field on each pending activity that is present **only** when decoding is active for the request |
| `GET /dead-letters` | each row's `input` (JSONB) + `error` (TEXT) |
| SSE `GET /executions/{id}/events/stream` | every frame (backfill + live), decoder resolved once at stream open |
| Vantage UI | workflow detail page (input/output cards, timeline, blocked-on panel) and the DLQ page |

Stored rows are **never** mutated — decoding happens on the in-memory
response copy only. The append-only event invariant is untouched.

## Graceful per-field degradation

A decode failure never fails the response. The affected field is replaced
with a typed marker and everything else still returns `200`:

```json
{"_harvest_undecodable": {"codec_id": "kms-v1", "reason": "codec_error"}}
```

Bounded reasons: `unknown_codec` (id not registered — e.g. rotated away),
`invalid_base64`, `codec_error` (bad key / corrupt ciphertext; the codec's own
error text is never echoed), `invalid_json`. Keep old codec ids registered or
their fields degrade to markers.

## Audit contract

Every request that actually decoded **or** marked at least one envelope
writes one best-effort `payload.decode_read` audit record (actor, route,
target execution — never payload content). A request that finds no envelopes
writes no row, so flipping the flag on a non-encrypted deployment is
audit-silent. The SSE stream is the one exception: frame counts are
unknowable up front, so it audits once at stream open whenever decode mode is
active for that stream (mirroring `execution.stream.open`). Audit inserts are
best-effort — an insert failure never fails the read.

## Known limits

- `GET /dead-letters/aggregate` is unchanged: `failure_signature` groups over
  the stored (possibly ciphertext) error first-line. Counts and ids remain
  correct; signatures on an encrypted deployment group by ciphertext shape.
- The engine's own write paths currently persist with identity codecs;
  envelopes appear wherever a writer (e.g. the client handle path or a future
  write-side integration) stored them. The read path decodes any envelope it
  finds and passes everything else through untouched.
- Offload envelopes (`_harvest_offload_envelope`, issue #524) and erasure
  tombstones (`_harvest_erased`, issue #495) pass through untouched.
