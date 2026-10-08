# Read-path payload decoding

Issue #143 shipped `PayloadCodec` so embedders can encrypt workflow payloads
at rest, but the operator read surfaces (management API + Vantage UI) always
returned the stored bytes — on an encryption-at-rest deployment every triage
screen showed opaque ciphertext. Read-path decoding (issue #608) closes that
gap: with an explicit opt-in, admin reads are decoded server-side using the
same in-process codec registry the engine already holds. No sidecar codec
server, no second key distribution.

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

Both export routes load the stored history **raw** (`store::load_history_undecoded`)
rather than through the strict identity-only decode path, so an encrypted
deployment can always export: with the flag off (or a non-admin caller) a
Full export carries the stored envelopes verbatim — the same ciphertext the
describe/history/SSE surfaces already show — and a Redacted export replaces
each payload field wholesale (envelope included; its `payload_digest` hashes
the stored, possibly-ciphertext bytes). Before this fix (PR #936 review) the
strict loader failed the whole export with `UnknownPayloadCodec` before
either policy could run. Identity-codec deployments store no envelopes, so
their exports are byte-identical to before.
| `GET /workflows/{id}` (describe) | execution `input`/`output`/`memo`/`search_attrs`/`error`, embedded history page, `last_completion_result`/`last_error` |
| `GET /workflows/{id}/history` | each event's `data` payload |
| `GET /workflows/{id}/stack` | pending-activity heartbeat checkpoints, plus a decoded `input` field on each pending activity that is present **only** when decoding is active for the request |
| `GET /dead-letters` | each row's `input` (JSONB) + `error` (TEXT) |
| SSE `GET /executions/{id}/events/stream` | every frame (backfill + live), decoder resolved once at stream open |
| Vantage UI | workflow detail page (input/output/memo/search-attrs cards + error banner, timeline event payloads, blocked-on heartbeat checkpoints) and the DLQ page (input, error, last-10 events). The detail page decodes **only what it renders**: the pending-activity `input`, pending-signal payloads, and the attempts/signals panel event copies are never displayed, so they are not decoded and never count toward the audit outcome (PR #936 review). |

### Deliberately undecoded surfaces

Two payload-carrying read surfaces are **not** decoded, by design, even with
the flag on and an admin session:

| Surface | Rationale |
|---|---|
| `GET /workflows` (the list endpoint, including the stalled-workflow loader) | Row counts are unbounded and each row flattens a full execution (input/output/memo/search_attrs/error); per-row decoding would put an O(rows × fields) codec cost on the fleet-navigation path. Lists are navigational — click through to the describe/detail views, which decode. |
| `GET /dags/{dag_name}/runs` | Same shape and rationale: an unbounded list of full execution rows used for navigation; the per-run detail surfaces decode. |

On an encrypted deployment these list responses show stored envelopes for the
same executions whose detail views show plaintext — that is expected, not a
bug.

Stored rows are **never** mutated — decoding happens on the in-memory
response copy only. The append-only event invariant is untouched.

## Provenance caveat — decoded values are not authenticated

A codec envelope is purely self-describing (`{"_harvest_codec_envelope": 1,
"codec_id", "data"}`): nothing binds it to "written by the engine's encode
path". The identity codec is always registered, and workflow inputs, signal
payloads, and activity error strings are caller-influenced — so **any writer
who can start a workflow (or shape an error string) can seed envelope-shaped
data**, and every decoded surface will render whatever the registered codec
yields for it, with no provenance indicator. During incident triage this
means a decoded view can display attacker-chosen "plaintext" that differs
from the bytes the workflow actually consumed (the stored bytes themselves
are what the engine ran on and are never altered).

When byte-level fidelity matters — forensics, disputes — read the stored
bytes instead: turn the flag off, use a non-admin session on an ungated
route, or use an undecoded surface such as the `GET /workflows` list. A
per-request raw escape hatch (`?decode=false`) is a possible follow-up, not
part of this slice.

Relatedly, nothing in the `PayloadCodec` trait requires authenticated
encryption; prefer an AEAD codec so tampered ciphertext fails decode outright
(the `codec_error` vs `invalid_json` reason split is visible only to admin
sessions, but an AEAD codec removes the distinction as a signal entirely).

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
active for that stream (mirroring `execution.stream.open`) — every stream
(re)open writes its own row, including automatic EventSource reconnects, so
reconnect-happy clients multiply rows. Audit inserts are best-effort — an
insert failure never fails the read.

Two attribution details:

- `GET /workflows/{id}/result` audits the **requested** execution id — the
  durable handle the operator asked about — even when the returned output
  belongs to a ContinuedAsNew/retry successor the endpoint's chain walk
  resolved (same logical run, same shard).
- Vantage UI page renders write their rows with `source: "ui"` (like every
  other UI-originated audit row); API reads carry the header-derived source
  (default `"api"`).

## Known limits

- `GET /workflows` (list, incl. the stalled loader) and
  `GET /dags/{dag_name}/runs` are deliberately undecoded — see "Deliberately
  undecoded surfaces" above.
- `GET /dead-letters/aggregate` is unchanged: `failure_signature` groups over
  the stored (possibly ciphertext) error first-line. Counts and ids remain
  correct; signatures on an encrypted deployment group by ciphertext shape.
- The engine's own write paths currently persist with identity codecs;
  envelopes appear wherever a writer (e.g. the client handle path or a future
  write-side integration) stored them. The read path decodes any envelope it
  finds and passes everything else through untouched.
- Because the walk is envelope-driven, business data stored as plaintext that
  happens to be byte-for-byte a codec envelope — at any nesting depth — is
  transformed on the decoded view: decoded when its `codec_id` is registered,
  replaced with an `_harvest_undecodable` marker when not (see the provenance
  caveat above; the stored bytes are never altered).
- Offload envelopes (`_harvest_offload_envelope`, issue #524) and erasure
  tombstones (`_harvest_erased`, issue #495) pass through untouched. In
  particular, decode-on-read never inflates offload refs: an offloaded field
  still surfaces as its opaque offload reference (and the offloaded blob's
  content remains ciphertext under an encrypting codec) even with decoding
  active.
