# ADR 0004 — Security extras: audit chain, signed WASM modules, OTel semconv

**Status**: Accepted
**Date**: 2026-10-06
**Issue**: [#1838](https://github.com/autumn-foundation/autumn-harvest/issues/1838)
(parent [#1786](https://github.com/autumn-foundation/autumn-harvest/issues/1786))

---

## Context

The September 2026 gap analysis found three P3 gaps:

1. The audit log goes to a SIEM, but the database copy is not tamper-evident.
2. WASM modules have a content hash, but no publisher signature.
3. Metrics do not follow the OTel semantic conventions, and Harvest has no
   native OTLP export.

The issue asks for each item to be implemented with tests, or declined here.
This ADR records the decision for each item. It also records the parts of
each item that are declined.

---

## 1. Tamper-evident audit log

**Decision: implemented.** An optional keyed hash chain covers the audit rows
that the exporter sequences.

- The exporter already gives each row a dense per-shard `export_seq` under the
  cursor row lock. The chain is stamped at that same point. The audit insert
  path gets no new lock and no new work.
- Each link is `HMAC-SHA256(key, domain || chain_prev || newest_before ||
  canonical row)`. The key lives outside the database, so a database writer
  cannot forge a link. `newest_before` is the newest `occurred_at` of every
  row chained before this one.
- Each row stores `chain_prev` and `chain_hash`. A row stays verifiable
  after retention deletes its predecessor.
- The cursor stores a keyed checkpoint: the chain start, the newest link, its
  `seq` and the newest `occurred_at` of the chain, under one HMAC. A database writer cannot move
  it. So stripped links and a deleted tail show.
- The exporter extends only a checkpoint that its key accepts. Otherwise a
  writer could move the head and have the exporter sign it. A missing or
  invalid checkpoint stops the chain until an operator calls
  `audit_chain::reanchor_shard_chain`.
- A key rotation uses accept keys. The exporter accepts a checkpoint under an
  accept key, but signs only with the active key.
- `audit_chain::verify_shard_chain` reports changed rows, broken links,
  unchained rows, missing sequence numbers, a missing newest row, and a
  missing or invalid checkpoint.
- With a retention cutoff, a gap counts as retention, not as a finding, when
  the keyed `newest_before` of the row after it is old. Retention keeps some
  old rows, so these gaps are normal. The ages of the surviving rows are no
  proof, because a late commit or a planted row can carry an old
  `occurred_at`.
- Exported records carry `chain_prev`, `chain_newest_before` and
  `chain_hash`. A SIEM that holds the
  key can verify the chain on its side. A deployment without a key ships the
  same bytes as before.
- Turn it on with `HarvestBuilder::audit_export_chain_key`. The key must be at
  least 32 bytes.

**Limits.**

- The chain needs audit export. Nothing protects a row from its insert until
  the next export tick. A database writer can change or delete it in that
  window, and nothing detects that. An insert-time chain would close the
  window; this ADR declines it below.
- A process that holds the key can forge links. The chain protects against
  database-level tampering, not a compromised Harvest process.
- A writer who removes every link and the whole checkpoint leaves a table
  that looks unchained. A writer can also delete rows that retention deletes
  within the hour. Only the SIEM copy detects these cases.
- Each sequenced row is written twice: once for `export_seq`, once for the
  chain columns. Only a deployment with a chain key pays this cost.
- Every exporter must hold the same key. Set it after a rolling upgrade
  ends. Rotate in two steps with an accept key.
- A re-anchor starts a new chain after the sequenced rows. It never changes
  a sequenced row, so a redrive stays byte-identical. The verifier does not
  check the rows before the new start. An operator compares them with the
  SIEM copy first.
- A signed checkpoint does not prove that it is the newest one. A writer can
  restore an older table and cursor. The verifier detects that only with a
  `known_head`: the newest link that an earlier check or the SIEM saw.

**Declined:**

- *A chain at insert time.* It would cover a row from its insert, not from
  the next export tick. But it needs one lock per shard on every audited
  request. The export-time chain adds no insert cost, and the window before
  the first tick is a stated limit.
- *A database trigger that computes the chain.* It needs `pgcrypto` and the
  same per-shard lock. It also puts the key in the database.
- *Signed export batches.* Batches already carry an HMAC signature (issue
  #605). The chain adds what batch signatures lack: evidence inside the
  database.
- *An append-only trigger on `harvest_audit_log`.* Retention and export
  writes need exceptions. The chain detects the same edits. It can follow
  later, as issue #1817 did for `harvest_events`.

## 2. Signed WASM modules

**Decision: implemented.** An optional Ed25519 trust policy covers WASM
activity modules.

- A publisher signs `domain || name || hash` offline with
  `wasm_signing::sign_wasm_module`. The name is in the message, so a
  signature cannot move to another activity.
- Workers hold only public keys, set with
  `HarvestBuilder::wasm_trusted_publisher_key`. A stolen worker config or
  database credential cannot sign a module.
- `wasm_store::publish_signed_wasm_module` checks the signature before it
  writes. The worker checks it again before each run, cache hit or not. So a
  module written by direct SQL, or by the unsigned publish call, does not run.
- With a trusted key set, `try_build` refuses a registered module that has no
  valid signature.
- A signature replaces a stored one only after a trust policy verifies it.
  So an unverified signature cannot disable a signed module. A re-signed
  module after a key rotation runs again.
- Without a trusted key, behavior is unchanged.
- The policy covers WASM activity modules. Hot-swap workflow modules keep
  their own HMAC check.

**Limits.**

- A signature has no version and no expiry. Anyone who can write the module
  table can reactivate any version a trusted key signed. Revoke by removing
  the key and re-signing the versions you keep.
- `wasm_signing` re-exports `SigningKey` and `VerifyingKey` from
  `ed25519-dalek` 2. A major version bump of that crate is an API change
  here.
- The signing helper needs the `wasm-activities` feature, so a publisher
  tool compiles `wasmtime`.

**Context correction.** No HTTP route publishes a WASM module today. Publish
is a library call. A future route under `/modules` or `/admin/modules` needs
the `admin` token scope (`ADMIN_SCOPE_PREFIXES`). So a stolen `mutate` token
cannot publish code. The signature is defence in depth.

**Declined:**

- *Sigstore or cosign.* Keyless signing needs a network trust root and an
  OIDC identity at publish time. That is a large new dependency for an opt-in
  R&D feature. Ed25519 keys cover the threat with one crate that the lockfile
  already holds.
- *HMAC, as hot-swap modules use.* A symmetric key on every worker can also
  sign. A worker compromise would then defeat the control.
- *A hash allow-list.* Every new module version would need a config change
  on every worker.

## 3. OTel semantic conventions

**Decision: implemented** as a Collector mapping. Native OTLP export is
declined.

- `telemetry::SEMCONV_METRIC_MAPPINGS` maps `harvest.*` metrics to the OTel
  messaging conventions where the meaning matches. It maps
  `harvest.queue.dispatched` to `messaging.client.consumed.messages`. It maps
  `harvest.activity.duration` to `messaging.process.duration`.
- [`docs/operations/otel-collector.md`](../operations/otel-collector.md)
  publishes the Collector recipe. It scrapes the Prometheus endpoint and
  copies each mapped series under its semconv name. It also sets the unit
  each semconv metric requires, because the Prometheus scrape carries none. A
  test renders the recipe from the table, so the two cannot drift.
- Harvest keeps its own metric names. Dashboards, alerts and SLO rules do not
  change.

**Declined:**

- *Native OTLP export.* The workspace has no `opentelemetry` crate. The
  `metrics-rs` adapter already feeds any `metrics` exporter, and the Collector
  speaks OTLP to every backend. A native exporter would add a large dependency
  tree for no new capability.
- *Renaming metrics in the emitter.* It breaks every existing dashboard and
  alert. Emitting both names doubles the series count.
- *RPC conventions.* Harvest emits no RPC or HTTP server metric. Autumn-web
  owns the HTTP metrics.
- *Other messaging mappings.* Connector metrics do not know the broker, so
  `messaging.system` would be wrong. Queue depth and schedule-to-start have no
  semconv equivalent. They keep their Harvest names.

---

## Consequences

- Two migrations add nullable columns: the audit chain columns and the WASM
  module `signature`. Neither needs a table rewrite.
- New public struct fields and changed function signatures break code that
  builds these structs by hand. The changelog fragment lists them.
- No `WorkflowEvent` variant and no change to `harvest_events`. Replay is not
  affected.
- All three features are opt-in. A deployment that sets nothing sees no
  change in runtime behavior or wire bytes.
