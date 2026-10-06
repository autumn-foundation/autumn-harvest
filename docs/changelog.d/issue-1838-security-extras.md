## Security — audit hash chain, signed WASM modules, OTel Collector recipe (issue #1838)

ADR 0004 (`docs/adr/0004-security-extras.md`) records each decision. All
three features are opt-in. A deployment that sets nothing sees no change in
runtime behavior or wire bytes.

**Audit hash chain.** `HarvestBuilder::audit_export_chain_key` turns on a
keyed HMAC-SHA256 chain over the audit rows that the exporter sequences. The
exporter stamps the chain together with `export_seq`, under the cursor row
lock, so the insert path gets no new lock. Each row stores `chain_prev`,
`chain_newest_before` and `chain_hash`. The cursor stores a keyed checkpoint:
the chain start, the head link, its `seq` and the newest `occurred_at`. `audit_chain::verify_shard_chain` and
`verify_shard_chain_with` report changed rows, broken links, unchained rows,
gaps, a missing head and a missing or invalid checkpoint. The exporter
extends only a checkpoint that its key accepts. A missing or invalid
checkpoint stops the chain until an operator calls
`audit_chain::reanchor_shard_chain`. `HarvestBuilder::audit_export_chain_accept_key`
adds a key for a two-step rotation. With a retention cutoff, a gap goes to
`retention_gaps` only when the keyed `chain_newest_before` after it is old.
Exported records carry `chain_prev`, `chain_newest_before` and `chain_hash`.
These fields are omitted when absent. A key shorter than 32 bytes fails `try_build` with
`AuditChainKeyTooShort`. The runtime config and `claim_shard_chained` take an
`audit_chain::AuditChainKey`, which only `AuditChainKey::new` can build, so a
short key cannot reach the stamp path by any route.

**Signed WASM modules.** `HarvestBuilder::wasm_trusted_publisher_key` sets an
Ed25519 trust policy. A publisher signs `(domain, activity, hash)` with
`wasm_signing::sign_wasm_module`. `wasm_store::publish_signed_wasm_module`
checks and stores the signature. The worker checks it again before each run,
so a module written by direct SQL does not run. `try_build` refuses an
unsigned registration while a key is set. A signature replaces a stored one
only after a policy verifies it.

**OTel semantic conventions.** `telemetry::SEMCONV_METRIC_MAPPINGS` maps
`harvest.queue.dispatched` to `messaging.client.consumed.messages`. It maps
`harvest.activity.duration` to `messaging.process.duration`.
`docs/operations/otel-collector.md` publishes the Collector recipe. The ADR
declines native OTLP export, emitter renames and RPC mappings.

**Breaking change.** Code that builds these structs with a literal, or calls
these functions, must change:

- New public fields: `AuditExportRecord::{chain_prev, chain_newest_before,
  chain_hash}`,
  `AuditExportBuilderConfig::{chain_key, chain_accept_keys}`,
  `AuditExportRuntimeConfig::chain_key`,
  `WasmActivityRegistration::signature`, and new columns on
  `models::{AuditExportRow, AuditExportCursor, NewHarvestWasmModule}`.
- WASM registrations are now `(name, bytes, signature)` triples:
  `seed_registered_wasm_modules` (which also takes a trust policy),
  `HandlerRegistry::with_wasm_activities`,
  `HandlerRegistry::wasm_module_registrations` and
  `BuiltHarvest::wasm_module_registrations`.

New APIs: `audit_chain` (`AuditChainKey`, `ChainVerifier`, `ChainVerifyOptions`,
`ChainCheckpoint`, `verify_shard_chain_with`, `reanchor_shard_chain`),
`HarvestBuilder::audit_export_chain_accept_key`, `audit_export::claim_shard_chained`,
`AuditExportRuntimeConfig::claim`, `audit_export::runtime_chain_key`,
`AuditExportRecord::from_row`,
`wasm_signing`, `publish_signed_wasm_module`, `seed_signed_wasm_module`,
`resolve_active_wasm_version`, `WasmModuleStore::{set_trust_policy,
trust_policy}`, `BuiltHarvest::wasm_store`, and the builder errors
`WasmTrustedKeyInvalid` and `WasmModuleSignatureRejected`.

**Migrations.** `20261006013518_harvest_audit_chain` and
`20261006013725_harvest_wasm_module_signature` add nullable columns only. No
`WorkflowEvent` variant, no change to `harvest_events`, no replay impact.

**Tests.** Unit tests in `audit_chain.rs` (with a fixed test vector),
`wasm_signing.rs`, `audit_export.rs` and `builder.rs`. DB tests in
`tests/integration/audit_chain_tests.rs`, `tests/integration/audit_export_tests.rs`
and `tests/integration/wasm_activities_tests.rs`. The docs test
`tests/integration/otel_semconv_docs.rs` renders the Collector recipe from the
table and checks the ADR.
