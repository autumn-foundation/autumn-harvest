## Security — audit hash chain, signed WASM modules, OTel Collector recipe (issue #1838)

ADR 0004 (`docs/adr/0004-security-extras.md`) records each decision. All
three features are opt-in. A deployment that sets nothing sees no change.

**Audit hash chain.** `HarvestBuilder::audit_export_chain_key` turns on a
keyed HMAC-SHA256 chain over the audit rows the exporter sequences. The chain
is stamped with `export_seq`, under the cursor row lock, so the insert path
gets no new lock. Each row stores `chain_prev` and `chain_hash`. The cursor
stores `chain_head`. `audit_chain::verify_shard_chain` reports changed rows,
broken links, sequence gaps and a missing newest row. Exported records carry
`chain_hash`; the field is omitted when absent, so unchained bytes do not
change. `audit_export::claim_shard_chained` is the keyed claim. A key shorter
than 32 bytes fails `try_build` with `AuditChainKeyTooShort`.

**Signed WASM modules.** `HarvestBuilder::wasm_trusted_publisher_key` sets an
Ed25519 trust policy. A publisher signs `(domain, activity, hash)` with
`wasm_signing::sign_wasm_module`. `wasm_store::publish_signed_wasm_module`
checks and stores the signature. The worker checks it again before each run,
so a module written by direct SQL does not run. `try_build` refuses an
unsigned registration while a key is set. `WasmActivityRegistration` gains
`with_signature`. `seed_registered_wasm_modules` now takes
`(name, bytes, signature)` entries.

**OTel semantic conventions.** `telemetry::SEMCONV_METRIC_MAPPINGS` maps
`harvest.queue.dispatched` to `messaging.client.consumed.messages` and
`harvest.activity.duration` to `messaging.process.duration`.
`docs/operations/otel-collector.md` publishes the Collector recipe. Native
OTLP export, emitter renames and RPC mappings are declined in the ADR.

**Migrations.** `20261006013518_harvest_audit_chain` and
`20261006013725_harvest_wasm_module_signature` add nullable columns only. No
`WorkflowEvent` variant, no change to `harvest_events`, no replay impact.

**Tests.** Unit tests in `audit_chain.rs`, `wasm_signing.rs`, `audit_export.rs`
and `builder.rs`. DB tests in `tests/integration/audit_chain_tests.rs` and
`tests/integration/wasm_activities_tests.rs`. The docs test
`tests/integration/otel_semconv_docs.rs` renders the Collector recipe from the
table and checks the ADR.
