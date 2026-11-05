# `cargo xtask`

All workspace automation runs through the `xtask` crate. The crate uses the
[`smop`](https://docs.rs/smop) scripting crate. [ADR 0015](adr/0015-xtask-workspace-automation.md)
records the decision.

```sh
cargo xtask --help                 # list the groups
cargo xtask audit --help           # list the commands of one group
cargo xtask audit comment-hygiene --base origin/trunk-dev
```

The alias in `.cargo/config.toml` runs `cargo run --quiet -p xtask --`. Each
command keeps the flags, the output and the exit codes of the script that it
replaced. A gate with fixtures takes `--self-test`, and `cargo test -p xtask`
runs the same fixtures.

Two script sets are not part of xtask. The Python lab of
`examples/autoresearch` is the workload of that example. The apparatus in
`docs/assays/apparatus/` and `docs/perf-artifacts/` is the recorded harness of
a published measurement.

To add a command, add a module under `xtask/src/<group>/`, then add its
variant to the `Command` enum in `xtask/src/<group>/mod.rs`. Add a row here.
An audit also needs a row in [`docs/audits/README.md`](audits/README.md).

## Audits (CI gates)

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask audit action-sha-pin` | `xtask/src/audit/action_sha_pin.rs` | `docs/audits/action-sha-pin.py` |
| `cargo xtask audit audit-catalog-coverage` | `xtask/src/audit/audit_catalog_coverage.rs` | `docs/audits/audit-catalog-coverage.py` |
| `cargo xtask audit cli-flag-coverage` | `xtask/src/audit/cli_flag_coverage.rs` | `docs/audits/cli-flag-coverage.py` |
| `cargo xtask audit comment-hygiene` | `xtask/src/audit/comment_hygiene.rs` | `docs/audits/comment-hygiene.py` |
| `cargo xtask audit config-cli-drift` | `xtask/src/audit/config_cli_drift.rs` | `docs/audits/config-cli-drift.py` |
| `cargo xtask audit corpus-link-check` | `xtask/src/audit/corpus_link_check/mod.rs` | `docs/audits/corpus-link-check.py` |
| `cargo xtask audit diesel-table-width` | `xtask/src/audit/diesel_table_width.rs` | `docs/audits/diesel-table-width.py` |
| `cargo xtask audit doc-claim-drift` | `xtask/src/audit/doc_claim_drift.rs` | `docs/audits/doc-claim-drift.py` |
| `cargo xtask audit doc-snippet-syntax` | `xtask/src/audit/doc_snippet_syntax.rs` | `docs/audits/doc-snippet-syntax.py` |
| `cargo xtask audit ledger-census` | `xtask/src/audit/ledger_census.rs` | `docs/audits/ledger-census.py` |
| `cargo xtask audit ledger-profiles` | `xtask/src/audit/ledger_profiles.rs` | `docs/audits/ledger-profiles.py` |
| `cargo xtask audit libpq-keyword-sync` | `xtask/src/audit/libpq_keyword_sync.rs` | `docs/audits/libpq-keyword-sync.py` |
| `cargo xtask audit mixed-version-contract` | `xtask/src/audit/mixed_version_contract.rs` | `docs/audits/mixed-version-contract.py` |
| `cargo xtask audit openapi-response-coverage` | `xtask/src/audit/openapi_response_coverage.rs` | `docs/audits/openapi-response-coverage.py` |
| `cargo xtask audit required-migration-versions-sync` | `xtask/src/audit/required_migration_versions_sync.rs` | `docs/audits/required-migration-versions-sync.py` |
| `cargo xtask audit root-files` | `xtask/src/audit/root_files.rs` | `docs/audits/root-files.py` |
| `cargo xtask audit shard-weight-drift` | `xtask/src/audit/shard_weight_drift.rs` | `docs/audits/shard-weight-drift.py` |
| `cargo xtask audit standalone-chapter-sync` | `xtask/src/audit/standalone_chapter_sync.rs` | `docs/audits/standalone-chapter-sync.py` |
| `cargo xtask audit tenant-header-validation-sync` | `xtask/src/audit/tenant_header_validation_sync.rs` | `docs/audits/tenant-header-validation-sync.py` |
| `cargo xtask audit tracked-artifacts` | `xtask/src/audit/tracked_artifacts.rs` | `docs/audits/tracked-artifacts.py` |
| `cargo xtask audit vantage-dashboard-contrast` | `xtask/src/audit/vantage_dashboard_contrast.rs` | `docs/audits/vantage-dashboard-contrast.py` |
| `cargo xtask audit vantage-degraded-banner-focus` | `xtask/src/audit/vantage_degraded_banner_focus.rs` | `docs/audits/vantage-degraded-banner-focus.py` |
| `cargo xtask audit vantage-flash-live-region` | `xtask/src/audit/vantage_flash_live_region.rs` | `docs/audits/vantage-flash-live-region.py` |
| `cargo xtask audit vantage-label-association` | `xtask/src/audit/vantage_label_association.rs` | `docs/audits/vantage-label-association.py` |
| `cargo xtask audit worker-image-contract` | `xtask/src/audit/worker_image_contract.rs` | `docs/audits/worker-image-contract.py` |
| `cargo xtask audit workflow-yaml-parse` | `xtask/src/audit/workflow_yaml_parse/mod.rs` | `docs/audits/workflow-yaml-parse.py` |

## Developer and release tooling

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask dev next-panel-id` | `xtask/src/dev/next_panel_id.rs` | `docs/dashboards/next-panel-id.py` |
| `cargo xtask dev build-typescript-client` | `xtask/src/dev/build_typescript_client.rs` | `scripts/build-typescript-client.sh` |
| `cargo xtask dev regenerate-openapi` | `xtask/src/dev/regenerate_openapi.rs` | `scripts/regenerate-openapi.sh` |
| `cargo xtask dev flake-rerun` | `xtask/src/dev/flake_rerun.rs` | `scripts/flake-rerun.sh` |
| `cargo xtask dev run-standalone-chapter` | `xtask/src/dev/run_standalone_chapter.rs` | `scripts/run-standalone-chapter.sh` |
| `cargo xtask dev run-mixed-version-smoke` | `xtask/src/dev/run_mixed_version_smoke.rs` | `scripts/run-mixed-version-smoke.sh` |
| `cargo xtask dev clean-room-getting-started` | `xtask/src/dev/clean_room_getting_started.rs` | `scripts/clean-room-getting-started.sh` |
| `cargo xtask dev formal-traces` | `xtask/src/dev/formal_traces.rs` | `scripts/formal_traces.py` |
| `cargo xtask dev fuzz-smoke` | `xtask/src/dev/fuzz_smoke.rs` | `fuzz/smoke.sh` |

## CI drivers

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask ci advisory-scan` | `xtask/src/ci/advisory_scan.rs` | `.github/ci/advisory-scan.sh` |
| `cargo xtask ci chaos-watchdog` | `xtask/src/ci/chaos_watchdog.rs` | `.github/ci/chaos-watchdog.sh` |
| `cargo xtask ci coverage` | `xtask/src/ci/coverage/mod.rs` | `.github/ci/coverage.py` |
| `cargo xtask ci pull-test-images` | `xtask/src/ci/pull_test_images.rs` | `.github/ci/pull-test-images.sh` |
| `cargo xtask ci run-suites` | `xtask/src/ci/run_suites.rs` | `.github/ci/run-suites.sh` |

## Example and doc guards

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask check agent-adapter-deps` | `xtask/src/check/agent_adapter_deps.rs` | `scripts/check-agent-adapter-deps.sh` |
| `cargo xtask check agentd-runtime-artifacts-ignored` | `xtask/src/check/agentd_runtime_artifacts_ignored.rs` | `scripts/check-agentd-runtime-artifacts-ignored.sh` |
| `cargo xtask check broker-connector-cutover-example` | `xtask/src/check/broker_connector_cutover_example.rs` | `scripts/check-broker-connector-cutover-example.sh` |
| `cargo xtask check broker-connector-dead-letter-sink-example` | `xtask/src/check/broker_connector_dead_letter_sink_example.rs` | `scripts/check-broker-connector-dead-letter-sink-example.sh` |
| `cargo xtask check changelog-connector-migration-version` | `xtask/src/check/changelog_connector_migration_version.rs` | `scripts/check-changelog-connector-migration-version.sh` |
| `cargo xtask check child-idempotency-examples` | `xtask/src/check/child_idempotency_examples.rs` | `scripts/check-child-idempotency-examples.sh` |
| `cargo xtask check continue-as-new-checkpoint-example` | `xtask/src/check/continue_as_new_checkpoint_example.rs` | `scripts/check-continue-as-new-checkpoint-example.sh` |
| `cargo xtask check dags-schedules-examples` | `xtask/src/check/dags_schedules_examples.rs` | `scripts/check-dags-schedules-examples.sh` |
| `cargo xtask check default-run` | `xtask/src/check/default_run.rs` | `scripts/check-default-run.sh` |
| `cargo xtask check dev-database-url-placeholder-caveat` | `xtask/src/check/dev_database_url_placeholder_caveat.rs` | `scripts/check-dev-database-url-placeholder-caveat.sh` |
| `cargo xtask check dev-runtime-root-caveat` | `xtask/src/check/dev_runtime_root_caveat.rs` | `scripts/check-dev-runtime-root-caveat.sh` |
| `cargo xtask check durable-timers-examples` | `xtask/src/check/durable_timers_examples.rs` | `scripts/check-durable-timers-examples.sh` |
| `cargo xtask check formal-models` | `xtask/src/check/formal_models.rs` | `scripts/check-formal-models.sh` |
| `cargo xtask check formal-traces` | `xtask/src/check/formal_traces.rs` | `scripts/check-formal-traces.sh` |
| `cargo xtask check kani-proofs` | `xtask/src/check/kani_proofs.rs` | `scripts/check-kani-proofs.sh` |
| `cargo xtask check license-files` | `xtask/src/check/license_files.rs` | `scripts/check-license-files.sh` |
| `cargo xtask check no-powershell-env-syntax` | `xtask/src/check/no_powershell_env_syntax.rs` | `scripts/check-no-powershell-env-syntax.sh` |
| `cargo xtask check otel-workflow-semconv` | `xtask/src/check/otel_workflow_semconv.rs` | `scripts/check-otel-workflow-semconv.sh` |
| `cargo xtask check preflight-declared-deps-example` | `xtask/src/check/preflight_declared_deps_example.rs` | `scripts/check-preflight-declared-deps-example.sh` |
| `cargo xtask check readme-front-door-snippets` | `xtask/src/check/readme_front_door_snippets.rs` | `scripts/check-readme-front-door-snippets.sh` |
| `cargo xtask check readme-testing-dep-version` | `xtask/src/check/readme_testing_dep_version.rs` | `scripts/check-readme-testing-dep-version.sh` |
| `cargo xtask check readme-upgrade-guide-current` | `xtask/src/check/readme_upgrade_guide_current.rs` | `scripts/check-readme-upgrade-guide-current.sh` |
| `cargo xtask check search-attr-query-syntax` | `xtask/src/check/search_attr_query_syntax.rs` | `scripts/check-search-attr-query-syntax.sh` |
| `cargo xtask check signals-clean-approval-example-determinism` | `xtask/src/check/signals_clean_approval_example_determinism.rs` | `scripts/check-signals-clean-approval-example-determinism.sh` |
| `cargo xtask check sqlite-outcome-match-example` | `xtask/src/check/sqlite_outcome_match_example.rs` | `scripts/check-sqlite-outcome-match-example.sh` |
| `cargo xtask check underscore-input-snippets` | `xtask/src/check/underscore_input_snippets.rs` | `scripts/check-underscore-input-snippets.sh` |
| `cargo xtask check verus-proofs` | `xtask/src/check/verus_proofs.rs` | `scripts/check-verus-proofs.sh` |
| `cargo xtask check webhooks-feature-example` | `xtask/src/check/webhooks_feature_example.rs` | `scripts/check-webhooks-feature-example.sh` |
| `cargo xtask check worker-routing-examples` | `xtask/src/check/worker_routing_examples.rs` | `scripts/check-worker-routing-examples.sh` |

## Sandbox guest image

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask sandbox build-guest-image` | `xtask/src/sandbox/build_guest_image.rs` | `scripts/sandbox/build-guest-image.sh` |
| `cargo xtask sandbox image-path` | `xtask/src/sandbox/image_path.rs` | `scripts/sandbox/image-path.sh` |
| `cargo xtask sandbox publish-image` | `xtask/src/sandbox/publish_image.rs` | `scripts/sandbox/publish-image.sh` |

## Benchmarks

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask bench run` | `xtask/src/bench/run.rs` | `benchmarks/run.sh` |
| `cargo xtask bench notify-commit` | `xtask/src/bench/notify_commit.rs` | `benchmarks/notify-commit.sh` |

## Performance evidence

| Command | Module | Replaces |
|---|---|---|
| `cargo xtask perf broken-session-scan` | `xtask/src/perf/broken_session_scan.rs` | `autumn-harvest/scripts/broken_session_scan_perf_repro.sh` |
| `cargo xtask perf build-reachability-fanout` | `xtask/src/perf/build_reachability_fanout.rs` | `autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh` |
| `cargo xtask perf capability-labels-claim` | `xtask/src/perf/capability_labels_claim.rs` | `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh` |
| `cargo xtask perf claim-batched-seek-and-refine` | `xtask/src/perf/claim_batched_seek_and_refine.rs` | `autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh` |
| `cargo xtask perf concurrency-key-claim` | `xtask/src/perf/concurrency_key_claim.rs` | `autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh` |
| `cargo xtask perf deep-backlog-ledger` | `xtask/src/perf/deep_backlog_ledger.rs` | `autumn-harvest/scripts/deep_backlog_ledger_repro.sh` |
| `cargo xtask perf external-outbox-scan-matrix` | `xtask/src/perf/external_outbox_scan_matrix.rs` | `autumn-harvest/scripts/external_outbox_scan_matrix.sh` |
| `cargo xtask perf external-outbox-scan` | `xtask/src/perf/external_outbox_scan.rs` | `autumn-harvest/scripts/external_outbox_scan_perf_repro.sh` |
| `cargo xtask perf history-ceiling-claim` | `xtask/src/perf/history_ceiling_claim.rs` | `autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh` |
| `cargo xtask perf notify-stage-probe` | `xtask/src/perf/notify_stage_probe.rs` | `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh` |
| `cargo xtask perf parent-close-cascade-unfinished-handlers` | `xtask/src/perf/parent_close_cascade_unfinished_handlers.rs` | `autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh` |
| `cargo xtask perf pause-array-size-claim` | `xtask/src/perf/pause_array_size_claim.rs` | `autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh` |
| `cargo xtask perf pool-recycle-ping` | `xtask/src/perf/pool_recycle_ping.rs` | `autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh` |
| `cargo xtask perf queue-pause-claim` | `xtask/src/perf/queue_pause_claim.rs` | `autumn-harvest/scripts/queue_pause_claim_perf_repro.sh` |
| `cargo xtask perf quota-history-bytes` | `xtask/src/perf/quota_history_bytes.rs` | `autumn-harvest/scripts/quota_history_bytes_perf_repro.sh` |
| `cargo xtask perf quota-reconcile-candidate-scan` | `xtask/src/perf/quota_reconcile_candidate_scan.rs` | `autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh` |
| `cargo xtask perf schedule-to-close-claim` | `xtask/src/perf/schedule_to_close_claim.rs` | `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh` |
| `cargo xtask perf sticky-routing-claim` | `xtask/src/perf/sticky_routing_claim.rs` | `autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh` |
| `cargo xtask perf usage-report-activity-lookback` | `xtask/src/perf/usage_report_activity_lookback.rs` | `autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh` |
| `cargo xtask perf worker-session-claim` | `xtask/src/perf/worker_session_claim.rs` | `autumn-harvest/scripts/worker_session_claim_perf_repro.sh` |
| `cargo xtask perf history-bloat` | `xtask/src/perf/history_bloat.rs` | `autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh` |
| `cargo xtask perf classify-dhat-allocations` | `xtask/src/perf/classify_dhat_allocations.rs` | `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py` |

