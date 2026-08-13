#![allow(dead_code)]

#[cfg(feature = "db")]
mod active_workflow_gauge_tests;
mod activity_default_floor_tests;
mod activity_failure_tests;
#[cfg(feature = "db")]
mod activity_info_tests;
mod activity_interceptor_tests;
mod activity_outcome_metrics_tests;
mod admission_gate_authoritative_tests;
mod admission_gate_tests;
mod alert_pack_docs;
mod audit_tests;
#[cfg(feature = "db")]
mod auto_heartbeat_tests;
mod awaitables_tests;
#[cfg(feature = "db")]
mod build_routing_tests;
mod cache_delta_load_tests;
#[cfg(feature = "db")]
mod canary_tests;
mod cancellation_tests;
#[cfg(feature = "db")]
mod chain_timeout_tests;
mod child_fanout_tests;
mod child_policy_tests;
#[cfg(feature = "db")]
mod child_timeout_tests;
mod ci_run_coverage;
mod circuit_breaker_wiring_tests;
mod claim_bench_support;
#[cfg(feature = "db")]
mod claim_budget_tests;
mod completion_callback_tests;
mod concurrency_key_tests;
mod context_headers_tests;
#[cfg(feature = "db")]
mod cross_type_continue_as_new_tests;
mod cross_workflow_await_tests;
mod cross_workflow_cancel_tests;
#[cfg(feature = "db")]
mod cross_workflow_signal_tests;
#[cfg(feature = "db")]
mod ctx_info_tests;
mod dag_builder;
mod dag_compensation_tests;
mod dag_execution_timeout_tests;
mod dag_input_binding_tests;
mod dag_mapping_tests;
mod dag_signal_gate_tests;
#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]
mod dag_unified_tests;
mod dashboard_pack_docs;
mod debounce_tests;
mod delayed_start_tests;
mod det_check_tests;
mod event_batch_tests;
mod executor_span_tests;
#[cfg(feature = "testing")]
mod external_completion_tests;
mod fanout_tests;
mod force_fail_tests;
mod guardrail_catalog_tests;
mod havoc_reentrancy;
mod havoc_tests;
#[cfg(feature = "testing")]
mod idempotency_tests;
mod integration_e2e;
mod legal_hold_tests;
mod lineage_store_tests;
mod macros_activity;
mod macros_collect;
#[cfg(feature = "testing")]
mod macros_compile_fail;
mod macros_dag;
#[cfg(feature = "testing")]
mod macros_query_handlers;
mod macros_webhook;
mod macros_workflow;
mod metrics_coverage;
mod metrics_integration;
mod metrics_rs_adapter;
mod migration_hygiene;
#[cfg(feature = "db")]
mod mutex_tests;
mod nd_block_tests;
mod panic_containment_tests;
mod pause_tests;
#[cfg(feature = "testing")]
mod payload_cap_tests;
#[cfg(feature = "db")]
mod payload_offload_db_tests;
#[cfg(feature = "testing")]
mod payload_offload_replay_tests;
mod performance_docs;
mod poison_pill_tests;
mod priority_tests;
#[cfg(feature = "testing")]
mod publish_progress_tests;
mod query_deadlock;
mod query_terminal_tests;
mod query_tests;
mod queue_fairness_tests;
mod queue_pause_success_metric_tests;
mod queue_pause_tests;
mod rate_limit_key_tests;
mod redrive_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replay_canary_tests;
mod replay_tests;
#[cfg(feature = "testing")]
mod replay_verifier_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replayer_integration_tests;
#[cfg(feature = "testing")]
mod replayer_tests;
mod retention_overrides_tests;
mod retention_summary_tests;
mod retry_after_tests;
mod retry_now_tests;
mod saga_tests;
mod scanner_liveness_tests;
mod scanner_tick_db_tests;
mod schedule_decisions;
mod schedule_runs_tests;
mod schedule_to_close_tests;
mod schedule_update_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduled_time_tests;
mod scheduler_auto_pause_tests;
mod scheduler_bounded_runs_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduler_carryover_tests;
mod scheduler_catchup_tests;
#[cfg(feature = "db")]
mod scheduler_ha_tests;
#[cfg(feature = "db")]
mod scheduler_overdue_tests;
#[cfg(feature = "db")]
mod scheduler_registration_tests;
mod security;
mod sharding_unit;
mod signal_tests;
#[cfg(feature = "db")]
mod signal_with_start_tests;
mod sla_breach_tests;
mod slot_tuner_tests;
mod sqlite_feasibility_docs;
#[cfg(feature = "db")]
mod start_idempotency_tests;
#[cfg(feature = "db")]
mod start_source_tests;
mod sticky_routing_tests;
mod telemetry_span_tests;
#[cfg(feature = "db")]
mod throttle_tests;
#[cfg(feature = "db")]
mod transactional_activity_tests;
#[cfg(feature = "db")]
mod transactional_start_tests;
#[cfg(feature = "db")]
mod triage_tests;
#[cfg(feature = "db")]
mod typed_stubs_tests;
mod typed_workflow_failure_tests;
mod updt_with_start_tests;
#[cfg(feature = "wasm-activities")]
mod wasm_activities_tests;
mod webhook_trigger_tests;
mod worker_session_tests;
#[cfg(feature = "db")]
mod workflow_handle_tests;
#[cfg(feature = "db")]
mod workflow_id_targeted_tests;
mod workflow_logger_tests;
mod workflow_logs_tests;
mod workflow_mutation_tests;
#[cfg(feature = "db")]
mod workflow_reachability_samples_tests;
mod workflow_retry_tests;
mod workflow_schema_contract_tests;
mod workflow_task_timeout_tests;
#[cfg(feature = "testing")]
mod workflow_test_env_tests;
