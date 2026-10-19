//! LLM token and cost budgets per run and per tenant (issue #1997).
//!
//! A runaway agent loop can spend without limit. A declared budget stops it.
//! The caps live on [`QuotaPolicy`], so a budget uses the quota key, the
//! quota violation rule and the quota metric (issue #946).
//!
//! # The two calls
//!
//! An LLM step is an activity that makes one model call. It calls
//! [`ActivityContext::check_llm_budget`](crate::context::ActivityContext::check_llm_budget)
//! before the model call. It records the call with
//! [`ActivityContext::record_llm_call`](crate::context::ActivityContext::record_llm_call)
//! after it. The check refuses the step when the run or the tenant has spent
//! its cap. The refusal is a non-retryable failure of type
//! [`ERROR_TYPE_LLM_BUDGET_EXCEEDED`]. Workflow code tests for it with
//! [`LlmBudgetExceeded::is_refusal`] and reads it with
//! [`LlmBudgetExceeded::from_error`].
//!
//! # The ledger
//!
//! The check sums the agent cost ledger, `harvest_llm_ledger` (issue #1996).
//! See [`crate::llm_ledger`]. The engine writes one row per recorded call
//! when the attempt completes. The cost caps are in millionths of a US
//! dollar, as the ledger is. An unpriced call adds no cost.
//!
//! # Limits of the guarantee
//!
//! - The budget is a soft cap. The last step that passes the check can pass
//!   the cap by its whole usage. Steps that run at the same time can all pass
//!   the check before any of them records.
//! - The ledger writes the calls of a completed attempt only. A call in an
//!   attempt that fails or times out is not counted.
//! - A run is one execution. A continue-as-new, a reset, a fork or a workflow
//!   retry starts a new execution, so its run caps start from zero. A reset
//!   fork keeps the key of its source.
//! - The tenant scope is `(workflow type, quota key)`, as for quota. It is
//!   shard-local. The check reads the key of each run. Admission sets the key
//!   when the policy of the type uses one.
//! - A run that starts before its type has a policy has no key. The tenant
//!   caps fail open for it, and its rows do not count for its tenant.
//!   `quota_reconcile` sets the key of such a run while it is `RUNNING` or
//!   `PAUSED`. A run that ends before the reconciler reaches it keeps no key,
//!   so its rows never count. This gap closes one reconcile pass after the
//!   policy rollout.
//! - A key that does not resolve fails open for the tenant caps. The run caps
//!   still apply.
//! - The check reads the policy in its own process. A worker that does not
//!   register the workflow type does not enforce the budget. It logs one
//!   warning for each such type.
//! - A check with a tenant cap reads the runs of the tenant that are active
//!   or ended in the window. A policy with only run caps skips that read.
//! - A ledger row cascades with its run. Keep retention longer than the
//!   tenant window.
//! - An activity that does not call the check is not budgeted. The budget
//!   stops mistakes. It is not a security boundary.

use crate::error::HarvestError;
use crate::failure::ActivityFailure;
use crate::quota::{QuotaPolicy, QuotaResource, QuotaViolation};

/// The default rolling window of the tenant LLM caps: one day.
pub const DEFAULT_TENANT_LLM_WINDOW_SECS: u32 = 86_400;

/// The error type of a step that a budget refuses.
pub const ERROR_TYPE_LLM_BUDGET_EXCEEDED: &str = "LlmBudgetExceeded";

/// The error type of a step whose spend read failed. It is retryable.
pub const ERROR_TYPE_LLM_BUDGET_CHECK_FAILED: &str = "LlmBudgetCheckFailed";

/// The retryable failure of a step whose spend read failed.
///
/// The check fails closed: a step that cannot read its spend does not run.
#[must_use]
pub fn check_failed(error: &dyn std::fmt::Display) -> ActivityFailure {
    ActivityFailure::retryable(
        ERROR_TYPE_LLM_BUDGET_CHECK_FAILED,
        format!("could not read the LLM spend: {error}"),
    )
}

/// What a run and its tenant spent, as the check reads it.
///
/// The fields are `i64`, as Postgres sums them. A negative value counts as
/// zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LlmSpend {
    /// Tokens of the run.
    pub run_tokens: i64,
    /// Cost of the run, in millionths of a US dollar.
    pub run_cost_usd_micros: i64,
    /// Tokens of the key in the window.
    pub tenant_tokens: i64,
    /// Cost of the key in the window, in millionths of a US dollar.
    pub tenant_cost_usd_micros: i64,
}

/// Compare a spend with the LLM caps of a policy.
///
/// The order is fixed: run tokens, run cost, tenant tokens, tenant cost. The
/// first spent cap is the result. The rule is the quota rule: a spend equal
/// to its cap refuses the next step. A cap above `i64::MAX` counts as
/// `i64::MAX`, the ceiling of the ledger sums.
#[must_use]
pub fn check_llm_budget(spend: &LlmSpend, policy: &QuotaPolicy) -> Option<QuotaViolation> {
    let clamp = |n: i64| -> u64 { u64::try_from(n).unwrap_or(0) };
    [
        (
            QuotaResource::RunLlmTokens,
            policy.max_run_llm_tokens,
            spend.run_tokens,
        ),
        (
            QuotaResource::RunLlmCostUsdMicros,
            policy.max_run_llm_cost_usd_micros,
            spend.run_cost_usd_micros,
        ),
        (
            QuotaResource::TenantLlmTokens,
            policy.max_tenant_llm_tokens,
            spend.tenant_tokens,
        ),
        (
            QuotaResource::TenantLlmCostUsdMicros,
            policy.max_tenant_llm_cost_usd_micros,
            spend.tenant_cost_usd_micros,
        ),
    ]
    .into_iter()
    .find_map(|(resource, cap, spent)| {
        // The ledger sums saturate at `i64::MAX`. A larger cap is clamped to
        // that ceiling, so it can still be reached.
        let limit = cap?.min(i64::MAX.unsigned_abs());
        let current = clamp(spent);
        (current >= limit).then_some(QuotaViolation {
            resource,
            limit,
            current,
        })
    })
}

/// A step that a budget refused, as workflow code reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LlmBudgetExceeded {
    /// The resource whose cap is spent.
    pub resource: QuotaResource,
    /// The cap.
    pub limit: u64,
    /// The spend at the check.
    pub current: u64,
}

impl LlmBudgetExceeded {
    /// The refusal of a violation.
    #[must_use]
    pub const fn from_violation(violation: QuotaViolation) -> Self {
        Self {
            resource: violation.resource,
            limit: violation.limit,
            current: violation.current,
        }
    }

    /// The non-retryable activity failure of this refusal.
    #[must_use]
    pub fn into_failure(self) -> ActivityFailure {
        let message = format!(
            "LLM budget spent: {} at {}/{}",
            self.resource, self.current, self.limit
        );
        let details = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        ActivityFailure::non_retryable(ERROR_TYPE_LLM_BUDGET_EXCEEDED, message)
            .with_details(details)
    }

    /// `true` when an activity error is a budget refusal.
    ///
    /// It reads the error type only. Branch on it, not on
    /// [`Self::from_error`]: the details can be unreadable, for example after
    /// an erasure, and the branch must not change on replay.
    #[must_use]
    pub fn is_refusal(error: &HarvestError) -> bool {
        error.activity_error_type() == Some(ERROR_TYPE_LLM_BUDGET_EXCEEDED)
    }

    /// Read a refusal back from an activity error.
    ///
    /// Returns `None` for every other error, and for a refusal whose details
    /// are unreadable.
    #[must_use]
    pub fn from_error(error: &HarvestError) -> Option<Self> {
        match error {
            HarvestError::ActivityFailed {
                error_type,
                details: Some(details),
                ..
            } if error_type == ERROR_TYPE_LLM_BUDGET_EXCEEDED => {
                serde_json::from_value(details.clone()).ok()
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// The ledger and the spend read
// ---------------------------------------------------------------------------

/// The spend of one run and of its tenant, in one round trip.
///
/// `$1` is the run. `$2` is the tenant window in seconds. `$3` is false when
/// the policy declares no tenant cap. The planner then skips the tenant sum.
///
/// The run sum reads the ledger key, which starts with the run. The tenant
/// scope is the `(workflow_name, quota_key)` of the run. A run with no key
/// has no tenant spend. The tenant sum joins each ledger row to its run:
///
/// - The rows of a run count once its key is set. A run with no key does not
///   count. See the module docs for the rollout gap.
/// - A row is never newer than the end of its run. So only a run that is
///   active, or that ended in the window, can hold a row in the window.
///   `idx_harvest_we_llm_tenant` finds those runs. The `LATERAL` sum then
///   reads the ledger of each run by its key, so the read never scans the
///   rows of other tenants.
/// - A run in a shard move counts on one shard only, as in the usage report
///   (issue #1996). The staged `MIGRATING` copy does not count. The sealed
///   `MIGRATED` source counts until the move is `DONE`.
///
/// The sums are `NUMERIC`, clamped to the `BIGINT` range. A huge recorded
/// value then reads as a spent cap, not as a read error.
#[cfg(feature = "db")]
const LLM_SPEND_SQL: &str = "\
    WITH run AS ( \
        SELECT workflow_name, quota_key FROM harvest_workflow_executions WHERE id = $1 \
    ), run_spend AS ( \
        SELECT LEAST(COALESCE(SUM(input_tokens::NUMERIC + output_tokens), 0), \
                     9223372036854775807)::BIGINT AS tokens, \
               LEAST(COALESCE(SUM(cost_usd_micros::NUMERIC), 0), \
                     9223372036854775807)::BIGINT AS cost_usd_micros \
        FROM harvest_llm_ledger WHERE workflow_exec_id = $1 \
    ), tenant_spend AS ( \
        SELECT LEAST(COALESCE(SUM(s.tokens), 0), 9223372036854775807)::BIGINT AS tokens, \
               LEAST(COALESCE(SUM(s.cost_usd_micros), 0), \
                     9223372036854775807)::BIGINT AS cost_usd_micros \
        FROM run r \
        JOIN harvest_workflow_executions w \
          ON w.workflow_name = r.workflow_name AND w.quota_key = r.quota_key \
        CROSS JOIN LATERAL ( \
            SELECT SUM(l.input_tokens::NUMERIC + l.output_tokens) AS tokens, \
                   SUM(l.cost_usd_micros::NUMERIC) AS cost_usd_micros \
            FROM harvest_llm_ledger l \
            WHERE l.workflow_exec_id = w.id \
              AND l.recorded_at >= NOW() - ($2::BIGINT * INTERVAL '1 second') \
        ) s \
        WHERE $3::BOOLEAN \
          AND COALESCE(w.completed_at, 'infinity'::timestamptz) \
              >= NOW() - ($2::BIGINT * INTERVAL '1 second') \
          AND ( \
              w.state NOT IN ('MIGRATING', 'MIGRATED') \
              OR (w.state = 'MIGRATED' AND EXISTS ( \
                  SELECT 1 FROM harvest_shard_migrations m \
                  WHERE m.execution_id = w.id AND m.phase = 'COMMITTED' \
              )) \
          ) \
    ) \
    SELECT run_spend.tokens AS run_tokens, \
           run_spend.cost_usd_micros AS run_cost_usd_micros, \
           tenant_spend.tokens AS tenant_tokens, \
           tenant_spend.cost_usd_micros AS tenant_cost_usd_micros \
    FROM run_spend, tenant_spend";

/// The planner setting of the tenant read.
///
/// **JIT off.** The planner cannot see which runs of a tenant fall in the
/// window, so it can estimate thousands of runs where a few match. That
/// estimate can pass `jit_above_cost`. JIT then compiles the plan on every
/// LLM step, for a read that runs in well under a millisecond. On a fixture
/// of 200,000 runs, JIT took the read from 0.2 ms to 65 ms. The claim path
/// turns JIT off for the same reason (`queue::CLAIM_PLAN_SETTINGS_SQL`).
///
/// `SET LOCAL` ends with the transaction, so the session settings of the
/// connection do not change.
#[cfg(feature = "db")]
const SPEND_PLAN_SETTINGS_SQL: &str = "SET LOCAL jit = off";

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct LlmSpendRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    run_tokens: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    run_cost_usd_micros: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    tenant_tokens: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    tenant_cost_usd_micros: i64,
}

/// Read the spend of a run and of its tenant.
///
/// With `with_tenant` false, the tenant spend reads as zero and costs no
/// scan. With it true, the read runs in a short transaction that turns JIT
/// off. See `SPEND_PLAN_SETTINGS_SQL`.
///
/// # Errors
///
/// Returns a database error when the read fails.
#[cfg(feature = "db")]
pub async fn load_llm_spend(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    window_secs: u32,
    with_tenant: bool,
) -> crate::error::HarvestResult<LlmSpend> {
    use diesel_async::{AsyncConnection as _, RunQueryDsl as _};

    let read = async |conn: &mut diesel_async::AsyncPgConnection| {
        diesel::sql_query(LLM_SPEND_SQL)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .bind::<diesel::sql_types::BigInt, _>(i64::from(window_secs))
            .bind::<diesel::sql_types::Bool, _>(with_tenant)
            .get_result::<LlmSpendRow>(conn)
            .await
            .map_err(crate::error::database_error)
    };
    let row = if with_tenant {
        Box::pin(
            conn.transaction::<_, crate::error::HarvestError, _>(async move |conn| {
                diesel::sql_query(SPEND_PLAN_SETTINGS_SQL)
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                read(conn).await
            }),
        )
        .await?
    } else {
        read(conn).await?
    };
    Ok(LlmSpend {
        run_tokens: row.run_tokens,
        run_cost_usd_micros: row.run_cost_usd_micros,
        tenant_tokens: row.tenant_tokens,
        tenant_cost_usd_micros: row.tenant_cost_usd_micros,
    })
}

/// The LLM budget of a workflow type, if it declares one.
///
/// It reads the process-global mirror of each registered type, as admission
/// does. A type with no LLM cap gives `None`. A type that this process does
/// not register also gives `None`, with one warning for each type.
#[cfg(feature = "db")]
#[must_use]
pub(crate) fn policy_for(workflow_type: &str) -> Option<QuotaPolicy> {
    let registered = crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref()
                .and_then(|map| map.get(workflow_type))
                .map(|meta| meta.quota)
        });
    registered.map_or_else(
        || {
            warn_unregistered_once(workflow_type);
            None
        },
        |quota| quota.filter(QuotaPolicy::has_llm_budget),
    )
}

/// Warn once that this process cannot read the budget of a workflow type.
#[cfg(feature = "db")]
fn warn_unregistered_once(workflow_type: &str) {
    static WARNED: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::LazyLock::new(Default::default);
    let first = WARNED
        .lock()
        .is_ok_and(|mut warned| warned.insert(workflow_type.to_owned()));
    if first {
        tracing::warn!(
            workflow_type,
            "an LLM step runs in a process that does not register its workflow type; \
             its LLM budget is not enforced here (issue #1997)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_caps() -> QuotaPolicy {
        QuotaPolicy::new("tenant")
            .with_max_run_llm_tokens(100)
            .with_max_run_llm_cost_usd_micros(1_000)
            .with_max_tenant_llm_tokens(500)
            .with_max_tenant_llm_cost_usd_micros(5_000)
    }

    #[test]
    fn only_a_tenant_cap_needs_the_tenant_sum() {
        let run_only = QuotaPolicy::new("tenant")
            .with_max_run_llm_tokens(1)
            .with_max_run_llm_cost_usd_micros(1);
        assert!(!run_only.has_tenant_llm_cap());
        assert!(
            QuotaPolicy::new("t")
                .with_max_tenant_llm_tokens(1)
                .has_tenant_llm_cap()
        );
        assert!(
            QuotaPolicy::new("t")
                .with_max_tenant_llm_cost_usd_micros(1)
                .has_tenant_llm_cap()
        );
    }

    #[test]
    fn a_policy_with_no_llm_cap_has_no_llm_budget() {
        let policy = QuotaPolicy::new("tenant").with_max_active_executions(3);
        assert!(!policy.has_llm_budget());
        assert!(policy.has_any_cap());
        assert_eq!(
            policy.tenant_llm_window_secs,
            DEFAULT_TENANT_LLM_WINDOW_SECS
        );
    }

    #[test]
    fn each_llm_cap_alone_is_an_llm_budget_and_not_an_admission_cap() {
        let base = QuotaPolicy::new("tenant");
        for policy in [
            base.with_max_run_llm_tokens(1),
            base.with_max_run_llm_cost_usd_micros(1),
            base.with_max_tenant_llm_tokens(1),
            base.with_max_tenant_llm_cost_usd_micros(1),
        ] {
            assert!(policy.has_llm_budget());
            assert!(!policy.has_any_cap());
        }
        assert_eq!(
            base.with_tenant_llm_window_secs(60).tenant_llm_window_secs,
            60
        );
        // A zero window would count nothing, so the builder raises it.
        assert_eq!(
            base.with_tenant_llm_window_secs(0).tenant_llm_window_secs,
            1
        );
    }

    #[test]
    fn a_spend_under_every_cap_passes() {
        let spend = LlmSpend {
            run_tokens: 99,
            run_cost_usd_micros: 999,
            tenant_tokens: 499,
            tenant_cost_usd_micros: 4_999,
        };
        assert_eq!(check_llm_budget(&spend, &all_caps()), None);
    }

    #[test]
    fn a_spend_equal_to_a_cap_refuses_the_next_step() {
        let spend = LlmSpend {
            run_tokens: 100,
            ..LlmSpend::default()
        };
        let violation = check_llm_budget(&spend, &all_caps()).unwrap();
        assert_eq!(violation.resource, QuotaResource::RunLlmTokens);
        assert_eq!(violation.limit, 100);
        assert_eq!(violation.current, 100);
    }

    #[test]
    fn the_caps_are_checked_in_a_fixed_order() {
        let mut spend = LlmSpend {
            run_tokens: 0,
            run_cost_usd_micros: 0,
            tenant_tokens: 0,
            tenant_cost_usd_micros: 9_000,
        };
        let order = [
            QuotaResource::TenantLlmCostUsdMicros,
            QuotaResource::TenantLlmTokens,
            QuotaResource::RunLlmCostUsdMicros,
            QuotaResource::RunLlmTokens,
        ];
        let mut seen = Vec::new();
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.tenant_tokens = 900;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.run_cost_usd_micros = 9_000;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.run_tokens = 900;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        assert_eq!(seen, order);
    }

    #[test]
    fn an_undeclared_cap_is_never_checked() {
        let spend = LlmSpend {
            run_tokens: i64::MAX,
            run_cost_usd_micros: i64::MAX,
            tenant_tokens: i64::MAX,
            tenant_cost_usd_micros: i64::MAX,
        };
        let only_tenant_cost = QuotaPolicy::new("tenant").with_max_tenant_llm_cost_usd_micros(10);
        assert_eq!(
            check_llm_budget(&spend, &only_tenant_cost)
                .unwrap()
                .resource,
            QuotaResource::TenantLlmCostUsdMicros
        );
        assert_eq!(check_llm_budget(&spend, &QuotaPolicy::new("tenant")), None);
    }

    #[test]
    fn a_cap_above_the_ledger_ceiling_is_still_reachable() {
        // The ledger sums saturate at `i64::MAX`, so a larger cap must refuse
        // there instead of never.
        let policy = QuotaPolicy::new("tenant").with_max_tenant_llm_cost_usd_micros(u64::MAX);
        let spend = LlmSpend {
            tenant_cost_usd_micros: i64::MAX,
            ..LlmSpend::default()
        };
        let violation = check_llm_budget(&spend, &policy).unwrap();
        assert_eq!(violation.resource, QuotaResource::TenantLlmCostUsdMicros);
        assert_eq!(violation.limit, i64::MAX.unsigned_abs());
    }

    #[test]
    fn a_negative_spend_counts_as_zero() {
        let spend = LlmSpend {
            run_tokens: -5,
            ..LlmSpend::default()
        };
        let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(1);
        assert_eq!(check_llm_budget(&spend, &policy), None);
    }

    #[test]
    fn a_zero_cap_refuses_every_step() {
        let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(0);
        let violation = check_llm_budget(&LlmSpend::default(), &policy).unwrap();
        assert_eq!(violation.current, 0);
    }

    #[test]
    fn the_refusal_is_non_retryable_and_reads_back_from_the_error() {
        use crate::failure::IntoActivityErrorString as _;

        let refusal = LlmBudgetExceeded {
            resource: QuotaResource::TenantLlmTokens,
            limit: 500,
            current: 512,
        };
        let failure = refusal.into_failure();
        assert!(failure.non_retryable);
        assert_eq!(failure.error_type, ERROR_TYPE_LLM_BUDGET_EXCEEDED);
        assert!(failure.message.contains("tenant_llm_tokens"));

        let error = HarvestError::ActivityFailed {
            name: "llm_step".into(),
            attempt: 1,
            error_type: failure.error_type.clone(),
            details: failure.details.clone(),
            source: failure.into_error_payload().into(),
        };
        assert_eq!(LlmBudgetExceeded::from_error(&error), Some(refusal));
    }

    #[test]
    fn another_error_is_not_a_refusal() {
        let other = HarvestError::ActivityFailed {
            name: "llm_step".into(),
            attempt: 1,
            error_type: "RateLimited".into(),
            details: Some(
                serde_json::json!({"resource": "run_llm_tokens", "limit": 1, "current": 1}),
            ),
            source: "boom".into(),
        };
        assert_eq!(LlmBudgetExceeded::from_error(&other), None);
        assert_eq!(
            LlmBudgetExceeded::from_error(&HarvestError::NotFound("x".into())),
            None
        );
    }

    #[test]
    fn a_refusal_is_found_by_its_type_even_with_unreadable_details() {
        let erased = HarvestError::ActivityFailed {
            name: "llm_step".into(),
            attempt: 1,
            error_type: ERROR_TYPE_LLM_BUDGET_EXCEEDED.into(),
            details: Some(serde_json::json!({"_harvest_erased": true})),
            source: "erased".into(),
        };
        assert!(LlmBudgetExceeded::is_refusal(&erased));
        assert_eq!(LlmBudgetExceeded::from_error(&erased), None);
        let other = HarvestError::NotFound("x".into());
        assert!(!LlmBudgetExceeded::is_refusal(&other));
    }

    #[test]
    fn a_failed_spend_read_fails_the_step_as_retryable() {
        let failure = check_failed(&"connection refused");
        assert!(!failure.non_retryable);
        assert_eq!(failure.error_type, ERROR_TYPE_LLM_BUDGET_CHECK_FAILED);
        assert!(failure.message.contains("connection refused"));
    }

    #[tokio::test]
    async fn a_context_with_no_database_passes() {
        let ctx = crate::context::ActivityContext::new_test();
        assert_eq!(ctx.check_llm_budget().await, Ok(()));
    }
}
