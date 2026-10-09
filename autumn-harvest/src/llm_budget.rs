//! LLM token and cost budgets per run and per tenant (issue #1997).
//!
//! A runaway agent loop can spend without limit. A budget stops it. The caps
//! live on [`QuotaPolicy`], so a budget uses the quota key, the quota
//! violation rule and the quota metric (issue #946).
//!
//! # The two calls
//!
//! An LLM step is an activity that makes one model call. It calls
//! [`ActivityContext::check_llm_budget`](crate::context::ActivityContext::check_llm_budget)
//! before the model call. It calls
//! [`ActivityContext::record_llm_usage`](crate::context::ActivityContext::record_llm_usage)
//! after it. The check refuses the step when the run or the tenant has spent
//! its cap. The refusal is a non-retryable failure of type
//! [`ERROR_TYPE_LLM_BUDGET_EXCEEDED`]. Workflow code reads it back with
//! [`LlmBudgetExceeded::from_error`].
//!
//! # The ledger
//!
//! Each recorded call is one row of `harvest_llm_ledger`. The row holds the
//! model, the tokens, the cost and the latency in clear columns, outside the
//! encrypted payload. SQL can sum them without the codec key. The row is not
//! an event, so replay does not change.
//!
//! # Limits of the guarantee
//!
//! - The budget is a soft cap. Steps that run at the same time all read the
//!   spend before any of them records. The overshoot is at most the steps in
//!   flight.
//! - The tenant scope is `(workflow type, quota key)`, as for quota. It is
//!   shard-local.
//! - A key that does not resolve fails open for the tenant caps. The run caps
//!   still apply.
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

/// The usage of one model call, as the ledger records it.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct LlmUsage {
    /// The model id that the provider reports.
    pub model: String,
    /// Prompt tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// The cost, in millionths of a currency unit.
    pub cost_micros: u64,
    /// The time of the call, in milliseconds.
    pub latency_ms: u64,
}

impl LlmUsage {
    /// Usage with a model id and token counts. The cost and the latency are
    /// zero.
    #[must_use]
    pub fn new(model: impl Into<String>, input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            model: model.into(),
            input_tokens,
            output_tokens,
            cost_micros: 0,
            latency_ms: 0,
        }
    }

    /// Set the cost, in millionths of a currency unit.
    #[must_use]
    pub const fn with_cost_micros(mut self, cost_micros: u64) -> Self {
        self.cost_micros = cost_micros;
        self
    }

    /// Set the latency of the call.
    #[must_use]
    pub fn with_latency(mut self, latency: std::time::Duration) -> Self {
        self.latency_ms = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);
        self
    }

    /// Input plus output tokens.
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

/// What a run and its tenant spent, as the check reads it.
///
/// The fields are `i64`, as Postgres sums them. A negative value counts as
/// zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LlmSpend {
    /// Tokens of the run.
    pub run_tokens: i64,
    /// Cost of the run.
    pub run_cost_micros: i64,
    /// Tokens of the key in the window.
    pub tenant_tokens: i64,
    /// Cost of the key in the window.
    pub tenant_cost_micros: i64,
}

/// Compare a spend with the LLM caps of a policy.
///
/// The order is fixed: run tokens, run cost, tenant tokens, tenant cost. The
/// first spent cap is the result. The rule is the quota rule: a spend equal
/// to its cap refuses the next step.
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
            QuotaResource::RunLlmCostMicros,
            policy.max_run_llm_cost_micros,
            spend.run_cost_micros,
        ),
        (
            QuotaResource::TenantLlmTokens,
            policy.max_tenant_llm_tokens,
            spend.tenant_tokens,
        ),
        (
            QuotaResource::TenantLlmCostMicros,
            policy.max_tenant_llm_cost_micros,
            spend.tenant_cost_micros,
        ),
    ]
    .into_iter()
    .find_map(|(resource, cap, spent)| {
        let limit = cap?;
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
    /// The spent cap.
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

    /// Read a refusal back from an activity error.
    ///
    /// Returns `None` for every other error.
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
/// `$1` is the run. `$2` is the tenant window in seconds. The tenant scope is
/// the `(workflow_name, quota_key)` of the run. A run with no key has no
/// tenant spend. Each sum reads one index of `harvest_llm_ledger`.
#[cfg(feature = "db")]
const LLM_SPEND_SQL: &str = "\
    WITH run AS ( \
        SELECT workflow_name, quota_key FROM harvest_workflow_executions WHERE id = $1 \
    ), run_spend AS ( \
        SELECT COALESCE(SUM(input_tokens + output_tokens), 0)::BIGINT AS tokens, \
               COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros \
        FROM harvest_llm_ledger WHERE execution_id = $1 \
    ), tenant_spend AS ( \
        SELECT COALESCE(SUM(l.input_tokens + l.output_tokens), 0)::BIGINT AS tokens, \
               COALESCE(SUM(l.cost_micros), 0)::BIGINT AS cost_micros \
        FROM harvest_llm_ledger l JOIN run r \
          ON l.workflow_name = r.workflow_name AND l.quota_key = r.quota_key \
        WHERE l.recorded_at >= NOW() - ($2::BIGINT * INTERVAL '1 second') \
    ) \
    SELECT run_spend.tokens AS run_tokens, run_spend.cost_micros AS run_cost_micros, \
           tenant_spend.tokens AS tenant_tokens, tenant_spend.cost_micros AS tenant_cost_micros \
    FROM run_spend, tenant_spend";

/// Write one ledger row. The run row gives the workflow type and the key.
///
/// No row is written when the run does not exist.
#[cfg(feature = "db")]
const RECORD_LLM_USAGE_SQL: &str = "\
    INSERT INTO harvest_llm_ledger \
        (execution_id, workflow_name, quota_key, activity_name, activity_id, attempt, \
         model, input_tokens, output_tokens, cost_micros, latency_ms) \
    SELECT id, workflow_name, quota_key, $2, $3, $4, $5, $6, $7, $8, $9 \
    FROM harvest_workflow_executions WHERE id = $1";

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct LlmSpendRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    run_tokens: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    run_cost_micros: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    tenant_tokens: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    tenant_cost_micros: i64,
}

/// Read the spend of a run and of its tenant.
///
/// # Errors
///
/// Returns a database error when the read fails.
#[cfg(feature = "db")]
pub async fn load_llm_spend(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    window_secs: u32,
) -> crate::error::HarvestResult<LlmSpend> {
    use diesel_async::RunQueryDsl as _;

    let row: LlmSpendRow = diesel::sql_query(LLM_SPEND_SQL)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::BigInt, _>(i64::from(window_secs))
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(LlmSpend {
        run_tokens: row.run_tokens,
        run_cost_micros: row.run_cost_micros,
        tenant_tokens: row.tenant_tokens,
        tenant_cost_micros: row.tenant_cost_micros,
    })
}

/// The attempt that recorded a ledger row.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy)]
pub struct LlmCallSite<'a> {
    /// The run.
    pub exec_id: crate::types::ExecutionId,
    /// The activity type.
    pub activity_name: &'a str,
    /// The activity invocation, stable across retries.
    pub activity_id: crate::types::ActivityExecId,
    /// The attempt, from 1.
    pub attempt: u32,
}

/// Write one ledger row. Returns `false` when the run does not exist.
///
/// # Errors
///
/// Returns a database error when the write fails.
#[cfg(feature = "db")]
pub async fn record_llm_usage(
    conn: &mut diesel_async::AsyncPgConnection,
    site: LlmCallSite<'_>,
    usage: &LlmUsage,
) -> crate::error::HarvestResult<bool> {
    use diesel::sql_types::{BigInt, Integer, Text, Uuid};
    use diesel_async::RunQueryDsl as _;

    let clamp = |n: u64| -> i64 { i64::try_from(n).unwrap_or(i64::MAX) };
    let written = diesel::sql_query(RECORD_LLM_USAGE_SQL)
        .bind::<Uuid, _>(site.exec_id.as_uuid())
        .bind::<Text, _>(site.activity_name)
        .bind::<Uuid, _>(site.activity_id.as_uuid())
        .bind::<Integer, _>(i32::try_from(site.attempt.max(1)).unwrap_or(i32::MAX))
        .bind::<Text, _>(usage.model.as_str())
        .bind::<BigInt, _>(clamp(usage.input_tokens))
        .bind::<BigInt, _>(clamp(usage.output_tokens))
        .bind::<BigInt, _>(clamp(usage.cost_micros))
        .bind::<BigInt, _>(clamp(usage.latency_ms))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(written > 0)
}

/// The LLM budget of a workflow type, if it declares one.
///
/// It reads the process-global mirror of each registered type, as admission
/// does. A type with no LLM cap gives `None`.
#[cfg(feature = "db")]
#[must_use]
pub fn policy_for(workflow_type: &str) -> Option<QuotaPolicy> {
    crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref()
                .and_then(|map| map.get(workflow_type))
                .and_then(|meta| meta.quota)
        })
        .filter(QuotaPolicy::has_llm_budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_caps() -> QuotaPolicy {
        QuotaPolicy::new("tenant")
            .with_max_run_llm_tokens(100)
            .with_max_run_llm_cost_micros(1_000)
            .with_max_tenant_llm_tokens(500)
            .with_max_tenant_llm_cost_micros(5_000)
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
            base.with_max_run_llm_cost_micros(1),
            base.with_max_tenant_llm_tokens(1),
            base.with_max_tenant_llm_cost_micros(1),
        ] {
            assert!(policy.has_llm_budget());
            assert!(!policy.has_any_cap());
        }
        assert_eq!(
            base.with_tenant_llm_window_secs(60).tenant_llm_window_secs,
            60
        );
    }

    #[test]
    fn a_spend_under_every_cap_passes() {
        let spend = LlmSpend {
            run_tokens: 99,
            run_cost_micros: 999,
            tenant_tokens: 499,
            tenant_cost_micros: 4_999,
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
            run_cost_micros: 0,
            tenant_tokens: 0,
            tenant_cost_micros: 9_000,
        };
        let order = [
            QuotaResource::TenantLlmCostMicros,
            QuotaResource::TenantLlmTokens,
            QuotaResource::RunLlmCostMicros,
            QuotaResource::RunLlmTokens,
        ];
        let mut seen = Vec::new();
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.tenant_tokens = 900;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.run_cost_micros = 9_000;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        spend.run_tokens = 900;
        seen.push(check_llm_budget(&spend, &all_caps()).unwrap().resource);
        assert_eq!(seen, order);
    }

    #[test]
    fn an_undeclared_cap_is_never_checked() {
        let spend = LlmSpend {
            run_tokens: i64::MAX,
            run_cost_micros: i64::MAX,
            tenant_tokens: i64::MAX,
            tenant_cost_micros: i64::MAX,
        };
        let only_tenant_cost = QuotaPolicy::new("tenant").with_max_tenant_llm_cost_micros(10);
        assert_eq!(
            check_llm_budget(&spend, &only_tenant_cost)
                .unwrap()
                .resource,
            QuotaResource::TenantLlmCostMicros
        );
        assert_eq!(check_llm_budget(&spend, &QuotaPolicy::new("tenant")), None);
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
    fn usage_saturates_and_converts_latency() {
        let usage = LlmUsage::new("m", u64::MAX, 1)
            .with_cost_micros(7)
            .with_latency(std::time::Duration::from_millis(1_250));
        assert_eq!(usage.total_tokens(), u64::MAX);
        assert_eq!(usage.cost_micros, 7);
        assert_eq!(usage.latency_ms, 1_250);
    }
}
