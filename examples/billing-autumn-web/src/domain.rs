use autumn_harvest_plugin::prelude::WorkflowStartRequest;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const CHECKOUT_QUEUE: &str = "billing";
pub const PAYMENT_QUEUE: &str = "payments";
pub const INVOICE_QUEUE: &str = "invoices";
pub const OPS_QUEUE: &str = "ops";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckoutRequest {
    pub tenant_id: String,
    pub customer_id: String,
    pub plan: String,
    pub seats: u32,
    pub payment_method_id: String,
}

impl CheckoutRequest {
    pub fn normalized(mut self) -> Self {
        self.tenant_id = self.tenant_id.trim().to_ascii_lowercase();
        self.customer_id = self.customer_id.trim().to_owned();
        self.plan = self.plan.trim().to_ascii_lowercase();
        self.payment_method_id = self.payment_method_id.trim().to_owned();
        self
    }

    pub fn unit_price_cents(&self) -> u64 {
        match self.plan.as_str() {
            "enterprise" => 5_000,
            "pro" => 2_900,
            _ => 1_200,
        }
    }

    pub fn subtotal_cents(&self) -> u64 {
        self.unit_price_cents() * u64::from(self.seats)
    }

    pub fn requires_manual_review(&self) -> bool {
        self.subtotal_cents() >= 250_000
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckoutStartResponse {
    pub workflow_id: String,
    pub outbox: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanQuote {
    plan: &'static str,
    monthly_cents_per_seat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvoiceRequest {
    pub tenant_id: String,
    pub customer_id: String,
    pub subscription_id: String,
    pub subtotal_cents: u64,
    pub tax_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvoiceResult {
    pub invoice_id: String,
    pub total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BillingOutcome {
    pub subscription_id: String,
    pub invoice_id: String,
    pub capture_id: String,
    pub status: String,
}

pub fn plan_quotes() -> Vec<PlanQuote> {
    vec![
        PlanQuote {
            plan: "starter",
            monthly_cents_per_seat: 1_200,
        },
        PlanQuote {
            plan: "pro",
            monthly_cents_per_seat: 2_900,
        },
        PlanQuote {
            plan: "enterprise",
            monthly_cents_per_seat: 5_000,
        },
    ]
}

pub fn checkout_start_request(request: CheckoutRequest) -> WorkflowStartRequest {
    let request = request.normalized();
    WorkflowStartRequest {
        workflow_name: "billing_checkout".to_owned(),
        workflow_id: checkout_workflow_id(&request),
        queue_name: CHECKOUT_QUEUE.to_owned(),
        input: json!(request),
        memo: Some(json!({
            "kind": "subscription_checkout",
        })),
        search_attrs: Some(json!({
            "tenant_id": request.tenant_id,
            "customer_id": request.customer_id,
            "plan": request.plan,
        })),
    }
}

fn checkout_workflow_id(request: &CheckoutRequest) -> String {
    format!(
        "billing-checkout:{}:{}:{}",
        request.tenant_id, request.customer_id, request.plan
    )
}
