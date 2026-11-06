//! The spend of a refine stage (issue #2380).
//!
//! A proposer, a composer or a selector calls its own model. It reports
//! each call with [`ProposeContext::charge`](super::ProposeContext::charge)
//! or the `charge` method of its context. The stage then records the
//! spend, and its activity writes one cost ledger row for each call.

use std::sync::{Mutex, PoisonError};

use autumn_harvest::llm_ledger::LlmCall;
use serde::{Deserialize, Serialize};

use crate::message::TokenUsage;
use crate::model::UNKNOWN_MODEL_ID;

/// One model call of a stage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Charge {
    /// The provider model id, as the cost ledger records it. `None` records
    /// [`UNKNOWN_MODEL_ID`].
    pub model_id: Option<String>,
    /// The provider-reported tokens.
    pub usage: TokenUsage,
    /// The cost in millionths of a US dollar. `None` is unpriced.
    pub cost_usd_micros: Option<u64>,
}

impl Charge {
    /// A call of `model_id` that used `usage`, with no price.
    #[must_use]
    pub fn new(model_id: Option<&str>, usage: TokenUsage) -> Self {
        Self {
            model_id: model_id.map(str::to_owned),
            usage,
            cost_usd_micros: None,
        }
    }

    /// Price the call at `micros` millionths of a US dollar.
    #[must_use]
    pub const fn cost(mut self, micros: u64) -> Self {
        self.cost_usd_micros = Some(micros);
        self
    }

    /// The cost ledger call. A model id that the ledger refuses becomes
    /// [`UNKNOWN_MODEL_ID`], and the call drops a refused cost.
    #[must_use]
    pub fn ledger_call(&self) -> LlmCall {
        let mut model = self.model_id.as_deref().unwrap_or(UNKNOWN_MODEL_ID);
        if LlmCall::new(model, 0, 0).validate().is_err() {
            model = UNKNOWN_MODEL_ID;
        }
        let call = LlmCall::new(
            model,
            u64::from(self.usage.input_tokens),
            u64::from(self.usage.output_tokens),
        );
        match self.cost_usd_micros {
            Some(cost) if call.clone().with_cost_usd_micros(cost).validate().is_ok() => {
                call.with_cost_usd_micros(cost)
            }
            _ => call,
        }
    }
}

/// The charges of one stage. A context holds it by reference.
#[derive(Debug, Default)]
pub struct Meter(Mutex<Vec<Charge>>);

impl Meter {
    /// Add one charge.
    pub fn charge(&self, charge: Charge) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(charge);
    }

    /// Take every charge so far.
    #[must_use]
    pub fn take(&self) -> Vec<Charge> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// The tokens and the cost of `charges`.
#[must_use]
pub fn total(charges: &[Charge]) -> (TokenUsage, u64) {
    charges
        .iter()
        .fold((TokenUsage::default(), 0), |(usage, cost), charge| {
            (
                usage.saturating_add(charge.usage),
                cost.saturating_add(charge.cost_usd_micros.unwrap_or(0)),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_meter_sums_its_charges() {
        let meter = Meter::default();
        meter.charge(Charge::new(Some("m"), TokenUsage::new(3, 2)).cost(5));
        meter.charge(Charge::new(None, TokenUsage::new(1, 0)));
        let charges = meter.take();
        assert_eq!(total(&charges), (TokenUsage::new(4, 2), 5));
        assert_eq!(meter.take(), Vec::<Charge>::new());
    }

    #[test]
    fn a_bad_model_id_is_recorded_as_unknown() {
        let call = Charge::new(Some(""), TokenUsage::new(1, 1)).ledger_call();
        assert_eq!(call.model(), UNKNOWN_MODEL_ID);
        let call = Charge::new(Some("good"), TokenUsage::new(1, 1))
            .cost(4)
            .ledger_call();
        assert_eq!(call.model(), "good");
        assert_eq!(call.cost_usd_micros(), Some(4));
    }
}
