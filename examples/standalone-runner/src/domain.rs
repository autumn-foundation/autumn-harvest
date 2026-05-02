use serde::{Deserialize, Serialize};

pub const RUNNER_QUEUE: &str = "standalone";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StandaloneOrder {
    pub order_id: String,
    pub sku: String,
    pub quantity: u32,
}
