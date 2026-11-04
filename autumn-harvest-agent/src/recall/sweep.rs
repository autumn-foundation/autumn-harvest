//! The recall sweep on the embedded SQLite backend.

use std::fmt::Write as _;

use autumn_harvest::entity::ENTITY_OP_SIGNAL;
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use sha2::{Digest, Sha256};

use super::{RecallFixture, RecallItem, RecallOutcome, RecallReport, RecallRow};
use crate::agent::{AgentId, AgentOp};
use crate::harness::AgentHarness;
use crate::score::{ExactMatch, ScoreContext, ScoreInput, Scorer};
use crate::sqlite;
use crate::types::AgentStop;

/// The hex digits of the run fingerprint in an agent name.
const FINGERPRINT_HEX: usize = 12;

/// Run each item of `fixture` under each of `configs`, and score each last
/// answer with [`ExactMatch`].
///
/// Register `harness` on `rt` first, with a profile for each config name.
/// [`harness`](super::harness) gives the standard configs.
///
/// Each item runs on the agent `<config>/<item>.<fingerprint>`. The
/// fingerprint hashes the item, the profile and the model id. Each op has
/// the idempotency key `recall:<agent name>:<n>`. A rerun with the same
/// inputs on the same database therefore calls no model for a finished
/// item. A changed item, profile or model gets new agents.
///
/// A failure ends its item only. The row holds the error, and the sweep goes
/// on, so the report always has one row for each config and item. A failed
/// item stays failed on that database.
pub async fn sweep(
    rt: &mut SqliteRuntime,
    harness: &AgentHarness,
    fixture: &RecallFixture,
    configs: &[String],
) -> RecallReport {
    let mut rows = Vec::with_capacity(configs.len() * fixture.items.len());
    for config in configs {
        for item in &fixture.items {
            let outcome = run_item(rt, harness, config, item)
                .await
                .unwrap_or_else(|error| RecallOutcome::Failed { error });
            tracing::info!(
                config = %config,
                item = %item.id,
                failed = matches!(outcome, RecallOutcome::Failed { .. }),
                "recall item done"
            );
            rows.push(RecallRow {
                config: config.clone(),
                item: item.id.clone(),
                outcome,
            });
        }
    }
    RecallReport {
        configs: configs.to_vec(),
        items: fixture.items.iter().map(|item| item.id.clone()).collect(),
        rows,
    }
}

/// Send each op of `item` to its agent under `config`, then score the last
/// answer.
async fn run_item(
    rt: &mut SqliteRuntime,
    harness: &AgentHarness,
    config: &str,
    item: &RecallItem,
) -> Result<RecallOutcome, String> {
    let question = item.ops.last().ok_or("the item has no op")?;
    let name = agent_name(harness, config, item)?;
    let id = AgentId::new(config, &name).map_err(|err| err.to_string())?;
    for (n, op) in item.ops.iter().enumerate() {
        let key = format!("recall:{name}:{n}");
        sqlite::send(rt, &id, &AgentOp::message(op.as_str()), Some(&key))
            .map_err(|err| format!("op {n} was not sent: {err}"))?;
        let state = sqlite::drive(rt, &id)
            .await
            .map_err(|err| format!("op {n} did not run: {err}"))?;
        // An idle agent waits for its next op. Any other state, such as a
        // retry timer, means that the op did not end.
        match state {
            Some(RunState::WaitingSignal(signal)) if signal == ENTITY_OP_SIGNAL => {}
            other => return Err(format!("op {n} did not end: {other:?}")),
        }
    }
    let state = sqlite::state(rt, &id)
        .await
        .map_err(|err| format!("the state does not read: {err}"))?
        .ok_or("the agent has no run")?;
    // After a failed op, `last_answer` can hold the answer of an older op.
    // An item with a failed op therefore has no score.
    if state.failures > 0 {
        return Err(state
            .last_error
            .unwrap_or_else(|| format!("{} ops failed", state.failures)));
    }
    let answer = state.last_answer.ok_or("the agent has no answer")?;
    let input = ScoreInput::new(
        question.as_str(),
        answer.as_str(),
        state.last_stop.unwrap_or(AgentStop::Completed),
    );
    let verdict = ExactMatch::new(item.expected.as_str())
        .score(&input, &ScoreContext::unmetered(harness))
        .await
        .map_err(|err| format!("the scorer failed: {err}"))?;
    Ok(RecallOutcome::Scored { answer, verdict })
}

/// The agent name of `item` under `config`: the item id and a fingerprint
/// of the item, the profile and the model id.
fn agent_name(harness: &AgentHarness, config: &str, item: &RecallItem) -> Result<String, String> {
    let profile = harness
        .agent_profile(config)
        .ok_or_else(|| format!("the harness has no profile {config:?}"))?;
    let model = harness.model(profile.model.as_deref())?;
    let inputs = serde_json::json!({
        "item": item,
        "profile": profile,
        "model": model.model_id(),
    });
    let digest = Sha256::digest(inputs.to_string().as_bytes());
    let hex = digest.iter().take(FINGERPRINT_HEX / 2).fold(
        String::with_capacity(FINGERPRINT_HEX),
        |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        },
    );
    Ok(format!("{}.{hex}", item.id))
}
