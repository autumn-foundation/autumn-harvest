//! Memory: bounded blocks the agent keeps across runs.
//!
//! A run reads the blocks once, at its start, through the activity
//! `agent_memory_snapshot`. The snapshot goes into the system prompt and stays
//! the same for the whole run, so the prompt prefix stays stable for the
//! provider cache. The `memory` tool writes through at once. Its edits show in
//! the next run, or in the next follow-up segment.
//!
//! The snapshot is an activity result, so replay reads the recorded snapshot
//! and never the store.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use crate::error::{AgentError, ErrorKind};
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolContext, ToolEffect};

/// The name of the built-in memory tool.
pub const MEMORY_TOOL: &str = "memory";

/// Whose memory a run reads and writes. The app picks the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoryScope(String);

impl MemoryScope {
    /// Wrap a scope key.
    #[must_use]
    pub fn new(scope: impl Into<String>) -> Self {
        Self(scope.into())
    }

    /// The agent-wide scope, `"agent"`.
    #[must_use]
    pub fn agent() -> Self {
        Self::new("agent")
    }

    /// The scope as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One labelled list of entries with a size limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryBlock {
    /// The label the model uses for the block, for example `memory`.
    pub label: String,
    /// What the block is for, for the model.
    pub description: String,
    /// The entries, oldest first.
    pub entries: Vec<String>,
    /// The most characters the entries may hold together.
    pub limit_chars: usize,
}

impl MemoryBlock {
    /// An empty block.
    #[must_use]
    pub fn new(
        label: impl Into<String>,
        description: impl Into<String>,
        limit_chars: usize,
    ) -> Self {
        Self {
            label: label.into(),
            description: description.into(),
            entries: Vec::new(),
            limit_chars,
        }
    }

    /// The characters that the entries use.
    #[must_use]
    pub fn used_chars(&self) -> usize {
        self.entries.iter().map(|entry| entry.chars().count()).sum()
    }

    /// The default blocks: `memory` (2 200 characters) and `user` (1 375).
    #[must_use]
    pub fn defaults() -> Vec<Self> {
        vec![
            Self::new(
                "memory",
                "Facts, decisions, and lessons to remember across runs.",
                2_200,
            ),
            Self::new(
                "user",
                "Who you work for: preferences, style, standing instructions.",
                1_375,
            ),
        ]
    }
}

/// One edit to a memory block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum MemoryOp {
    /// Add an entry.
    Add {
        /// The block label.
        block: String,
        /// The entry text.
        text: String,
    },
    /// Replace the one entry that contains `old`.
    Replace {
        /// The block label.
        block: String,
        /// Text that occurs in exactly one entry.
        old: String,
        /// The new entry text.
        text: String,
    },
    /// Remove the one entry that contains `old`.
    Remove {
        /// The block label.
        block: String,
        /// Text that occurs in exactly one entry.
        old: String,
    },
}

impl MemoryOp {
    /// The label of the block that this edit changes.
    #[must_use]
    pub fn block(&self) -> &str {
        match self {
            Self::Add { block, .. } | Self::Replace { block, .. } | Self::Remove { block, .. } => {
                block
            }
        }
    }
}

/// Apply one edit to a block. It keeps the size limit and needs a unique
/// match.
///
/// # Errors
///
/// Returns a `Tool` error, written for the model, for an empty entry, an edit
/// that does not fit, or an `old` text that matches no entry or more than
/// one.
pub fn apply_op(block: &mut MemoryBlock, op: &MemoryOp) -> Result<(), AgentError> {
    match op {
        MemoryOp::Add { text, .. } => {
            let text = non_empty(text)?;
            check_fits(block, None, text)?;
            block.entries.push(text.to_owned());
        }
        MemoryOp::Replace { old, text, .. } => {
            let text = non_empty(text)?;
            let index = find_unique(block, old)?;
            check_fits(block, Some(index), text)?;
            if let Some(entry) = block.entries.get_mut(index) {
                text.clone_into(entry);
            }
        }
        MemoryOp::Remove { old, .. } => {
            let index = find_unique(block, old)?;
            block.entries.remove(index);
        }
    }
    Ok(())
}

fn non_empty(text: &str) -> Result<&str, AgentError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(AgentError::new(
            ErrorKind::Tool,
            "a memory entry must not be empty",
        ));
    }
    Ok(text)
}

fn check_fits(block: &MemoryBlock, replacing: Option<usize>, text: &str) -> Result<(), AgentError> {
    let freed = replacing
        .and_then(|index| block.entries.get(index))
        .map_or(0, |entry| entry.chars().count());
    let after = block
        .used_chars()
        .saturating_sub(freed)
        .saturating_add(text.chars().count());
    if after > block.limit_chars {
        return Err(AgentError::new(
            ErrorKind::Tool,
            format!(
                "memory block {:?} is full: this edit needs {after} of {} characters. \
                 Remove or merge entries first.",
                block.label, block.limit_chars
            ),
        ));
    }
    Ok(())
}

fn find_unique(block: &MemoryBlock, old: &str) -> Result<usize, AgentError> {
    let old = old.trim();
    if old.is_empty() {
        return Err(AgentError::new(
            ErrorKind::Tool,
            "give `old`: text from the entry to change",
        ));
    }
    let matches: Vec<usize> = block
        .entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.contains(old))
        .map(|(index, _)| index)
        .collect();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(AgentError::new(
            ErrorKind::Tool,
            format!("no entry in {:?} contains {old:?}", block.label),
        )),
        _ => Err(AgentError::new(
            ErrorKind::Tool,
            format!(
                "{} entries in {:?} contain {old:?}: give more text",
                matches.len(),
                block.label
            ),
        )),
    }
}

/// Render the blocks as the frozen system-prompt section.
#[must_use]
pub fn render_snapshot(blocks: &[MemoryBlock]) -> String {
    let mut out = String::from(
        "## Memory\nYour memory as it was when this run started. Edit it with the \
         `memory` tool. Your edits show from the next run.\n",
    );
    for block in blocks {
        let _ = write!(
            out,
            "\n<memory block=\"{}\" used=\"{}/{}\">\n{}\n",
            block.label,
            block.used_chars(),
            block.limit_chars,
            block.description
        );
        for entry in &block.entries {
            out.push_str("- ");
            out.push_str(entry);
            out.push('\n');
        }
        out.push_str("</memory>\n");
    }
    out
}

/// Storage for memory blocks.
pub trait MemoryStore: Send + Sync + std::fmt::Debug {
    /// Load every block of a scope. A new scope gets the default blocks.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>>;

    /// Apply one edit and store it. Call [`apply_op`] so the rules are the
    /// same in every store.
    ///
    /// # Errors
    ///
    /// Returns the [`apply_op`] error, or an error when the store cannot
    /// write.
    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>>;
}

/// A [`MemoryStore`] in process memory. Its contents go when the process
/// stops, so use it for tests and demos only.
#[derive(Debug)]
pub struct InMemoryMemoryStore {
    template: Vec<MemoryBlock>,
    scopes: Mutex<HashMap<MemoryScope, Vec<MemoryBlock>>>,
}

impl Default for InMemoryMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMemoryStore {
    /// A store whose new scopes start with [`MemoryBlock::defaults`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_template(MemoryBlock::defaults())
    }

    /// A store whose new scopes start with copies of `template`.
    #[must_use]
    pub fn with_template(template: Vec<MemoryBlock>) -> Self {
        Self {
            template,
            scopes: Mutex::new(HashMap::new()),
        }
    }
}

impl MemoryStore for InMemoryMemoryStore {
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        let blocks = self
            .scopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(scope)
            .cloned()
            .unwrap_or_else(|| self.template.clone());
        Box::pin(std::future::ready(Ok(blocks)))
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        let mut scopes = self.scopes.lock().unwrap_or_else(PoisonError::into_inner);
        let blocks = scopes
            .entry(scope.clone())
            .or_insert_with(|| self.template.clone());
        let result = apply_to(blocks, &op);
        drop(scopes);
        Box::pin(std::future::ready(result))
    }
}

/// Apply `op` to the block it names. Returns the block after the edit.
fn apply_to(blocks: &mut [MemoryBlock], op: &MemoryOp) -> Result<MemoryBlock, AgentError> {
    let Some(block) = blocks.iter_mut().find(|block| block.label == op.block()) else {
        return Err(unknown_block(op.block(), blocks));
    };
    apply_op(block, op)?;
    Ok(block.clone())
}

fn unknown_block(label: &str, blocks: &[MemoryBlock]) -> AgentError {
    let known: Vec<&str> = blocks.iter().map(|block| block.label.as_str()).collect();
    AgentError::new(
        ErrorKind::Tool,
        format!("no memory block {label:?}; the blocks are {known:?}"),
    )
}

/// The built-in `memory` tool, bound to one store and one scope.
///
/// The harness adds it to a run that has a memory scope. Its effect is
/// [`ToolEffect::Internal`], so a read-only run may still use it.
#[derive(Debug, Clone)]
pub struct MemoryTool {
    store: Arc<dyn MemoryStore>,
    scope: MemoryScope,
}

impl MemoryTool {
    /// Bind the tool to a store and a scope.
    #[must_use]
    pub fn new(store: Arc<dyn MemoryStore>, scope: MemoryScope) -> Self {
        Self { store, scope }
    }
}

impl Tool for MemoryTool {
    fn name(&self) -> &str {
        MEMORY_TOOL
    }

    fn description(&self) -> &'static str {
        "Edit your memory. action=add adds `text` to `block`. action=replace \
         changes the entry that contains `old` to `text`. action=remove deletes \
         the entry that contains `old`. Keep facts and lessons, not logs."
    }

    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["add", "replace", "remove"]},
                "block": {"type": "string", "description": "The block label, for example memory or user."},
                "text": {"type": "string", "description": "The new entry text (add, replace)."},
                "old": {"type": "string", "description": "Text from the entry to change (replace, remove)."}
            },
            "required": ["action", "block"]
        })
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::Internal
    }

    fn execute<'a>(
        &'a self,
        input: serde_json::Value,
        _ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        Box::pin(async move {
            let op: MemoryOp = serde_json::from_value(input).map_err(|err| {
                AgentError::new(ErrorKind::Tool, format!("not a valid memory edit: {err}"))
            })?;
            let block = self.store.apply(&self.scope, op).await?;
            Ok(serde_json::json!({
                "ok": true,
                "block": block.label,
                "used": block.used_chars(),
                "limit": block.limit_chars,
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block() -> MemoryBlock {
        MemoryBlock::new("memory", "notes", 20)
    }

    fn add(text: &str) -> MemoryOp {
        MemoryOp::Add {
            block: "memory".into(),
            text: text.into(),
        }
    }

    #[test]
    fn add_replace_and_remove_keep_the_limit() {
        let mut b = block();
        apply_op(&mut b, &add("tea")).unwrap();
        apply_op(&mut b, &add("coffee")).unwrap();
        assert_eq!(b.used_chars(), 9);
        let err = apply_op(&mut b, &add("a much longer entry")).unwrap_err();
        assert!(err.message().contains("full"));
        apply_op(
            &mut b,
            &MemoryOp::Replace {
                block: "memory".into(),
                old: "tea".into(),
                text: "green tea".into(),
            },
        )
        .unwrap();
        apply_op(
            &mut b,
            &MemoryOp::Remove {
                block: "memory".into(),
                old: "coffee".into(),
            },
        )
        .unwrap();
        assert_eq!(b.entries, vec!["green tea".to_owned()]);
    }

    #[test]
    fn an_ambiguous_or_missing_match_is_refused() {
        let mut b = block();
        apply_op(&mut b, &add("tea a")).unwrap();
        apply_op(&mut b, &add("tea b")).unwrap();
        let remove = |old: &str| MemoryOp::Remove {
            block: "memory".into(),
            old: old.into(),
        };
        assert!(
            apply_op(&mut b, &remove("tea"))
                .unwrap_err()
                .message()
                .contains("2 entries")
        );
        assert!(
            apply_op(&mut b, &remove("milk"))
                .unwrap_err()
                .message()
                .contains("no entry")
        );
        assert!(apply_op(&mut b, &add("  ")).is_err());
    }

    #[tokio::test]
    async fn the_store_keeps_scopes_apart() {
        let store = InMemoryMemoryStore::new();
        let a = MemoryScope::new("a");
        let b = MemoryScope::agent();
        store
            .apply(
                &a,
                MemoryOp::Add {
                    block: "user".into(),
                    text: "likes tea".into(),
                },
            )
            .await
            .unwrap();
        let loaded = store.load(&a).await.unwrap();
        assert_eq!(loaded[1].entries, vec!["likes tea".to_owned()]);
        assert_eq!(
            store.load(&b).await.unwrap()[1].entries,
            Vec::<String>::new()
        );
        let unknown = store
            .apply(
                &a,
                MemoryOp::Add {
                    block: "nope".into(),
                    text: "x".into(),
                },
            )
            .await
            .unwrap_err();
        assert!(unknown.message().contains("no memory block"));
    }

    #[test]
    fn the_snapshot_lists_each_block_and_entry() {
        let mut b = block();
        apply_op(&mut b, &add("tea")).unwrap();
        let text = render_snapshot(&[b]);
        assert!(text.contains("<memory block=\"memory\" used=\"3/20\">"));
        assert!(text.contains("- tea"));
    }
}
