//! Agent mesh primitives: peers, mandates and delegation.
//!
//! A run with a [`Mandate`] can delegate a task to a [`Peer`]. The peer runs
//! as a sub-agent inside the parent workflow. Each child model call and tool
//! call is an activity of the parent history. After a crash, replay reads
//! each finished call and does not send it again.
//!
//! A mandate bounds the delegation tree. A child mandate is the meet of the
//! parent mandate and the peer role, so authority only narrows down the tree.
//! A refused delegation returns a named [`Refusal`].
//!
//! See `docs/agent-mesh.md` for the whole pattern.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::message::{TokenUsage, ToolDefinition};
use crate::types::AgentStop;

/// The name of the workflow tool that delegates a task to a peer.
pub const DELEGATE_TOOL: &str = "delegate";

/// The metadata key that carries a mandate between processes.
pub const MANDATE_META_KEY: &str = "io.autumn-harvest/mandate";

/// The default bound on delegation depth. The root agent has depth 0.
pub const DEFAULT_MAX_DEPTH: u32 = 3;

/// The default bound on delegations in one run tree.
pub const DEFAULT_MAX_DELEGATIONS: u32 = 16;

/// The most characters in an agent name.
pub const MAX_NAME_LEN: usize = 64;

/// The most bytes in a tool name or a flow label of a mandate.
pub const MAX_ITEM_LEN: usize = 128;

/// The most entries in one set or chain of a mandate.
pub const MAX_SET_LEN: usize = 256;

/// The name that a record shows for a peer name that is not valid.
pub const INVALID_NAME: &str = "_invalid";

/// The keys of a mandate on the wire. [`Mandate::from_meta`] refuses any
/// other key, because a newer sender can add a field that narrows the run.
const WIRE_KEYS: [&str; 7] = [
    "chain",
    "max_depth",
    "peers",
    "tools",
    "max_delegations",
    "read_only",
    "flow_labels",
];

/// Whether `item` is a valid tool name or flow label: 1 to
/// [`MAX_ITEM_LEN`] bytes with no control character.
fn valid_item(item: &str) -> bool {
    !item.is_empty() && item.len() <= MAX_ITEM_LEN && !item.chars().any(char::is_control)
}

/// Whether `name` is a valid agent name.
///
/// A name has 1 to [`MAX_NAME_LEN`] characters from `a-z`, `0-9`, `_` and
/// `-`. A name shows in a tool description and in a run id, so it carries no
/// other text.
#[must_use]
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name
            .bytes()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

/// The meet of two tool allowlists. `None` means every tool.
fn meet_tools(
    left: Option<&BTreeSet<String>>,
    right: Option<&BTreeSet<String>>,
) -> Option<BTreeSet<String>> {
    match (left, right) {
        (None, None) => None,
        (Some(only), None) | (None, Some(only)) => Some(only.clone()),
        (Some(left), Some(right)) => Some(left.intersection(right).cloned().collect()),
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

/// One agent in the roster of a harness: its role.
///
/// The roster is operator state. A run reads the peers it may call through
/// one recorded activity, so a roster change cannot change a recorded run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// The name that a mandate and the `delegate` tool use.
    pub name: String,
    /// What the peer does. The delegating model reads it.
    pub description: String,
    /// The system prompt of the peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// The harness tools that the peer may call. `None` means every tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<BTreeSet<String>>,
    /// The peers that this peer may delegate to.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub peers: BTreeSet<String>,
    /// The bound on tool rounds of one delegated run.
    pub max_steps: u32,
    /// The bound on tokens of one delegated run. The parent remainder can
    /// lower it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
    /// Refuse every tool call of the peer that writes or acts outside.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

impl Peer {
    /// A peer with every harness tool, no peers of its own, and the default
    /// step bound.
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            system: None,
            tools: None,
            peers: BTreeSet::new(),
            max_steps: crate::types::DEFAULT_MAX_STEPS,
            max_total_tokens: None,
            read_only: false,
        }
    }

    /// Set the system prompt.
    #[must_use]
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Let the peer call only these harness tools.
    #[must_use]
    pub fn tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    /// Let the peer delegate to these peers.
    #[must_use]
    pub fn peers<I, S>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.peers = peers.into_iter().map(Into::into).collect();
        self
    }

    /// Set the bound on tool rounds of one delegated run.
    #[must_use]
    pub const fn max_steps(mut self, max_steps: u32) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set the bound on tokens of one delegated run.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }

    /// Refuse every tool call of the peer that writes or acts outside.
    #[must_use]
    pub const fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
}

/// The authority of one agent in a delegation tree.
///
/// A mandate names the chain of agents from the root to its holder. It bounds
/// the depth of the tree and the delegations in it. It names the peers and
/// the tools that the holder may use.
///
/// Authority only narrows down the tree. [`Mandate::delegate`] and
/// [`Mandate::accept`] return a meet, never a union.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mandate {
    /// The agents from the root to the holder. The holder is last.
    pub chain: Vec<String>,
    /// The bound on depth. The root agent has depth 0.
    pub max_depth: u32,
    /// The peers that the holder may delegate to.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub peers: BTreeSet<String>,
    /// The harness tools that the holder may call. `None` means every tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<BTreeSet<String>>,
    /// The bound on delegations in the whole run tree.
    pub max_delegations: u32,
    /// Refuse every tool call that writes or acts outside.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    /// The flow labels that the caller tree exercised. A receiver adds them
    /// to its own run tree. Labels only grow, so a caller can only narrow a
    /// run with them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
}

impl Mandate {
    /// The root mandate of the agent `holder`, with no peers and the default
    /// bounds.
    #[must_use]
    pub fn new(holder: impl Into<String>) -> Self {
        Self {
            chain: vec![holder.into()],
            max_depth: DEFAULT_MAX_DEPTH,
            peers: BTreeSet::new(),
            tools: None,
            max_delegations: DEFAULT_MAX_DELEGATIONS,
            read_only: false,
            flow_labels: BTreeSet::new(),
        }
    }

    /// Let the holder delegate to these peers.
    #[must_use]
    pub fn peers<I, S>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.peers = peers.into_iter().map(Into::into).collect();
        self
    }

    /// Let the holder call only these harness tools.
    #[must_use]
    pub fn tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    /// Set the bound on depth.
    #[must_use]
    pub const fn max_depth(mut self, max_depth: u32) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Set the bound on delegations in the run tree.
    #[must_use]
    pub const fn max_delegations(mut self, max: u32) -> Self {
        self.max_delegations = max;
        self
    }

    /// Refuse every tool call that writes or acts outside.
    #[must_use]
    pub const fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// The agent that holds this mandate.
    #[must_use]
    pub fn holder(&self) -> &str {
        self.chain.last().map_or("", String::as_str)
    }

    /// The depth of the holder. The root agent has depth 0.
    #[must_use]
    pub fn depth(&self) -> u32 {
        u32::try_from(self.chain.len().saturating_sub(1)).unwrap_or(u32::MAX)
    }

    /// Whether the holder may call the harness tool `name`.
    #[must_use]
    pub fn allows_tool(&self, name: &str) -> bool {
        self.tools.as_ref().is_none_or(|tools| tools.contains(name))
    }

    /// Check the mandate.
    ///
    /// # Errors
    ///
    /// Returns why the mandate is not valid.
    pub fn validate(&self) -> Result<(), String> {
        if self.chain.is_empty() {
            return Err("the mandate chain is empty".to_owned());
        }
        let tools = self.tools.as_ref().map_or(0, BTreeSet::len);
        if [
            self.chain.len(),
            self.peers.len(),
            tools,
            self.flow_labels.len(),
        ]
        .into_iter()
        .any(|len| len > MAX_SET_LEN)
        {
            return Err(format!(
                "a mandate set holds more than {MAX_SET_LEN} entries"
            ));
        }
        if let Some(bad) = self
            .tools
            .iter()
            .flatten()
            .chain(&self.flow_labels)
            .find(|item| !valid_item(item))
        {
            return Err(format!(
                "{:?} is not a valid tool name or label",
                crate::harness::truncate(bad, MAX_NAME_LEN)
            ));
        }
        if let Some(bad) = self
            .chain
            .iter()
            .chain(&self.peers)
            .find(|name| !valid_name(name))
        {
            return Err(format!(
                "{:?} is not a valid agent name",
                crate::harness::truncate(bad, MAX_NAME_LEN)
            ));
        }
        let distinct: BTreeSet<&String> = self.chain.iter().collect();
        if distinct.len() != self.chain.len() {
            return Err("the mandate chain names an agent twice".to_owned());
        }
        if self.depth() > self.max_depth {
            return Err(format!(
                "the chain depth {} passes the bound {}",
                self.depth(),
                self.max_depth
            ));
        }
        Ok(())
    }

    /// The mandate of `peer` when the holder delegates to it.
    ///
    /// `used` is the count of delegations in the run tree so far.
    ///
    /// # Errors
    ///
    /// Returns the [`Refusal`] when the holder may not delegate to `peer`.
    pub fn delegate(&self, peer: &Peer, used: u32) -> Result<Self, Refusal> {
        if !self.peers.contains(&peer.name) {
            return Err(Refusal::NotPermitted);
        }
        if self.chain.contains(&peer.name) {
            return Err(Refusal::Cycle);
        }
        if self.depth().saturating_add(1) > self.max_depth {
            return Err(Refusal::DepthExceeded);
        }
        if used >= self.max_delegations {
            return Err(Refusal::FanOutExhausted);
        }
        let mut chain = self.chain.clone();
        chain.push(peer.name.clone());
        Ok(Self {
            chain,
            max_depth: self.max_depth,
            peers: self.peers.intersection(&peer.peers).cloned().collect(),
            tools: meet_tools(self.tools.as_ref(), peer.tools.as_ref()),
            max_delegations: self.max_delegations,
            read_only: self.read_only || peer.read_only,
            flow_labels: self.flow_labels.clone(),
        })
    }

    /// The mandate to send with a call to `peer` in another process.
    ///
    /// It is [`Mandate::delegate`] with two changes for the wire. The
    /// receiver starts its own delegation count at zero, so `max_delegations`
    /// becomes the remainder of this tree. `flow_labels` holds the labels
    /// that this tree exercised, so the receiver keeps them.
    ///
    /// # Errors
    ///
    /// Returns the [`Refusal`] when the holder may not delegate to `peer`.
    /// Returns [`Refusal::InvalidMandate`] when the result fails
    /// [`Mandate::validate`], so the receiver could not read it.
    pub fn outbound(
        &self,
        peer: &Peer,
        used: u32,
        flow_labels: &BTreeSet<String>,
    ) -> Result<Self, Refusal> {
        let mut child = self.delegate(peer, used)?;
        child.max_delegations = self.max_delegations.saturating_sub(used.saturating_add(1));
        child.flow_labels.extend(flow_labels.iter().cloned());
        child.validate().map_err(|_| Refusal::InvalidMandate)?;
        Ok(child)
    }

    /// Narrow this local mandate by a mandate that a caller in another
    /// process sent.
    ///
    /// This mandate must be a root mandate, such as [`Mandate::new`] gives.
    /// The result is the meet of both mandates. Its flow labels are the union,
    /// because more labels only narrow a run.
    ///
    /// # Errors
    ///
    /// Returns the [`Refusal`] when either mandate is malformed, or when the
    /// inbound chain makes a cycle or passes a depth bound.
    pub fn accept(&self, inbound: &Self) -> Result<Self, Refusal> {
        if self.depth() != 0 {
            return Err(Refusal::InvalidMandate);
        }
        inbound.validate().map_err(|_| Refusal::InvalidMandate)?;
        let holder = self.holder();
        let mut chain = inbound.chain.clone();
        if inbound.holder() != holder {
            // The caller named this agent by another name, or not at all.
            // The local holder joins the chain, so a later hop sees it.
            if chain.iter().any(|name| name == holder) {
                return Err(Refusal::Cycle);
            }
            chain.push(holder.to_owned());
        }
        let merged = Self {
            chain,
            max_depth: self.max_depth.min(inbound.max_depth),
            peers: self.peers.intersection(&inbound.peers).cloned().collect(),
            tools: meet_tools(self.tools.as_ref(), inbound.tools.as_ref()),
            max_delegations: self.max_delegations.min(inbound.max_delegations),
            read_only: self.read_only || inbound.read_only,
            flow_labels: self
                .flow_labels
                .union(&inbound.flow_labels)
                .cloned()
                .collect(),
        };
        if merged.depth() > merged.max_depth {
            return Err(Refusal::DepthExceeded);
        }
        merged.validate().map_err(|_| Refusal::InvalidMandate)?;
        Ok(merged)
    }

    /// Put this mandate into `meta` under [`MANDATE_META_KEY`].
    pub fn insert_into(&self, meta: &mut Map<String, Value>) {
        // A mandate holds strings, sets, numbers and flags only, so it always
        // encodes.
        if let Ok(value) = serde_json::to_value(self) {
            meta.insert(MANDATE_META_KEY.to_owned(), value);
        }
    }

    /// Read a mandate from `meta`. Returns `None` when `meta` has none.
    ///
    /// The read is strict. An unknown key fails it, so a field that a newer
    /// sender adds to narrow the run is never dropped without a sign.
    ///
    /// # Errors
    ///
    /// Returns why the value under [`MANDATE_META_KEY`] is not a valid
    /// mandate.
    pub fn from_meta(meta: &Map<String, Value>) -> Result<Option<Self>, String> {
        let Some(value) = meta.get(MANDATE_META_KEY) else {
            return Ok(None);
        };
        let Some(fields) = value.as_object() else {
            return Err("the mandate is not a JSON object".to_owned());
        };
        if let Some(unknown) = fields.keys().find(|key| !WIRE_KEYS.contains(&key.as_str())) {
            return Err(format!(
                "the mandate has the unknown key {:?}",
                crate::harness::truncate(unknown, MAX_NAME_LEN)
            ));
        }
        let mandate: Self = serde_json::from_value(value.clone())
            .map_err(|err| format!("the mandate does not decode: {err}"))?;
        mandate.validate()?;
        Ok(Some(mandate))
    }
}

/// Why a delegation did not run.
///
/// A delegation runs the checks in the order of the variants, from
/// `InvalidArguments` to `BudgetExhausted`. Only [`Mandate::accept`] returns
/// `InvalidMandate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[error("the delegation is refused: {}", self.code())]
#[non_exhaustive]
pub enum Refusal {
    /// The arguments of the `delegate` call do not decode.
    InvalidArguments,
    /// The mandate does not name the peer.
    NotPermitted,
    /// The roster has no valid peer of that name.
    UnknownPeer,
    /// The peer is already in the chain.
    Cycle,
    /// The delegation passes the depth bound.
    DepthExceeded,
    /// The run tree has used every delegation that its mandate allows.
    FanOutExhausted,
    /// The run tree has no tokens left for a child.
    BudgetExhausted,
    /// An inbound mandate is malformed.
    InvalidMandate,
}

impl Refusal {
    /// The stable code of the verdict.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid_arguments",
            Self::NotPermitted => "not_permitted",
            Self::UnknownPeer => "unknown_peer",
            Self::Cycle => "cycle",
            Self::DepthExceeded => "depth_exceeded",
            Self::FanOutExhausted => "fan_out_exhausted",
            Self::BudgetExhausted => "budget_exhausted",
            Self::InvalidMandate => "invalid_mandate",
        }
    }

    /// The text that the delegating model reads. The peer name is cut to
    /// [`MAX_NAME_LEN`] characters.
    #[must_use]
    pub fn message(self, peer: &str) -> String {
        let peer = crate::harness::truncate(peer, MAX_NAME_LEN);
        match self {
            Self::InvalidArguments => {
                "the delegate call needs exactly the arguments agent and task".to_owned()
            }
            Self::NotPermitted => format!("this agent may not delegate to {peer:?}"),
            Self::UnknownPeer => format!("no agent named {peer:?} is known"),
            Self::Cycle => format!("{peer:?} is already in the delegation chain"),
            Self::DepthExceeded => format!("a delegation to {peer:?} passes the depth bound"),
            Self::FanOutExhausted => {
                format!("the run has used every delegation; {peer:?} cannot start")
            }
            Self::BudgetExhausted => format!("no tokens are left for {peer:?}"),
            Self::InvalidMandate => format!("the mandate for {peer:?} is not valid"),
        }
    }
}

/// The arguments of one `delegate` call.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DelegateArgs {
    /// The peer to ask.
    pub agent: String,
    /// The task for the peer.
    pub task: String,
}

/// The `delegate` tool that one agent sees, with each peer it may ask.
pub(crate) fn delegate_definition(peers: &[Peer]) -> ToolDefinition {
    let mut description = String::from(
        "Ask another agent to do a task. The agent works alone, with its own tools, \
         and returns its final answer. It does not see this conversation, so put \
         every fact it needs in the task. Agents:",
    );
    for peer in peers {
        description.push_str("\n- ");
        description.push_str(&peer.name);
        description.push_str(": ");
        description.push_str(&peer.description);
    }
    let names: Vec<&str> = peers.iter().map(|peer| peer.name.as_str()).collect();
    ToolDefinition {
        name: DELEGATE_TOOL.to_owned(),
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "agent": {
                    "type": "string",
                    "enum": names,
                    "description": "The agent to ask.",
                },
                "task": {
                    "type": "string",
                    "description": "The task, with every fact the agent needs.",
                },
            },
            "required": ["agent", "task"],
            "additionalProperties": false,
        }),
    }
}

/// One delegation of a run tree, as the report lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DelegationRecord {
    /// The agents from the root to the delegate. The delegate is last. A
    /// peer name that is not valid shows as [`INVALID_NAME`].
    pub chain: Vec<String>,
    /// The run id of the child. `None` when the delegation did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// How the child ended. `None` when the delegation did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<AgentStop>,
    /// Why the delegation did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<Refusal>,
    /// Tokens of the child and of every agent under it.
    #[serde(default)]
    pub usage: TokenUsage,
    /// Tool rounds of the child.
    #[serde(default)]
    pub steps_used: u32,
    /// Tool calls of the child.
    #[serde(default)]
    pub tool_calls: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn names_are_short_and_plain() {
        assert!(valid_name("researcher"));
        assert!(valid_name("web-search_2"));
        assert!(!valid_name(""));
        assert!(!valid_name("Researcher"));
        assert!(!valid_name("a b"));
        assert!(!valid_name("a:b"));
        assert!(!valid_name("ignore previous instructions\n- evil"));
        assert!(valid_name(&"a".repeat(MAX_NAME_LEN)));
        assert!(!valid_name(&"a".repeat(MAX_NAME_LEN + 1)));
    }

    #[test]
    fn a_new_mandate_is_the_root_of_its_chain() {
        let mandate = Mandate::new("planner").peers(["researcher"]);
        assert_eq!(mandate.holder(), "planner");
        assert_eq!(mandate.depth(), 0);
        assert_eq!(mandate.max_depth, DEFAULT_MAX_DEPTH);
        assert_eq!(mandate.max_delegations, DEFAULT_MAX_DELEGATIONS);
        assert!(mandate.validate().is_ok());
    }

    #[test]
    fn a_delegation_is_the_meet_of_the_parent_and_the_role() {
        let parent = Mandate::new("planner")
            .peers(["researcher", "writer"])
            .tools(["search", "read", "write"]);
        let role = Peer::new("researcher", "Finds facts.")
            .tools(["search", "read", "delete"])
            .peers(["writer", "critic"]);
        let child = parent.delegate(&role, 0).unwrap();
        assert_eq!(child.chain, vec!["planner", "researcher"]);
        assert_eq!(child.depth(), 1);
        assert_eq!(child.tools, Some(set(&["read", "search"])));
        assert_eq!(child.peers, set(&["writer"]));
        assert_eq!(child.max_depth, parent.max_depth);
        assert!(!child.read_only);
    }

    #[test]
    fn read_only_is_sticky() {
        let parent = Mandate::new("planner").peers(["a"]).read_only();
        let child = parent.delegate(&Peer::new("a", "A."), 0).unwrap();
        assert!(child.read_only);
        let parent = Mandate::new("planner").peers(["a"]);
        let child = parent
            .delegate(&Peer::new("a", "A.").read_only(), 0)
            .unwrap();
        assert!(child.read_only);
    }

    #[test]
    fn an_unlisted_peer_is_not_permitted() {
        let parent = Mandate::new("planner").peers(["writer"]);
        assert_eq!(
            parent.delegate(&Peer::new("researcher", "R."), 0),
            Err(Refusal::NotPermitted)
        );
    }

    #[test]
    fn a_peer_in_the_chain_is_a_cycle() {
        let root = Mandate::new("a").peers(["b", "a"]);
        assert_eq!(root.delegate(&Peer::new("a", "A."), 0), Err(Refusal::Cycle));
        let b = root
            .delegate(&Peer::new("b", "B.").peers(["a"]), 0)
            .unwrap();
        // `b` may not name `a`: the meet drops it. Widen by hand to test the
        // chain check on its own.
        let b = Mandate {
            peers: set(&["a"]),
            ..b
        };
        assert_eq!(b.delegate(&Peer::new("a", "A."), 1), Err(Refusal::Cycle));
    }

    #[test]
    fn the_depth_bound_holds() {
        let root = Mandate::new("a").peers(["b", "c"]).max_depth(1);
        let b = root
            .delegate(&Peer::new("b", "B.").peers(["c"]), 0)
            .unwrap();
        assert_eq!(b.depth(), 1);
        assert_eq!(
            b.delegate(&Peer::new("c", "C."), 1),
            Err(Refusal::DepthExceeded)
        );
        let flat = Mandate::new("a").peers(["b"]).max_depth(0);
        assert_eq!(
            flat.delegate(&Peer::new("b", "B."), 0),
            Err(Refusal::DepthExceeded)
        );
    }

    #[test]
    fn the_fan_out_bound_holds() {
        let root = Mandate::new("a").peers(["b"]).max_delegations(2);
        assert!(root.delegate(&Peer::new("b", "B."), 1).is_ok());
        assert_eq!(
            root.delegate(&Peer::new("b", "B."), 2),
            Err(Refusal::FanOutExhausted)
        );
    }

    #[test]
    fn verdict_codes_are_stable_and_distinct() {
        let all = [
            Refusal::InvalidArguments,
            Refusal::NotPermitted,
            Refusal::UnknownPeer,
            Refusal::Cycle,
            Refusal::DepthExceeded,
            Refusal::FanOutExhausted,
            Refusal::BudgetExhausted,
            Refusal::InvalidMandate,
        ];
        let codes: BTreeSet<&str> = all.iter().map(|refusal| refusal.code()).collect();
        assert_eq!(codes.len(), all.len());
        for refusal in all {
            assert_eq!(json!(refusal), json!(refusal.code()));
            assert!(refusal.to_string().contains(refusal.code()));
            if refusal != Refusal::InvalidArguments {
                assert!(refusal.message("researcher").contains("researcher"));
            }
        }
        assert_eq!(Refusal::FanOutExhausted.code(), "fan_out_exhausted");
        assert_eq!(MANDATE_META_KEY, "io.autumn-harvest/mandate");
    }

    #[test]
    fn a_long_peer_name_is_cut_in_the_message() {
        let message = Refusal::UnknownPeer.message(&"x".repeat(10_000));
        assert!(message.len() < 200, "{}", message.len());
    }

    #[test]
    fn outbound_sends_the_remainder_and_the_labels() {
        let parent = Mandate::new("planner")
            .peers(["researcher"])
            .max_delegations(10);
        let labels = set(&["untrusted_input"]);
        let child = parent
            .outbound(&Peer::new("researcher", "R."), 3, &labels)
            .unwrap();
        assert_eq!(child.max_delegations, 6, "10 less the 3 used and this one");
        assert_eq!(child.flow_labels, labels);
        assert_eq!(child.chain, vec!["planner", "researcher"]);
        assert_eq!(
            parent.outbound(&Peer::new("researcher", "R."), 10, &labels),
            Err(Refusal::FanOutExhausted)
        );
    }

    #[test]
    fn outbound_refuses_a_mandate_that_the_receiver_cannot_read() {
        let parent = Mandate::new("planner").peers(["researcher"]);
        let peer = Peer::new("researcher", "R.");
        let control = set(&["bad\u{7}label"]);
        assert_eq!(
            parent.outbound(&peer, 0, &control),
            Err(Refusal::InvalidMandate)
        );
        let many: BTreeSet<String> = (0..=MAX_SET_LEN).map(|n| format!("l{n}")).collect();
        assert_eq!(
            parent.outbound(&peer, 0, &many),
            Err(Refusal::InvalidMandate)
        );
    }

    #[test]
    fn accept_unions_the_labels_and_needs_a_root() {
        let local = Mandate {
            flow_labels: set(&["private_data"]),
            ..Mandate::new("researcher")
        };
        let inbound = Mandate {
            flow_labels: set(&["untrusted_input"]),
            ..Mandate::new("planner")
        };
        let merged = local.accept(&inbound).unwrap();
        assert_eq!(
            merged.flow_labels,
            set(&["private_data", "untrusted_input"])
        );
        let not_root = Mandate::new("a")
            .peers(["researcher"])
            .delegate(&Peer::new("researcher", "R."), 0)
            .unwrap();
        assert_eq!(not_root.accept(&inbound), Err(Refusal::InvalidMandate));
    }

    #[test]
    fn validate_bounds_tools_labels_and_sizes() {
        assert!(Mandate::new("a").tools(["read"]).validate().is_ok());
        assert!(Mandate::new("a").tools([""]).validate().is_err());
        assert!(Mandate::new("a").tools(["a\nb"]).validate().is_err());
        assert!(
            Mandate::new("a")
                .tools(["x".repeat(MAX_ITEM_LEN + 1)])
                .validate()
                .is_err()
        );
        let many = (0..=MAX_SET_LEN).map(|n| format!("t{n}"));
        assert!(Mandate::new("a").tools(many).validate().is_err());
        let labels = Mandate {
            flow_labels: set(&["ok", ""]),
            ..Mandate::new("a")
        };
        assert!(labels.validate().is_err());
    }

    #[test]
    fn from_meta_refuses_an_unknown_key() {
        let mut meta = Map::new();
        meta.insert(
            MANDATE_META_KEY.into(),
            json!({"chain": ["a"], "max_depth": 1, "max_delegations": 1, "tool": ["x"]}),
        );
        let err = Mandate::from_meta(&meta).unwrap_err();
        assert!(err.contains("tool"), "{err}");
        meta.insert(MANDATE_META_KEY.into(), json!(["not", "an", "object"]));
        assert!(Mandate::from_meta(&meta).is_err());
    }

    #[test]
    fn the_tool_allowlist_is_closed() {
        let open = Mandate::new("a");
        assert!(open.allows_tool("anything"));
        let closed = Mandate::new("a").tools(["read"]);
        assert!(closed.allows_tool("read"));
        assert!(!closed.allows_tool("write"));
    }

    #[test]
    fn validate_refuses_a_bad_mandate() {
        let empty = Mandate {
            chain: Vec::new(),
            ..Mandate::new("a")
        };
        assert!(empty.validate().is_err());
        assert!(Mandate::new("Bad Name").validate().is_err());
        assert!(Mandate::new("a").peers(["b c"]).validate().is_err());
        let twice = Mandate {
            chain: vec!["a".into(), "b".into(), "a".into()],
            ..Mandate::new("a")
        };
        assert!(twice.validate().is_err());
        let deep = Mandate {
            chain: vec!["a".into(), "b".into()],
            max_depth: 0,
            ..Mandate::new("a")
        };
        assert!(deep.validate().is_err());
    }

    #[test]
    fn a_mandate_round_trips_through_meta() {
        let mandate = Mandate::new("planner")
            .peers(["researcher"])
            .tools(["read"]);
        let mut meta = Map::new();
        mandate.insert_into(&mut meta);
        assert!(meta.contains_key(MANDATE_META_KEY));
        assert_eq!(Mandate::from_meta(&meta), Ok(Some(mandate)));
        assert_eq!(Mandate::from_meta(&Map::new()), Ok(None));
        let mut bad = Map::new();
        bad.insert(MANDATE_META_KEY.into(), json!({"chain": []}));
        assert!(Mandate::from_meta(&bad).is_err());
        let mut invalid = Map::new();
        invalid.insert(
            MANDATE_META_KEY.into(),
            json!({"chain": ["Bad Name"], "max_depth": 1, "max_delegations": 1}),
        );
        assert!(Mandate::from_meta(&invalid).is_err());
    }

    #[test]
    fn accept_narrows_by_an_inbound_mandate() {
        let local = Mandate::new("researcher")
            .peers(["search", "critic"])
            .tools(["read", "fetch"])
            .max_depth(4)
            .max_delegations(10);
        let inbound = Mandate {
            chain: vec!["planner".into(), "researcher".into()],
            max_depth: 2,
            peers: set(&["critic", "writer"]),
            tools: Some(set(&["read", "write"])),
            max_delegations: 3,
            read_only: true,
            flow_labels: BTreeSet::new(),
        };
        let merged = local.accept(&inbound).unwrap();
        assert_eq!(merged.chain, vec!["planner", "researcher"]);
        assert_eq!(merged.max_depth, 2);
        assert_eq!(merged.peers, set(&["critic"]));
        assert_eq!(merged.tools, Some(set(&["read"])));
        assert_eq!(merged.max_delegations, 3);
        assert!(merged.read_only);
    }

    #[test]
    fn accept_appends_the_local_holder_when_the_caller_named_another() {
        let local = Mandate::new("researcher").max_depth(3);
        let inbound = Mandate::new("planner").max_depth(3);
        let merged = local.accept(&inbound).unwrap();
        assert_eq!(merged.chain, vec!["planner", "researcher"]);
        assert_eq!(merged.depth(), 1);
    }

    #[test]
    fn accept_refuses_a_cycle_and_a_depth_overflow() {
        let local = Mandate::new("researcher");
        let looped = Mandate {
            chain: vec!["researcher".into(), "planner".into()],
            ..Mandate::new("x")
        };
        assert_eq!(local.accept(&looped), Err(Refusal::Cycle));
        let deep = Mandate {
            chain: vec!["a".into(), "b".into()],
            max_depth: 1,
            ..Mandate::new("x")
        };
        assert_eq!(local.accept(&deep), Err(Refusal::DepthExceeded));
    }

    #[test]
    fn accept_refuses_a_malformed_inbound_mandate() {
        let local = Mandate::new("researcher");
        assert_eq!(
            local.accept(&Mandate::new("Bad Name")),
            Err(Refusal::InvalidMandate)
        );
        let twice = Mandate {
            chain: vec!["a".into(), "a".into()],
            ..Mandate::new("a")
        };
        assert_eq!(local.accept(&twice), Err(Refusal::InvalidMandate));
    }

    fn name() -> impl Strategy<Value = String> {
        prop::sample::select(vec!["a", "b", "c", "d", "e"]).prop_map(str::to_owned)
    }

    fn names() -> impl Strategy<Value = BTreeSet<String>> {
        prop::collection::btree_set(name(), 0..5)
    }

    fn tools() -> impl Strategy<Value = Option<BTreeSet<String>>> {
        prop::option::of(prop::collection::btree_set(
            prop::sample::select(vec!["t1", "t2", "t3", "t4"]).prop_map(str::to_owned),
            0..4,
        ))
    }

    fn mandate() -> impl Strategy<Value = Mandate> {
        (
            prop::collection::btree_set(name(), 1..4),
            0_u32..5,
            names(),
            tools(),
            0_u32..5,
            any::<bool>(),
            names(),
        )
            .prop_map(
                |(chain, max_depth, peers, tools, max_delegations, read_only, flow_labels)| {
                    Mandate {
                        chain: chain.into_iter().collect(),
                        max_depth,
                        peers,
                        tools,
                        max_delegations,
                        read_only,
                        flow_labels,
                    }
                },
            )
    }

    fn peer() -> impl Strategy<Value = Peer> {
        (name(), names(), tools(), any::<bool>()).prop_map(|(name, peers, tools, read_only)| Peer {
            tools,
            peers,
            read_only,
            ..Peer::new(name, "A peer.")
        })
    }

    /// Whether `child` grants nothing that `parent` does not.
    fn narrower(child: &Mandate, parent: &Mandate) -> bool {
        let tools = match (&child.tools, &parent.tools) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(c), Some(p)) => c.is_subset(p),
        };
        tools
            && child.peers.is_subset(&parent.peers)
            && child.max_depth <= parent.max_depth
            && child.max_delegations <= parent.max_delegations
            && (child.read_only || !parent.read_only)
    }

    proptest! {
        #[test]
        fn a_delegation_never_widens_authority(parent in mandate(), role in peer(), used in 0_u32..5) {
            if let Ok(child) = parent.delegate(&role, used) {
                prop_assert!(narrower(&child, &parent));
                prop_assert_eq!(child.depth(), parent.depth() + 1);
                prop_assert!(child.depth() <= child.max_depth);
                prop_assert_eq!(child.holder(), role.name.as_str());
                prop_assert_eq!(&child.chain[..child.chain.len() - 1], &parent.chain[..]);
                let role_tools_ok = match (&child.tools, &role.tools) {
                    (_, None) => true,
                    (None, Some(_)) => false,
                    (Some(c), Some(r)) => c.is_subset(r),
                };
                prop_assert!(role_tools_ok);
                prop_assert!(child.peers.is_subset(&role.peers));
                prop_assert!(child.read_only || !role.read_only);
                prop_assert!(used < parent.max_delegations);
                prop_assert!(parent.peers.contains(&role.name));
                prop_assert!(!parent.chain.contains(&role.name));
            }
        }

        #[test]
        fn accept_is_never_wider_than_either_side(local in mandate(), inbound in mandate()) {
            if let Ok(merged) = local.accept(&inbound) {
                prop_assert!(narrower(&merged, &local));
                prop_assert!(narrower(&merged, &inbound));
                prop_assert_eq!(merged.holder(), local.holder());
                prop_assert!(merged.depth() <= merged.max_depth);
                prop_assert!(merged.validate().is_ok());
                prop_assert!(merged.flow_labels.is_superset(&local.flow_labels));
                prop_assert!(merged.flow_labels.is_superset(&inbound.flow_labels));
            }
        }

        #[test]
        fn a_delegation_that_meets_every_bound_is_admitted(parent in mandate(), role in peer(), used in 0_u32..5) {
            let admissible = parent.peers.contains(&role.name)
                && !parent.chain.contains(&role.name)
                && parent.depth() < parent.max_depth
                && used < parent.max_delegations;
            prop_assert_eq!(parent.delegate(&role, used).is_ok(), admissible);
        }

        #[test]
        fn outbound_never_widens_the_delegation(parent in mandate(), role in peer(), used in 0_u32..5) {
            if let Ok(child) = parent.outbound(&role, used, &BTreeSet::new()) {
                prop_assert!(narrower(&child, &parent));
                prop_assert!(child.max_delegations < parent.max_delegations);
            }
        }

        #[test]
        fn a_mandate_survives_the_wire(mandate in mandate()) {
            let mut meta = Map::new();
            mandate.insert_into(&mut meta);
            let back: Mandate = serde_json::from_value(meta[MANDATE_META_KEY].clone()).unwrap();
            prop_assert_eq!(back, mandate);
        }
    }
}
