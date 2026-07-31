//! Shard routing and per-shard database pools.
//!
//! Autumn-Harvest can spread workflow state across several independent
//! Postgres databases. Each workflow execution lives entirely on a single
//! shard — the event log, task queue rows, timers, signals, and dead-letter
//! entries for an execution all join back to the same database — so per-
//! workflow ACID guarantees are preserved without cross-shard transactions.
//!
//! This module provides the two primitives used to wire that design into the
//! runtime:
//!
//! * [`ShardRouter`] picks a [`ShardId`] for a *new* workflow. For existing
//!   workflows the shard is already encoded in the [`ExecutionId`] UUID and
//!   no routing decision is required.
//! * [`ShardedDbPool`] owns one [`crate::worker::DbPool`] per shard and
//!   resolves a pool from either a `ShardId` or an `ExecutionId`. Single-DB
//!   deployments use [`ShardedDbPool::single`], which places the only pool at
//!   `ShardId(0)` and behaves identically to the pre-sharding code path.
//!
//! Routing is deliberately directory-less. Because every `ExecutionId` writes
//! its shard into the UUID's first two bytes, any holder of an
//! `ExecutionId` can resolve to the correct pool in O(1). Lookups for ids that
//! were minted before sharding (or in tests) produce
//! [`ShardId::UNENCODED`]; the pool falls back to a configured default shard
//! for those cases.

use std::collections::BTreeMap;
use std::hash::Hasher;

use crate::types::{ExecutionId, ShardId};
#[cfg(feature = "db")]
use crate::worker::DbPool;

/// Where a brand-new workflow should be placed (issue #697).
///
/// The default, [`ShardPlacement::Auto`], is today's rendezvous routing and is
/// byte-for-byte unchanged. The other two variants are *explicit placement
/// policy* for deployments with a data-residency or tenant-isolation
/// obligation, where "whichever shard the hash picked" is not an acceptable
/// answer.
///
/// Placement is decided **before** the execution id is minted, so the chosen
/// shard is baked into the `ExecutionId` bytes and every later lookup — plus
/// every child workflow, continue-as-new successor, retry, and reset fork,
/// which all inherit `exec_id.shard()` — stays on the pinned shard. No event
/// is written and no migration is involved.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::shard::{ShardPlacement, ShardRouter};
/// use autumn_harvest::types::ShardId;
///
/// let router = ShardRouter::new(
///     vec![ShardId::new(0), ShardId::new(1)],
///     vec![ShardId::new(0), ShardId::new(1)],
///     ShardId::new(0),
/// )
/// .with_residency_map([("eu".to_string(), ShardId::new(1))]);
///
/// // Unpinned: today's hash.
/// let auto = router.resolve_placement(&ShardPlacement::Auto, "wf", "order-42")?;
/// assert_eq!(auto, router.pick_for_new_workflow("wf", "order-42"));
///
/// // Pinned by residency key: always the EU database.
/// let eu = router.resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "order-42")?;
/// assert_eq!(eu, ShardId::new(1));
/// # Ok::<(), autumn_harvest::shard::ShardPlacementError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ShardPlacement {
    /// Route by rendezvous hash over `(workflow_name, workflow_id)`.
    ///
    /// This is the pre-#697 behaviour and the default for every caller that
    /// does not opt in.
    #[default]
    Auto,
    /// Pin to a concrete shard.
    ///
    /// Rejected unless the shard is in the router's `readable_shards` *and*
    /// `writable_shards` sets.
    Shard(ShardId),
    /// Pin via a stable, operator-declared residency key (e.g. `"eu"`).
    ///
    /// Resolved through [`ShardRouter::with_residency_map`]. An unmapped key is
    /// an error, never a hash fallback.
    ResidencyKey(String),
}

impl ShardPlacement {
    /// Convenience constructor for [`ShardPlacement::ResidencyKey`].
    #[must_use]
    pub fn residency_key(key: impl Into<String>) -> Self {
        Self::ResidencyKey(key.into())
    }

    /// Is this the default (unpinned) placement?
    #[must_use]
    pub const fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }
}

/// Why an explicit [`ShardPlacement`] could not be honoured.
///
/// Every variant is a caller error that must surface as a `400`-class failure.
/// Placement never falls back to the default shard: a silent fallback is
/// exactly the failure mode explicit pinning exists to remove.
///
/// # Disclosure
///
/// [`Display`](std::fmt::Display) renders the **operator** view and enumerates
/// the valid shard set / declared residency keys, which is what you want in a
/// log line or a Rust-API caller's error. Do **not** return it verbatim to an
/// untrusted HTTP caller — residency keys are frequently tenant- or
/// region-identifying, so echoing the declared set lets one probe enumerate
/// your shard topology, live drain state, and other tenants' keys. Use
/// [`ShardPlacementError::client_message`] on any caller-facing boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ShardPlacementError {
    /// The requested shard is not in the deployment's `readable_shards` set.
    #[error(
        "shard {requested} is not a configured shard; readable shards are {}",
        format_shards(readable)
    )]
    #[non_exhaustive]
    UnknownShard {
        /// The shard the caller asked for.
        requested: ShardId,
        /// The shards this deployment can read from.
        readable: Vec<ShardId>,
    },
    /// The requested shard exists but has been drained out of
    /// `writable_shards`, so it is deliberately not accepting new workflows.
    #[error(
        "shard {requested} is readable but not writable (drained); writable shards are {}",
        format_shards(writable)
    )]
    #[non_exhaustive]
    ShardNotWritable {
        /// The shard the caller asked for.
        requested: ShardId,
        /// The shards that currently accept new workflows.
        writable: Vec<ShardId>,
    },
    /// The residency key has no entry in the router's residency map.
    #[error(
        "residency key '{key}' is not mapped to a shard; declared keys are [{}]",
        known.join(", ")
    )]
    #[non_exhaustive]
    UnknownResidencyKey {
        /// The key the caller asked for.
        key: String,
        /// The residency keys this deployment declares.
        known: Vec<String>,
    },
    /// The residency key was empty or whitespace-only.
    #[error("residency key must not be blank")]
    EmptyResidencyKey,
}

impl ShardPlacementError {
    /// The **caller-facing** rendering, safe to return over HTTP.
    ///
    /// Names the value the caller got wrong and what to do about it, but never
    /// enumerates the deployment's shard set, live drain state, or the declared
    /// residency keys — see the type-level "Disclosure" note. A caller does not
    /// need the valid set to correct their own request; an operator reads the
    /// full [`Display`](std::fmt::Display) form from the log and the audit row.
    #[must_use]
    pub fn client_message(&self) -> String {
        match self {
            Self::UnknownShard { requested, .. } => {
                format!("shard {requested} is not a placeable shard for this deployment")
            }
            Self::ShardNotWritable { requested, .. } => {
                format!(
                    "shard {requested} is not currently accepting new workflows; \
                     it is being drained"
                )
            }
            Self::UnknownResidencyKey { key, .. } => {
                format!("residency key '{key}' is not declared for this deployment")
            }
            Self::EmptyResidencyKey => "residency key must not be blank".to_string(),
        }
    }
}

/// Largest shard number an [`ExecutionId`] can carry.
///
/// `ExecutionId::new_for_shard` encodes the shard into the UUID's first two
/// bytes as `shard & 0xFFFF`, and `0xFFFF` is reserved for
/// [`ShardId::UNENCODED`], so `0xFFFE` is the highest usable value.
pub const MAX_ENCODABLE_SHARD: i32 = 0xFFFE;

/// Can this shard number survive a round trip through an [`ExecutionId`]?
///
/// [`ShardId::new`] is an unbounded `const fn`, so an out-of-range value is
/// constructible even though its own docs call `0..=0xFFFE` the valid range.
/// Anything outside that range is silently truncated by the encoder, which would
/// route later id-based lookups to a different shard than the one the row was
/// written to — so placement rejects it up front.
#[must_use]
pub const fn is_encodable_shard(shard: ShardId) -> bool {
    let raw = shard.as_i32();
    raw >= 0 && raw <= MAX_ENCODABLE_SHARD
}

fn format_shards(shards: &[ShardId]) -> String {
    let rendered: Vec<String> = shards.iter().map(ToString::to_string).collect();
    format!("[{}]", rendered.join(", "))
}

/// Decides which shard a newly started workflow should live on.
///
/// The router carries two lists:
///
/// * `readable_shards`: the superset of shards the deployment can load from.
///   Rendezvous-hashing is performed over this set so the hash is stable
///   across deployments where the writable set is being widened or narrowed
///   (e.g. adding a new shard for new workflows only).
/// * `writable_shards`: the subset that accepts *new* workflows. When the
///   initial rendezvous pick lands outside this subset the router re-hashes
///   among the writable subset.
///
/// Hashes use `seahash` rather than `std::hash` because `std::hash::BuildHasher`
/// is seeded randomly per-process and would produce different placements on
/// every boot, breaking idempotent outbox retries.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    readable_shards: Vec<ShardId>,
    writable_shards: Vec<ShardId>,
    default_shard: ShardId,
    /// Operator-declared residency key → shard mapping (issue #697).
    ///
    /// Empty by default. Populated via [`ShardRouter::with_residency_map`].
    residency_map: BTreeMap<String, ShardId>,
}

impl ShardRouter {
    /// Build a router from an explicit list of readable and writable shards.
    ///
    /// `default_shard` is returned when a lookup is asked to resolve an
    /// `ExecutionId` that carries [`ShardId::UNENCODED`] in its shard bits.
    ///
    /// # Panics
    ///
    /// Panics if `readable_shards` is empty or if any entry in
    /// `writable_shards` is absent from `readable_shards`.
    #[must_use]
    pub fn new(
        readable_shards: Vec<ShardId>,
        writable_shards: Vec<ShardId>,
        default_shard: ShardId,
    ) -> Self {
        assert!(
            !readable_shards.is_empty(),
            "ShardRouter requires at least one readable shard"
        );
        for writable in &writable_shards {
            assert!(
                readable_shards.contains(writable),
                "writable shard {writable} is not in the readable set"
            );
        }
        Self {
            readable_shards,
            writable_shards,
            default_shard,
            residency_map: BTreeMap::new(),
        }
    }

    /// Attach an operator-declared residency key → shard mapping (issue #697).
    ///
    /// This is the *placement policy* for data-residency and tenant-isolation
    /// deployments: the operator — who alone knows which physical database sits
    /// in which jurisdiction — declares `"eu" -> ShardId(1)`, and application
    /// code then starts workflows with the semantic key rather than a shard
    /// number. Renumbering shards is a config change, not a redeploy.
    ///
    /// The mapping is deliberately **declared, not derived**. A hash (rendezvous
    /// or otherwise) cannot know which database is physically in the EU, and
    /// moves roughly `1/N` of its keys whenever a shard is added — so a hashed
    /// residency key could silently migrate a tenant's placement mid-contract.
    /// A declared map is stable by construction across process restarts and
    /// across any widening of the readable/writable sets.
    ///
    /// Keys are trimmed on insert, matching how a request's `residency_key` is
    /// trimmed before lookup — so a declared `" eu "` is reachable as `"eu"`
    /// rather than becoming a permanently dead entry.
    ///
    /// Calling this twice replaces the previous map.
    ///
    /// A declared target that is readable but currently drained out of
    /// `writable_shards` is **not** a panic — draining a residency shard is a
    /// legitimate transient state and crash-looping the fleet over it would be
    /// worse than the outage it signals. It logs a warning instead, because
    /// every start under that key will be refused until the shard is restored
    /// or the key remapped.
    ///
    /// # Panics
    ///
    /// Panics if a key is blank, maps to a shard outside `readable_shards`, or
    /// two keys normalize to the same trimmed key with *conflicting* shards
    /// (e.g. `("eu", 1)` and `(" eu ", 2)`) — which target survived would depend
    /// on the source's iteration order, and from an unordered source could
    /// differ across restarts. A duplicate that maps to the *same* shard is
    /// harmless and accepted. This mirrors [`ShardRouter::new`]'s existing panic
    /// contract so a misconfigured residency map fails at boot rather than at
    /// the first residency-pinned start.
    #[must_use]
    pub fn with_residency_map(
        mut self,
        entries: impl IntoIterator<Item = (String, ShardId)>,
    ) -> Self {
        // Insert one at a time rather than `.collect()`ing: two RAW keys that
        // normalize to the same trimmed key (`"eu"` and `" eu "`) would silently
        // overwrite one another, and from an UNORDERED source (a `HashMap`)
        // *which* target survives can differ across process restarts — breaking
        // the stable-mapping guarantee that is the entire point of a declared
        // map, and potentially moving work between jurisdictions on a restart.
        // Fail construction instead, consistent with the blank-key and
        // shard-outside-the-readable-set panics below.
        let mut map: BTreeMap<String, ShardId> = BTreeMap::new();
        for (raw, shard) in entries {
            let key = raw.trim().to_string();
            if let Some(existing) = map.insert(key.clone(), shard) {
                assert_eq!(
                    existing, shard,
                    "residency key '{key}' is declared more than once (after trimming) with \
                     conflicting shards {existing} and {shard}; which one survives would depend \
                     on iteration order"
                );
            }
        }
        for (key, shard) in &map {
            assert!(
                !key.is_empty(),
                "residency key must not be blank (mapped to shard {shard})"
            );
            assert!(
                self.readable_shards.contains(shard),
                "residency key '{key}' maps to shard {shard}, which is not in the readable set"
            );
            // Readable but drained: the router boots, yet EVERY start under this
            // key will be refused with `ShardNotWritable` until the shard is
            // restored to `writable_shards` or the key is remapped. That is the
            // likelier operational mistake (draining a shard is routine;
            // removing one from `readable_shards` is rare), so surface it at
            // boot rather than leaving it to be discovered from production 400s.
            if !self.writable_shards.contains(shard) {
                tracing::warn!(
                    residency_key = %key,
                    shard = %shard,
                    "residency key targets a shard that is readable but not writable (drained); \
                     new workflows pinned to this key will be rejected until the shard is \
                     restored to the writable set or the key is remapped"
                );
            }
        }
        self.residency_map = map;
        self
    }

    /// The declared residency key → shard mapping.
    ///
    /// Empty unless [`ShardRouter::with_residency_map`] was used.
    #[must_use]
    pub const fn residency_map(&self) -> &BTreeMap<String, ShardId> {
        &self.residency_map
    }

    /// Resolve a [`ShardPlacement`] to a concrete shard for a *new* workflow.
    ///
    /// [`ShardPlacement::Auto`] delegates to [`Self::pick_for_new_workflow`] and
    /// is byte-for-byte identical to the pre-#697 code path. The explicit
    /// variants validate first and never fall back to the default shard.
    ///
    /// Because the resolved shard is encoded into the `ExecutionId` the caller
    /// then mints, residency is transitive: children, continue-as-new
    /// successors, workflow-level retries, and reset forks all inherit
    /// `exec_id.shard()` and therefore stay on the pinned database.
    ///
    /// # Errors
    ///
    /// Returns [`ShardPlacementError`] when an explicit shard is unknown or
    /// drained, or a residency key is blank or undeclared.
    pub fn resolve_placement(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Result<ShardId, ShardPlacementError> {
        self.resolve_placement_inner(placement, workflow_name, workflow_id, true)
    }

    /// Resolve a [`ShardPlacement`] to the shard an execution for it *would*
    /// live on, **without** enforcing writability.
    ///
    /// Use this when the resolved shard is only being used to *look something
    /// up* — an idempotency-key dedup probe, an operator read — rather than to
    /// place new work. Writability is a fresh-start-only concern: refusing to
    /// even *read* from a shard the operator happens to be draining would, for
    /// example, turn an at-least-once retry of an already-committed keyed start
    /// into a spurious `400` instead of the `200` no-op it must return.
    ///
    /// Every other validation still applies — an unknown shard or an undeclared
    /// residency key is still an error, never a silent fall back to the default
    /// shard.
    ///
    /// # Errors
    ///
    /// Returns [`ShardPlacementError`] when an explicit shard is unknown, or a
    /// residency key is blank or undeclared. Never returns
    /// [`ShardPlacementError::ShardNotWritable`].
    pub fn resolve_placement_for_lookup(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Result<ShardId, ShardPlacementError> {
        self.resolve_placement_inner(placement, workflow_name, workflow_id, false)
    }

    fn resolve_placement_inner(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
        require_writable: bool,
    ) -> Result<ShardId, ShardPlacementError> {
        match placement {
            ShardPlacement::Auto => Ok(self.pick_for_new_workflow(workflow_name, workflow_id)),
            ShardPlacement::Shard(shard) => self.validate_pinned_shard(*shard, require_writable),
            ShardPlacement::ResidencyKey(key) => {
                let trimmed = key.trim();
                if trimmed.is_empty() {
                    return Err(ShardPlacementError::EmptyResidencyKey);
                }
                let shard = self.residency_map.get(trimmed).copied().ok_or_else(|| {
                    ShardPlacementError::UnknownResidencyKey {
                        key: trimmed.to_string(),
                        known: self.residency_map.keys().cloned().collect(),
                    }
                })?;
                // Defence in depth: `with_residency_map` already rejects targets
                // outside the readable set, but a router mutated by other means
                // (or a shard drained after the map was declared) must still
                // fail loud rather than place residency-bound work incorrectly.
                self.validate_pinned_shard(shard, require_writable)
            }
        }
    }

    /// Shared validation for an explicitly pinned shard.
    ///
    /// `require_writable` distinguishes placing new work (`true`) from resolving
    /// where existing work lives (`false`) — see
    /// [`Self::resolve_placement_for_lookup`].
    fn validate_pinned_shard(
        &self,
        shard: ShardId,
        require_writable: bool,
    ) -> Result<ShardId, ShardPlacementError> {
        // A shard number outside `0..=0xFFFE` is not representable in an
        // `ExecutionId`: `ExecutionId::new_for_shard` writes `shard & 0xFFFF`
        // into the UUID's first two bytes, so `65536` would encode as `0` and
        // `-1` as the `0xFFFF` sentinel. `ShardId::new` is an unbounded
        // `const fn`, so a router CAN be configured with such a value; accepting
        // the pin would write the row to the requested pool while every later
        // id-based lookup routed to the truncated shard, leaving a pinned
        // workflow inaccessible or visible through the wrong database. Reject it
        // before the set membership checks — the value is unusable regardless of
        // how the deployment is configured.
        //
        // `is_unencoded` (`0xFFFF`) is inside this rejected range: the sentinel
        // is a routing marker ("resolve to the deployment default"), never a
        // shard number, and accepting it would silently place pinned work on the
        // default shard — the exact failure this feature removes.
        if !is_encodable_shard(shard) {
            return Err(ShardPlacementError::UnknownShard {
                requested: shard,
                readable: self.readable_shards.clone(),
            });
        }
        if !self.readable_shards.contains(&shard) {
            return Err(ShardPlacementError::UnknownShard {
                requested: shard,
                readable: self.readable_shards.clone(),
            });
        }
        if require_writable && !self.writable_shards.contains(&shard) {
            return Err(ShardPlacementError::ShardNotWritable {
                requested: shard,
                writable: self.writable_shards.clone(),
            });
        }
        Ok(shard)
    }

    /// Build a router for a single-shard deployment.
    ///
    /// Equivalent to the pre-sharding runtime: all workflows land on
    /// `ShardId(0)` and all reads resolve to the same database.
    #[must_use]
    pub fn single() -> Self {
        let shard = ShardId::new(0);
        Self::new(vec![shard], vec![shard], shard)
    }

    /// Shards this router accepts reads from.
    #[must_use]
    pub fn readable_shards(&self) -> &[ShardId] {
        &self.readable_shards
    }

    /// Shards this router accepts *new* workflows on.
    #[must_use]
    pub fn writable_shards(&self) -> &[ShardId] {
        &self.writable_shards
    }

    /// Does this router currently accept *new* workflows on `shard`?
    ///
    /// A shard that is readable but not writable is being drained: existing
    /// work there is still served, but new work must not be placed on it.
    #[must_use]
    pub fn is_writable(&self, shard: ShardId) -> bool {
        self.writable_shards.contains(&shard)
    }

    /// The shard returned when an `ExecutionId` carries the unencoded sentinel.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Rendezvous-pick a *writable* shard for a brand new workflow, keyed on
    /// two arbitrary string tokens.
    ///
    /// The initial pick is taken over the full readable set (so the hash is
    /// stable while the writable set is widened/narrowed) and, when it lands
    /// outside the writable subset, re-hashed among the writable shards.
    fn pick_writable(&self, primary: &str, secondary: &str) -> ShardId {
        let initial = rendezvous_pick(&self.readable_shards, primary, secondary);
        if self.writable_shards.contains(&initial) {
            return initial;
        }
        if self.writable_shards.is_empty() {
            return self.default_shard;
        }
        rendezvous_pick(&self.writable_shards, primary, secondary)
    }

    /// Pick a shard for a brand new workflow using rendezvous hashing.
    ///
    /// The input is `(workflow_name, workflow_id)` which uniquely identifies
    /// the logical workflow independent of its execution UUID — the same
    /// `(name, id)` therefore always hashes to the same shard, making outbox
    /// retries idempotent.
    #[must_use]
    pub fn pick_for_new_workflow(&self, workflow_name: &str, workflow_id: &str) -> ShardId {
        self.pick_writable(workflow_name, workflow_id)
    }

    /// Pick a shard for a new workflow started via a request-scoped
    /// `idempotency_key` (issue #808).
    ///
    /// Unlike [`Self::pick_for_new_workflow`], which routes by `(workflow_name,
    /// workflow_id)`, this routes by `(workflow_name, idempotency_key)` so two
    /// same-key retries — whose `workflow_id` may be independently
    /// auto-generated per request — deterministically co-locate on the *same*
    /// shard, hit the same `harvest_start_idempotency` claim row, and
    /// deduplicate. Routing by `workflow_id` would scatter same-key retries
    /// across shards in a multi-shard deployment (one claim row per shard →
    /// one execution per shard), defeating dedup.
    ///
    /// The same writable-subset redirect as [`Self::pick_for_new_workflow`]
    /// applies (a keyed start still creates a brand-new workflow, so it must
    /// land on a writable shard).
    ///
    /// The caller (`api.rs`) only routes here when the `workflow_id` was
    /// auto-generated; an explicit `workflow_id` routes by `workflow_id`
    /// instead. That split is the resolution of the P1↔P2 routing tension —
    /// routing *all* keyed starts by the key would break the reuse-policy
    /// matrix for explicit-`workflow_id` starts (see the routing comment in
    /// `api.rs` and `docs/getting-started/06-idempotency.md`).
    ///
    /// KNOWN LIMITATION (Codex #808 P2): because this reuses `pick_writable`
    /// verbatim, keyed dedup inherits the *exact* shard-drain behavior of
    /// `(workflow_name, workflow_id)` uniqueness. If a key first claims a run on
    /// shard 0 and shard 0 is later removed from the writable set (drained to
    /// read-only) while still readable, a same-key retry rehashes via the
    /// writable-subset redirect to a *different* writable shard, probes/reserves
    /// a different shard-local `harvest_start_idempotency` row, and can create a
    /// second execution within the retention window. Issue #808 scopes dedup to
    /// be shard-local, exactly as `(name, workflow_id)` uniqueness already is —
    /// so this matches (and is bounded by) that existing guarantee. See the
    /// "Known limitation" note in `docs/getting-started/06-idempotency.md`.
    #[must_use]
    pub fn pick_for_idempotency_key(&self, workflow_name: &str, idempotency_key: &str) -> ShardId {
        self.pick_writable(workflow_name, idempotency_key)
    }

    /// Resolve the shard for an arbitrary `ExecutionId`.
    ///
    /// Returns the encoded shard when present, or the configured default
    /// shard for ids carrying [`ShardId::UNENCODED`].
    #[must_use]
    pub fn shard_for_execution(&self, exec_id: ExecutionId) -> ShardId {
        let encoded = exec_id.shard();
        if encoded.is_unencoded() {
            return self.default_shard;
        }
        if self.readable_shards.contains(&encoded) {
            encoded
        } else {
            self.default_shard
        }
    }

    /// Pick a shard for a DAG at catalog-compile time.
    ///
    /// DAG schedules (`harvest_schedules`) are scoped per database, so each
    /// DAG must be pinned to a single shard that owns it.
    /// The same name always maps to the same shard because rendezvous hashing
    /// is stable.
    #[must_use]
    pub fn pick_for_dag(&self, dag_name: &str) -> ShardId {
        let primary = if self.writable_shards.is_empty() {
            &self.readable_shards
        } else {
            &self.writable_shards
        };
        rendezvous_pick(primary, dag_name, "")
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::single()
    }
}

fn rendezvous_pick(shards: &[ShardId], primary: &str, secondary: &str) -> ShardId {
    debug_assert!(!shards.is_empty(), "rendezvous pick requires candidates");
    let mut best = shards[0];
    let mut best_hash = rendezvous_hash(best, primary, secondary);
    for shard in &shards[1..] {
        let candidate = rendezvous_hash(*shard, primary, secondary);
        if candidate > best_hash {
            best = *shard;
            best_hash = candidate;
        }
    }
    best
}

fn rendezvous_hash(shard: ShardId, primary: &str, secondary: &str) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    hasher.write_i32(shard.as_i32());
    hasher.write(primary.as_bytes());
    hasher.write_u8(0);
    hasher.write(secondary.as_bytes());
    hasher.finish()
}

/// Collection of [`DbPool`] handles keyed by [`ShardId`].
///
/// In single-shard deployments the map has one entry and
/// [`ShardedDbPool::pool_for`] / [`ShardedDbPool::pool_for_execution`] always
/// return it. Multi-shard deployments populate one pool per shard and rely on
/// the encoded shard bits in each [`ExecutionId`] for routing.
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct ShardedDbPool {
    pools: BTreeMap<ShardId, DbPool>,
    default_shard: ShardId,
}

#[cfg(feature = "db")]
impl std::fmt::Debug for ShardedDbPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedDbPool")
            .field("shards", &self.pools.keys())
            .field("default_shard", &self.default_shard)
            .finish()
    }
}

#[cfg(feature = "db")]
pub static GLOBAL_SHARDED_POOL: std::sync::RwLock<Option<ShardedDbPool>> =
    std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub static GLOBAL_SHARD_ROUTER: std::sync::RwLock<Option<ShardRouter>> =
    std::sync::RwLock::new(None);

/// Install a `ShardRouter` into the global registry.
///
/// This should only be called once during runtime initialization (e.g. by
/// the `HarvestRunner` or `HarvestApiRuntime`) to avoid race conditions and
/// overwrites from temporary constructors in tests.
#[cfg(feature = "db")]
pub fn install_global_router(router: ShardRouter) {
    if let Ok(mut lock) = GLOBAL_SHARD_ROUTER.write() {
        *lock = Some(router);
    }
}

#[cfg(feature = "db")]
impl ShardedDbPool {
    /// Wrap an existing single pool as a one-shard sharded pool at `ShardId(0)`.
    ///
    /// This is the shape used by every pre-sharding deployment. All lookups
    /// resolve to the same pool and no behavior changes.
    #[must_use]
    pub fn single(pool: DbPool) -> Self {
        let shard = ShardId::new(0);
        let mut pools = BTreeMap::new();
        pools.insert(shard, pool);
        let this = Self {
            pools,
            default_shard: shard,
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// Build a sharded pool from a pre-computed map of shard → pool.
    ///
    /// # Panics
    ///
    /// Panics if `pools` is empty or does not contain `default_shard`.
    #[must_use]
    pub fn from_map(pools: BTreeMap<ShardId, DbPool>, default_shard: ShardId) -> Self {
        assert!(
            !pools.is_empty(),
            "ShardedDbPool requires at least one pool"
        );
        assert!(
            pools.contains_key(&default_shard),
            "default_shard {default_shard} has no configured pool"
        );
        let this = Self {
            pools,
            default_shard,
        };
        if let Ok(mut lock) = GLOBAL_SHARDED_POOL.write() {
            *lock = Some(this.clone());
        }
        this
    }

    /// The default shard used when an `ExecutionId` carries the unencoded
    /// sentinel or references a shard that isn't configured locally.
    #[must_use]
    pub const fn default_shard(&self) -> ShardId {
        self.default_shard
    }

    /// Look up the pool for a shard. Falls back to the default shard when the
    /// requested shard is not present in this map.
    ///
    /// # Panics
    ///
    /// Panics only if the pool was constructed by bypassing the public API
    /// and the default shard entry has been removed; [`ShardedDbPool::single`]
    /// and [`ShardedDbPool::from_map`] guarantee a default entry exists.
    #[must_use]
    pub fn pool_for(&self, shard: ShardId) -> &DbPool {
        self.pools
            .get(&shard)
            .or_else(|| self.pools.get(&self.default_shard))
            .expect("default shard pool is always present")
    }

    /// Look up the pool for a shard exactly, with no default fallback.
    #[must_use]
    pub fn exact_pool_for(&self, shard: ShardId) -> Option<&DbPool> {
        self.pools.get(&shard)
    }

    /// Resolve the pool that owns a given `ExecutionId`.
    #[must_use]
    pub fn pool_for_execution(&self, exec_id: ExecutionId) -> &DbPool {
        let shard = exec_id.shard();
        if shard.is_unencoded() {
            return self.pool_for(self.default_shard);
        }
        self.pool_for(shard)
    }

    /// Resolve the pool that owns a given `ExecutionId` exactly, with no default fallback.
    #[must_use]
    pub fn exact_pool_for_execution(&self, exec_id: ExecutionId) -> Option<&DbPool> {
        let shard = exec_id.shard();
        if shard.is_unencoded() {
            return self.pools.get(&self.default_shard);
        }
        self.pools.get(&shard)
    }

    /// Iterate over `(shard, pool)` pairs in ascending shard order.
    pub fn iter_shards(&self) -> impl Iterator<Item = (ShardId, &DbPool)> {
        self.pools.iter().map(|(shard, pool)| (*shard, pool))
    }

    /// Shards this pool serves, in ascending order.
    #[must_use]
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.pools.keys().copied().collect()
    }

    /// How many shards are represented.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Is this an empty pool map?
    ///
    /// Always `false` for values constructed through the public API but
    /// exposed for completeness.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router_with(shards: &[i32]) -> ShardRouter {
        let ids: Vec<ShardId> = shards.iter().copied().map(ShardId::new).collect();
        ShardRouter::new(ids.clone(), ids.clone(), ids[0])
    }

    #[test]
    fn single_router_always_returns_shard_zero() {
        let router = ShardRouter::single();
        assert_eq!(
            router.pick_for_new_workflow("onboarding", "user-1"),
            ShardId::new(0)
        );
        assert_eq!(
            router.pick_for_new_workflow("etl", "nightly"),
            ShardId::new(0)
        );
        assert_eq!(router.default_shard(), ShardId::new(0));
    }

    #[test]
    fn rendezvous_hash_is_stable_across_runs() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_new_workflow("onboarding", "user-42");
        let b = router.pick_for_new_workflow("onboarding", "user-42");
        assert_eq!(a, b);
    }

    #[test]
    fn rendezvous_hash_distributes_across_shards() {
        let router = router_with(&[0, 1, 2]);
        let mut counts = [0usize; 3];
        for i in 0..300 {
            let shard = router.pick_for_new_workflow("onboarding", &format!("user-{i}"));
            counts[usize::try_from(shard.as_i32()).unwrap()] += 1;
        }
        // With 300 samples across 3 shards we expect ~100 each; allow a wide
        // band to stay robust against hash skew.
        for count in counts {
            assert!(count > 50, "shard counts too imbalanced: {counts:?}");
            assert!(count < 200, "shard counts too imbalanced: {counts:?}");
        }
    }

    #[test]
    fn writable_subset_redirects_when_initial_pick_is_read_only() {
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));

        for i in 0..20 {
            let picked = router.pick_for_new_workflow("wf", &format!("id-{i}"));
            assert_eq!(picked, ShardId::new(1));
        }
    }

    #[test]
    fn shard_for_execution_falls_back_on_unencoded_sentinel() {
        let router = router_with(&[0, 1, 2]);
        let unencoded = ExecutionId::new();
        assert_eq!(router.shard_for_execution(unencoded), ShardId::new(0));
    }

    #[test]
    fn shard_for_execution_honours_encoded_shard() {
        let router = router_with(&[0, 1, 2]);
        let id = ExecutionId::new_for_shard(ShardId::new(2));
        assert_eq!(router.shard_for_execution(id), ShardId::new(2));
    }

    #[test]
    fn shard_for_execution_falls_back_when_shard_is_unknown() {
        let router = router_with(&[0, 1]);
        let id = ExecutionId::new_for_shard(ShardId::new(7));
        assert_eq!(router.shard_for_execution(id), ShardId::new(0));
    }

    #[test]
    fn pick_for_dag_is_stable() {
        let router = router_with(&[0, 1, 2, 3]);
        let a = router.pick_for_dag("daily_etl");
        let b = router.pick_for_dag("daily_etl");
        assert_eq!(a, b);
    }

    // ── issue #808: key-based idempotency routing ────────────────────────────

    #[test]
    fn pick_for_idempotency_key_is_deterministic_for_same_name_and_key() {
        let router = router_with(&[0, 1, 2, 3]);
        // The same (name, key) always resolves to the same shard so two
        // same-key retries co-locate and dedup — independent of workflow_id.
        let a = router.pick_for_idempotency_key("order_flow", "delivery-42");
        let b = router.pick_for_idempotency_key("order_flow", "delivery-42");
        assert_eq!(a, b);
    }

    #[test]
    fn pick_for_idempotency_key_ignores_workflow_id_entirely() {
        // Routing must depend only on (name, key), never on workflow_id: a
        // same-key retry whose workflow_id was auto-generated per request must
        // still land on the same shard. The method takes no workflow_id, so
        // this is guaranteed by construction — assert two independent calls
        // (as two separate requests would make) agree.
        let router = router_with(&[0, 1, 2, 3, 4]);
        let first = router.pick_for_idempotency_key("wf", "same-key");
        let second = router.pick_for_idempotency_key("wf", "same-key");
        assert_eq!(
            first, second,
            "same key must co-locate regardless of per-request workflow_id"
        );
    }

    #[test]
    fn distinct_keys_can_map_to_distinct_shards() {
        let router = router_with(&[0, 1, 2, 3]);
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..200 {
            seen.insert(router.pick_for_idempotency_key("wf", &format!("key-{i}")));
        }
        assert!(
            seen.len() > 1,
            "distinct keys must spread across shards: {seen:?}"
        );
    }

    #[test]
    fn keyed_pick_honours_writable_subset() {
        // A keyed start still creates a brand-new workflow, so it must land on
        // a writable shard even when the key hashes to a read-only shard.
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));
        for i in 0..20 {
            assert_eq!(
                router.pick_for_idempotency_key("wf", &format!("k-{i}")),
                ShardId::new(1)
            );
        }
    }

    // ── issue #697: explicit shard pinning for data residency ────────────────

    /// Build a three-shard router with an `eu`/`us` residency map.
    fn residency_router() -> ShardRouter {
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        )
        .with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            ("us".to_string(), ShardId::new(2)),
        ])
    }

    #[test]
    fn auto_placement_is_byte_for_byte_todays_rendezvous_routing() {
        // AC1: when no placement is supplied, behaviour must be identical to
        // `pick_for_new_workflow` for every input — zero regression.
        let router = residency_router();
        for i in 0..200 {
            let wid = format!("user-{i}");
            assert_eq!(
                router
                    .resolve_placement(&ShardPlacement::Auto, "onboarding", &wid)
                    .expect("Auto never fails"),
                router.pick_for_new_workflow("onboarding", &wid),
            );
        }
    }

    #[test]
    fn default_placement_is_auto() {
        assert_eq!(ShardPlacement::default(), ShardPlacement::Auto);
    }

    #[test]
    fn explicit_shard_placement_wins_over_the_hash() {
        let router = residency_router();
        // Find a workflow id whose hash does NOT land on shard 2 so the pin is
        // observably doing the work.
        let wid = (0..500)
            .map(|i| format!("id-{i}"))
            .find(|wid| router.pick_for_new_workflow("wf", wid) != ShardId::new(2))
            .expect("some id must hash off shard 2");
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::Shard(ShardId::new(2)), "wf", &wid)
                .expect("shard 2 is readable and writable"),
            ShardId::new(2)
        );
    }

    #[test]
    fn explicit_shard_outside_readable_set_is_rejected_not_defaulted() {
        // AC2: typed, actionable error — never a panic and never a silent
        // fallback to the default shard.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(9)), "wf", "id-1")
            .expect_err("shard 9 is not configured");
        match err {
            ShardPlacementError::UnknownShard {
                requested,
                readable,
            } => {
                assert_eq!(requested, ShardId::new(9));
                assert_eq!(
                    readable,
                    vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)]
                );
            }
            other => panic!("expected UnknownShard, got {other:?}"),
        }
        // The message must name the offending shard and the valid set.
        let msg = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(9)), "wf", "id-1")
            .unwrap_err()
            .to_string();
        assert!(msg.contains('9'), "message must name the shard: {msg}");
        assert!(
            msg.contains("readable"),
            "message must name the valid set: {msg}"
        );
    }

    #[test]
    fn explicit_shard_that_is_readable_but_drained_is_rejected() {
        // A shard drained out of `writable_shards` is deliberately not
        // accepting new work; pinning new work onto it must fail loud rather
        // than contradict the operator's drain.
        let router = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0)],
            ShardId::new(0),
        );
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::new(1)), "wf", "id-1")
            .expect_err("shard 1 is readable but drained");
        assert!(
            matches!(err, ShardPlacementError::ShardNotWritable { requested, .. } if requested == ShardId::new(1)),
            "expected ShardNotWritable, got {err:?}"
        );
    }

    #[test]
    fn unencoded_sentinel_is_never_a_valid_pin() {
        // `ShardId::UNENCODED` is a routing sentinel, not a shard number.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::Shard(ShardId::UNENCODED), "wf", "id-1")
            .expect_err("the sentinel is not a placeable shard");
        assert!(
            matches!(err, ShardPlacementError::UnknownShard { .. }),
            "expected UnknownShard, got {err:?}"
        );
    }

    /// Codex P1 (issue #697 review): `ShardId::new` is an unbounded `const fn`,
    /// but `ExecutionId::new_for_shard` masks the value to 16 bits. A pin to a
    /// shard outside `0..=0xFFFE` would write the row to the requested pool
    /// while every id-based lookup routed to the TRUNCATED shard, leaving the
    /// workflow inaccessible or visible through the wrong database. Rejected
    /// even when the router is (mis)configured to consider it readable+writable.
    #[test]
    fn shard_outside_the_encodable_range_is_rejected_even_when_configured() {
        for raw in [0x1_0000, 0x1_0001, i32::MAX, -1, i32::MIN] {
            let out_of_range = ShardId::new(raw);
            // Deliberately configure the router to accept it, so the rejection
            // can only come from the encodability check.
            let router = ShardRouter::new(
                vec![ShardId::new(0), out_of_range],
                vec![ShardId::new(0), out_of_range],
                ShardId::new(0),
            );
            let err = router
                .resolve_placement(&ShardPlacement::Shard(out_of_range), "wf", "id-1")
                .expect_err("a non-encodable shard must never be a valid pin");
            assert!(
                matches!(err, ShardPlacementError::UnknownShard { .. }),
                "expected UnknownShard for {raw}, got {err:?}"
            );
        }
    }

    #[test]
    fn encodable_shard_predicate_matches_the_uuid_round_trip() {
        // Everything the predicate ACCEPTS must survive the `ExecutionId`
        // encoding unchanged, and must not be the reserved sentinel.
        for raw in [0, 1, 0xFFFE] {
            let shard = ShardId::new(raw);
            assert!(is_encodable_shard(shard), "{raw} should be encodable");
            assert_eq!(ExecutionId::new_for_shard(shard).shard(), shard);
            assert!(!shard.is_unencoded(), "{raw} must not be the sentinel");
        }

        // Everything it REJECTS is unsafe to pin, for one of two reasons.
        //
        // (a) The value does not fit in the two shard bytes, so
        //     `ExecutionId::new_for_shard` silently truncates it (`& 0xFFFF`)
        //     and the id reads back as a DIFFERENT shard -- the exact
        //     cross-jurisdiction hazard the predicate exists to prevent.
        for raw in [-1, 0x1_0000, 0x1_0001] {
            let shard = ShardId::new(raw);
            assert!(!is_encodable_shard(shard), "{raw} should not be encodable");
            assert_ne!(
                ExecutionId::new_for_shard(shard).shard(),
                shard,
                "{raw} silently truncates through the ExecutionId encoding"
            );
        }

        // (b) `0xFFFE + 1 == 0xFFFF` DOES round-trip byte-for-byte -- it is not
        //     truncated -- but it is `ShardId::UNENCODED`, the reserved "no
        //     shard encoded, fall back to the default shard" sentinel. Pinning
        //     to it would mint ids the router resolves to the DEFAULT shard,
        //     so it is rejected for a different reason than (a).
        let sentinel = ShardId::new(0xFFFF);
        assert!(sentinel.is_unencoded());
        assert!(
            !is_encodable_shard(sentinel),
            "the reserved sentinel is never a pinnable shard"
        );
        assert_eq!(
            ExecutionId::new_for_shard(sentinel).shard(),
            sentinel,
            "the sentinel round-trips (it is reserved, not truncated)"
        );
    }

    /// Codex P1 (issue #697 review): two RAW keys that normalize to the same
    /// trimmed key with CONFLICTING shards would silently discard one mapping,
    /// and from an unordered source which one survives could vary across
    /// restarts — breaking the stable-mapping guarantee that is the whole point
    /// of a declared map, and potentially moving work between jurisdictions.
    #[test]
    #[should_panic(expected = "declared more than once")]
    fn conflicting_residency_keys_that_collide_after_trimming_panic() {
        let _ = router_with(&[0, 1, 2]).with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            (" eu ".to_string(), ShardId::new(2)),
        ]);
    }

    #[test]
    fn duplicate_residency_keys_agreeing_on_the_shard_are_accepted() {
        // Harmless: there is no ambiguity about which target survives.
        let router = router_with(&[0, 1]).with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            (" eu ".to_string(), ShardId::new(1)),
        ]);
        assert_eq!(router.residency_map().get("eu"), Some(&ShardId::new(1)));
        assert_eq!(router.residency_map().len(), 1);
    }

    #[test]
    fn residency_key_resolves_through_the_declared_map() {
        let router = residency_router();
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "id-1")
                .expect("eu is mapped"),
            ShardId::new(1)
        );
        assert_eq!(
            router
                .resolve_placement(&ShardPlacement::residency_key("us"), "wf", "id-1")
                .expect("us is mapped"),
            ShardId::new(2)
        );
    }

    #[test]
    fn unmapped_residency_key_is_rejected_never_hashed() {
        // A silent hash fallback is exactly the "hope the hash cooperates"
        // failure mode this feature exists to remove.
        let router = residency_router();
        let err = router
            .resolve_placement(&ShardPlacement::residency_key("apac"), "wf", "id-1")
            .expect_err("apac is not mapped");
        match err {
            ShardPlacementError::UnknownResidencyKey { ref key, ref known } => {
                assert_eq!(key, "apac");
                assert_eq!(known, &vec!["eu".to_string(), "us".to_string()]);
            }
            other => panic!("expected UnknownResidencyKey, got {other:?}"),
        }
        assert!(
            err.to_string().contains("apac"),
            "message must name the key: {err}"
        );
    }

    #[test]
    fn residency_key_on_a_router_with_no_map_is_rejected() {
        let router = router_with(&[0, 1, 2]);
        let err = router
            .resolve_placement(&ShardPlacement::residency_key("eu"), "wf", "id-1")
            .expect_err("no residency map is configured");
        assert!(
            matches!(err, ShardPlacementError::UnknownResidencyKey { .. }),
            "expected UnknownResidencyKey, got {err:?}"
        );
    }

    #[test]
    fn blank_residency_key_is_rejected() {
        let router = residency_router();
        for blank in ["", "   ", "\t"] {
            let err = router
                .resolve_placement(&ShardPlacement::residency_key(blank), "wf", "id-1")
                .expect_err("blank keys are not resolvable");
            assert!(
                matches!(err, ShardPlacementError::EmptyResidencyKey),
                "expected EmptyResidencyKey for {blank:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn residency_key_resolution_is_stable_across_a_widening_of_the_shard_set() {
        // AC3 (the money test): a key that resolves to shard N must KEEP
        // resolving to N when shards are added. Rendezvous hashing cannot
        // promise this — a declared map can, by construction.
        let map = [
            ("eu".to_string(), ShardId::new(1)),
            ("us".to_string(), ShardId::new(2)),
        ];
        let before = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        )
        .with_residency_map(map.clone());

        // Widen both the readable and writable sets by three new shards.
        let after = ShardRouter::new(
            (0..6).map(ShardId::new).collect(),
            (0..6).map(ShardId::new).collect(),
            ShardId::new(0),
        )
        .with_residency_map(map);

        for key in ["eu", "us"] {
            let placement = ShardPlacement::residency_key(key);
            assert_eq!(
                before.resolve_placement(&placement, "wf", "id-1").unwrap(),
                after.resolve_placement(&placement, "wf", "id-1").unwrap(),
                "residency key {key} moved when shards were added"
            );
        }

        // Contrast: the rendezvous hash genuinely does move keys on widening,
        // which is precisely why residency cannot be built on it.
        let moved = (0..200)
            .map(|i| format!("id-{i}"))
            .filter(|wid| {
                before.pick_for_new_workflow("wf", wid) != after.pick_for_new_workflow("wf", wid)
            })
            .count();
        assert!(
            moved > 0,
            "the hash is expected to move some keys on widening; if it does not, \
             this test no longer proves the map buys anything"
        );
    }

    #[test]
    fn residency_conformance_n_workflows_across_k_keys() {
        // Success metric: 100% of starts under a residency key land on that
        // key's shard, independent of workflow name/id.
        let router = residency_router();
        let keys = [("eu", ShardId::new(1)), ("us", ShardId::new(2))];
        for (key, expected) in keys {
            for i in 0..100 {
                let resolved = router
                    .resolve_placement(
                        &ShardPlacement::residency_key(key),
                        &format!("wf-{}", i % 7),
                        &format!("id-{i}"),
                    )
                    .expect("mapped key resolves");
                assert_eq!(resolved, expected, "key {key} escaped its shard on run {i}");
            }
        }
    }

    #[test]
    fn residency_map_accessor_exposes_the_declared_mapping() {
        let router = residency_router();
        let map = router.residency_map();
        assert_eq!(map.get("eu"), Some(&ShardId::new(1)));
        assert_eq!(map.get("us"), Some(&ShardId::new(2)));
        assert_eq!(map.len(), 2);
        assert!(router_with(&[0]).residency_map().is_empty());
    }

    #[test]
    #[should_panic(expected = "residency key")]
    fn residency_map_targeting_an_unconfigured_shard_panics_at_construction() {
        // Misconfiguration must fail at boot, not at the first EU start.
        let _ = router_with(&[0, 1]).with_residency_map([("eu".to_string(), ShardId::new(7))]);
    }

    #[test]
    #[should_panic(expected = "residency key")]
    fn residency_map_with_a_blank_key_panics_at_construction() {
        let _ = router_with(&[0, 1]).with_residency_map([(String::new(), ShardId::new(1))]);
    }
}
