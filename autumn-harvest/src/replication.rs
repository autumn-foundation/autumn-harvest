//! Cross-region disaster recovery: write-authority fencing and replication-lag
//! measurement (issue #954).
//!
//! Harvest does **not** ship replication. Stock Postgres logical (or physical)
//! replication moves the bytes to a standby region; this module supplies the
//! two things stock Postgres cannot: a way to **revoke a region's write
//! authority**, and a **measured RPO**.
//!
//! # The fence
//!
//! Each shard's database carries one `harvest_shard_generation` row: a
//! monotonic epoch for "who is allowed to write here". A worker reads the
//! generation once, at startup, and pins it for its lifetime
//! ([`FenceRegistry`]). Two structural checks then use that pinned value:
//!
//! * **Claim gate** — [`crate::queue::claim_task`] cross-joins the generation
//!   row into its candidate CTE. A worker whose pinned generation no longer
//!   matches the database selects zero candidates: it cannot claim work at
//!   all. No extra round trip; the check rides the statement that was already
//!   being issued.
//! * **Persist assert** — [`assert_fence`] takes the generation row `FOR
//!   SHARE` at the top of a persist. `FOR SHARE` is the load-bearing detail:
//!   the fence bump takes the same row exclusively, so it cannot commit while
//!   any in-flight persist holds it, and any persist that begins after it
//!   commits observes the new generation and fails. That is a commit-order
//!   barrier, not a best-effort read — the same technique
//!   [`crate::queue::claim_task`] already uses for the queue-pause hold.
//!
//! Promoting a standby therefore looks like: bump the generation on the new
//! primary, and every worker still pinned to the old one is structurally
//! unable to claim or append — it self-fences loudly with
//! [`crate::error::HarvestError::ShardFenced`] rather than forking a history.
//!
//! # What this does NOT do — read this before relying on it
//!
//! Fencing is a property of **one database**. It cannot stop a worker in a
//! partitioned old region from writing to that region's *own*, still-running
//! Postgres: nothing on the promoted primary can reach it. The fence bites at
//! exactly two moments, which are the two that decide whether a history forks:
//!
//! 1. When a surviving old-region worker reconnects **to the promoted
//!    primary** (a DSN flip, a DNS failover, a restart) it is rejected.
//! 2. When the old region is re-seeded from the new primary for fail-back, the
//!    bumped generation arrives with the data, and every worker still pinned to
//!    the pre-failover epoch is rejected there too.
//!
//! Isolating the old primary's database — demote it, cut it off, or take its
//! role's connections to zero — remains a **mandatory** operator step, not an
//! optional one. See `docs/cross-region-dr.md` and
//! `docs/runbooks/cross-region-failover.md`.
//!
//! # On by default where DR is configured (issue #1823)
//!
//! [`pin_process_fence`] runs at process startup. In the default
//! [`DrFencing::Auto`] mode it probes each shard database for a DR marker
//! ([`DrMarkers`]) and fences only when it finds one. A process configured
//! [`DrFencing::Disabled`] refuses to start on a DR database.
//!
//! A process that pins nothing pays one probe per shard at startup, and
//! nothing after. [`FenceRegistry::is_enabled`] reports `false`. `claim_task`
//! issues the byte-for-byte unchanged claim SQL. [`assert_fence`] issues no
//! statement at all.
//!
//! # Measured RPO
//!
//! [`query_replication_status`] reads `pg_stat_replication` and
//! `pg_replication_slots` on the primary and reduces them to the worst-case
//! numbers an operator needs at failover time. The reduction is deliberately
//! pessimistic: an empty standby set, or a standby whose `replay_lag` has not
//! yet been reported, yields `None` ("unknown"), **never** `0.0`. Reporting a
//! perfect RPO for replication that is dead is the single most dangerous thing
//! this module could do.

/// How long `bump_generation` waits for the fencing table's exclusive lock
/// before giving up.
///
/// Long enough to ride out an ordinary in-flight persist, short enough that an
/// operator under RTO pressure gets an actionable `lock_timeout` error rather
/// than a hang — while it waits, it is itself blocking every claim and persist
/// queued behind it.
#[cfg(feature = "db")]
const BUMP_LOCK_TIMEOUT_MS: u64 = 5_000;

/// Per-statement ceiling for `advance_sequences_after_promotion`.
///
/// Generous, because a `MAX(col)` over a large un-indexed serial column on a
/// cold standby is legitimately slow — but bounded, because this runs inside a
/// 15-minute RTO budget and an unbounded hang is indistinguishable from a wedge.
#[cfg(feature = "db")]
const PROMOTE_STATEMENT_TIMEOUT_MS: u64 = 120_000;

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::types::ShardId;

/// A shard's write-authority epoch.
///
/// Monotonic and per-shard. `0` is the value a freshly-migrated database
/// starts at; every [`bump_generation`] increments it by one and the sequence
/// travels to the standby with the data, so a promoted standby inherits the
/// epoch its primary had and continues from there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardGeneration(i64);

impl ShardGeneration {
    /// The epoch a freshly-migrated database is seeded at.
    pub const INITIAL: Self = Self(0);

    /// Wrap a raw epoch value.
    ///
    /// The field is private to match [`ShardId`]'s newtype convention: a
    /// generation only ever originates from the database, so there is no reason
    /// for a caller to reach past the constructor.
    #[must_use]
    pub const fn new(generation: i64) -> Self {
        Self(generation)
    }

    /// The raw epoch value.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for ShardGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One connected standby, as reported by `pg_stat_replication` on the primary.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StandbyLag {
    /// `pg_stat_replication.state` — `streaming`, `catchup`, `startup`, ...
    pub state: String,
    /// `pg_stat_replication.replay_lag` in seconds.
    ///
    /// `None` when Postgres has not yet computed one (no feedback round-trip
    /// has completed). Never coerced to `0.0`: see the module docs.
    pub replay_lag_seconds: Option<f64>,
    /// WAL bytes between `pg_current_wal_lsn()` and this standby's `replay_lsn`.
    pub lag_bytes: Option<i64>,
}

/// One replication slot, as reported by `pg_replication_slots` on the primary.
///
/// Slots outlive their walsender: a disabled subscription or a dead standby
/// leaves an inactive slot pinning WAL. The time lag is then unknowable from
/// the primary, but the byte backlog is not — which is why this is tracked
/// separately from [`StandbyLag`] rather than folded into it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SlotLag {
    /// `pg_replication_slots.slot_name`.
    pub slot_name: String,
    /// Whether a walsender currently holds the slot.
    pub active: bool,
    /// WAL bytes between `pg_current_wal_lsn()` and the slot's
    /// `confirmed_flush_lsn` (logical) or `restart_lsn` (physical).
    pub lag_bytes: Option<i64>,
}

/// What the primary can currently say about replication for one shard.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ReplicationStatus {
    /// The replication views could not be read — most often because the
    /// connecting role lacks `pg_monitor`.
    ///
    /// Deliberately a *status*, not an error: a missing `GRANT` must degrade
    /// the RPO signal, never take the sampler down with it.
    Unavailable {
        /// Why the read failed, for logs and the admin surface.
        reason: String,
    },
    /// The views were read. Either collection may still be empty — an empty
    /// `standbys` means replication is **down**, not healthy.
    Observed {
        /// Rows from `pg_stat_replication`.
        standbys: Vec<StandbyLag>,
        /// Rows from `pg_replication_slots`.
        slots: Vec<SlotLag>,
        /// What the watermark trail can say about the RPO.
        ///
        /// Resolution is bounded below by the beat interval: a healthy
        /// deployment reports somewhere between zero and one interval, never a
        /// hard zero.
        heartbeat: WatermarkReading,
    },
}

/// What the watermark trail can say about a shard's RPO.
///
/// Five states, not `Option<f64>`, because several "no number" cases mean
/// opposite things. Collapsing them is dangerous. A trail the standby has
/// fallen off the end of means the RPO is **huge**. A trail with nothing
/// confirmed yet means it is merely **unmeasured**. A trail this process
/// could not even read is neither one — it is a state `replay_lag` must
/// never be allowed to paper over.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum WatermarkReading {
    /// The age of the newest watermark the slowest standby has confirmed.
    Measured(f64),
    /// The standby is further behind than the **whole retained trail**, so the
    /// true RPO is at least `floor_seconds` and unbounded above.
    ///
    /// This must never fall back to `pg_stat_replication.replay_lag`. That
    /// column freezes for a stuck logical apply worker, and a stuck apply
    /// worker is precisely how a trail gets exhausted — so the fallback would
    /// replace a known-enormous RPO with a small stale one, at the moment an
    /// operator is deciding whether to fail over.
    BeyondTrail {
        /// The age of the *oldest* retained watermark: a lower bound.
        floor_seconds: f64,
    },
    /// Nothing to say yet — no slot, no beat written, or the first beat not
    /// yet confirmed. Distinct from [`Self::BeyondTrail`]: here `replay_lag` is
    /// a legitimate fallback, because a standby that has consumed nothing has
    /// not stalled mid-apply.
    Unknown,
    /// Some, but not all, DR slots for this shard have a confirmed position.
    ///
    /// Distinct from [`Self::Unknown`] on purpose. `Unknown` means "nothing
    /// has consumed anything yet", where `replay_lag` is still a fair
    /// fallback. This means "one target is unmeasurable while another is
    /// fine" — an abandoned or never-connected slot sitting next to a
    /// healthy standby. Falling back to `replay_lag` here would report only
    /// the healthy standby's small lag. That would hide the abandoned slot
    /// completely, which is the failure this variant exists to stop.
    PartiallyMeasured {
        /// The watermark reading for the slots that DO have a position, if
        /// the trail has confirmed one for them yet. Never backed by
        /// `replay_lag`.
        measured_seconds: Option<f64>,
        /// How many matching slots have no position at all. The separate
        /// signal an abandoned slot pages on, since it does not show up in
        /// `measured_seconds`.
        unmeasurable_slots: usize,
    },
    /// The watermark trail could not be read (a query error).
    ///
    /// Not the same as [`Self::Unknown`]: an unmeasured trail may still trust
    /// `replay_lag`. A trail read that FAILED is different — it is the one
    /// moment `replay_lag` is least trustworthy. It is frozen or NULL
    /// whenever a logical apply worker is stuck, which is the incident this
    /// trail exists to measure. Never eligible for the `replay_lag` fallback.
    Failed,
}

impl WatermarkReading {
    /// The measured or floor seconds for the states that carry one directly,
    /// never via the `replay_lag` fallback.
    ///
    /// A private helper `measure_rpo` uses to fold its own [`Self::Measured`]
    /// / [`Self::BeyondTrail`] result into a `PartiallyMeasured` reading
    /// without duplicating the match.
    #[cfg(feature = "db")]
    const fn measured_or_floor_seconds(&self) -> Option<f64> {
        match self {
            Self::Measured(seconds) => Some(*seconds),
            Self::BeyondTrail { floor_seconds } => Some(*floor_seconds),
            Self::Unknown | Self::PartiallyMeasured { .. } | Self::Failed => None,
        }
    }
}

impl ReplicationStatus {
    /// The measured RPO in seconds — how much acknowledged work failing over
    /// right now would lose.
    ///
    /// Prefers the watermark trail over `pg_stat_replication.replay_lag`, and
    /// that precedence is the whole point rather than a tie-break. `replay_lag`
    /// is derived from the subscriber's reply messages, so a subscriber whose
    /// apply worker is **stuck** — precisely the incident an RPO number exists
    /// for — stops replying and leaves `replay_lag` NULL or frozen while real
    /// data loss accumulates. This was measured, not assumed: blocking a
    /// subscriber's apply worker grew the byte backlog monotonically while
    /// `replay_lag` never left NULL.
    ///
    /// `None` means unknown, and unknown is not zero. See
    /// [`Self::max_replay_lag_seconds`].
    ///
    /// When the standby has fallen off the end of the retained trail this
    /// returns the **lower bound** rather than `None`: an absent series alarms
    /// on nothing, and `standbys` is still non-zero there, so the shard would
    /// page on neither signal. The floor is at least the retention window,
    /// which clears any sane threshold — it understates the number while
    /// telling the truth about the severity. [`Self::rpo_is_lower_bound`]
    /// distinguishes the two.
    #[must_use]
    pub fn rpo_seconds(&self) -> Option<f64> {
        match self {
            Self::Unavailable { .. } => None,
            Self::Observed { heartbeat, .. } => match heartbeat {
                WatermarkReading::Measured(seconds) => Some(*seconds),
                WatermarkReading::BeyondTrail { floor_seconds } => Some(*floor_seconds),
                WatermarkReading::Unknown => self.max_replay_lag_seconds(),
                // Never falls back to `replay_lag`: see the two variants'
                // docs for why each is a moment `replay_lag` is untrustworthy.
                WatermarkReading::PartiallyMeasured {
                    measured_seconds, ..
                } => *measured_seconds,
                WatermarkReading::Failed => None,
            },
        }
    }

    /// How many DR slots for this shard have no confirmed position at all.
    ///
    /// `0` in every state except [`WatermarkReading::PartiallyMeasured`]. This
    /// is the separate signal an abandoned or never-connected slot pages on,
    /// since it does not lower [`Self::rpo_seconds`] the way a
    /// slow-but-connected standby would.
    #[must_use]
    pub const fn unmeasurable_slot_count(&self) -> usize {
        match self {
            Self::Observed {
                heartbeat:
                    WatermarkReading::PartiallyMeasured {
                        unmeasurable_slots, ..
                    },
                ..
            } => *unmeasurable_slots,
            _ => 0,
        }
    }

    /// Whether [`Self::rpo_seconds`] is an exact reading or only a lower bound.
    ///
    /// An operator deciding whether to fail over needs the difference:
    /// "42 seconds" and "at least an hour, we cannot see how much more" are
    /// different decisions.
    #[must_use]
    pub const fn rpo_is_lower_bound(&self) -> bool {
        matches!(
            self,
            Self::Observed {
                heartbeat: WatermarkReading::BeyondTrail { .. },
                ..
            }
        )
    }

    /// Worst-case replay lag across every connected standby, in seconds, as
    /// Postgres itself reports it.
    ///
    /// Exposed alongside [`Self::rpo_seconds`] rather than hidden behind it so
    /// an operator can see the two disagree — a large watermark RPO next to a
    /// NULL `replay_lag` is the signature of a stuck apply worker.
    ///
    /// `None` means *unknown*, and unknown is the honest answer in three
    /// distinct situations that all look like "no number". The views were
    /// unreadable, or no standby is connected at all, or **any** connected
    /// standby has not reported a `replay_lag` yet. Each is a reason to
    /// page, and none of them is `0.0`.
    ///
    /// A worst-case reduction must treat one missing reading as unknown, not
    /// absent. Dropping an unmeasurable standby from the `max` and keeping the
    /// rest would let a healthy peer mask it. The fleet's worst case would
    /// then read as that peer's small lag.
    #[must_use]
    pub fn max_replay_lag_seconds(&self) -> Option<f64> {
        let Self::Observed { standbys, .. } = self else {
            return None;
        };
        if standbys.is_empty() {
            return None;
        }
        let mut worst = f64::NEG_INFINITY;
        for standby in standbys {
            worst = worst.max(standby.replay_lag_seconds?);
        }
        Some(worst)
    }

    /// Worst-case WAL backlog in bytes across standbys **and** slots.
    ///
    /// Slots are included precisely because they survive the walsender: this
    /// stays a real number through the disconnection that makes
    /// [`Self::max_replay_lag_seconds`] unknowable.
    #[must_use]
    pub fn max_lag_bytes(&self) -> Option<i64> {
        let Self::Observed {
            standbys, slots, ..
        } = self
        else {
            return None;
        };
        standbys
            .iter()
            .filter_map(|s| s.lag_bytes)
            .chain(slots.iter().filter_map(|s| s.lag_bytes))
            .max()
    }

    /// How many standbys currently have a walsender on the primary.
    ///
    /// `0` is the "replication is down" signal the starter alert keys on —
    /// deliberately *not* expressed as a lag threshold, because a dead standby
    /// produces no lag reading to threshold.
    #[must_use]
    pub const fn connected_standbys(&self) -> usize {
        match self {
            Self::Unavailable { .. } => 0,
            Self::Observed { standbys, .. } => standbys.len(),
        }
    }

    /// Inactive slots — WAL retained for a standby that is not consuming it.
    #[must_use]
    pub fn inactive_slots(&self) -> usize {
        match self {
            Self::Unavailable { .. } => 0,
            Self::Observed { slots, .. } => slots.iter().filter(|s| !s.active).count(),
        }
    }
}

// ── Process-global DR configuration ────────────────────────────────────────

/// The DR knobs, published once per process.
///
/// These live beside [`FenceRegistry`] rather than on `WorkerRuntimeConfig`
/// for the reason `crate::mutex::set_mutex_lease_ttl` does: the runtime config
/// is constructed literally at ~50 call sites, and three new required fields
/// would be 50 mechanical edits obscuring the change that matters. It is also
/// the more coherent home — the pin these knobs govern is *already* process-
/// global, because the persist assert has to reach 100+ `append_events` call
/// sites without a config in hand.
///
/// Published by `From<WorkerConfig> for WorkerRuntimeConfig`, which is the
/// single choke point every worker's configuration passes through.
// Deliberately NOT `#[non_exhaustive]`: that attribute forbids external
// struct-expression construction entirely — including `..Default::default()` —
// so no downstream crate could ever build one to hand to the public
// `set_dr_config`. Every field is `pub` and the type is `Copy`; growing it is a
// breaking change we accept in exchange for the setter being usable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrConfig {
    /// How this process decides whether to fence (issue #1823).
    pub fencing: DrFencing,
    /// DR sampler cadence: the RPO's resolution floor and the bound on
    /// fence-detection latency.
    pub sample_interval: std::time::Duration,
    /// Trailing watermark retention: the ceiling on measurable lag.
    pub watermark_retain: std::time::Duration,
    /// Slot-name prefix identifying **this shard's DR replication**.
    ///
    /// Without it, every walsender for the shard's database counts as a DR
    /// standby — including an unrelated logical-decoding consumer such as a CDC
    /// pipeline. A shard with CDC attached would then report itself protected
    /// while its actual cross-region subscriber was disconnected, and
    /// `harvest_replication_down` would never fire: the most dangerous possible
    /// false negative for this feature.
    ///
    /// Defaults to `harvest_dr`, which is the naming the topology doc's setup
    /// SQL prescribes (`harvest_dr_shard0`, ...). Deployments that name their
    /// slots otherwise must set this — including physical ones, whose slot
    /// should carry the same prefix.
    pub slot_prefix: String,
}

impl Default for DrConfig {
    /// Fencing in [`DrFencing::Auto`] mode. A database with no DR marker runs
    /// the byte-for-byte pre-#954 runtime.
    fn default() -> Self {
        Self {
            fencing: DrFencing::Auto,
            sample_interval: std::time::Duration::from_secs(15),
            watermark_retain: std::time::Duration::from_secs(3600),
            slot_prefix: DEFAULT_DR_SLOT_PREFIX.to_string(),
        }
    }
}

/// The slot-name prefix `docs/cross-region-dr.md`'s setup SQL prescribes.
pub const DEFAULT_DR_SLOT_PREFIX: &str = "harvest_dr";

/// How a process decides whether to fence its writes (issue #1823).
///
/// The fence used to be opt-in per process. A process started without it
/// could write to a demoted primary after a failover. `Auto` closes that gap:
/// a process on a DR database fences itself with no setting at all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrFencing {
    /// Fence when a shard database carries a DR marker. See [`DrMarkers`].
    #[default]
    Auto,
    /// Always fence. Provision the generation row when it is absent.
    Enabled,
    /// Never fence. A shard database with a DR marker refuses this process.
    Disabled,
}

impl DrFencing {
    /// Decide whether to fence, from whether any shard has a DR marker.
    ///
    /// # Errors
    ///
    /// A message for the operator when the mode disagrees with the database:
    /// [`Self::Disabled`] on a database that carries a DR marker. The process
    /// must refuse to start. An unfenced writer on a DR database is the
    /// split-brain hazard the fence exists to stop.
    pub fn resolve(self, dr_configured: bool) -> Result<bool, String> {
        match (self, dr_configured) {
            (Self::Auto, found) => Ok(found),
            (Self::Enabled, _) => Ok(true),
            (Self::Disabled, false) => Ok(false),
            (Self::Disabled, true) => Err(
                "DR fencing is Disabled, but a shard database carries a DR marker (a \
                 harvest_shard_generation row, a DR replication slot or a DR subscription). An \
                 unfenced process could write to a demoted primary after a failover. Remove \
                 with_dr_fencing(false) or with_dr_fencing_mode(Disabled) to use the default \
                 Auto mode."
                    .to_string(),
            ),
        }
    }
}

/// What a direct-database admin write changes (issue #1823).
///
/// The kind decides whether the write may run on a logical standby.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminWrite {
    /// Schema only, such as partition DDL. Logical replication carries no
    /// DDL, so the docs run it on both sides. Allowed on a logical standby.
    SchemaOnly,
    /// Rows, such as a shard rebalance. On a logical standby it would collide
    /// with replicated rows. Refused on any standby.
    Data,
}

/// What a startup probe found in one shard database (issue #1823).
///
/// Any one signal means DR is configured for the database.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrMarkers {
    /// The shards that have a `harvest_shard_generation` row here.
    pub generation_shards: Vec<ShardId>,
    /// Replication slots with the DR prefix, scoped like the RPO metric.
    pub dr_slots: i64,
    /// Subscriptions in this database whose name or slot name has the DR
    /// prefix. A subscription may have any local name, but its slot is the
    /// one the DR setup names.
    pub dr_subscriptions: i64,
    /// Every subscription in this database, whatever its name.
    ///
    /// Not a DR marker by itself. A direct-database data write refuses any
    /// subscriber, because the CLI cannot know a custom DR prefix.
    pub subscriptions: i64,
    /// Whether the server is a physical standby (`pg_is_in_recovery()`).
    pub in_recovery: bool,
}

impl DrMarkers {
    /// Whether any DR signal is present.
    ///
    /// Recovery alone is not a signal. A plain read replica is not DR.
    #[must_use]
    pub const fn is_dr(&self) -> bool {
        !self.generation_shards.is_empty() || self.dr_slots > 0 || self.dr_subscriptions > 0
    }

    /// Whether this database is a DR standby that has not been promoted.
    ///
    /// A DR subscription means a logical standby. Recovery means a physical
    /// standby. No Harvest process may start on either. The runbook starts
    /// workers only after promotion.
    #[must_use]
    pub const fn is_standby(&self) -> bool {
        self.in_recovery || self.dr_subscriptions > 0
    }
}

static DR_CONFIG: RwLock<Option<DrConfig>> = RwLock::new(None);

/// Publish this process's DR configuration.
///
/// Called from `From<WorkerConfig> for WorkerRuntimeConfig`. Last write wins;
/// a process running two workers with different DR settings is a
/// misconfiguration this deliberately does not try to paper over — the
/// registry it governs is process-global too, so there is no coherent
/// per-worker answer.
pub fn set_dr_config(config: DrConfig) {
    let mut config = config;
    // Mirrors `crate::mutex::set_mutex_lease_ttl`, which rejects degenerate
    // durations for the same reason: a zero interval turns the sampler's
    // `tokio::time::sleep(interval)` into a hot loop issuing several queries
    // per iteration per shard, against the database a failover depends on.
    if config.sample_interval.is_zero() {
        tracing::warn!(
            "replication_sample_interval of zero would spin the DR sampler; using {:?}",
            DrConfig::default().sample_interval
        );
        config.sample_interval = DrConfig::default().sample_interval;
    }
    let mut guard = DR_CONFIG
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(config);
    drop(guard);
}

/// This process's DR configuration, or the fencing-off default.
#[must_use]
pub fn dr_config() -> DrConfig {
    let guard = DR_CONFIG
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let found = guard.clone().unwrap_or_default();
    drop(guard);
    found
}

// ── Fence registry ─────────────────────────────────────────────────────────

/// The generations this process pinned at startup, one per shard.
static PINNED: RwLock<Option<Pinned>> = RwLock::new(None);

/// Whether a worker must skip writes to `shard` (issue #1823).
///
/// A held shard may be an unpromoted logical standby, so no worker task
/// writes there until the resolver releases it. `None` names a single-pool
/// worker's database, which resolves through the default shard. With no
/// fence on, this is one atomic load.
#[must_use]
pub fn shard_writes_held(shard: Option<ShardId>) -> bool {
    FenceRegistry::is_held(shard.unwrap_or(ShardId::UNENCODED))
}

/// The sentinel a held shard is pinned to (issue #1823). No row holds it,
/// so the claim gate selects nothing and the persist assert fails closed.
const HELD: ShardGeneration = ShardGeneration(i64::MIN);

/// Fast, lock-free "is fencing on at all" gate.
///
/// Every persist consults this. Keeping it an atomic means a deployment that
/// never enables DR pays one acquire load per persist rather than an `RwLock`
/// acquisition.
static ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
struct Pinned {
    generations: BTreeMap<i32, ShardGeneration>,
    default_shard: Option<ShardId>,
    /// How many holders each held shard has (issue #1823). Two workers in one
    /// process can hold the same shard. The sentinel goes only when the last
    /// one releases it.
    holders: BTreeMap<i32, usize>,
}

/// A [`FenceRegistry::publish`] rejected because the shard is already pinned at
/// a different generation.
///
/// The fencing unit is the process: a worker started after a fence must not be
/// able to re-authorize a worker the fence already stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinConflict {
    /// The shard whose pin was already set.
    pub shard_id: i32,
    /// The generation this process is already pinned to.
    pub pinned: i64,
    /// The generation the rejected publish tried to install.
    pub attempted: i64,
}

impl std::fmt::Display for PinConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard {} is already pinned to generation {} in this process; refusing to re-pin it \
             to {}. A process cannot host workers at two different write-authority epochs — \
             re-pinning would hand authority back to a worker the fence already stopped. Restart \
             the process.",
            self.shard_id, self.pinned, self.attempted
        )
    }
}

/// A [`FenceRegistry::set_default_shard`] rejected because this process is
/// already pinned to a different default shard.
///
/// Same shape as [`PinConflict`], one level up. The default shard resolves
/// every [`ShardId::UNENCODED`] execution id. Two workers in one process
/// disagreeing about it is the same "write once per process" hazard as a
/// generation conflict (finding 13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultShardConflict {
    /// The default shard this process is already pinned to.
    pub pinned: i32,
    /// The default shard the rejected call tried to install.
    pub attempted: i32,
}

impl std::fmt::Display for DefaultShardConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this process is already pinned to default shard {}; refusing to re-pin the \
             default shard to {}. A process cannot resolve UNENCODED execution ids to two \
             different shards. Restart the process.",
            self.pinned, self.attempted
        )
    }
}

/// A [`FenceRegistry::publish`] rejected: either a shard's generation
/// conflicted, or the default shard did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishConflict {
    /// A shard's generation conflicted with one already pinned.
    Generation(PinConflict),
    /// The default shard conflicted with one already pinned.
    DefaultShard(DefaultShardConflict),
}

impl std::fmt::Display for PublishConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generation(conflict) => write!(f, "{conflict}"),
            Self::DefaultShard(conflict) => write!(f, "{conflict}"),
        }
    }
}

impl From<PinConflict> for PublishConflict {
    fn from(conflict: PinConflict) -> Self {
        Self::Generation(conflict)
    }
}

impl From<DefaultShardConflict> for PublishConflict {
    fn from(conflict: DefaultShardConflict) -> Self {
        Self::DefaultShard(conflict)
    }
}

/// Process-global record of the write-authority epoch this process pinned for
/// each shard.
///
/// Populated once by the worker at startup (before its first poll) and never
/// mutated afterwards. **A worker must never re-read and adopt a newer
/// generation**: adopting is exactly the split-brain the epoch exists to
/// prevent, so a bump is only ever resolved by restarting the fleet. That
/// asymmetry is the whole mechanism, and it is why this is a pin rather than
/// a cache.
///
/// Global rather than threaded through call sites because the persist assert
/// has to reach 100+ `store::append_events*` call sites; the same shape
/// [`crate::chaos`] uses for its injection state.
pub struct FenceRegistry;

impl FenceRegistry {
    /// Pin `shard` at `generation` for the lifetime of this process.
    ///
    /// Conflict-checked exactly like [`Self::publish`] (finding 3):
    /// re-pinning an already-pinned shard to a *different* generation is
    /// refused rather than silently overwriting it. Without this check a
    /// worker started after a fence could call this directly. It could then
    /// hand write authority back to a worker the fence already stopped — the
    /// same split-brain [`Self::publish`]'s conflict check exists to prevent.
    /// Re-registering the *same* generation is fine and idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`PinConflict`] when `shard` is already pinned at a different
    /// generation. Nothing is mutated.
    pub fn register(shard: ShardId, generation: ShardGeneration) -> Result<(), PinConflict> {
        {
            let mut guard = PINNED
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pinned = guard.get_or_insert_with(Pinned::default);
            if let Some(existing) = pinned.generations.get(&shard.as_i32())
                && *existing != generation
            {
                let conflict = PinConflict {
                    shard_id: shard.as_i32(),
                    pinned: existing.as_i64(),
                    attempted: generation.as_i64(),
                };
                drop(guard);
                return Err(conflict);
            }
            pinned.generations.insert(shard.as_i32(), generation);
            // Stored under the write lock, after the map. A reader that sees
            // `true` then waits on the lock, so it never finds an empty
            // registry. A release cannot clear the flag between the two steps.
            ENABLED.store(true, Ordering::Release);
            drop(guard);
        }
        Ok(())
    }

    /// Publish a complete set of pins in **one** write.
    ///
    /// The whole registry becomes visible at once, default shard included.
    /// [`Self::register`] plus a trailing [`Self::set_default_shard`] is not
    /// equivalent: a startup that failed partway through that sequence left
    /// `ENABLED` true with a partial map and *no* default shard, under which
    /// [`Self::expected`] returns `None` for [`ShardId::UNENCODED`] and every
    /// pre-sharding execution id in the process silently persists **unfenced**.
    /// A worker pins every shard it can reach or none of them, so it publishes
    /// once.
    ///
    /// # A pin is immutable once published
    ///
    /// Publishing a *different* generation for a shard this process has already
    /// pinned is **refused**, and that refusal is the whole "never adopt a newer
    /// epoch" invariant made structural rather than documented.
    ///
    /// Without it, a process hosting more than one worker had a hole straight
    /// through the fence: a worker started *after* a fence would publish the new
    /// generation, overwrite the shared pin, and the already-running worker —
    /// the one the fence was for — would read the replacement in both its claim
    /// gate and its persist assert and silently **regain write authority**. It
    /// would go on appending to a history another region owns, which is exactly
    /// the split-brain this module exists to prevent.
    ///
    /// So the fencing unit is the **process**, not the worker, and the error
    /// says so: a process cannot host workers at two different epochs, and the
    /// remedy is to restart it. Re-publishing the *same* generation is fine and
    /// idempotent — that is two workers covering the same shards at the same
    /// epoch, which is ordinary.
    ///
    /// # Errors
    ///
    /// Returns the conflicting generation or default shard when either is
    /// already pinned to a different value. Finding 13 covers the default
    /// shard: it is validated in the same pre-flight pass as the
    /// generations, so a rejected publish mutates neither. The caller must
    /// refuse to start; nothing is published.
    pub fn publish(
        pins: &[(ShardId, ShardGeneration)],
        default_shard: ShardId,
    ) -> Result<(), PublishConflict> {
        {
            let mut guard = PINNED
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pinned = guard.get_or_insert_with(Pinned::default);

            // Validate every pin AND the default shard BEFORE mutating
            // anything, so a rejected publish leaves the running workers'
            // registry exactly as it was.
            for (shard, generation) in pins {
                if let Some(existing) = pinned.generations.get(&shard.as_i32())
                    && *existing != *generation
                {
                    let conflict = PinConflict {
                        shard_id: shard.as_i32(),
                        pinned: existing.as_i64(),
                        attempted: generation.as_i64(),
                    };
                    drop(guard);
                    return Err(conflict.into());
                }
            }
            if let Some(existing) = pinned.default_shard
                && existing != default_shard
            {
                let conflict = DefaultShardConflict {
                    pinned: existing.as_i32(),
                    attempted: default_shard.as_i32(),
                };
                drop(guard);
                return Err(conflict.into());
            }

            for (shard, generation) in pins {
                pinned.generations.insert(shard.as_i32(), *generation);
            }
            pinned.default_shard = Some(default_shard);
            // Stored under the write lock, after the map AND the default
            // shard. A reader that observes `is_enabled()` then waits on the
            // lock, so it never finds a half-built registry.
            ENABLED.store(true, Ordering::Release);
            drop(guard);
        }
        Ok(())
    }

    /// Hold `shards`: pin each to a sentinel generation that no row holds
    /// (issue #1823).
    ///
    /// A worker holds a shard it could not probe at startup. The claim gate
    /// then selects nothing on it, and the persist assert fails closed. A
    /// shard this process already pinned keeps its pin. Each call adds one
    /// holder to each held shard; [`Self::release_held`] removes one.
    ///
    /// # Errors
    ///
    /// The default shard conflicts with one already pinned.
    pub fn hold(shards: &[ShardId], default_shard: ShardId) -> Result<(), PublishConflict> {
        // One write lock covers the check, the sentinel and the holder count.
        // A release between two locks could drop the sentinel before the new
        // holder was counted, and leave an unprobed shard unfenced.
        let mut guard = PINNED
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pinned = guard.get_or_insert_with(Pinned::default);
        if let Some(existing) = pinned.default_shard
            && existing != default_shard
        {
            let conflict = DefaultShardConflict {
                pinned: existing.as_i32(),
                attempted: default_shard.as_i32(),
            };
            drop(guard);
            return Err(conflict.into());
        }
        pinned.default_shard = Some(default_shard);
        for shard in shards {
            let key = shard.as_i32();
            // A shard this process already pinned keeps its real pin.
            let generation = *pinned.generations.entry(key).or_insert(HELD);
            if generation == HELD {
                *pinned.holders.entry(key).or_insert(0) += 1;
            }
        }
        if !pinned.generations.is_empty() {
            ENABLED.store(true, Ordering::Release);
        }
        drop(guard);
        Ok(())
    }

    /// Remove one holder from a held shard (issue #1823). When the last
    /// holder goes, the shard runs unfenced.
    ///
    /// Only a sentinel pin is removed. A real pin is fixed for the life of the
    /// process, so this never touches one.
    pub fn release_held(shard: ShardId) {
        let mut guard = PINNED
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let empty = guard.as_mut().is_none_or(|pinned| {
            let key = shard.as_i32();
            let remaining = pinned.holders.get_mut(&key).map_or(0, |count| {
                *count = count.saturating_sub(1);
                *count
            });
            if remaining == 0 {
                pinned.holders.remove(&key);
                if pinned.generations.get(&key) == Some(&HELD) {
                    pinned.generations.remove(&key);
                }
            }
            pinned.generations.is_empty()
        });
        // Stored under the write lock. A publisher waits on the lock, so its
        // `true` always lands after this `false` and never before it.
        if empty {
            ENABLED.store(false, Ordering::Release);
        }
        drop(guard);
    }

    /// Whether this process pins any shard to a real generation.
    #[must_use]
    pub fn has_real_pin() -> bool {
        if !Self::is_enabled() {
            return false;
        }
        let guard = PINNED
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = guard
            .as_ref()
            .is_some_and(|pinned| pinned.generations.values().any(|pin| *pin != HELD));
        drop(guard);
        found
    }

    /// Whether `shard` is held: pinned to the sentinel, waiting for a probe.
    #[must_use]
    pub fn is_held(shard: ShardId) -> bool {
        Self::expected(shard) == Some(HELD)
    }

    /// Set the shard that [`ShardId::UNENCODED`] execution ids resolve to.
    ///
    /// Execution ids minted before sharding carry no shard bits. They still
    /// live in a real database and must still be fenced, so they resolve to
    /// the pool's default shard exactly as [`crate::shard::ShardedDbPool`]
    /// routes them.
    ///
    /// Conflict-checked like [`Self::publish`] (finding 13):
    /// re-pinning an already-pinned default shard to a *different* shard is
    /// refused. Two workers in one process could otherwise disagree about
    /// the default shard. The later one would then silently redirect every
    /// `UNENCODED` execution id the earlier one resolves. That is a
    /// spurious fence, or a check against the wrong epoch. Re-setting the
    /// *same* default shard is fine and idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`DefaultShardConflict`] when the default shard is already
    /// pinned to a different value. Nothing is mutated.
    pub fn set_default_shard(shard: ShardId) -> Result<(), DefaultShardConflict> {
        let mut guard = PINNED
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pinned = guard.get_or_insert_with(Pinned::default);
        if let Some(existing) = pinned.default_shard
            && existing != shard
        {
            let conflict = DefaultShardConflict {
                pinned: existing.as_i32(),
                attempted: shard.as_i32(),
            };
            drop(guard);
            return Err(conflict);
        }
        pinned.default_shard = Some(shard);
        drop(guard);
        Ok(())
    }

    /// The generation pinned for `shard`, or `None` when this shard is not
    /// fenced.
    #[must_use]
    pub fn expected(shard: ShardId) -> Option<ShardGeneration> {
        if !Self::is_enabled() {
            return None;
        }
        let guard = PINNED
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = guard.as_ref().and_then(|pinned| {
            let key = if shard.is_unencoded() {
                pinned.default_shard?.as_i32()
            } else {
                shard.as_i32()
            };
            pinned.generations.get(&key).copied()
        });
        drop(guard);
        found
    }

    /// The `(shard, generation)` this process is pinned to for `shard`, in one
    /// lock acquisition.
    ///
    /// The claim hot path's entry point. Resolving the shard and reading its
    /// generation separately would take the read lock twice and put the
    /// `UNENCODED` → default-shard rule in two modules.
    #[must_use]
    pub fn binding(shard: ShardId) -> Option<(ShardId, ShardGeneration)> {
        if !Self::is_enabled() {
            return None;
        }
        let guard = PINNED
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = guard.as_ref().and_then(|pinned| {
            let resolved = if shard.is_unencoded() {
                pinned.default_shard?
            } else {
                shard
            };
            pinned
                .generations
                .get(&resolved.as_i32())
                .map(|g| (resolved, *g))
        });
        drop(guard);
        found
    }

    /// The shard whose fencing row backs `shard`, resolving
    /// [`ShardId::UNENCODED`] through the default shard.
    #[must_use]
    pub fn resolve_shard(shard: ShardId) -> Option<ShardId> {
        if !shard.is_unencoded() {
            return Some(shard);
        }
        let guard = PINNED
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = guard.as_ref().and_then(|p| p.default_shard);
        drop(guard);
        found
    }

    /// Whether any shard is fenced in this process.
    ///
    /// The hot-path gate: `false` means every fencing check compiles down to
    /// this single acquire load.
    #[must_use]
    pub fn is_enabled() -> bool {
        ENABLED.load(Ordering::Acquire)
    }

    /// Every pinned `(shard, generation)` pair, for diagnostics and the admin
    /// surface.
    #[must_use]
    pub fn snapshot() -> Vec<(ShardId, ShardGeneration)> {
        let guard = PINNED
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = guard.as_ref().map_or_else(Vec::new, |p| {
            p.generations
                .iter()
                .map(|(k, v)| (ShardId::new(*k), *v))
                .collect()
        });
        drop(guard);
        snapshot
    }

    /// Drop every pin, returning the process to the unfenced default.
    ///
    /// For tests and for a CLI process that pinned a generation only to run one
    /// command. A *worker* must never call this: see the type docs.
    pub fn clear() {
        {
            let mut guard = PINNED
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = None;
            ENABLED.store(false, Ordering::Release);
            drop(guard);
        }
    }
}

// ── SQL identifier quoting ─────────────────────────────────────────────────

/// Quote a Postgres identifier for inline interpolation.
///
/// `setval` takes its target as a `regclass` *name*, and a table name cannot be
/// a bind parameter, so [`advance_sequences_after_promotion`] has to build SQL
/// by concatenation. Quoting is therefore the security boundary and it must be
/// **complete**, not a best-effort screen.
///
/// An earlier revision screened instead: it accepted only `[a-z_][a-z0-9_]*`
/// and silently skipped everything else. That was wrong in both directions —
/// an embedder on a `PascalCase` ORM schema had *every* sequence skipped while
/// the command reported success, and a perfectly ordinary table named `user` or
/// `order` passed the screen and then failed as a bare keyword in the generated
/// SQL. Doubling embedded quotes inside `"…"` is the complete, universal escape
/// for a Postgres identifier, so there is nothing left to screen for and
/// nothing to skip.
///
/// Note this is *not* the whole defence: the catalog query that feeds it also
/// restricts the owning relation to `relkind IN ('r','p')`. A sequence can be
/// `OWNED BY` a **view** column, and `FROM <view>` would execute that view's
/// query — including any volatile function in it — on the operator's
/// high-privilege DR connection. No amount of quoting addresses that; the
/// relation-kind filter does.
#[cfg(feature = "db")]
#[must_use]
fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Schema-qualify a quoted identifier.
///
/// Qualification is load-bearing, not tidiness: `pg_temp` is searched *ahead
/// of* the resolved `search_path` for relation lookups while
/// `current_schema()` still reports `public`, so an unqualified `FROM t` can
/// silently resolve to a session-local temp table and set a sequence from the
/// wrong data — producing exactly the duplicate-key outage
/// `advance_sequences_after_promotion` exists to prevent, with no error.
/// A qualified name is immune.
#[cfg(feature = "db")]
#[must_use]
fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

// ── Database surface (feature = "db") ──────────────────────────────────────

#[cfg(feature = "db")]
mod db {
    use diesel::sql_types::{BigInt, Bool, Double, Integer, Nullable, Text};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use super::{
        BUMP_LOCK_TIMEOUT_MS, DrMarkers, FenceRegistry, PROMOTE_STATEMENT_TIMEOUT_MS,
        ReplicationStatus, ShardGeneration, SlotLag, StandbyLag, WatermarkReading, qualified,
        quote_ident,
    };
    use crate::error::{HarvestResult, database_error};
    use crate::types::ShardId;

    #[derive(diesel::QueryableByName)]
    struct GenerationRow {
        #[diesel(sql_type = BigInt)]
        generation: i64,
    }

    /// Read the shard's current write-authority epoch, if the row exists.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn current_generation(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
    ) -> HarvestResult<Option<ShardGeneration>> {
        let rows: Vec<GenerationRow> = diesel::sql_query(
            "SELECT generation FROM harvest_shard_generation WHERE shard_id = $1",
        )
        .bind::<Integer, _>(shard.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows
            .into_iter()
            .next()
            .map(|r| ShardGeneration(r.generation)))
    }

    /// Provision this shard's fencing row if it is absent, and return the
    /// epoch now in force.
    ///
    /// Idempotent, and idempotent in the direction that matters: `ON CONFLICT
    /// DO NOTHING` means re-provisioning an already-fenced shard **returns**
    /// its epoch rather than resetting it to zero. A reset would silently hand
    /// write authority back to the region that was just fenced off, which is
    /// the single worst thing this function could do — pinned by
    /// `a_fresh_database_provisions_generation_zero_and_is_idempotent`.
    ///
    /// The fallback read is a **separate statement**, not a `UNION ALL`
    /// trailing the `INSERT` (finding 8). Under READ COMMITTED every part of
    /// one statement shares the snapshot taken at that statement's start.
    /// Two workers can race to provision the same shard. The loser's
    /// `ON CONFLICT DO NOTHING` blocks on the winner's transaction and, once
    /// it commits, inserts nothing. A fallback read in the SAME statement
    /// still uses the pre-commit snapshot, though, and sees no row either.
    /// The whole statement then returns empty, and this function spuriously
    /// refuses to start. A fleet-wide first start, where every worker starts
    /// at once, is exactly when this race bites hardest. A separate
    /// statement takes a fresh snapshot and is guaranteed to see the
    /// winner's now-committed row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn ensure_generation_row(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
    ) -> HarvestResult<ShardGeneration> {
        let inserted: Vec<GenerationRow> = diesel::sql_query(
            "INSERT INTO harvest_shard_generation (shard_id, generation, fenced_reason) \
             VALUES ($1, 0, 'provisioned') \
             ON CONFLICT (shard_id) DO NOTHING \
             RETURNING generation",
        )
        .bind::<Integer, _>(shard.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;
        if let Some(row) = inserted.into_iter().next() {
            return Ok(ShardGeneration(row.generation));
        }

        // This process's INSERT did not win the row: either it already
        // existed, or a concurrent first start just provisioned it.
        let existing: Vec<GenerationRow> = diesel::sql_query(
            "SELECT generation FROM harvest_shard_generation WHERE shard_id = $1",
        )
        .bind::<Integer, _>(shard.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;

        existing
            .into_iter()
            .next()
            .map(|r| ShardGeneration(r.generation))
            .ok_or_else(|| {
                crate::error::HarvestError::Database(format!(
                    "harvest_shard_generation row for shard {} could not be provisioned",
                    shard.as_i32()
                ))
            })
    }

    /// Revoke the old region's write authority: bump this shard's epoch.
    ///
    /// **This is the fence.** Run it on the promoted primary. Every worker
    /// still pinned to the previous epoch — anywhere — becomes structurally
    /// unable to claim tasks or append events against this database, and stops
    /// with [`crate::error::HarvestError::ShardFenced`] rather than forking a
    /// history.
    ///
    /// It is also a **fleet-stopping** operation: workers in the *new* region
    /// pinned to the old epoch are fenced too, which is why the runbook's order
    /// is fence → promote → verify → **start workers**, and why healthy-region
    /// use is a mistake to be recovered by restarting the fleet, never by
    /// bumping again.
    ///
    /// The `UPDATE` takes the row's exclusive lock, which is what makes
    /// [`assert_fence`]'s `FOR SHARE` a commit-order barrier rather than a
    /// racy read: this cannot commit while an in-flight persist holds the row,
    /// and every persist that starts afterwards sees the new epoch.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure, or
    /// [`crate::error::HarvestError::NotFound`] when the shard has no fencing
    /// row to bump (provision it first — bumping a shard that was never
    /// provisioned would fence nothing while looking like it worked).
    pub async fn bump_generation(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        reason: &str,
        actor: &str,
    ) -> HarvestResult<ShardGeneration> {
        let reason = reason.to_string();
        let actor = actor.to_string();
        let shard_id = shard.as_i32();
        let rows: Vec<GenerationRow> = Box::pin(
            conn.transaction::<_, crate::error::HarvestError, _>(async move |conn| {
                diesel::sql_query(format!(
                    "SET LOCAL lock_timeout = '{BUMP_LOCK_TIMEOUT_MS}ms'"
                ))
                .execute(conn)
                .await
                .map_err(database_error)?;
                // Wait for every fenced pass in flight (issue #1823). A pass
                // holds this lock shared, so its writes commit before the
                // bump. The wait comes before the table lock, so a pass's
                // own `FOR SHARE` checks never queue behind this bump.
                diesel::sql_query(FENCE_PASS_LOCK_EXCLUSIVE)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                diesel::sql_query("LOCK TABLE harvest_shard_generation IN ACCESS EXCLUSIVE MODE")
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                diesel::sql_query(
                    "UPDATE harvest_shard_generation \
                     SET generation = generation + 1, fenced_at = NOW(), \
                         fenced_reason = $2, fenced_by = $3 \
                     WHERE shard_id = $1 \
                     RETURNING generation",
                )
                .bind::<Integer, _>(shard_id)
                .bind::<Text, _>(reason)
                .bind::<Text, _>(actor)
                .load(conn)
                .await
                .map_err(database_error)
            }),
        )
        .await?;

        rows.into_iter()
            .next()
            .map(|r| ShardGeneration(r.generation))
            .ok_or_else(|| {
                crate::error::HarvestError::NotFound(format!(
                    "no harvest_shard_generation row for shard {} — provision it before fencing",
                    shard.as_i32()
                ))
            })
    }

    /// Take the pass lock shared, for a fenced pass (issue #1823).
    const FENCE_PASS_LOCK_SHARED: &str =
        "SELECT pg_advisory_xact_lock_shared(hashtext('harvest:dr_fence_pass:v1'))";
    /// Take the pass lock exclusive, for a bump (issue #1823).
    const FENCE_PASS_LOCK_EXCLUSIVE: &str =
        "SELECT pg_advisory_xact_lock(hashtext('harvest:dr_fence_pass:v1'))";
    /// How often a fence guard pings its session (issue #1823). The ping
    /// keeps an idle proxy from closing it, and finds a lost session.
    const FENCE_PASS_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(1);
    /// How long a pass waits to open its fence connection.
    const FENCE_PASS_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    /// A commit-order barrier for a pass of several writes (issue #1823).
    ///
    /// A scheduler pass or a partition-maintenance pass writes in many
    /// statements and transactions. A check before the pass is not a
    /// barrier: a bump can commit between the check and a write. This guard
    /// holds a transaction open on its own connection, with the pass lock
    /// shared and the generation checked. [`bump_generation`] takes the
    /// pass lock exclusive, so it cannot commit while the pass runs. A pass
    /// that starts after the bump sees the new generation and stops.
    ///
    /// The connection is not from the pool, so the guard never starves the
    /// pass of a connection. Dropping the guard closes the connection. The
    /// server then ends the transaction and frees the lock, even when the
    /// pass is cancelled.
    pub struct FencePassGuard {
        lost: std::sync::Arc<std::sync::atomic::AtomicBool>,
        keepalive: tokio::task::JoinHandle<()>,
    }

    impl FencePassGuard {
        /// Whether the guard's session has ended (issue #1823).
        ///
        /// The server then frees the pass lock, so a bump can commit. A pass
        /// checks this before it writes, and stops when it is set. A pass
        /// already mid-write when the session ends can still race a bump,
        /// for at most one keepalive interval.
        #[must_use]
        pub fn is_lost(&self) -> bool {
            // UFCS: diesel's blanket `RunQueryDsl::load` shadows this method.
            std::sync::atomic::AtomicBool::load(&self.lost, std::sync::atomic::Ordering::Acquire)
        }
    }

    impl Drop for FencePassGuard {
        fn drop(&mut self) {
            // The task owns the connection. Aborting it closes the
            // connection, and the server then frees the pass lock.
            self.keepalive.abort();
        }
    }

    /// Open a [`FencePassGuard`] for `shard`, or `None` when this process
    /// pins no generation for it (issue #1823).
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::ShardFenced`] when the shard is at
    /// another generation. [`crate::error::HarvestError::Database`] when
    /// the connection or a query fails.
    pub async fn begin_fenced_pass(
        pool: &crate::worker::DbPool,
        shard: ShardId,
    ) -> HarvestResult<Option<FencePassGuard>> {
        let Some(pinned) = FenceRegistry::expected(shard) else {
            return Ok(None);
        };
        let resolved = FenceRegistry::resolve_shard(shard).unwrap_or(shard);
        begin_fenced_pass_at(pool, resolved, pinned).await.map(Some)
    }

    /// [`begin_fenced_pass`] at an explicit generation, for a process that
    /// pins nothing (issue #1823). The CLI states the epoch an operator gave.
    ///
    /// # Errors
    ///
    /// As [`begin_fenced_pass`].
    pub async fn begin_fenced_pass_at(
        pool: &crate::worker::DbPool,
        shard: ShardId,
        expected: ShardGeneration,
    ) -> HarvestResult<FencePassGuard> {
        use deadpool::managed::Manager as _;
        let conn = tokio::time::timeout(FENCE_PASS_CONNECT_TIMEOUT, pool.manager().create())
            .await
            .map_err(|_| {
                crate::error::HarvestError::Database(
                    "timed out opening the DR fence connection".to_string(),
                )
            })?
            .map_err(|error| crate::error::HarvestError::Database(error.to_string()))?;
        begin_fenced_pass_on(conn, shard, expected).await
    }

    /// [`begin_fenced_pass_at`] on a connection the caller opened (issue
    /// #1823). The guard owns it, and closes it on drop. The caller must
    /// not run its own writes on this connection.
    ///
    /// # Errors
    ///
    /// As [`begin_fenced_pass`].
    pub async fn begin_fenced_pass_on(
        mut conn: AsyncPgConnection,
        shard: ShardId,
        expected: ShardGeneration,
    ) -> HarvestResult<FencePassGuard> {
        use diesel_async::SimpleAsyncConnection as _;
        conn.batch_execute("BEGIN").await.map_err(database_error)?;
        // The guard runs no query while the pass works. A server timeout
        // must not end its transaction and free the lock mid-pass, so this
        // transaction turns them off. PostgreSQL 17 adds
        // `transaction_timeout`; older servers do not know it.
        conn.batch_execute(
            "SET LOCAL idle_in_transaction_session_timeout = 0; \
             SET LOCAL statement_timeout = 0; \
             SET LOCAL application_name = 'harvest_dr_fence_pass'; \
             DO $$ BEGIN \
               IF current_setting('server_version_num')::int >= 170000 THEN \
                 PERFORM set_config('transaction_timeout', '0', true); \
               END IF; \
             END $$",
        )
        .await
        .map_err(database_error)?;
        diesel::sql_query(FENCE_PASS_LOCK_SHARED)
            .execute(&mut conn)
            .await
            .map_err(database_error)?;
        assert_generation(&mut conn, shard, expected).await?;
        let lost = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&lost);
        let keepalive = tokio::spawn(async move {
            loop {
                tokio::time::sleep(FENCE_PASS_KEEPALIVE).await;
                if diesel::sql_query("SELECT 1")
                    .execute(&mut conn)
                    .await
                    .is_err()
                {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                    tracing::error!(
                        shard_id = shard.as_i32(),
                        "the DR fence guard lost its session; its pass stops writing"
                    );
                    return;
                }
            }
        });
        Ok(FencePassGuard { lost, keepalive })
    }

    /// Assert that this process still holds write authority for `shard`.
    ///
    /// Call at the top of a persist. Costs **nothing** — not even a round trip
    /// — when this process pinned no generation. That is every process that
    /// found no DR marker at startup (issue #1823).
    ///
    /// When it does run it takes the fencing row `FOR SHARE`. Inside a
    /// transaction that makes the check a commit-order barrier:
    /// [`bump_generation`]'s exclusive `UPDATE` cannot commit while this
    /// transaction holds the row, so a persist that passes this check is
    /// guaranteed to commit *before* the fence takes effect, and one that
    /// starts after the fence commits observes the new epoch and fails. Called
    /// outside a transaction the lock is released at statement end, so the
    /// check is a very tight read rather than a barrier — the claim gate, not
    /// this assert, is the structural guarantee for work that has not started.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::ShardFenced`] when the pinned epoch is not
    /// the database's current one, **including when the row is absent** (fail
    /// closed). [`crate::error::HarvestError::Database`] on query failure.
    pub async fn assert_fence(conn: &mut AsyncPgConnection, shard: ShardId) -> HarvestResult<()> {
        let Some(pinned) = FenceRegistry::expected(shard) else {
            return Ok(());
        };
        let resolved = FenceRegistry::resolve_shard(shard).unwrap_or(shard);
        assert_generation(conn, resolved, pinned).await
    }

    /// Fail with `ShardFenced` unless `shard` is at `expected`.
    ///
    /// The one check both the persist assert and the admin check run. It
    /// takes the row `FOR SHARE`; see [`assert_fence`] for why. An absent row
    /// fails closed.
    async fn assert_generation(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        expected: ShardGeneration,
    ) -> HarvestResult<()> {
        let rows: Vec<GenerationRow> = diesel::sql_query(
            "SELECT generation FROM harvest_shard_generation WHERE shard_id = $1 FOR SHARE",
        )
        .bind::<Integer, _>(shard.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;

        let current = rows.into_iter().next().map(|r| r.generation);
        if current == Some(expected.as_i64()) {
            return Ok(());
        }
        Err(crate::error::HarvestError::ShardFenced {
            shard_id: shard.as_i32(),
            pinned: expected.as_i64(),
            current,
        })
    }

    #[derive(diesel::QueryableByName)]
    struct TableRow {
        #[diesel(sql_type = Bool)]
        present: bool,
    }

    #[derive(diesel::QueryableByName)]
    struct MarkerRow {
        #[diesel(sql_type = diesel::sql_types::Array<Integer>)]
        generation_shards: Vec<i32>,
        #[diesel(sql_type = BigInt)]
        dr_slots: i64,
        #[diesel(sql_type = BigInt)]
        dr_subscriptions: i64,
        #[diesel(sql_type = BigInt)]
        subscriptions: i64,
        #[diesel(sql_type = Bool)]
        in_recovery: bool,
    }

    /// Probe this database for DR markers (issue #1823).
    ///
    /// The slot test uses the same scope as the RPO metric. A logical slot
    /// counts for its own database. A physical slot has no database, so it
    /// counts for every database on the cluster. Physical replication copies
    /// the whole cluster, so that is the correct answer. The query uses
    /// `starts_with`, not `LIKE`, because `_` is a wildcard in `LIKE`.
    ///
    /// Every catalog read here needs no special grant. `pg_subscription`
    /// hides only its connection string from ordinary roles.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn probe_dr_markers(
        conn: &mut AsyncPgConnection,
        slot_prefix: &str,
    ) -> HarvestResult<DrMarkers> {
        // A database without the fence table has no generation row. Before
        // issue #1823 an unfenced process issued no DR query at all, so a
        // missing table must not fail its start now.
        let has_table: TableRow = diesel::sql_query(
            "SELECT to_regclass('harvest_shard_generation') IS NOT NULL AS present",
        )
        .get_result(conn)
        .await
        .map_err(database_error)?;
        let generation_shards = if has_table.present {
            "ARRAY(SELECT shard_id FROM harvest_shard_generation ORDER BY shard_id)"
        } else {
            "ARRAY[]::integer[]"
        };
        let row: MarkerRow = diesel::sql_query(format!(
            "SELECT \
                 {generation_shards} AS generation_shards, \
                 (SELECT COUNT(*) FROM pg_replication_slots s \
                  WHERE (s.database IS NULL OR s.database = current_database()) \
                    AND starts_with(s.slot_name, $1)) AS dr_slots, \
                 (SELECT COUNT(*) FROM pg_subscription s \
                  JOIN pg_database d ON d.oid = s.subdbid \
                  WHERE d.datname = current_database() \
                    AND (starts_with(s.subname::text, $1) \
                         OR starts_with(COALESCE(s.subslotname::text, ''), $1))) \
                     AS dr_subscriptions, \
                 (SELECT COUNT(*) FROM pg_subscription s \
                  JOIN pg_database d ON d.oid = s.subdbid \
                  WHERE d.datname = current_database()) AS subscriptions, \
                 pg_is_in_recovery() AS in_recovery"
        ))
        .bind::<Text, _>(slot_prefix)
        .get_result(conn)
        .await
        .map_err(database_error)?;
        Ok(DrMarkers {
            generation_shards: row
                .generation_shards
                .into_iter()
                .map(ShardId::new)
                .collect(),
            dr_slots: row.dr_slots,
            dr_subscriptions: row.dr_subscriptions,
            subscriptions: row.subscriptions,
            in_recovery: row.in_recovery,
        })
    }

    /// How many times startup probes one shard before it refuses to start.
    const PROBE_ATTEMPTS: u32 = 5;

    /// Probe one pool, and retry a failure with backoff (issue #1823).
    ///
    /// Before the default became `Auto`, an unfenced worker issued no probe.
    /// A shard that is briefly unreachable at boot must not stop it at once.
    /// After the last attempt the error stands, and the process refuses to
    /// start. An unknown shard could carry a DR marker, so running unfenced
    /// would fail open.
    async fn probe_pool(
        pool: &crate::worker::DbPool,
        slot_prefix: &str,
    ) -> HarvestResult<DrMarkers> {
        let mut delay = std::time::Duration::from_millis(500);
        let mut attempt = 1;
        loop {
            let probed = async {
                let mut conn = crate::pool::acquire_within_pool_bound(pool).await?;
                probe_dr_markers(&mut conn, slot_prefix).await
            }
            .await;
            match probed {
                Ok(markers) => return Ok(markers),
                Err(error) if attempt < PROBE_ATTEMPTS => {
                    tracing::warn!(
                        attempt,
                        error = %error,
                        "DR marker probe failed; retrying before startup refuses"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Check that a direct-database admin write may run on `shard`
    /// (issue #1823).
    ///
    /// A CLI process pins nothing, so [`assert_fence`] cannot help it. A pin
    /// taken at connect time would match a demoted primary too. The operator
    /// therefore states the epoch that holds authority, as `expected`.
    ///
    /// - `Some(expected)`: the shard must be at exactly that generation.
    /// - `None`: allowed only on a database with no DR marker.
    ///
    /// A logical standby carries the replicated row at the primary's
    /// generation, so a matching epoch does not prove authority there. The
    /// probe therefore always runs. A server in recovery refuses every write.
    /// Any subscription in the database refuses an [`AdminWrite::Data`]
    /// write, whatever its name. The CLI cannot know a custom DR prefix.
    ///
    /// The check is a preflight read, not a commit-order barrier.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::ShardFenced`] when the shard is at another
    /// generation, or has no row. [`crate::error::HarvestError::Config`] when
    /// `expected` is `None` on a DR database, or the write may not run on this
    /// standby.
    /// [`crate::error::HarvestError::Database`] on query failure.
    pub async fn assert_admin_write_authority(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        expected: Option<ShardGeneration>,
        slot_prefix: &str,
        kind: super::AdminWrite,
    ) -> HarvestResult<()> {
        let markers = probe_dr_markers(conn, slot_prefix).await?;
        let standby_refuses =
            markers.in_recovery || (kind == super::AdminWrite::Data && markers.subscriptions > 0);
        if standby_refuses {
            return Err(crate::error::HarvestError::Config(format!(
                "shard {} is a DR standby, so this admin write may not run here. Point it at \
                 the primary, or promote this database first.",
                shard.as_i32()
            )));
        }
        if let Some(expected) = expected {
            return assert_generation(conn, shard, expected).await;
        }
        if markers.is_dr() {
            return Err(crate::error::HarvestError::Config(format!(
                "shard {} is a DR database, so an admin write must state the generation that \
                 holds write authority. Read it from the promoted primary.",
                shard.as_i32()
            )));
        }
        Ok(())
    }

    /// Resolve this process's fencing mode and pin every shard it serves
    /// (issue #1823).
    ///
    /// Workers and the management API both call this at startup, before
    /// they write anything.
    ///
    /// `targets` is the `(shard, pool)` set and the default shard, when the
    /// caller knows its shard identity. With `None`, the probe runs on
    /// `fallback_pool`. If the fence turns on, exactly one
    /// `harvest_shard_generation` row there names the shard. That row is how
    /// an operator addressed this database with `harvest dr fence`.
    ///
    /// Returns the fenced targets, or `None` when the process runs unfenced.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::Config`] when the process must refuse to
    /// start. Three causes: the mode disagrees with the database, the shard
    /// cannot be named, or a pin conflicts with one already in this process.
    /// A database error also refuses the start. A fenced process never falls
    /// back to unfenced.
    pub async fn pin_process_fence(
        mode: super::DrFencing,
        slot_prefix: &str,
        targets: Option<(Vec<(ShardId, crate::worker::DbPool)>, ShardId)>,
        fallback_pool: &crate::worker::DbPool,
    ) -> HarvestResult<Option<Vec<(ShardId, crate::worker::DbPool)>>> {
        pin_fence(mode, slot_prefix, targets, fallback_pool, None)
            .await
            .map(|(fenced, _)| fenced)
    }

    /// [`pin_process_fence`] for a worker, which holds a shard it cannot
    /// probe instead of refusing to start (issue #1823).
    ///
    /// A worker tolerates an unreachable shard at boot (issue #961). It
    /// registers, retries and serves the other shards. A shard it cannot
    /// probe might carry a DR marker, so running it unfenced would fail
    /// open. The worker therefore pins it to a sentinel generation that no
    /// row holds. The claim gate selects nothing there, and the persist
    /// assert fails closed. [`resolve_held`] later releases or refuses it.
    ///
    /// Returns the fenced targets and the held `(shard, pool)` pairs. With no
    /// shard identity, the held shard is [`ShardId::UNENCODED`].
    ///
    /// # Errors
    ///
    /// As [`pin_process_fence`]. In addition, a fenced worker refuses to
    /// start when it cannot probe an assigned shard: it cannot pin that
    /// shard. An unassigned shard is held instead, so an outage there does
    /// not stop the worker. `assigned` empty means every target is assigned.
    #[allow(clippy::type_complexity)]
    pub async fn pin_worker_fence(
        mode: super::DrFencing,
        slot_prefix: &str,
        targets: Option<(Vec<(ShardId, crate::worker::DbPool)>, ShardId)>,
        fallback_pool: &crate::worker::DbPool,
        assigned: &[ShardId],
    ) -> HarvestResult<(
        Option<Vec<(ShardId, crate::worker::DbPool)>>,
        Vec<(ShardId, crate::worker::DbPool)>,
    )> {
        pin_fence(mode, slot_prefix, targets, fallback_pool, Some(assigned)).await
    }

    /// Re-probe the shards [`pin_worker_fence`] held (issue #1823).
    ///
    /// A shard that still cannot be probed stays held. A shard with no DR
    /// marker is released, and its claims resume unfenced. A shard with a
    /// marker cannot be pinned now. A pin is fixed for the life of a process.
    /// So the process must restart, and the startup pin then covers the
    /// shard.
    ///
    /// # Errors
    ///
    /// [`crate::error::HarvestError::Config`] when a held shard now carries a
    /// DR marker. The caller stops the process.
    pub async fn resolve_held(
        held: &mut Vec<(ShardId, crate::worker::DbPool)>,
        slot_prefix: &str,
    ) -> HarvestResult<()> {
        let mut index = 0;
        while index < held.len() {
            let (shard, pool) = &held[index];
            let probed = async {
                let mut conn = crate::pool::acquire_within_pool_bound(pool).await?;
                probe_dr_markers(&mut conn, slot_prefix).await
            }
            .await;
            match probed {
                Err(_) => index += 1,
                // A fencing process must pin a returning shard. A pin is
                // taken only at startup, so the process restarts.
                Ok(markers) if markers.is_dr() || FenceRegistry::has_real_pin() => {
                    return Err(crate::error::HarvestError::Config(format!(
                        "shard {} was unreachable at startup and must now be pinned. A pin \
                         is fixed for the life of a process, so this process stops. Restart \
                         it to pin the shard.",
                        shard.as_i32()
                    )));
                }
                Ok(_) => {
                    FenceRegistry::release_held(*shard);
                    tracing::info!(
                        shard_id = shard.as_i32(),
                        "held shard has no DR marker; it runs unfenced"
                    );
                    held.swap_remove(index);
                }
            }
        }
        Ok(())
    }

    /// The shared startup path. `worker` is the worker's shard assignment,
    /// or `None` for a process pin. A worker tolerates a shard it cannot
    /// probe. See [`pin_worker_fence`].
    #[allow(clippy::type_complexity, clippy::too_many_lines)]
    async fn pin_fence(
        mode: super::DrFencing,
        slot_prefix: &str,
        targets: Option<(Vec<(ShardId, crate::worker::DbPool)>, ShardId)>,
        fallback_pool: &crate::worker::DbPool,
        worker: Option<&[ShardId]>,
    ) -> HarvestResult<(
        Option<Vec<(ShardId, crate::worker::DbPool)>>,
        Vec<(ShardId, crate::worker::DbPool)>,
    )> {
        let probe_targets: Vec<(ShardId, &crate::worker::DbPool)> = targets.as_ref().map_or_else(
            || vec![(ShardId::UNENCODED, fallback_pool)],
            |(targets, _)| targets.iter().map(|(shard, pool)| (*shard, pool)).collect(),
        );
        // `None` marks a shard this worker could not probe. Only a worker
        // tolerates that; every other caller refuses to start.
        let mut probed: Vec<Option<DrMarkers>> = Vec::with_capacity(probe_targets.len());
        let mut held: Vec<(ShardId, crate::worker::DbPool)> = Vec::new();
        // Shards this process pinned before, keyed by their position. A
        // worker reuses such a pin when its probe fails, so a brief outage
        // does not hold a shard the process already fences.
        let mut reused: Vec<(usize, ShardId, ShardGeneration)> = Vec::new();
        for (index, (shard, pool)) in probe_targets.iter().enumerate() {
            if worker.is_some() {
                let once = async {
                    let mut conn = crate::pool::acquire_within_pool_bound(pool).await?;
                    probe_dr_markers(&mut conn, slot_prefix).await
                }
                .await;
                let pinned = FenceRegistry::binding(*shard).filter(|(_, pin)| *pin != super::HELD);
                match (once, pinned) {
                    (Ok(markers), _) => probed.push(Some(markers)),
                    (Err(error), Some((resolved, generation))) => {
                        tracing::warn!(
                            shard_id = resolved.as_i32(),
                            generation = generation.as_i64(),
                            error = %error,
                            "DR marker probe failed; the worker reuses this process's pin"
                        );
                        probed.push(None);
                        reused.push((index, resolved, generation));
                    }
                    (Err(error), None) => {
                        tracing::warn!(
                            shard_id = shard.as_i32(),
                            error = %error,
                            "DR marker probe failed; the worker holds this shard until it can \
                             probe it"
                        );
                        probed.push(None);
                        held.push((*shard, (*pool).clone()));
                    }
                }
            } else {
                probed.push(Some(probe_pool(pool, slot_prefix).await?));
            }
        }
        let dr_configured = !reused.is_empty() || probed.iter().flatten().any(DrMarkers::is_dr);
        let fence = mode
            .resolve(dr_configured)
            .map_err(crate::error::HarvestError::Config)?;
        if !fence {
            if !held.is_empty() {
                let default_shard = targets
                    .as_ref()
                    .map_or(ShardId::UNENCODED, |(_, default)| *default);
                let shards: Vec<ShardId> = held.iter().map(|(shard, _)| *shard).collect();
                FenceRegistry::hold(&shards, default_shard)
                    .map_err(|conflict| crate::error::HarvestError::Config(conflict.to_string()))?;
            }
            return Ok((None, held));
        }
        // A shard outside this worker's assignment does not block it. The
        // worker holds that shard, so a cross-shard write there fails closed.
        // An assigned shard, or a worker with no explicit assignment, refuses.
        let assigned_held = held
            .iter()
            .filter(|(shard, _)| match (worker, targets.as_ref()) {
                (Some(assigned), Some(_)) if !assigned.is_empty() => assigned.contains(shard),
                _ => true,
            })
            .count();
        if assigned_held > 0 {
            return Err(crate::error::HarvestError::Config(format!(
                "DR fencing is on, but {assigned_held} assigned shard(s) could not be \
                 probed, so they cannot be pinned. Refusing to start rather than run them \
                 unfenced."
            )));
        }
        if probed.iter().flatten().any(DrMarkers::is_standby) {
            return Err(crate::error::HarvestError::Config(
                "this database is a DR standby: it has a DR subscription or is in recovery. \
                 No Harvest process may write to a standby. Promote it first (runbook step 2), \
                 or point this process at the primary."
                    .to_string(),
            ));
        }

        let (targets, default_shard) = match targets {
            Some(targets) => targets,
            // The fallback pool was not probed, but the process pin names
            // its shard.
            None if !reused.is_empty() => {
                let shard = reused[0].1;
                (vec![(shard, fallback_pool.clone())], shard)
            }
            None => match probed[0]
                .as_ref()
                .map_or(&[][..], |markers| markers.generation_shards.as_slice())
            {
                [shard] => (vec![(*shard, fallback_pool.clone())], *shard),
                rows => {
                    return Err(crate::error::HarvestError::Config(format!(
                        "DR fencing is on, but this process has no shard identity and the \
                         database names {} shards in harvest_shard_generation. Set \
                         WorkerConfig::with_shard_assignments([shard]) or use a sharded pool. \
                         Refusing to start rather than pin a guessed shard.",
                        rows.len()
                    )));
                }
            },
        };

        // A shard database holds its own row only. A row for another shard
        // means this process names the database wrongly. Pinning would add a
        // second row, and `harvest dr fence` on the real shard would then
        // fence nothing this process checks.
        //
        // Logical shards that share one pool share one database. A row for
        // any of them is then expected there.
        //
        // A process on one database may share it with processes for other
        // logical shards. Their rows are expected there, so it only warns.
        let single_database = targets
            .iter()
            .all(|(_, pool)| std::ptr::eq(pool.manager(), fallback_pool.manager()));
        for ((shard, pool), markers) in targets.iter().zip(&probed) {
            let Some(markers) = markers else { continue };
            if single_database {
                if !markers.generation_shards.is_empty()
                    && !markers.generation_shards.contains(shard)
                {
                    tracing::warn!(
                        shard_id = shard.as_i32(),
                        rows = ?markers
                            .generation_shards
                            .iter()
                            .map(|s| s.as_i32())
                            .collect::<Vec<_>>(),
                        "this database holds the rows of other logical shards; this process \
                         provisions its own row beside them"
                    );
                }
                continue;
            }
            let colocated = |row: &ShardId| {
                targets.iter().any(|(peer, peer_pool)| {
                    peer == row && std::ptr::eq(peer_pool.manager(), pool.manager())
                })
            };
            if !markers.generation_shards.is_empty()
                && !markers.generation_shards.contains(shard)
                && !markers.generation_shards.iter().any(colocated)
            {
                return Err(crate::error::HarvestError::Config(format!(
                    "this process serves shard {} on a database whose harvest_shard_generation \
                     names shard(s) {:?}. Configure the shard number the operator fences with \
                     `harvest dr fence`. Refusing to start.",
                    shard.as_i32(),
                    markers
                        .generation_shards
                        .iter()
                        .map(|s| s.as_i32())
                        .collect::<Vec<_>>()
                )));
            }
        }

        let held_shards: Vec<ShardId> = held.iter().map(|(shard, _)| *shard).collect();
        let mut pins = Vec::with_capacity(targets.len());
        for (index, (shard, pool)) in targets.iter().enumerate() {
            if held_shards.contains(shard) {
                continue;
            }
            if let Some((_, _, generation)) = reused.iter().find(|(at, ..)| *at == index) {
                pins.push((*shard, *generation));
                continue;
            }
            let mut conn = crate::pool::acquire_within_pool_bound(pool).await?;
            pins.push((*shard, ensure_generation_row(&mut conn, *shard).await?));
        }
        // Held first, so the default shard is never briefly unpinned.
        if !held_shards.is_empty() {
            FenceRegistry::hold(&held_shards, default_shard)
                .map_err(|conflict| crate::error::HarvestError::Config(conflict.to_string()))?;
        }
        FenceRegistry::publish(&pins, default_shard)
            .map_err(|conflict| crate::error::HarvestError::Config(conflict.to_string()))?;
        for (shard, generation) in &pins {
            tracing::info!(
                shard_id = shard.as_i32(),
                generation = generation.as_i64(),
                "pinned shard write-authority generation for cross-region DR fencing"
            );
        }
        let fenced = targets
            .into_iter()
            .filter(|(shard, _)| !held_shards.contains(shard))
            .collect();
        Ok((Some(fenced), held))
    }

    // ── Replication lag ────────────────────────────────────────────────────

    #[derive(diesel::QueryableByName)]
    struct StandbyRow {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = Nullable<Double>)]
        replay_lag_seconds: Option<f64>,
        #[diesel(sql_type = Nullable<BigInt>)]
        lag_bytes: Option<i64>,
    }

    #[derive(diesel::QueryableByName)]
    struct SlotRow {
        #[diesel(sql_type = Text)]
        slot_name: String,
        #[diesel(sql_type = Bool)]
        active: bool,
        #[diesel(sql_type = Nullable<BigInt>)]
        lag_bytes: Option<i64>,
    }

    // ── Scoping a cluster-wide view to one shard ───────────────────────
    //
    // `pg_replication_slots` and `pg_stat_replication` are CLUSTER-wide, but a
    // Harvest shard is a *database*. Every query below therefore carries
    // `(s.database IS NULL OR s.database = current_database())`. Without it a
    // cluster hosting two shards reports each shard's lag as the worst of both,
    // and a cluster hosting anything else at all — another application's slot,
    // a leftover slot from a decommissioned standby — pegs every shard's RPO to
    // a stranger.
    //
    // Logical slots carry their database. Physical slots have `database IS
    // NULL` because physical replication ships the whole cluster, so they
    // genuinely do apply to every shard in it and are deliberately kept.

    /// `pg_stat_replication`, reduced to what an RPO reading needs.
    ///
    /// Two ways for a walsender to be recognised as **this shard's DR sender**,
    /// because the two supported topologies identify themselves differently.
    ///
    /// * It holds a slot for this database whose name carries the DR prefix.
    ///   `pg_stat_replication` has no database column of its own, so the join
    ///   is also what stops a sibling shard's walsender being counted here.
    /// * It holds **no slot at all** and its `application_name` carries the
    ///   prefix. A physical standby configured without `primary_slot_name`
    ///   (WAL archiving, `wal_keep_size`) is a supported topology and appears
    ///   in `pg_stat_replication` with no slot row; an inner join dropped it,
    ///   `connected_standbys()` returned `0` — the value documented as
    ///   "replication is down" — and the starter alert paged permanently on
    ///   healthy replication.
    ///
    /// The prefix is what makes this a *DR* count rather than a walsender
    /// count. Without it an unrelated logical-decoding consumer — a CDC
    /// pipeline, say — reads as a connected standby, and a shard whose real
    /// cross-region subscriber had disconnected would report itself protected
    /// while `harvest_replication_down` stayed silent.
    ///
    /// `replay_lag` is left `NULL` rather than coerced: Postgres reports NULL
    /// until a feedback round-trip has completed, and "we have not measured
    /// this standby yet" must not read as "this standby is caught up".
    ///
    /// `starts_with($1)`, not `LIKE $1 || '%'` (finding 7). `LIKE` treats `_`
    /// and `%` as wildcards, and the shipped default prefix `harvest_dr`
    /// contains an underscore. So on the default configuration `LIKE` also
    /// matched an unrelated slot or `application_name` such as
    /// `harvestXdr_shard0`. `starts_with` is a literal prefix comparison.
    const STANDBY_SQL: &str = "SELECT \
            r.state::text AS state, \
            EXTRACT(EPOCH FROM r.replay_lag)::double precision AS replay_lag_seconds, \
            CASE WHEN r.replay_lsn IS NULL THEN NULL \
                 ELSE (pg_current_wal_lsn() - r.replay_lsn)::bigint END AS lag_bytes \
         FROM pg_stat_replication r \
         LEFT JOIN pg_replication_slots s ON s.active_pid = r.pid \
         WHERE ( \
                 s.slot_name IS NOT NULL \
                 AND (s.database IS NULL OR s.database = current_database()) \
                 AND starts_with(s.slot_name, $1) \
               ) \
            OR (s.slot_name IS NULL AND starts_with(r.application_name, $1))";

    /// `pg_replication_slots`, which outlives the walsender.
    ///
    /// Physical slots track `restart_lsn`; logical slots track
    /// `confirmed_flush_lsn`. `COALESCE` picks whichever the slot has, so one
    /// query covers both replication styles the topology doc offers.
    ///
    /// `starts_with($1)`, not `LIKE $1 || '%'`. See [`STANDBY_SQL`] (finding 7).
    const SLOT_SQL: &str = "SELECT \
            s.slot_name::text AS slot_name, \
            s.active, \
            CASE WHEN COALESCE(s.confirmed_flush_lsn, s.restart_lsn) IS NULL THEN NULL \
                 ELSE (pg_current_wal_lsn() \
                       - COALESCE(s.confirmed_flush_lsn, s.restart_lsn))::bigint END AS lag_bytes \
         FROM pg_replication_slots s \
         WHERE (s.database IS NULL OR s.database = current_database()) \
           AND starts_with(s.slot_name, $1)";

    /// Write one replication watermark for `shard` and prune the trail.
    ///
    /// `(NOW(), pg_current_wal_lsn())` — a wall-clock instant stamped against
    /// the WAL position current at that instant. [`measure_rpo`] later reads
    /// the trail backwards from a standby's confirmed position to turn "how far
    /// behind in bytes" into "how far behind in seconds".
    ///
    /// The write is also load-bearing on an **idle** primary: with no other
    /// traffic, WAL does not advance, the standby has nothing to confirm, and
    /// any position-based lag reading would drift upward on a perfectly healthy
    /// system. A beat keeps the position moving so an idle deployment reports a
    /// live RPO.
    ///
    /// `ON CONFLICT DO NOTHING` because the primary key is
    /// `(shard_id, beat_lsn)`: two beats within a single WAL position are the
    /// same observation, not a conflict worth failing on.
    ///
    /// `interval` is the sampler cadence, and it is what makes the beat rate
    /// **per shard** rather than per worker: a beat is written only when the
    /// newest one is older than half an interval. The advisory lock alone would
    /// only stop simultaneous writers, so staggered workers would each still
    /// beat once per interval and the trail would scale with fleet size.
    ///
    /// The prune keeps `retain` of trailing history. That window is the ceiling
    /// on the lag this can *measure*: a standby further behind than the oldest
    /// retained watermark reports `None` (unknown) rather than a floor value
    /// that would understate the loss.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn record_replication_heartbeat(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        retain: std::time::Duration,
        interval: std::time::Duration,
    ) -> HarvestResult<()> {
        let retain_secs = i64::try_from(retain.as_secs()).unwrap_or(i64::MAX);
        // Half an interval of slack (see the INSERT's predicate).
        let min_gap_secs = interval.as_secs_f64() / 2.0;
        let shard_id = shard.as_i32();

        Box::pin(
            conn.transaction::<(), crate::error::HarvestError, _>(async move |conn| {
                // One writer per shard per tick, whatever the fleet size. Every
                // worker runs this sampler (each also needs its own self-fence
                // check), so without the gate a 200-worker fleet writes 200
                // watermarks and 200 prunes per tick per shard — and the trail
                // then holds `N x retention` rows rather than the `retention`
                // the migration comment assumes, which is also what
                // `measure_rpo` has to scan.
                //
                // **`_xact_` is load-bearing.** A session-scoped
                // `pg_try_advisory_lock` is released only by an explicit
                // unlock, so any `?` between acquire and release leaks it — and
                // on a pooled connection it then leaks *permanently*: every
                // other sampler skips its beat forever, re-acquiring on the
                // same session only bumps the lock count, and the measured RPO
                // goes stale during exactly the database trouble it exists to
                // measure. A transaction-scoped lock is released by the commit
                // *and* by the rollback, so there is no error path to get wrong.
                let locked: Vec<LockRow> =
                    diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS locked")
                        .bind::<BigInt, _>(heartbeat_lock_key(shard_id))
                        .load(conn)
                        .await
                        .map_err(database_error)?;
                if !locked.into_iter().next().is_some_and(|r| r.locked) {
                    // Another worker holds the beat this tick. Its own fence
                    // check and gauges still ran; only the write is skipped.
                    return Ok(());
                }

                // The lock alone only prevents SIMULTANEOUS writers. Workers
                // sampling on staggered schedules each take it uncontended a
                // moment apart, so without this predicate every worker still
                // writes and prunes once per interval and the trail scales with
                // fleet size — the exact cost the lock was added to remove.
                //
                // `WHERE NOT EXISTS (a beat newer than one interval)` makes the
                // cadence per SHARD rather than per worker: the first sampler
                // to arrive in a window writes, the rest no-op. Enforced in the
                // statement rather than in Rust because the fleet has no shared
                // clock, and Postgres' `NOW()` is the one all of them agree on.
                //
                // Half an interval of slack, so ordinary scheduling jitter does
                // not skip a window outright and halve the effective cadence.
                // `fence_generation` stamps the write-authority epoch in
                // force when this beat was written (finding 10).
                // `docs/cross-region-dr.md`'s setup SQL replicates this table
                // with `FOR ALL TABLES`. So after a promotion the standby can
                // carry pre-promotion beats whose LSNs belong to the OLD
                // cluster's WAL stream. Those numbers are not comparable to
                // the new primary's. `measure_rpo` filters on this column, so
                // a beat from a superseded generation is never read back as
                // if it were in the current WAL stream. Defaults to `0` (no
                // fencing row yet) via `COALESCE`, matching the pre-#954
                // behaviour on a shard where fencing was never enabled.
                diesel::sql_query(
                    "INSERT INTO harvest_replication_heartbeat \
                         (shard_id, beat_lsn, beat_at, fence_generation) \
                     SELECT $1, pg_current_wal_lsn(), NOW(), \
                            COALESCE( \
                                (SELECT generation FROM harvest_shard_generation \
                                 WHERE shard_id = $1), \
                                0) \
                     WHERE NOT EXISTS ( \
                         SELECT 1 FROM harvest_replication_heartbeat h \
                         WHERE h.shard_id = $1 \
                           AND h.beat_at > NOW() \
                               - make_interval(secs => $3::double precision) \
                     ) \
                     ON CONFLICT (shard_id, beat_lsn) DO NOTHING",
                )
                .bind::<Integer, _>(shard_id)
                .bind::<BigInt, _>(retain_secs)
                .bind::<Double, _>(min_gap_secs)
                .execute(conn)
                .await
                .map_err(database_error)?;

                diesel::sql_query(
                    "DELETE FROM harvest_replication_heartbeat \
                     WHERE shard_id = $1 \
                       AND beat_at < NOW() - make_interval(secs => $2::double precision)",
                )
                .bind::<Integer, _>(shard_id)
                .bind::<BigInt, _>(retain_secs)
                .execute(conn)
                .await
                .map_err(database_error)?;

                Ok(())
            }),
        )
        .await
    }

    /// The single-argument advisory key for a shard's watermark writer.
    ///
    /// **Single-argument by requirement, not preference.** `queue_pause` owns
    /// the two-argument `(classid, objid)` keyspace outright — its keys spend
    /// all 64 bits on a SHA-256 of the queue name and reserve no class id, so a
    /// second user there could silently stall queue dispatch. That ownership is
    /// enforced by `queue_pause::tests::queue_pause_owns_the_two_argument_advisory_keyspace`,
    /// which walks every other source file in the crate.
    ///
    /// The single-argument space is shared with the concurrency-key locks, but
    /// those are `hashtext(key)::bigint` and `hashtext` returns `int4` — so
    /// they occupy only `i32::MIN..=i32::MAX`. Shifting the issue number into
    /// the high word puts these keys far outside that range, so a collision is
    /// impossible by construction rather than by luck. The shard id occupies
    /// the low word, so two shards never contend with each other either.
    #[allow(clippy::cast_sign_loss)]
    pub(super) const fn heartbeat_lock_key(shard_id: i32) -> i64 {
        (954_i64 << 32) | (shard_id as u32 as i64)
    }

    #[derive(diesel::QueryableByName)]
    struct LockRow {
        #[diesel(sql_type = Bool)]
        locked: bool,
    }

    #[derive(diesel::QueryableByName)]
    struct StandbyPositionRow {
        #[diesel(sql_type = Nullable<Text>)]
        position: Option<String>,
        #[diesel(sql_type = BigInt)]
        unmeasurable_slots: i64,
        #[diesel(sql_type = BigInt)]
        total_slots: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct RpoRow {
        #[diesel(sql_type = Nullable<Double>)]
        lag_seconds: Option<f64>,
        #[diesel(sql_type = Nullable<Double>)]
        oldest_seconds: Option<f64>,
    }

    /// Measure the RPO for `shard` in seconds from the watermark trail.
    ///
    /// Two steps, deliberately not one query.
    ///
    /// **Step 1 — the standbys' positions.** `MIN(COALESCE(confirmed_flush_lsn,
    /// replay_lsn, restart_lsn))` over every replication slot scoped to this
    /// shard's database — the worst standby sets the RPO. `COALESCE` covers
    /// logical (`confirmed_flush_lsn`) and physical (`restart_lsn`) slots
    /// with one query. The `MIN` is taken only over slots that DO have a
    /// position. Slots that do not are counted separately as
    /// `unmeasurable_slots` rather than dropped silently. Folding "one slot
    /// unmeasurable" into "nothing consumed anywhere" would let an abandoned
    /// slot hide behind a healthy peer's small lag. That is precisely the
    /// "report a perfect RPO for replication that is dead" outcome this
    /// module exists to avoid (finding 1). An abandoned slot is retaining
    /// WAL and is exactly what an operator must be told about; see
    /// [`ReplicationStatus::inactive_slots`] and
    /// [`ReplicationStatus::unmeasurable_slot_count`].
    ///
    /// **Step 2 — the watermark.** Only issued when step 1 produced a
    /// position for at least one slot. Doing it in a separate statement
    /// matters: with no slot the predicate was never true. The index scan
    /// then walked the *entire* retained trail to return nothing (measured:
    /// 200k rows, 2646 buffers, 22 ms). That happened on every sampler tick
    /// of every worker of a deployment that has not finished wiring up
    /// replication.
    ///
    /// Returns [`WatermarkReading::Unknown`] when there is no slot at all,
    /// or when every slot lacks a position. Nothing has consumed anything
    /// yet in that case, so `replay_lag` is still a fair fallback there.
    /// Returns [`WatermarkReading::PartiallyMeasured`] when some slots have
    /// a position and others do not — never fallback-eligible. Otherwise
    /// returns [`WatermarkReading::Measured`] or
    /// [`WatermarkReading::BeyondTrail`] from the confirmed slots'
    /// watermark.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn measure_rpo(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        slot_prefix: &str,
    ) -> HarvestResult<WatermarkReading> {
        // Position precedence, and each rung is load-bearing:
        //
        //   1. `s.confirmed_flush_lsn` — LOGICAL slots. The position the
        //      subscriber has durably confirmed, which is the conservative and
        //      correct answer for an RPO.
        //   2. `r.replay_lsn` — PHYSICAL standbys, via the walsender. Physical
        //      slots leave `confirmed_flush_lsn` NULL.
        //   3. `s.restart_lsn` — a physical slot with no walsender attached.
        //
        // `restart_lsn` is deliberately LAST and never preferred: it is the
        // oldest WAL the slot still needs *retained*, not the standby's replay
        // position, and it can sit far behind a fully caught-up standby. Using
        // it as the progress signal reported an inflated RPO for the physical
        // topology this feature claims to support. It remains the right input
        // for the byte backlog (`SLOT_SQL`), which is a retention and
        // disk-pressure signal — that is exactly what `restart_lsn` measures.
        //
        // `starts_with($1)`, not `LIKE $1 || '%'` (finding 7). `LIKE` treats
        // `_` and `%` as wildcards, and the shipped default prefix
        // `harvest_dr` contains an underscore. So on the default
        // configuration `LIKE` matched an unrelated slot such as
        // `harvestXdr_shard0`. `starts_with` is a literal prefix comparison.
        let positions: Vec<StandbyPositionRow> = diesel::sql_query(
            "SELECT (MIN(pos) FILTER (WHERE pos IS NOT NULL))::text AS position, \
                    COUNT(*) FILTER (WHERE pos IS NULL) AS unmeasurable_slots, \
                    COUNT(*) AS total_slots \
             FROM ( \
                 SELECT COALESCE(s.confirmed_flush_lsn, r.replay_lsn, s.restart_lsn) AS pos \
                 FROM pg_replication_slots s \
                 LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid \
                 WHERE (s.database IS NULL OR s.database = current_database()) \
                   AND starts_with(s.slot_name, $1) \
             ) matched",
        )
        .bind::<Text, _>(slot_prefix)
        .load(conn)
        .await
        .map_err(database_error)?;

        let Some(row) = positions.into_iter().next() else {
            return Ok(WatermarkReading::Unknown);
        };
        if row.total_slots == 0 {
            // No DR slot for this shard at all.
            return Ok(WatermarkReading::Unknown);
        }
        let unmeasurable_slots = usize::try_from(row.unmeasurable_slots).unwrap_or(usize::MAX);
        let Some(position) = row.position else {
            // Every matching slot lacks a position: nothing has consumed
            // anything yet, which is not the same as an abandoned slot beside
            // a healthy one. `replay_lag` is still a fair fallback here.
            return Ok(WatermarkReading::Unknown);
        };

        let heartbeat = heartbeat_reading(conn, shard, position).await?;
        if unmeasurable_slots == 0 {
            return Ok(heartbeat);
        }
        // At least one matching slot has no position: flag as partial and
        // never let this fall back to `replay_lag` (finding 1).
        Ok(WatermarkReading::PartiallyMeasured {
            measured_seconds: heartbeat.measured_or_floor_seconds(),
            unmeasurable_slots,
        })
    }

    /// Step 2 of [`measure_rpo`]: translate a confirmed standby position into
    /// a watermark reading.
    ///
    /// Split out so [`measure_rpo`] can share it between the ordinary path
    /// (every slot measurable) and the partial path (finding 1). The caller
    /// decides whether the result may stand on its own, or must be wrapped
    /// as [`WatermarkReading::PartiallyMeasured`].
    async fn heartbeat_reading(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        position: String,
    ) -> HarvestResult<WatermarkReading> {
        // One query, two answers, so the two cannot disagree across a round
        // trip: the age of the newest CONSUMED watermark (the reading), and the
        // age of the OLDEST retained one (the floor, used only when nothing has
        // been consumed but the trail is non-empty — i.e. the standby has
        // fallen off the end of it).
        //
        // Both are single index probes on `(shard_id, beat_lsn)` /
        // `(shard_id, beat_at DESC)`; neither aggregates the trail.
        //
        // Both branches of `current_gen` are also filtered to the shard's
        // CURRENT fence generation (finding 10). The setup SQL in
        // `docs/cross-region-dr.md` replicates this table with `FOR ALL
        // TABLES`, so a standby can carry beats from a superseded
        // generation. Those beats' LSNs belong to a different WAL stream —
        // comparable neither to each other nor to the new primary's
        // positions. Restricting to the current generation keeps every
        // comparison inside one WAL stream.
        //
        // `LEFT JOIN ... ON true`, not a comma join. A shard with no
        // `harvest_shard_generation` row makes `current_gen` empty. That
        // happens when fencing was never enabled for it, or when this trail
        // was written directly rather than through the fencing-gated
        // sampler. A comma join against an empty CTE returns no rows at
        // all, regardless of the `WHERE` clause. That silently made every
        // reading `Unknown`.
        // `COALESCE(current_gen.generation, 0)` matches this table's own
        // default: every beat is stamped `fence_generation = 0` when no
        // fencing row exists at write time. An absent row still compares
        // equal, so the trail reads exactly as it did before this column
        // existed.
        let rows: Vec<RpoRow> = diesel::sql_query(
            "WITH current_gen AS ( \
                 SELECT generation FROM harvest_shard_generation WHERE shard_id = $1 \
             ) \
             SELECT ( \
                 SELECT EXTRACT(EPOCH FROM (NOW() - h.beat_at))::double precision \
                 FROM harvest_replication_heartbeat h \
                 LEFT JOIN current_gen ON true \
                 WHERE h.shard_id = $1 AND h.beat_lsn <= $2::pg_lsn \
                   AND h.fence_generation = COALESCE(current_gen.generation, 0) \
                 ORDER BY h.beat_lsn DESC LIMIT 1 \
             ) AS lag_seconds, \
             ( \
                 SELECT EXTRACT(EPOCH FROM (NOW() - h.beat_at))::double precision \
                 FROM harvest_replication_heartbeat h \
                 LEFT JOIN current_gen ON true \
                 WHERE h.shard_id = $1 \
                   AND h.fence_generation = COALESCE(current_gen.generation, 0) \
                 ORDER BY h.beat_at ASC LIMIT 1 \
             ) AS oldest_seconds",
        )
        .bind::<Integer, _>(shard.as_i32())
        .bind::<Text, _>(position)
        .load(conn)
        .await
        .map_err(database_error)?;

        let Some(row) = rows.into_iter().next() else {
            return Ok(WatermarkReading::Unknown);
        };

        // A negative reading is impossible in principle (`NOW()` is monotonic
        // relative to a row already committed) but clamps to `0.0` rather than
        // being emitted as a nonsense negative RPO if the clock is adjusted.
        Ok(match (row.lag_seconds, row.oldest_seconds) {
            (Some(lag), _) => WatermarkReading::Measured(lag.max(0.0)),
            // Nothing consumed, but the trail is NOT empty: the standby is
            // behind every watermark we still hold. The RPO is at least the
            // oldest one's age — which is at least the retention window — and
            // unbounded above.
            (None, Some(oldest)) => WatermarkReading::BeyondTrail {
                floor_seconds: oldest.max(0.0),
            },
            // No trail at all: the sampler has not written a beat yet.
            (None, None) => WatermarkReading::Unknown,
        })
    }

    #[derive(diesel::QueryableByName)]
    struct SerialColumn {
        #[diesel(sql_type = Text)]
        table_schema: String,
        #[diesel(sql_type = Text)]
        table_name: String,
        #[diesel(sql_type = Text)]
        column_name: String,
        #[diesel(sql_type = Text)]
        sequence_schema: String,
        #[diesel(sql_type = Text)]
        sequence_name: String,
        /// `pg_sequences.increment_by`. Negative for a descending sequence
        /// (finding 11) — see [`advance_sequences_in_transaction`].
        #[diesel(sql_type = BigInt)]
        increment_by: i64,
        /// `pg_sequences.min_value`: the ascending floor, used in place of a
        /// hardcoded `1` (finding 11).
        #[diesel(sql_type = BigInt)]
        min_value: i64,
        /// `pg_sequences.max_value`: the descending ceiling.
        #[diesel(sql_type = BigInt)]
        max_value: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct SetvalRow {
        #[diesel(sql_type = BigInt)]
        value: i64,
    }

    /// Advance every sequence to match the data — the mandatory step after
    /// promoting a **logical** standby.
    ///
    /// Logical replication copies rows; it does **not** copy sequence values.
    /// A promoted logical standby therefore holds a full copy of
    /// `harvest_events` while `harvest_events_id_seq` still sits where it was
    /// when the subscription was created, so the new primary's very first
    /// append collides with an already-replicated primary key. The failure is
    /// immediate, total, and mystifying if you have not seen it before, which
    /// is why this ships as a function the runbook calls rather than a sentence
    /// in the runbook hoping to be read.
    ///
    /// Physical (streaming) replicas do not need this — they replicate the
    /// WAL itself, sequences included. Running it there is harmless. The
    /// target folds in `last_value` rather than using the table's extreme
    /// value alone. So a sequence already ahead of its table's data is
    /// left where it is, never rewound. See the statement below for why
    /// "ahead of the table" is an ordinary, expected state rather than
    /// corruption.
    ///
    /// # Scope
    ///
    /// **Every** sequence whose owning relation is an ordinary or partitioned
    /// table in the connection's `current_schema()`, not only Harvest's. A
    /// promoted primary with any stale sequence is broken, and an embedder's
    /// own tables — replicated by the same `FOR ALL TABLES` publication the
    /// topology doc prescribes — carry the identical hazard. A helper that
    /// fixed only `harvest_*` would leave the operator with a half-promoted
    /// database and no signal. Every sequence it touches is returned, so the
    /// scope is visible rather than assumed.
    ///
    /// # Why the relation-kind filter is a security control
    ///
    /// `relkind IN ('r','p')` is **not** tidiness. A sequence can be
    /// `ALTER SEQUENCE ... OWNED BY <view>.<column>`, and Postgres accepts it.
    /// Without the filter a view reaches the `FROM {tbl}` below and its query
    /// body — including any volatile function in it — **executes** on the
    /// operator's DR connection, which is the highest-privilege connection
    /// anyone opens all quarter, during an incident, on a command whose output
    /// nobody is reading closely. Anyone with `CREATE` in the schema can plant
    /// that view months in advance; the `FOR ALL TABLES` publication even
    /// replicates it to the standby. Identifier quoting does not help, because
    /// the attacker's names are already ordinary identifiers.
    ///
    /// Names are quoted with [`quote_ident`] and schema-qualified with
    /// [`qualified`] — see those for why screening and unqualified names were
    /// both wrong.
    ///
    /// Returns each `(qualified sequence, new_value)` pair it set, so the
    /// runbook step has evidence to paste into the incident log.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn advance_sequences_after_promotion(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<Vec<(String, i64)>> {
        Box::pin(
            conn.transaction::<_, crate::error::HarvestError, _>(async move |conn| {
                advance_sequences_in_transaction(conn).await
            }),
        )
        .await
    }

    /// The body of [`advance_sequences_after_promotion`], inside a transaction.
    ///
    /// The transaction exists for `SET LOCAL`, which Postgres **ignores** —
    /// with only a `WARNING` — outside a transaction block. Verified: issued
    /// standalone, `statement_timeout` reads back as `0`, so the ceiling this
    /// function documents was silently absent and a `MAX()` scan on a large or
    /// blocked table could hang the promotion past the RTO budget with no
    /// signal.
    ///
    /// It does **not** make the promotion atomic, and must not be described as
    /// if it did: `setval` is explicitly non-transactional in Postgres, so a
    /// rollback does not rewind the sequences already advanced. The returned
    /// list remains the record of what actually changed.
    async fn advance_sequences_in_transaction(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<Vec<(String, i64)>> {
        // Promotion sits on the RTO critical path and `MAX(col)` on a serial
        // column with no index is a full sequential scan of a cold table on a
        // freshly promoted standby. Bound it: a timeout names the table it gave
        // up on, which an operator can act on; an unbounded hang inside a
        // 15-minute RTO budget cannot be distinguished from a wedge.
        diesel::sql_query(format!(
            "SET LOCAL statement_timeout = '{PROMOTE_STATEMENT_TIMEOUT_MS}ms'"
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

        // `tn.nspname` names the OWNING TABLE's schema, not `sn.nspname`,
        // the sequence's own schema (finding 5). Postgres keeps an owned
        // sequence in the same schema as its table. The two schemas never
        // diverge in practice. This filter still names the table, since
        // promotion advances sequences for tables in the current schema.
        // The choice keeps intent clear, even though it behaves the same
        // as filtering on the sequence today.
        let columns: Vec<SerialColumn> = diesel::sql_query(
            "SELECT tn.nspname::text AS table_schema, \
                    c.relname::text  AS table_name, \
                    a.attname::text  AS column_name, \
                    sn.nspname::text AS sequence_schema, \
                    s.relname::text  AS sequence_name, \
                    sq.increment_by  AS increment_by, \
                    sq.min_value     AS min_value, \
                    sq.max_value     AS max_value \
             FROM pg_class s \
             JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass \
             JOIN pg_class c ON c.oid = d.refobjid \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid \
             JOIN pg_namespace sn ON sn.oid = s.relnamespace \
             JOIN pg_namespace tn ON tn.oid = c.relnamespace \
             JOIN pg_sequences sq \
                  ON sq.schemaname = sn.nspname AND sq.sequencename = s.relname \
             WHERE s.relkind = 'S' \
               AND d.refclassid = 'pg_class'::regclass \
               AND d.deptype IN ('a', 'i') \
               AND c.relkind IN ('r', 'p') \
               AND tn.nspname = current_schema()",
        )
        .load(conn)
        .await
        .map_err(database_error)?;

        let mut advanced = Vec::with_capacity(columns.len());
        for col in columns {
            // `setval`'s first argument is `regclass`, i.e. a *name* rather
            // than a relation reference, so the schema-qualified identifier is
            // passed as a single-quoted literal with any `'` doubled.
            let seq = qualified(&col.sequence_schema, &col.sequence_name);
            let seq_literal = seq.replace('\'', "''");
            let tbl = qualified(&col.table_schema, &col.table_name);
            let ident_col = quote_ident(&col.column_name);

            // `is_called = true` so the NEXT value handed out is one step past
            // whichever bound wins.
            //
            // `pg_sequence_last_value` is in the reduction for a reason. A
            // sequence can legitimately sit AHEAD of the table's extreme
            // value — cached values, a rolled-back transaction, deleted
            // rows. A physical replica replicates sequences already, so on
            // that topology this command is meant to be a no-op. Skipping
            // it would let `setval` *rewind* the sequence and re-issue ids
            // the database already handed out. That is a duplicate-key
            // outage caused by the very command that exists to prevent one.
            // Measured on live Postgres: insert two rows, delete the
            // second, and MAX is 1 while last_value is 2.
            //
            // Branches on `increment_by` (finding 11). An ASCENDING
            // sequence's "furthest issued" value is its MAXIMUM. So the
            // reduction is `GREATEST`, bounded below by `min_value` — never
            // a hardcoded `1`, which can sit outside a custom-bounded
            // sequence's own range. A DESCENDING sequence issues in
            // decreasing order, so "furthest issued" is its MINIMUM. The
            // reduction there is `LEAST` bounded above by `max_value`. Using
            // `GREATEST` unconditionally reset a descending sequence that
            // had issued 100 then 99 back to 100. It re-issued 99 next — a
            // collision from the helper whose purpose is preventing one.
            let sql = if col.increment_by > 0 {
                format!(
                    "SELECT setval('{seq_literal}', \
                            GREATEST( \
                                COALESCE((SELECT MAX({ident_col}) FROM {tbl}), {min_value}), \
                                COALESCE(pg_sequence_last_value('{seq_literal}'), {min_value})), \
                            true)::bigint AS value",
                    min_value = col.min_value,
                )
            } else {
                format!(
                    "SELECT setval('{seq_literal}', \
                            LEAST( \
                                COALESCE((SELECT MIN({ident_col}) FROM {tbl}), {max_value}), \
                                COALESCE(pg_sequence_last_value('{seq_literal}'), {max_value})), \
                            true)::bigint AS value",
                    max_value = col.max_value,
                )
            };
            let rows: Vec<SetvalRow> = diesel::sql_query(sql)
                .load(conn)
                .await
                .map_err(database_error)?;
            if let Some(row) = rows.into_iter().next() {
                advanced.push((seq, row.value));
            }
        }
        advanced.sort();
        Ok(advanced)
    }

    /// Read this primary's replication position views.
    ///
    /// A permission or availability failure is reported as
    /// [`ReplicationStatus::Unavailable`], not as an `Err`: reading these views
    /// needs `pg_monitor`, and a deployment that has not run that `GRANT` must
    /// lose the RPO *signal*, not have its metrics sampler fail. The runbook
    /// names the grant.
    ///
    /// # Errors
    ///
    /// Never returns `Err` for a view-read failure — see above. The signature
    /// stays fallible for future non-degradable failures.
    pub async fn query_replication_status(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        slot_prefix: &str,
    ) -> HarvestResult<ReplicationStatus> {
        let standbys: Vec<StandbyRow> = match diesel::sql_query(STANDBY_SQL)
            .bind::<Text, _>(slot_prefix)
            .load(conn)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                return Ok(ReplicationStatus::Unavailable {
                    reason: format!("pg_stat_replication unreadable: {error}"),
                });
            }
        };
        let slots: Vec<SlotRow> = match diesel::sql_query(SLOT_SQL)
            .bind::<Text, _>(slot_prefix)
            .load(conn)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                return Ok(ReplicationStatus::Unavailable {
                    reason: format!("pg_replication_slots unreadable: {error}"),
                });
            }
        };

        // A watermark-read failure degrades the same way a view-read failure
        // does: lose the number, never the sampler. `Failed`, not `Unknown`
        // (finding 12) — `Unknown` is fallback-eligible, and a failed read
        // is precisely the moment `replay_lag` is least trustworthy. It is
        // frozen or NULL whenever a logical apply worker is stuck, which is
        // the incident this trail exists to measure.
        let heartbeat = measure_rpo(conn, shard, slot_prefix)
            .await
            .unwrap_or(WatermarkReading::Failed);

        Ok(ReplicationStatus::Observed {
            heartbeat,
            standbys: standbys
                .into_iter()
                .map(|r| StandbyLag {
                    state: r.state,
                    replay_lag_seconds: r.replay_lag_seconds,
                    lag_bytes: r.lag_bytes,
                })
                .collect(),
            slots: slots
                .into_iter()
                .map(|r| SlotLag {
                    slot_name: r.slot_name,
                    active: r.active,
                    lag_bytes: r.lag_bytes,
                })
                .collect(),
        })
    }
}

#[cfg(feature = "db")]
pub use db::{
    FencePassGuard, advance_sequences_after_promotion, assert_admin_write_authority, assert_fence,
    begin_fenced_pass, begin_fenced_pass_at, begin_fenced_pass_on, bump_generation,
    current_generation, ensure_generation_row, measure_rpo, pin_process_fence, pin_worker_fence,
    probe_dr_markers, query_replication_status, record_replication_heartbeat, resolve_held,
};

#[cfg(test)]
mod tests {
    use super::*;

    /// [`FenceRegistry`] is process-global, so the tests that mutate it must
    /// not interleave with each other under the default parallel harness.
    static REGISTRY_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn registry_guard() -> std::sync::MutexGuard<'static, ()> {
        REGISTRY_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn standby(state: &str, lag: Option<f64>, bytes: Option<i64>) -> StandbyLag {
        StandbyLag {
            state: state.to_string(),
            replay_lag_seconds: lag,
            lag_bytes: bytes,
        }
    }

    fn observed(standbys: Vec<StandbyLag>, slots: Vec<SlotLag>) -> ReplicationStatus {
        ReplicationStatus::Observed {
            standbys,
            slots,
            heartbeat: WatermarkReading::Unknown,
        }
    }

    // ── RPO source-of-truth precedence ─────────────────────────────────────

    /// The watermark reading wins over `pg_stat_replication.replay_lag`.
    ///
    /// Measured against a live pair of databases: with the subscriber's apply
    /// worker blocked, byte lag grew while `replay_lag` never left NULL,
    /// because a stuck logical apply worker stops sending the reply messages
    /// `replay_lag` is derived from. Preferring the watermark is what makes the
    /// RPO real in the only situation where anyone reads it.
    #[test]
    fn the_watermark_reading_wins_over_replay_lag() {
        let s = ReplicationStatus::Observed {
            standbys: vec![standby("streaming", Some(0.0), Some(1_000_000))],
            slots: vec![],
            heartbeat: WatermarkReading::Measured(41.5),
        };
        assert_eq!(s.rpo_seconds(), Some(41.5));
        assert!(!s.rpo_is_lower_bound());
    }

    /// A standby that has fallen off the end of the retained trail must NOT
    /// fall back to `replay_lag`.
    ///
    /// `replay_lag` freezes for a stuck logical apply worker, and a stuck apply
    /// worker is precisely how a trail gets exhausted — so the fallback would
    /// replace a known-enormous RPO with a small stale one, at the moment an
    /// operator is deciding whether to fail over. The floor is reported
    /// instead: it clears any threshold, so the shard still pages, and it is
    /// flagged as a lower bound rather than passed off as a measurement.
    #[test]
    fn an_exhausted_trail_reports_its_floor_not_a_stale_replay_lag() {
        let s = ReplicationStatus::Observed {
            // A frozen, reassuringly small replay_lag — the trap.
            standbys: vec![standby("streaming", Some(0.4), Some(9_000_000))],
            slots: vec![],
            heartbeat: WatermarkReading::BeyondTrail {
                floor_seconds: 3_600.0,
            },
        };
        assert_eq!(
            s.rpo_seconds(),
            Some(3_600.0),
            "the floor must win over a frozen replay_lag"
        );
        assert!(
            s.rpo_is_lower_bound(),
            "the caller must be able to tell a bound from a measurement"
        );
    }

    /// With no watermark, `replay_lag` is still better than nothing.
    #[test]
    fn replay_lag_is_the_rpo_fallback_when_no_watermark_exists() {
        let s = ReplicationStatus::Observed {
            standbys: vec![standby("streaming", Some(2.5), Some(10))],
            slots: vec![],
            heartbeat: WatermarkReading::Unknown,
        };
        assert_eq!(s.rpo_seconds(), Some(2.5));
        assert!(!s.rpo_is_lower_bound());
    }

    /// Neither source: unknown. Never zero.
    #[test]
    fn an_rpo_with_no_source_is_unknown_not_zero() {
        assert_eq!(observed(vec![], vec![]).rpo_seconds(), None);
        assert_eq!(
            ReplicationStatus::Unavailable {
                reason: "no grant".into()
            }
            .rpo_seconds(),
            None
        );
    }

    // ── R6: never report "0 seconds behind" when replication is dead ────────
    #[test]
    fn no_standbys_reports_unknown_lag_not_zero() {
        let s = observed(vec![], vec![]);
        assert_eq!(
            s.max_replay_lag_seconds(),
            None,
            "an empty standby set must be UNKNOWN, not 0"
        );
        assert_eq!(s.connected_standbys(), 0);
    }

    #[test]
    fn unavailable_reports_unknown_lag() {
        let s = ReplicationStatus::Unavailable {
            reason: "permission denied".into(),
        };
        assert_eq!(s.max_replay_lag_seconds(), None);
        assert_eq!(s.max_lag_bytes(), None);
        assert_eq!(s.connected_standbys(), 0);
    }

    #[test]
    fn lag_is_the_worst_standby_not_the_best() {
        let s = observed(
            vec![
                standby("streaming", Some(0.5), Some(100)),
                standby("streaming", Some(12.25), Some(9_000)),
            ],
            vec![],
        );
        assert_eq!(s.max_replay_lag_seconds(), Some(12.25));
        assert_eq!(s.max_lag_bytes(), Some(9_000));
        assert_eq!(s.connected_standbys(), 2);
    }

    /// A worst-case reduction must not let a healthy standby mask an
    /// unmeasurable one (finding 9). This is the same defect as finding 1,
    /// one layer down.
    #[test]
    fn an_unmeasurable_standby_is_not_masked_by_a_healthy_peer() {
        let s = observed(
            vec![
                standby("catchup", None, Some(1_000)),
                standby("streaming", Some(2.0), Some(10)),
            ],
            vec![],
        );
        assert_eq!(
            s.max_replay_lag_seconds(),
            None,
            "one standby with no replay_lag must make the whole reduction unknown, \
             not fall back to the healthy peer's small lag"
        );
    }

    /// A partial watermark reading must never fall back to `replay_lag`.
    /// That column is a trap here precisely: it can look small and healthy
    /// while the abandoned slot stays invisible in it (finding 1).
    #[test]
    fn a_partially_measured_reading_never_falls_back_to_replay_lag() {
        let s = ReplicationStatus::Observed {
            standbys: vec![standby("streaming", Some(0.3), Some(10))],
            slots: vec![],
            heartbeat: WatermarkReading::PartiallyMeasured {
                measured_seconds: Some(4.0),
                unmeasurable_slots: 1,
            },
        };
        assert_eq!(s.rpo_seconds(), Some(4.0));
        assert_eq!(s.unmeasurable_slot_count(), 1);
    }

    /// The measured side of a partial reading can itself be unconfirmed yet;
    /// that must report unknown, not the frozen `replay_lag`.
    #[test]
    fn a_partially_measured_reading_with_no_confirmed_watermark_is_unknown() {
        let s = ReplicationStatus::Observed {
            standbys: vec![standby("streaming", Some(0.3), Some(10))],
            slots: vec![],
            heartbeat: WatermarkReading::PartiallyMeasured {
                measured_seconds: None,
                unmeasurable_slots: 1,
            },
        };
        assert_eq!(s.rpo_seconds(), None);
    }

    /// A failed watermark read must not become a fallback-eligible `Unknown`
    /// (finding 12): the read failing is precisely when `replay_lag`
    /// is least trustworthy.
    #[test]
    fn a_failed_watermark_read_never_falls_back_to_replay_lag() {
        let s = ReplicationStatus::Observed {
            standbys: vec![standby("streaming", Some(0.4), Some(9_000_000))],
            slots: vec![],
            heartbeat: WatermarkReading::Failed,
        };
        assert_eq!(
            s.rpo_seconds(),
            None,
            "a failed watermark read must report unknown, never a frozen replay_lag"
        );
    }

    #[test]
    fn unmeasurable_slot_count_is_zero_outside_partial_readings() {
        assert_eq!(observed(vec![], vec![]).unmeasurable_slot_count(), 0);
        assert_eq!(
            ReplicationStatus::Unavailable {
                reason: "no grant".into()
            }
            .unmeasurable_slot_count(),
            0
        );
    }

    #[test]
    fn a_connected_standby_with_null_replay_lag_is_not_silently_zero() {
        // pg_stat_replication.replay_lag is NULL until the first feedback
        // round-trip. Treating NULL as 0.0 would report a perfect RPO for a
        // standby we know nothing about.
        let s = observed(vec![standby("catchup", None, Some(4_096))], vec![]);
        assert_eq!(s.max_replay_lag_seconds(), None);
        assert_eq!(s.max_lag_bytes(), Some(4_096));
        assert_eq!(s.connected_standbys(), 1);
    }

    #[test]
    fn slot_bytes_are_counted_when_no_walsender_is_connected() {
        // A disabled subscription leaves the slot behind with no walsender:
        // the time lag is genuinely unknowable from the primary, but the byte
        // backlog is not.
        let s = observed(
            vec![],
            vec![SlotLag {
                slot_name: "harvest_dr_s0".into(),
                active: false,
                lag_bytes: Some(77),
            }],
        );
        assert_eq!(s.max_replay_lag_seconds(), None);
        assert_eq!(s.max_lag_bytes(), Some(77));
        assert_eq!(s.connected_standbys(), 0);
    }

    /// A published pin is immutable: a later worker cannot re-authorize an
    /// earlier one.
    ///
    /// Without this, a process hosting two workers had a hole straight through
    /// the fence — a worker started *after* a fence publishes the new
    /// generation, overwrites the shared pin, and the already-running worker
    /// reads the replacement in its claim gate and persist assert and silently
    /// regains write authority over a database another region owns.
    #[test]
    fn a_published_pin_cannot_be_overwritten_with_a_different_generation() {
        let _serial = registry_guard();
        FenceRegistry::clear();

        FenceRegistry::publish(
            &[(ShardId::new(0), ShardGeneration::new(4))],
            ShardId::new(0),
        )
        .expect("first publish");

        // Idempotent: two workers covering the same shard at the same epoch.
        FenceRegistry::publish(
            &[(ShardId::new(0), ShardGeneration::new(4))],
            ShardId::new(0),
        )
        .expect("re-publishing the same generation is ordinary");

        // A worker started after a fence must be refused, not admitted.
        let conflict = FenceRegistry::publish(
            &[(ShardId::new(0), ShardGeneration::new(5))],
            ShardId::new(0),
        )
        .expect_err("re-pinning to a newer generation must be refused");
        let PublishConflict::Generation(conflict) = conflict else {
            panic!("expected a generation conflict, got {conflict:?}");
        };
        assert_eq!(conflict.shard_id, 0);
        assert_eq!(conflict.pinned, 4);
        assert_eq!(conflict.attempted, 5);

        // And the running worker's pin is untouched — it stays fenced.
        assert_eq!(
            FenceRegistry::expected(ShardId::new(0)),
            Some(ShardGeneration::new(4))
        );
        FenceRegistry::clear();
    }

    /// A rejected publish must not partially apply.
    #[test]
    fn a_rejected_publish_leaves_every_other_pin_untouched() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::publish(
            &[(ShardId::new(0), ShardGeneration::new(4))],
            ShardId::new(0),
        )
        .expect("first publish");

        // Shard 1 is new and would be accepted; shard 0 conflicts. The whole
        // publish must be refused rather than half-applied.
        FenceRegistry::publish(
            &[
                (ShardId::new(1), ShardGeneration::new(9)),
                (ShardId::new(0), ShardGeneration::new(5)),
            ],
            ShardId::new(0),
        )
        .expect_err("a conflicting publish must be refused wholesale");
        assert_eq!(
            FenceRegistry::expected(ShardId::new(1)),
            None,
            "the non-conflicting pin must not have been installed"
        );
        assert_eq!(
            FenceRegistry::expected(ShardId::new(0)),
            Some(ShardGeneration::new(4))
        );
        FenceRegistry::clear();
    }

    /// `register` must refuse a conflicting re-pin exactly like `publish`
    /// (finding 3). It is the one other public entry point into the same
    /// process-global state. An unconditional `insert` there was a hole
    /// straight through the "write once per process" invariant.
    #[test]
    fn register_refuses_a_conflicting_re_pin() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::register(ShardId::new(0), ShardGeneration::new(4)).expect("first pin");
        // Idempotent re-registration is ordinary.
        FenceRegistry::register(ShardId::new(0), ShardGeneration::new(4))
            .expect("re-registering the same generation is ordinary");

        let conflict = FenceRegistry::register(ShardId::new(0), ShardGeneration::new(5))
            .expect_err("re-pinning to a different generation must be refused");
        assert_eq!(conflict.pinned, 4);
        assert_eq!(conflict.attempted, 5);
        assert_eq!(
            FenceRegistry::expected(ShardId::new(0)),
            Some(ShardGeneration::new(4)),
            "a refused register must not mutate the pin"
        );
        FenceRegistry::clear();
    }

    /// `publish`'s default shard must be conflict-checked exactly like its
    /// generations (finding 13). The default shard resolves every
    /// `UNENCODED` execution id. Two workers disagreeing about it is the
    /// same process-wide hazard as a generation conflict.
    #[test]
    fn publish_refuses_a_conflicting_default_shard() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::publish(
            &[(ShardId::new(0), ShardGeneration::new(1))],
            ShardId::new(0),
        )
        .expect("first publish");

        // A new, non-conflicting generation but a DIFFERENT default shard.
        let conflict = FenceRegistry::publish(
            &[(ShardId::new(1), ShardGeneration::new(1))],
            ShardId::new(1),
        )
        .expect_err("a conflicting default shard must be refused");
        let PublishConflict::DefaultShard(conflict) = conflict else {
            panic!("expected a default-shard conflict, got {conflict:?}");
        };
        assert_eq!(conflict.pinned, 0);
        assert_eq!(conflict.attempted, 1);
        assert_eq!(
            FenceRegistry::expected(ShardId::new(1)),
            None,
            "a refused publish must not install the non-conflicting generation either"
        );
        FenceRegistry::clear();
    }

    /// `set_default_shard` must refuse a conflicting re-pin (finding 13).
    /// It was unconditional like `register` was; the same fix applies.
    #[test]
    fn set_default_shard_refuses_a_conflicting_re_pin() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::set_default_shard(ShardId::new(0)).expect("first default shard");
        FenceRegistry::set_default_shard(ShardId::new(0))
            .expect("re-setting the same default shard is ordinary");

        let conflict = FenceRegistry::set_default_shard(ShardId::new(1))
            .expect_err("re-pinning the default shard must be refused");
        assert_eq!(conflict.pinned, 0);
        assert_eq!(conflict.attempted, 1);
        FenceRegistry::clear();
    }

    // ── advisory keyspace ──────────────────────────────────────────────────

    /// The watermark lock must not collide with the concurrency-key locks it
    /// shares the single-argument advisory keyspace with.
    ///
    /// Those are `hashtext(key)::bigint`, and `hashtext` returns `int4`, so
    /// they can only land in `i32::MIN..=i32::MAX`. Keeping these keys strictly
    /// above that range makes a collision impossible by construction — and this
    /// pins it, because the alternative failure is a DR sampler silently
    /// blocking a workflow's concurrency-key claim.
    #[cfg(feature = "db")]
    #[test]
    fn heartbeat_lock_keys_sit_outside_the_concurrency_key_range() {
        for shard in [0_i32, 1, 7, 255, i32::MAX] {
            let key = super::db::heartbeat_lock_key(shard);
            assert!(
                key > i64::from(i32::MAX),
                "shard {shard} key {key} falls inside the hashtext keyspace"
            );
        }
        // Distinct per shard, so two shards never contend for one beat.
        assert_ne!(
            super::db::heartbeat_lock_key(0),
            super::db::heartbeat_lock_key(1)
        );
    }

    // ── promotion: identifier quoting ──────────────────────────────────────
    //
    // `db`-gated with the helpers themselves: without that feature — which is
    // how `autumn-harvest-sqlite` builds this crate — they are not compiled,
    // and CI's `clippy -p autumn-harvest-sqlite -- -D warnings` fails the build
    // on the unused items.

    /// Catalog identifiers go into `setval` SQL inline (they cannot be bind
    /// parameters), so they must be *quoted*, not merely *screened*.
    ///
    /// An earlier revision screened with a lowercase-only predicate and
    /// silently skipped anything else. That was wrong twice over: an embedder
    /// on a `PascalCase` ORM schema (Prisma, `TypeORM`, EF Core) had **every**
    /// sequence skipped while `harvest dr promote` reported success, and an
    /// ordinary table named `user` or `order` passed the screen and then blew
    /// up as a bare keyword in the generated SQL.
    #[cfg(feature = "db")]
    #[test]
    fn identifiers_are_quoted_not_screened() {
        assert_eq!(quote_ident("harvest_events"), "\"harvest_events\"");
        assert_eq!(quote_ident("User"), "\"User\"");
        // Reserved words are ordinary identifiers once quoted.
        assert_eq!(quote_ident("user"), "\"user\"");
        assert_eq!(quote_ident("order"), "\"order\"");
        // A quote inside an identifier is doubled, which is the complete
        // escape for a Postgres quoted identifier.
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[cfg(feature = "db")]
    #[test]
    fn qualified_names_are_schema_pinned() {
        assert_eq!(
            qualified("public", "harvest_events"),
            "\"public\".\"harvest_events\""
        );
    }

    // ── fencing mode (issue #1823) ─────────────────────────────────────────
    #[test]
    fn the_default_mode_is_auto() {
        assert_eq!(DrFencing::default(), DrFencing::Auto);
        assert_eq!(DrConfig::default().fencing, DrFencing::Auto);
    }

    #[test]
    fn auto_fences_exactly_when_a_dr_marker_is_found() {
        assert_eq!(DrFencing::Auto.resolve(false), Ok(false));
        assert_eq!(DrFencing::Auto.resolve(true), Ok(true));
    }

    #[test]
    fn enabled_always_fences() {
        assert_eq!(DrFencing::Enabled.resolve(false), Ok(true));
        assert_eq!(DrFencing::Enabled.resolve(true), Ok(true));
    }

    #[test]
    fn disabled_refuses_a_dr_database() {
        assert_eq!(DrFencing::Disabled.resolve(false), Ok(false));
        let refusal = DrFencing::Disabled
            .resolve(true)
            .expect_err("a disagreeing config must refuse to start");
        assert!(refusal.contains("Disabled"), "{refusal}");
        assert!(refusal.contains("DR"), "{refusal}");
    }

    #[test]
    fn markers_report_dr_when_any_signal_is_present() {
        assert!(!DrMarkers::default().is_dr());
        let row = DrMarkers {
            generation_shards: vec![ShardId::new(4)],
            ..DrMarkers::default()
        };
        assert!(row.is_dr());
        let slot = DrMarkers {
            dr_slots: 1,
            ..DrMarkers::default()
        };
        assert!(slot.is_dr());
        let subscription = DrMarkers {
            dr_subscriptions: 1,
            ..DrMarkers::default()
        };
        assert!(subscription.is_dr());
    }

    #[test]
    fn the_mode_serializes_in_snake_case() {
        assert_eq!(
            serde_json::to_value(DrFencing::Auto).unwrap(),
            serde_json::json!("auto")
        );
        assert_eq!(
            serde_json::to_value(DrFencing::Disabled).unwrap(),
            serde_json::json!("disabled")
        );
    }

    /// A held shard is pinned to a sentinel no row holds, and release
    /// removes only that sentinel (issue #1823).
    #[test]
    fn a_held_shard_fails_closed_until_released() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::hold(&[ShardId::new(3)], ShardId::new(3)).expect("hold");
        assert!(FenceRegistry::is_held(ShardId::new(3)));
        assert!(FenceRegistry::is_enabled(), "a held shard is checked");
        assert_eq!(
            FenceRegistry::binding(ShardId::UNENCODED),
            Some((ShardId::new(3), HELD)),
            "pre-sharding ids resolve to the held default shard"
        );

        // A second holder keeps the shard held until it releases too.
        FenceRegistry::hold(&[ShardId::new(3)], ShardId::new(3)).expect("second holder");
        FenceRegistry::release_held(ShardId::new(3));
        assert!(
            FenceRegistry::is_held(ShardId::new(3)),
            "one holder remains"
        );

        FenceRegistry::release_held(ShardId::new(3));
        assert!(!FenceRegistry::is_held(ShardId::new(3)));
        assert_eq!(FenceRegistry::expected(ShardId::new(3)), None);
        assert!(!FenceRegistry::is_enabled(), "nothing left to check");

        // A real pin is never released, and never replaced by a hold.
        FenceRegistry::register(ShardId::new(4), ShardGeneration(2)).expect("pin");
        FenceRegistry::hold(&[ShardId::new(4)], ShardId::new(3)).expect("hold skips a pin");
        FenceRegistry::release_held(ShardId::new(4));
        assert_eq!(
            FenceRegistry::expected(ShardId::new(4)),
            Some(ShardGeneration(2))
        );
        FenceRegistry::clear();
    }

    /// A release that races a publish must not switch the fence off over
    /// the new pin (issue #1823). The flag and the map change under one lock.
    #[test]
    fn a_release_racing_a_publish_leaves_the_fence_on() {
        let _serial = registry_guard();
        for round in 0..2_000 {
            FenceRegistry::clear();
            FenceRegistry::hold(&[ShardId::new(3)], ShardId::new(3)).expect("hold");
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let release = {
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    FenceRegistry::release_held(ShardId::new(3));
                })
            };
            barrier.wait();
            FenceRegistry::register(ShardId::new(4), ShardGeneration(2)).expect("pin");
            release.join().expect("release thread");
            assert!(
                FenceRegistry::is_enabled(),
                "round {round}: a live pin must keep the fence on"
            );
            assert_eq!(
                FenceRegistry::binding(ShardId::new(4)),
                Some((ShardId::new(4), ShardGeneration(2)))
            );
        }
        FenceRegistry::clear();
    }

    /// A second hold that races the first holder's release keeps the shard
    /// held (issue #1823). The sentinel and the holder count change under
    /// one lock, so the shard is never left unpinned between them.
    #[test]
    fn a_hold_racing_a_release_keeps_the_shard_held() {
        let _serial = registry_guard();
        for round in 0..2_000 {
            FenceRegistry::clear();
            FenceRegistry::hold(&[ShardId::new(3)], ShardId::new(3)).expect("first holder");
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let release = {
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    FenceRegistry::release_held(ShardId::new(3));
                })
            };
            barrier.wait();
            FenceRegistry::hold(&[ShardId::new(3)], ShardId::new(3)).expect("second holder");
            release.join().expect("release thread");
            assert!(
                FenceRegistry::is_held(ShardId::new(3)),
                "round {round}: the second holder keeps the shard held"
            );
        }
        FenceRegistry::clear();
    }

    // ── fence registry ─────────────────────────────────────────────────────
    #[test]
    fn registry_round_trips_and_defaults_to_disabled() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        assert!(
            !FenceRegistry::is_enabled(),
            "an empty registry fences nothing"
        );
        assert_eq!(FenceRegistry::expected(ShardId::new(3)), None);

        FenceRegistry::register(ShardId::new(3), ShardGeneration(7)).expect("first registration");
        assert!(FenceRegistry::is_enabled());
        assert_eq!(
            FenceRegistry::expected(ShardId::new(3)),
            Some(ShardGeneration(7))
        );
        assert_eq!(FenceRegistry::expected(ShardId::new(4)), None);

        FenceRegistry::clear();
        assert!(!FenceRegistry::is_enabled());
    }

    #[test]
    fn unencoded_execution_shard_resolves_to_the_default_shard() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        FenceRegistry::register(ShardId::new(0), ShardGeneration(2)).expect("first registration");
        FenceRegistry::set_default_shard(ShardId::new(0)).expect("first default shard");
        assert_eq!(
            FenceRegistry::expected(ShardId::UNENCODED),
            Some(ShardGeneration(2)),
            "pre-sharding execution ids must still be fenced via the default shard"
        );
        FenceRegistry::clear();
    }
}
