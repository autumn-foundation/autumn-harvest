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
//! # Opt-in by construction
//!
//! A deployment that never registers a generation pays nothing: [`FenceRegistry`]
//! reports [`FenceRegistry::is_enabled`] `false`, `claim_task` issues the
//! byte-for-byte unchanged claim SQL, and [`assert_fence`] issues no statement
//! at all.
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
/// Three states, not `Option<f64>`, because two of the "no number" cases mean
/// opposite things and collapsing them is dangerous: a trail the standby has
/// fallen off the end of means the RPO is **huge**, while a trail with nothing
/// confirmed yet means it is merely **unmeasured**.
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
            },
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
    /// distinct situations that all look like "no number": the views were
    /// unreadable, no standby is connected at all, or no connected standby has
    /// reported a `replay_lag` yet. Each is a reason to page, and none of them
    /// is `0.0`.
    #[must_use]
    pub fn max_replay_lag_seconds(&self) -> Option<f64> {
        let Self::Observed { standbys, .. } = self else {
            return None;
        };
        standbys
            .iter()
            .filter_map(|s| s.replay_lag_seconds)
            .fold(None, |acc: Option<f64>, v| {
                Some(acc.map_or(v, |a| a.max(v)))
            })
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
    /// Whether write-authority fencing is enabled for this process.
    pub fencing: bool,
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
    /// Fencing **off**, which is byte-for-byte the pre-#954 runtime.
    fn default() -> Self {
        Self {
            fencing: false,
            sample_interval: std::time::Duration::from_secs(15),
            watermark_retain: std::time::Duration::from_secs(3600),
            slot_prefix: DEFAULT_DR_SLOT_PREFIX.to_string(),
        }
    }
}

/// The slot-name prefix `docs/cross-region-dr.md`'s setup SQL prescribes.
pub const DEFAULT_DR_SLOT_PREFIX: &str = "harvest_dr";

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
    pub fn register(shard: ShardId, generation: ShardGeneration) {
        {
            let mut guard = PINNED
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pinned = guard.get_or_insert_with(Pinned::default);
            pinned.generations.insert(shard.as_i32(), generation);
            drop(guard);
        }
        // Published only after the map is visible and the lock is released, so
        // a reader that observes `is_enabled() == true` can never then find an
        // empty registry.
        ENABLED.store(true, Ordering::Release);
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
    /// Returns the conflicting `(shard, already pinned, attempted)` when a shard
    /// is already pinned at a different generation. The caller must refuse to
    /// start; nothing is published.
    pub fn publish(
        pins: &[(ShardId, ShardGeneration)],
        default_shard: ShardId,
    ) -> Result<(), PinConflict> {
        {
            let mut guard = PINNED
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pinned = guard.get_or_insert_with(Pinned::default);

            // Validate every pin BEFORE mutating any of them, so a rejected
            // publish leaves the running workers' registry exactly as it was.
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
                    return Err(conflict);
                }
            }

            for (shard, generation) in pins {
                pinned.generations.insert(shard.as_i32(), *generation);
            }
            pinned.default_shard = Some(default_shard);
            drop(guard);
        }
        // Published only after the map AND the default shard are visible, so a
        // reader that observes `is_enabled()` can never find a half-built
        // registry.
        ENABLED.store(true, Ordering::Release);
        Ok(())
    }

    /// Set the shard that [`ShardId::UNENCODED`] execution ids resolve to.
    ///
    /// Execution ids minted before sharding carry no shard bits. They still
    /// live in a real database and must still be fenced, so they resolve to
    /// the pool's default shard exactly as [`crate::shard::ShardedDbPool`]
    /// routes them.
    pub fn set_default_shard(shard: ShardId) {
        let mut guard = PINNED
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get_or_insert_with(Pinned::default).default_shard = Some(shard);
        drop(guard);
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
            drop(guard);
        }
        ENABLED.store(false, Ordering::Release);
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
        BUMP_LOCK_TIMEOUT_MS, FenceRegistry, PROMOTE_STATEMENT_TIMEOUT_MS, ReplicationStatus,
        ShardGeneration, SlotLag, StandbyLag, WatermarkReading, qualified, quote_ident,
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
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn ensure_generation_row(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
    ) -> HarvestResult<ShardGeneration> {
        let rows: Vec<GenerationRow> = diesel::sql_query(
            "WITH ins AS ( \
                 INSERT INTO harvest_shard_generation (shard_id, generation, fenced_reason) \
                 VALUES ($1, 0, 'provisioned') \
                 ON CONFLICT (shard_id) DO NOTHING \
                 RETURNING generation \
             ) \
             SELECT generation FROM ins \
             UNION ALL \
             SELECT generation FROM harvest_shard_generation WHERE shard_id = $1 \
             LIMIT 1",
        )
        .bind::<Integer, _>(shard.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;

        rows.into_iter()
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

    /// Assert that this process still holds write authority for `shard`.
    ///
    /// Call at the top of a persist. Costs **nothing** — not even a round trip
    /// — when this process pinned no generation, which is every deployment that
    /// has not opted into DR fencing.
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

        let rows: Vec<GenerationRow> = diesel::sql_query(
            "SELECT generation FROM harvest_shard_generation WHERE shard_id = $1 FOR SHARE",
        )
        .bind::<Integer, _>(resolved.as_i32())
        .load(conn)
        .await
        .map_err(database_error)?;

        let current = rows.into_iter().next().map(|r| r.generation);
        if current == Some(pinned.as_i64()) {
            return Ok(());
        }
        Err(crate::error::HarvestError::ShardFenced {
            shard_id: resolved.as_i32(),
            pinned: pinned.as_i64(),
            current,
        })
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
                 AND s.slot_name LIKE $1 || '%' \
               ) \
            OR (s.slot_name IS NULL AND r.application_name LIKE $1 || '%')";

    /// `pg_replication_slots`, which outlives the walsender.
    ///
    /// Physical slots track `restart_lsn`; logical slots track
    /// `confirmed_flush_lsn`. `COALESCE` picks whichever the slot has, so one
    /// query covers both replication styles the topology doc offers.
    const SLOT_SQL: &str = "SELECT \
            s.slot_name::text AS slot_name, \
            s.active, \
            CASE WHEN COALESCE(s.confirmed_flush_lsn, s.restart_lsn) IS NULL THEN NULL \
                 ELSE (pg_current_wal_lsn() \
                       - COALESCE(s.confirmed_flush_lsn, s.restart_lsn))::bigint END AS lag_bytes \
         FROM pg_replication_slots s \
         WHERE (s.database IS NULL OR s.database = current_database()) \
           AND s.slot_name LIKE $1 || '%'";

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
                diesel::sql_query(
                    "INSERT INTO harvest_replication_heartbeat (shard_id, beat_lsn, beat_at) \
                     SELECT $1, pg_current_wal_lsn(), NOW() \
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
    /// **Step 1 — the standby's position.** `MIN(COALESCE(confirmed_flush_lsn,
    /// restart_lsn))` over every replication slot scoped to this shard's
    /// database — the worst standby sets the RPO, and `COALESCE` covers logical
    /// (`confirmed_flush_lsn`) and physical (`restart_lsn`) slots with one
    /// query. **A slot with no position at all makes the whole reading
    /// unknown**, because SQL's `MIN` skips NULLs: without the explicit
    /// `bool_or(... IS NULL)` guard, a slot that has consumed *nothing* is
    /// dropped from the reduction and the healthy standby's small lag is
    /// reported as the fleet's RPO. That is precisely the "report a perfect RPO
    /// for replication that is dead" outcome this module exists to avoid.
    ///
    /// An abandoned slot therefore pegs the reading, which is correct and not
    /// a bug to paper over: an abandoned slot is retaining WAL and is exactly
    /// what an operator must be told about. [`ReplicationStatus::inactive_slots`]
    /// names it.
    ///
    /// **Step 2 — the watermark.** Only issued when step 1 produced a position.
    /// Doing it in one statement meant that with no slot the predicate was
    /// never true and the index scan walked the *entire* retained trail to
    /// return nothing (measured: 200k rows, 2646 buffers, 22 ms) — on every
    /// sampler tick of every worker of a deployment that has not finished
    /// wiring up replication.
    ///
    /// `None` — unknown — when there is no slot, when a slot has no position,
    /// when no watermark has been confirmed, or when the standby is further
    /// behind than the retained trail.
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
        let positions: Vec<StandbyPositionRow> = diesel::sql_query(
            "SELECT CASE \
                        WHEN COUNT(*) = 0 THEN NULL \
                        WHEN bool_or( \
                            COALESCE(s.confirmed_flush_lsn, r.replay_lsn, s.restart_lsn) IS NULL \
                        ) THEN NULL \
                        ELSE MIN( \
                            COALESCE(s.confirmed_flush_lsn, r.replay_lsn, s.restart_lsn) \
                        )::text \
                    END AS position \
             FROM pg_replication_slots s \
             LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid \
             WHERE (s.database IS NULL OR s.database = current_database()) \
               AND s.slot_name LIKE $1 || '%'",
        )
        .bind::<Text, _>(slot_prefix)
        .load(conn)
        .await
        .map_err(database_error)?;

        let Some(position) = positions.into_iter().next().and_then(|r| r.position) else {
            return Ok(WatermarkReading::Unknown);
        };

        // One query, two answers, so the two cannot disagree across a round
        // trip: the age of the newest CONSUMED watermark (the reading), and the
        // age of the OLDEST retained one (the floor, used only when nothing has
        // been consumed but the trail is non-empty — i.e. the standby has
        // fallen off the end of it).
        //
        // Both are single index probes on `(shard_id, beat_lsn)` /
        // `(shard_id, beat_at DESC)`; neither aggregates the trail.
        let rows: Vec<RpoRow> = diesel::sql_query(
            "SELECT ( \
                 SELECT EXTRACT(EPOCH FROM (NOW() - h.beat_at))::double precision \
                 FROM harvest_replication_heartbeat h \
                 WHERE h.shard_id = $1 AND h.beat_lsn <= $2::pg_lsn \
                 ORDER BY h.beat_lsn DESC LIMIT 1 \
             ) AS lag_seconds, \
             ( \
                 SELECT EXTRACT(EPOCH FROM (NOW() - h.beat_at))::double precision \
                 FROM harvest_replication_heartbeat h \
                 WHERE h.shard_id = $1 \
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
    /// Physical (streaming) replicas do not need this — they replicate the WAL
    /// itself, sequences included — and running it there is harmless *because*
    /// the target is `GREATEST(MAX(col), last_value, 1)` rather than
    /// `MAX(col)`: a sequence already ahead of its table's maximum is left
    /// where it is, never rewound. See the statement below for why "ahead of
    /// MAX" is an ordinary, expected state rather than corruption.
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

        let columns: Vec<SerialColumn> = diesel::sql_query(
            "SELECT tn.nspname::text AS table_schema, \
                    c.relname::text  AS table_name, \
                    a.attname::text  AS column_name, \
                    sn.nspname::text AS sequence_schema, \
                    s.relname::text  AS sequence_name \
             FROM pg_class s \
             JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass \
             JOIN pg_class c ON c.oid = d.refobjid \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid \
             JOIN pg_namespace sn ON sn.oid = s.relnamespace \
             JOIN pg_namespace tn ON tn.oid = c.relnamespace \
             WHERE s.relkind = 'S' \
               AND d.refclassid = 'pg_class'::regclass \
               AND d.deptype IN ('a', 'i') \
               AND c.relkind IN ('r', 'p') \
               AND sn.nspname = current_schema()",
        )
        .load(conn)
        .await
        .map_err(database_error)?;

        let mut advanced = Vec::with_capacity(columns.len());
        for col in columns {
            // `is_called = true` so the NEXT value handed out is `max + 1`.
            // `GREATEST(..., 1)` keeps `setval` legal on an empty table, where
            // `MAX` is NULL and `0` is below the sequence minimum.
            //
            // `setval`'s first argument is `regclass`, i.e. a *name* rather
            // than a relation reference, so the schema-qualified identifier is
            // passed as a single-quoted literal with any `'` doubled.
            let seq = qualified(&col.sequence_schema, &col.sequence_name);
            // `pg_sequence_last_value` is inside the GREATEST for a reason: a
            // sequence can legitimately sit AHEAD of `MAX(col)` — cached
            // values, a rolled-back transaction, deleted rows — and a physical
            // replica replicates sequences already, so on that topology this
            // command is meant to be a no-op. Without it `setval` would
            // *rewind* the sequence and start re-issuing ids the database has
            // already handed out: a duplicate-key outage caused by the very
            // command that exists to prevent one. Measured on live Postgres:
            // insert two rows, delete the second, and MAX is 1 while
            // last_value is 2.
            //
            // NULL until the sequence has been called at least once, hence the
            // COALESCE.
            let sql = format!(
                "SELECT setval('{seq_literal}', \
                        GREATEST( \
                            (SELECT COALESCE(MAX({col}), 0) FROM {tbl}), \
                            COALESCE(pg_sequence_last_value('{seq_literal}'), 0), \
                            1), \
                        true)::bigint AS value",
                seq_literal = seq.replace('\'', "''"),
                col = quote_ident(&col.column_name),
                tbl = qualified(&col.table_schema, &col.table_name),
            );
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
        // does: lose the number, never the sampler.
        let heartbeat = measure_rpo(conn, shard, slot_prefix)
            .await
            .unwrap_or(WatermarkReading::Unknown);

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
    advance_sequences_after_promotion, assert_fence, bump_generation, current_generation,
    ensure_generation_row, measure_rpo, query_replication_status, record_replication_heartbeat,
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

    // ── fence registry ─────────────────────────────────────────────────────
    #[test]
    fn registry_round_trips_and_defaults_to_disabled() {
        let _serial = registry_guard();
        FenceRegistry::clear();
        assert!(!FenceRegistry::is_enabled(), "fencing is opt-in");
        assert_eq!(FenceRegistry::expected(ShardId::new(3)), None);

        FenceRegistry::register(ShardId::new(3), ShardGeneration(7));
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
        FenceRegistry::register(ShardId::new(0), ShardGeneration(2));
        FenceRegistry::set_default_shard(ShardId::new(0));
        assert_eq!(
            FenceRegistry::expected(ShardId::UNENCODED),
            Some(ShardGeneration(2)),
            "pre-sharding execution ids must still be fenced via the default shard"
        );
        FenceRegistry::clear();
    }
}
