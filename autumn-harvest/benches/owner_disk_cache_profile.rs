//! Deterministic (non-criterion) instruction-count profiling harness for the
//! owner-local disk cache, `OwnerDiskCache` (issue #2258).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! The harness removes `OWNER_DISK_CACHE_PROFILE_DIR` and creates it again
//! with the mode `0700`. So each run starts from the same empty directory.
//! The harness refuses to remove a directory that holds other files.
//!
//! A history starts with `WorkflowStarted`. Pairs of `ActivityScheduled` and
//! `ActivityCompleted` follow, and each one carries an order payload.
//! `OWNER_DISK_CACHE_PROFILE_SHAPE` picks the workload:
//!
//! - `store` (default): one fresh write of a history of `N` events.
//! - `append`: one fresh write of `N` events, then [`APPEND_DECISIONS`]
//!   decisions. Each decision adds two events and stores the state again.
//!   `DiskTier::write` does the same after each committed suspension.
//! - `load`: one fresh write of `N` events, then one load of that run.
//! - `entity`: one `store_entity` and one `load_entity` of an entity state
//!   that holds `N` order items.
//!
//! Each entry point runs in its own `#[inline(never)]` wrapper, `measure_*`.
//! So `callgrind_annotate --inclusive=yes` separates each entry point from
//! the setup that builds its input. Before each measured step, `settle_heap`
//! makes glibc merge the small chunks that the setup freed.
//!
//! `OWNER_DISK_CACHE_PROFILE_REPS` (default 1) repeats the workload. A
//! `store`, `append` or `load` repetition uses a new run, so the file grows.
//! An `entity` repetition stores a newer snapshot of the same run.
//!
//! The run ids, activity ids and timestamps are fixed. The node key and the
//! nonce prefix come from the OS random source. The AES-GCM instruction count
//! does not depend on them.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --features owner-disk-cache \
//!   --bench owner_disk_cache_profile --bench entity_sql_profile --no-run \
//!   --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="owner_disk_cache_profile") | .executable')
//! OWNER_DISK_CACHE_PROFILE_DIR=cache-dir OWNER_DISK_CACHE_PROFILE_N=1000 \
//!   valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
//!   --callgrind-out-file=cg.out "$BIN"
//! ```

use std::path::{Path, PathBuf};

use autumn_harvest::cache::CachedWorkflowState;
use autumn_harvest::decision_owner::{DecisionPartition, OwnerEpoch, OwnerKey};
use autumn_harvest::entity::{EntitySnapshot, EntityStats};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::owner_disk_cache::{
    CACHE_FILE_EXTENSION, DiskEntry, DiskLoad, DiskSlot, DiskStore, EntityLoad, LOCK_FILE_NAME,
    OwnerDiskCache, ProjectedEntity,
};
use autumn_harvest::types::{ActivityExecId, ShardId};
use serde_json::{Value, json};
use uuid::Uuid;

/// The decisions that one `append` repetition stores after the first write.
const APPEND_DECISIONS: usize = 100;

/// The events that one `append` decision adds.
const EVENTS_PER_DECISION: usize = 2;

/// The first fixed run id. Repetition `r` uses `RUN_ID_BASE + r`.
const RUN_ID_BASE: u128 = 0x2258_0000_0000_0000_0000_0000_0000_0000;

/// The first fixed activity id.
const ACTIVITY_ID_BASE: u128 = 0xac70_0000_0000_0000_0000_0000_0000_0000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Store,
    Append,
    Load,
    Entity,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("OWNER_DISK_CACHE_PROFILE_SHAPE") else {
            return Self::Store;
        };
        match value.as_str() {
            "store" => Self::Store,
            "append" => Self::Append,
            "load" => Self::Load,
            "entity" => Self::Entity,
            other => panic!(
                "OWNER_DISK_CACHE_PROFILE_SHAPE must be store, append, load or entity, \
                 got {other:?}"
            ),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Store => "store",
            Self::Append => "append",
            Self::Load => "load",
            Self::Entity => "entity",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

/// The cache directory. A fixed path keeps the path length the same in
/// every run.
fn cache_dir() -> PathBuf {
    std::env::var_os("OWNER_DISK_CACHE_PROFILE_DIR").map_or_else(
        || std::env::temp_dir().join("owner_disk_cache_profile"),
        PathBuf::from,
    )
}

/// Whether `name` is the lock file, a cache file or a SQLite side file of
/// one. A cache file is `s<shard>-p<partition>.<CACHE_FILE_EXTENSION>`.
fn is_cache_entry(name: &str) -> bool {
    if name == LOCK_FILE_NAME {
        return true;
    }
    let base = ["-journal", "-wal", "-shm"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name);
    base.starts_with('s')
        && Path::new(base)
            .extension()
            .is_some_and(|ext| ext == CACHE_FILE_EXTENSION)
}

/// Remove `dir` and create it again with the mode `0700`.
///
/// The harness refuses a directory that holds a file other than the lock
/// file or a cache file. A typo in the path then deletes nothing.
///
/// A run leaves the lock file behind. A missing `dir` is created with an
/// empty lock file. So the first run reads and removes the same files as a
/// later run, and both runs count the same instructions.
fn fresh_dir(dir: &Path) {
    let create = || {
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
    };
    let lock = dir.join(LOCK_FILE_NAME);
    if !dir.exists() {
        create();
        std::fs::File::create(&lock)
            .unwrap_or_else(|e| panic!("cannot create {}: {e}", lock.display()));
    }
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    for entry in entries {
        let name = entry.expect("a directory entry").file_name();
        let name = name.to_string_lossy();
        assert!(
            is_cache_entry(&name),
            "{} holds {name:?}, which is not a cache file; the harness deletes nothing",
            dir.display()
        );
    }
    std::fs::remove_dir_all(dir).unwrap_or_else(|e| panic!("cannot remove {}: {e}", dir.display()));
    create();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("cannot chmod {}: {e}", dir.display()));
    }
}

fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer_id": i % 4096,
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
        "items": [{ "sku": format!("sku-{}", i % 512), "qty": 1 + i % 5 }],
    })
}

const fn activity_id(pair: usize) -> ActivityExecId {
    ActivityExecId::from_uuid(Uuid::from_u128(ACTIVITY_ID_BASE + pair as u128))
}

/// Event `index` of the history. Event 0 starts the run. Then each pair of
/// events schedules and completes one activity.
fn event(index: usize) -> WorkflowEvent {
    if index == 0 {
        return WorkflowEvent::WorkflowStarted {
            input: payload(0),
            timestamp: chrono::DateTime::from_timestamp(1_790_000_000, 0)
                .expect("a fixed timestamp is in range"),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };
    }
    let pair = (index - 1) / 2;
    if index % 2 == 1 {
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id(pair),
            name: "charge".to_string(),
            input: payload(pair),
            queue: "default".to_string(),
        }
    } else {
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id(pair),
            output: payload(pair),
        }
    }
}

fn next_event_id(len: usize) -> i32 {
    i32::try_from(len).expect("the history length fits in i32")
}

fn state(len: usize) -> CachedWorkflowState {
    CachedWorkflowState {
        events: (0..len).map(event).collect(),
        next_event_id: next_event_id(len),
    }
}

const fn slot() -> DiskSlot {
    DiskSlot {
        key: OwnerKey {
            shard: ShardId::new(0),
            partition: DecisionPartition::new(7).expect("partition 7 is in range"),
        },
        epoch: OwnerEpoch::new(1),
        codec_generation: 0,
    }
}

const fn run_id(rep: usize) -> Uuid {
    Uuid::from_u128(RUN_ID_BASE + rep as u128)
}

const fn still_owned() -> bool {
    true
}

/// The entity state of the `entity` shape: `n` order items and an owner.
fn entity_state(n: usize) -> Value {
    json!({
        "owner": "ada",
        "items": (0..n).map(payload).collect::<Vec<_>>(),
    })
}

fn projected(event_id: i32, state: Value) -> ProjectedEntity {
    ProjectedEntity {
        event_id,
        deleted: false,
        snapshot: EntitySnapshot {
            state,
            stats: EntityStats::default(),
        },
    }
}

/// Ask the allocator for one 4 KiB block, then free it.
///
/// A request of this size makes glibc merge its small free chunks. The setup
/// frees many small chunks. Without this call, the next entry point pays for
/// that merge. The harness calls it before each measured step, but not
/// between two `append` decisions.
#[inline(never)]
fn settle_heap() {
    drop(std::hint::black_box(Vec::<u8>::with_capacity(4096)));
}

#[inline(never)]
fn measure_open(dir: &Path) -> OwnerDiskCache {
    OwnerDiskCache::open(dir).unwrap_or_else(|e| panic!("the cache must open: {e}"))
}

/// One fresh write. It asserts that every event of `state` went to disk.
#[inline(never)]
fn measure_store(cache: &OwnerDiskCache, slot: &DiskSlot, exec: Uuid, state: &CachedWorkflowState) {
    let outcome = cache.store(slot, exec, state, still_owned);
    assert_eq!(
        outcome,
        DiskStore::Written {
            appended: state.events.len()
        },
        "a fresh store must write every event"
    );
}

/// One decision of the `append` shape. It asserts that only the new events
/// went to disk.
#[inline(never)]
fn measure_append(
    cache: &OwnerDiskCache,
    slot: &DiskSlot,
    exec: Uuid,
    state: &CachedWorkflowState,
) {
    let outcome = cache.store(slot, exec, state, still_owned);
    assert_eq!(
        outcome,
        DiskStore::Written {
            appended: EVENTS_PER_DECISION
        },
        "a decision must append only its new events"
    );
}

#[inline(never)]
fn measure_load(cache: &OwnerDiskCache, slot: &DiskSlot, exec: Uuid) -> DiskEntry {
    match cache.load(slot, exec) {
        DiskLoad::Hit(entry) => entry,
        other => panic!("the load must hit, got {}", other.outcome()),
    }
}

#[inline(never)]
fn measure_store_entity(
    cache: &OwnerDiskCache,
    slot: &DiskSlot,
    exec: Uuid,
    entity: &ProjectedEntity,
) {
    let outcome = cache.store_entity(slot, exec, entity, still_owned);
    assert_eq!(
        outcome,
        DiskStore::Written { appended: 1 },
        "the entity store must write its row"
    );
}

#[inline(never)]
fn measure_load_entity(cache: &OwnerDiskCache, slot: &DiskSlot, exec: Uuid) -> ProjectedEntity {
    match cache.load_entity(slot, exec, still_owned) {
        EntityLoad::Hit(entity) => entity,
        other => panic!("the entity load must hit, got {other:?}"),
    }
}

/// Check that `entry` holds the `n` events that the harness stored.
fn check_loaded(entry: &DiskEntry, n: usize) {
    assert_eq!(
        entry.state.events.len(),
        n,
        "the load must return every event"
    );
    assert_eq!(entry.state.next_event_id, next_event_id(n));
    let pair = (n - 2) / 2;
    let (id, payload) = match entry.state.events.last() {
        Some(WorkflowEvent::ActivityScheduled {
            activity_id, input, ..
        }) => (activity_id, input),
        Some(WorkflowEvent::ActivityCompleted {
            activity_id,
            output,
        }) => (activity_id, output),
        other => panic!("the last loaded event must be an activity event, got {other:?}"),
    };
    assert_eq!(
        *id,
        activity_id(pair),
        "the last loaded event has another activity"
    );
    assert_eq!(payload["order_id"], format!("order-{pair:08}"));
}

/// The encoded size of one event, for the summary line.
fn encoded_bytes(event: &WorkflowEvent) -> usize {
    serde_json::to_vec(event).expect("an event encodes").len()
}

fn main() {
    let shape = Shape::from_env();
    let n = env_usize("OWNER_DISK_CACHE_PROFILE_N", 1_000);
    let reps = env_usize("OWNER_DISK_CACHE_PROFILE_REPS", 1);
    assert!(
        shape == Shape::Entity || n >= 2,
        "OWNER_DISK_CACHE_PROFILE_N must be 2 or more for the {} shape",
        shape.name()
    );
    let dir = cache_dir();
    fresh_dir(&dir);
    settle_heap();
    let cache = measure_open(&dir);
    let slot = slot();
    let detail = match shape {
        Shape::Store | Shape::Load => {
            let base = state(n);
            for rep in 0..reps {
                let exec = run_id(rep);
                settle_heap();
                measure_store(&cache, &slot, exec, &base);
                if shape == Shape::Load {
                    settle_heap();
                    let entry = measure_load(&cache, &slot, exec);
                    check_loaded(&entry, n);
                }
            }
            format!(
                "scheduled_bytes={} completed_bytes={}",
                encoded_bytes(&base.events[1]),
                encoded_bytes(&base.events[2])
            )
        }
        Shape::Append => {
            for rep in 0..reps {
                let exec = run_id(rep);
                let mut grown = state(n);
                settle_heap();
                measure_store(&cache, &slot, exec, &grown);
                settle_heap();
                for _ in 0..APPEND_DECISIONS {
                    let len = grown.events.len();
                    grown
                        .events
                        .extend((len..len + EVENTS_PER_DECISION).map(event));
                    grown.next_event_id = next_event_id(grown.events.len());
                    measure_append(&cache, &slot, exec, &grown);
                }
                assert!(cache.contains(exec), "the appended run must stay cached");
            }
            format!("decisions={APPEND_DECISIONS}")
        }
        Shape::Entity => {
            let exec = run_id(0);
            let state = entity_state(n);
            let state_bytes = serde_json::to_vec(&state).expect("the state encodes").len();
            for rep in 0..reps {
                let event_id = next_event_id(rep + 1);
                let entity = projected(event_id, state.clone());
                settle_heap();
                measure_store_entity(&cache, &slot, exec, &entity);
                settle_heap();
                let loaded = measure_load_entity(&cache, &slot, exec);
                assert_eq!(loaded, entity, "the entity load must return the stored row");
            }
            format!("state_bytes={state_bytes}")
        }
    };

    drop(cache);
    println!(
        "owner_disk_cache_profile: shape={} n={n} reps={reps} {detail}",
        shape.name()
    );
}
