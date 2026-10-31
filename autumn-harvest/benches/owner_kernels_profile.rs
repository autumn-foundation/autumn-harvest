//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for the pure kernels of the decision owner (issues #2249, #2256).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! `OWNER_KERNELS_PROFILE_SHAPE` picks the kernel. `OWNER_KERNELS_PROFILE_N`
//! sets the size, and its meaning depends on the shape:
//!
//! - `partition` (default): `DecisionPartition::for_workflow_id` over `N`
//!   ids. Even ids look like `order-00000042`. Odd ids are hyphenated UUIDs.
//! - `mask`: `N` calls of `OwnedClaim::mask`. The claim holds
//!   `OWNER_KERNELS_PROFILE_HELD` partitions (default 512), in ascending
//!   order, as `OwnedPartitions::owned_claim` builds them.
//! - `plan`: one drain of a lane queue of `N` waiting writers. The drain
//!   copies the private `Lane::take` of `group_commit.rs`. It plans a batch
//!   with `share` and `plan_batch`, removes the batch, and repeats until the
//!   queue is empty.
//! - `lease`: `N` calls of `LeaseWrite::done` with `HELD` matched partitions
//!   and no lost partition. `OWNER_KERNELS_PROFILE_ORDER` picks their order:
//!   `shuffled` (default), a fixed shuffle, or `sorted`, the order that the
//!   lock statement returns in production.
//! - `lease_scans`: `N` calls of a copy of the three linear scans that one
//!   renewal runs over its `HELD` tenures. Two are in the private
//!   `locked_write`, and one is in the renew step of the owner loop.
//!
//! The `plan` queue uses a bench-local tenure. Its fields and their order are
//! those of `OwnerTenure`, so the derived `PartialEq` makes the same
//! comparisons. `OwnerTenure` has no public constructor.
//! `OWNER_KERNELS_PROFILE_HOLDER` picks the holder of each entry:
//!
//! - `shared` (default): every entry clones one `Arc<str>`, as the tenures of
//!   one worker do in production.
//! - `distinct`: each entry has its own `Arc<str>` with equal content. Each
//!   holder comparison then reads the bytes.
//!
//! Every input is built before the measured call, from fixed values. The
//! inclusive cost of the measured functions leaves the setup out. Those
//! functions are `partition_all`, `mask_calls`, `drain`, `plan`,
//! `lease_done_all` and `renew_scans`. `OWNER_KERNELS_PROFILE_REPS` (default
//! 1) repeats the measured call.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --features testing --bench owner_kernels_profile \
//!   --bench entity_snapshot_replay_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="owner_kernels_profile") | .executable')
//! OWNER_KERNELS_PROFILE_SHAPE=partition OWNER_KERNELS_PROFILE_N=100000 valgrind \
//!   --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! OWNER_KERNELS_PROFILE_SHAPE=plan OWNER_KERNELS_PROFILE_N=256 OWNER_KERNELS_PROFILE_REPS=1000 \
//!   valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;

use autumn_harvest::decision_owner::{
    DECISION_PARTITIONS, DecisionPartition, LeaseWrite, OwnedClaim, OwnerEpoch, OwnerLeaseTtl,
};
use autumn_harvest::group_commit::{GroupCommitConfig, Queued, plan_batch, share};
use autumn_harvest::types::{ExecutionId, ShardId};
use uuid::Uuid;

/// The number of partitions, as an index bound.
const PARTITIONS: usize = DECISION_PARTITIONS as usize;

/// A holder token of the production form: a worker id, a slash and 32 hex
/// digits.
const HOLDER: &str = "worker-7f3a9c/5f0e2b8c4d6a4e1f9b3c7d2a8e6f4b1c";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Partition,
    Mask,
    Plan,
    Lease,
    LeaseScans,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("OWNER_KERNELS_PROFILE_SHAPE") else {
            return Self::Partition;
        };
        match value.as_str() {
            "partition" => Self::Partition,
            "mask" => Self::Mask,
            "plan" => Self::Plan,
            "lease" => Self::Lease,
            "lease_scans" => Self::LeaseScans,
            other => panic!(
                "OWNER_KERNELS_PROFILE_SHAPE must be partition, mask, plan, lease or \
                 lease_scans, got {other:?}"
            ),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Partition => "partition",
            Self::Mask => "mask",
            Self::Plan => "plan",
            Self::Lease => "lease",
            Self::LeaseScans => "lease_scans",
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

/// A fixed `SplitMix64` stream. It makes the shuffled orders.
struct SplitMix(u64);

impl SplitMix {
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A Fisher-Yates shuffle of `items`.
    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let bound = u64::try_from(i + 1).expect("a length fits in u64");
            let j = usize::try_from(self.next() % bound).expect("an index fits in usize");
            items.swap(i, j);
        }
    }
}

fn partition(n: usize) -> DecisionPartition {
    let n = u16::try_from(n).expect("a partition number fits in u16");
    DecisionPartition::new(n).expect("the partition is below the count")
}

/// `held` partitions spread evenly over the range, in ascending order.
fn spread(held: usize) -> Vec<usize> {
    assert!(
        (1..=PARTITIONS).contains(&held),
        "OWNER_KERNELS_PROFILE_HELD must be from 1 to {PARTITIONS}, got {held}"
    );
    (0..held).map(|k| k * PARTITIONS / held).collect()
}

/// The workflow id of execution `i`: an order id or a hyphenated UUID.
fn workflow_id(i: usize) -> String {
    if i.is_multiple_of(2) {
        format!("order-{i:08}")
    } else {
        let bits = (i as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835);
        Uuid::from_u128(bits).hyphenated().to_string()
    }
}

/// The partition of each id, as a histogram over the partitions.
#[inline(never)]
fn partition_all(ids: &[String]) -> Vec<u64> {
    let mut histogram = vec![0_u64; PARTITIONS];
    for id in ids {
        let p = DecisionPartition::for_workflow_id(black_box(id));
        histogram[usize::from(p.get())] += 1;
    }
    histogram
}

fn run_partition(n: usize, reps: usize) -> u64 {
    let ids: Vec<String> = (0..n).map(workflow_id).collect();
    let mut mapped = 0_u64;
    for _ in 0..reps {
        let histogram = partition_all(&ids);
        mapped += histogram.iter().sum::<u64>();
        if n >= 16 * PARTITIONS {
            assert!(
                histogram.iter().all(|&count| count > 0),
                "every partition must receive an id at n={n}"
            );
        }
    }
    mapped
}

/// Build the claim mask `n` times. It returns the number of set bits.
#[inline(never)]
fn mask_calls(claim: &OwnedClaim, n: usize) -> u64 {
    let mut bits = 0_u64;
    for _ in 0..n {
        let mask = black_box(claim).mask();
        bits += mask.iter().map(|b| u64::from(b.count_ones())).sum::<u64>();
    }
    bits
}

fn run_mask(n: usize, reps: usize, held: usize) -> u64 {
    let claim = OwnedClaim {
        holder: HOLDER.to_string(),
        partitions: spread(held)
            .into_iter()
            .map(|p| i16::try_from(p).expect("a partition fits in i16"))
            .collect(),
    };
    let mut bits = 0_u64;
    for _ in 0..reps {
        bits += mask_calls(&claim, n);
    }
    bits
}

/// The holder of a bench tenure. `OwnerHolder` has the same layout.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Holder(Arc<str>);

/// A copy of `OwnerTenure`, with the same field types in the same order.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Tenure {
    shard: ShardId,
    partition: DecisionPartition,
    holder: Holder,
    epoch: OwnerEpoch,
    ttl: OwnerLeaseTtl,
}

/// One waiting decision. A production entry also holds a boxed stage and a
/// reply sender. The three words of `stage` stand in for them.
#[derive(Debug, Clone)]
struct Entry {
    id: u64,
    tenure: Tenure,
    execution: ExecutionId,
    stage: [usize; 3],
}

/// The batches of one drain.
#[derive(Debug, Default)]
struct Drained {
    batches: usize,
    taken: usize,
    id_sum: u64,
}

/// The `plan_batch` call of one take. It is a function of its own, so its
/// inclusive cost shows apart from the drain.
#[inline(never)]
fn plan(views: &[Queued<&Tenure, ExecutionId>], own: usize, max_batch: usize) -> Vec<usize> {
    plan_batch(views, own, max_batch)
}

/// Take batches from `queue` until it is empty, as `Lane::take` does.
///
/// The head leads each batch. The semaphore of a lane wakes the oldest
/// waiter first, and the oldest waiter is at the head.
#[inline(never)]
fn drain(mut queue: VecDeque<Entry>, config: GroupCommitConfig) -> Drained {
    let mut drained = Drained::default();
    while !queue.is_empty() {
        let own = 0;
        let max_batch = share(queue.len(), config.width(), config.max_batch());
        let views: Vec<Queued<&Tenure, ExecutionId>> = queue
            .iter()
            .map(|entry| Queued {
                tenure: &entry.tenure,
                execution: entry.execution,
            })
            .collect();
        let order = plan(&views, own, max_batch);
        drop(views);
        let expected = max_batch.min(queue.len());
        // `Lane::take` drains through a lock guard and keeps the buffer.
        #[allow(
            clippy::iter_with_drain,
            reason = "the copy keeps the buffer as `Lane::take` does"
        )]
        let mut slots: Vec<Option<Entry>> = queue.drain(..).map(Some).collect();
        let batch: Vec<Entry> = order.iter().filter_map(|&at| slots[at].take()).collect();
        queue.extend(slots.into_iter().flatten());
        assert_eq!(
            batch.len(),
            expected,
            "one tenure and distinct executions must fill each batch to its share"
        );
        drained.batches += 1;
        drained.taken += batch.len();
        drained.id_sum += batch
            .iter()
            .map(|entry| entry.id + black_box(entry.stage[0]) as u64)
            .sum::<u64>();
    }
    drained
}

fn tenure(holder: Holder) -> Tenure {
    Tenure {
        shard: ShardId::new(3),
        partition: partition(17),
        holder,
        epoch: OwnerEpoch::new(41),
        ttl: OwnerLeaseTtl::DEFAULT,
    }
}

fn run_plan(depth: usize, reps: usize, distinct: bool) -> (usize, usize) {
    let shared = Holder(Arc::from(HOLDER));
    let template: VecDeque<Entry> = (0..depth)
        .map(|i| Entry {
            id: i as u64,
            tenure: tenure(if distinct {
                Holder(Arc::from(HOLDER.to_string()))
            } else {
                shared.clone()
            }),
            execution: ExecutionId::from_uuid(Uuid::from_u128(0x2256_0000 + i as u128)),
            stage: [0; 3],
        })
        .collect();
    let expected_ids = (0..depth as u64).sum::<u64>();
    let config = GroupCommitConfig::default();
    let mut batches = 0_usize;
    let mut taken = 0_usize;
    for _ in 0..reps {
        let drained = drain(template.clone(), config);
        assert_eq!(drained.taken, depth, "the drain must take every entry");
        assert_eq!(
            drained.id_sum, expected_ids,
            "the drain must take each entry once"
        );
        batches += drained.batches;
        taken += drained.taken;
    }
    (batches, taken)
}

type LeaseInput = (Vec<DecisionPartition>, Vec<DecisionPartition>);

/// Call `LeaseWrite::done` on each input. It returns the matched count.
#[inline(never)]
fn lease_done_all(inputs: Vec<LeaseInput>) -> usize {
    let mut matched = 0;
    for (m, l) in inputs {
        match LeaseWrite::done(black_box(m), black_box(l)) {
            LeaseWrite::Done(done) => {
                assert!(
                    done.matched.windows(2).all(|w| w[0] < w[1]),
                    "done must sort and dedup the matched list"
                );
                matched += done.matched.len() + done.lost.len();
            }
            LeaseWrite::Busy => panic!("done must return a Done result"),
        }
    }
    matched
}

fn run_lease(n: usize, reps: usize, held: usize, shuffled: bool) -> usize {
    let mut order: Vec<DecisionPartition> = spread(held).into_iter().map(partition).collect();
    if shuffled {
        SplitMix(2249).shuffle(&mut order);
    }
    let mut matched = 0;
    for _ in 0..reps {
        let inputs: Vec<LeaseInput> = (0..n).map(|_| (order.clone(), Vec::new())).collect();
        matched += lease_done_all(inputs);
    }
    matched
}

/// A copy of the linear scans of one renewal of `held`.
///
/// - The TTL of each locked row: `partitions.iter().position` in
///   `locked_write`, `decision_owner.rs:1652`.
/// - The lost tenures: `locked.contains` in `locked_write`,
///   `decision_owner.rs:1687`.
/// - The renewed tenures: `matched.contains` in the renew step,
///   `decision_owner.rs:2621`.
///
/// Production passes both lists in ascending partition order, so this does
/// too.
#[inline(never)]
fn renew_scans(
    partitions: &[i32],
    ttls: &[f64],
    locked: &[i32],
    held: &[DecisionPartition],
    matched: &[DecisionPartition],
) -> (Vec<f64>, Vec<DecisionPartition>, usize) {
    let locked_ttls: Vec<f64> = locked
        .iter()
        .map(|p| {
            partitions
                .iter()
                .position(|q| q == p)
                .map_or(0.0, |at| ttls[at])
        })
        .collect();
    let lost: Vec<DecisionPartition> = held
        .iter()
        .copied()
        .filter(|p| !locked.contains(&i32::from(p.get())))
        .collect();
    let renewed = held.iter().filter(|p| matched.contains(p)).count();
    (locked_ttls, lost, renewed)
}

fn run_lease_scans(n: usize, reps: usize, held: usize) -> usize {
    let tenures: Vec<DecisionPartition> = spread(held).into_iter().map(partition).collect();
    let partitions: Vec<i32> = tenures.iter().map(|p| i32::from(p.get())).collect();
    let ttls: Vec<f64> = tenures
        .iter()
        .map(|_| OwnerLeaseTtl::DEFAULT.get().as_secs_f64())
        .collect();
    let locked = partitions.clone();
    let mut total = 0;
    for _ in 0..reps {
        for _ in 0..n {
            let (locked_ttls, lost, renewed) = renew_scans(
                black_box(&partitions),
                &ttls,
                black_box(&locked),
                &tenures,
                &tenures,
            );
            assert!(
                lost.is_empty() && renewed == held,
                "a renewal that locks every row loses none"
            );
            total += locked_ttls.len() + renewed;
        }
    }
    total
}

fn main() {
    let shape = Shape::from_env();
    let reps = env_usize("OWNER_KERNELS_PROFILE_REPS", 1);
    let held = env_usize("OWNER_KERNELS_PROFILE_HELD", PARTITIONS);
    let name = shape.name();
    match shape {
        Shape::Partition => {
            let n = env_usize("OWNER_KERNELS_PROFILE_N", 100_000);
            let mapped = run_partition(n, reps);
            assert_eq!(
                mapped,
                (n * reps) as u64,
                "every id must map to a partition"
            );
            println!("owner_kernels_profile: shape={name} n={n} reps={reps} mapped={mapped}");
        }
        Shape::Mask => {
            let n = env_usize("OWNER_KERNELS_PROFILE_N", 100_000);
            let bits = run_mask(n, reps, held);
            assert_eq!(
                bits,
                (n * reps * held) as u64,
                "each mask must set one bit per held partition"
            );
            println!(
                "owner_kernels_profile: shape={name} n={n} held={held} reps={reps} bits={bits}"
            );
        }
        Shape::Plan => {
            let n = env_usize("OWNER_KERNELS_PROFILE_N", 64);
            let holder = std::env::var("OWNER_KERNELS_PROFILE_HOLDER")
                .unwrap_or_else(|_| "shared".to_string());
            let distinct = match holder.as_str() {
                "shared" => false,
                "distinct" => true,
                other => {
                    panic!("OWNER_KERNELS_PROFILE_HOLDER must be shared or distinct, got {other:?}")
                }
            };
            let (batches, taken) = run_plan(n, reps, distinct);
            println!(
                "owner_kernels_profile: shape={name} n={n} holder={holder} reps={reps} \
                 batches={batches} taken={taken}"
            );
        }
        Shape::Lease => {
            let n = env_usize("OWNER_KERNELS_PROFILE_N", 10_000);
            let order = std::env::var("OWNER_KERNELS_PROFILE_ORDER")
                .unwrap_or_else(|_| "shuffled".to_string());
            let shuffled = match order.as_str() {
                "shuffled" => true,
                "sorted" => false,
                other => {
                    panic!("OWNER_KERNELS_PROFILE_ORDER must be shuffled or sorted, got {other:?}")
                }
            };
            let matched = run_lease(n, reps, held, shuffled);
            assert_eq!(matched, n * reps * held, "every partition must match");
            println!(
                "owner_kernels_profile: shape={name} n={n} held={held} order={order} \
                 reps={reps} matched={matched}"
            );
        }
        Shape::LeaseScans => {
            let n = env_usize("OWNER_KERNELS_PROFILE_N", 100);
            let total = run_lease_scans(n, reps, held);
            assert_eq!(total, 2 * n * reps * held, "every tenure must renew");
            println!(
                "owner_kernels_profile: shape={name} n={n} held={held} reps={reps} \
                 renewed={total}"
            );
        }
    }
}
