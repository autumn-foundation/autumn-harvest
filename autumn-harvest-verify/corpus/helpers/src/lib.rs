//! `harvest-verify-corpus-helpers` — the **one-hop** crate of the issue #962
//! seeded determinism corpus.
//!
//! Two jobs:
//!
//! 1. **Launder** the raw sources in `harvest-verify-corpus-helpers-deep` behind
//!    one more crate boundary and behind *type-level* indirection (a generic
//!    function, a trait impl, a trait object, a closure parameter) so that even a
//!    hypothetical whole-workspace one-hop *syntactic* resolver would still see
//!    nothing: `pairs::<T>` contains no `HashMap` token, `<HashSet<String> as
//!    Plan>::steps` contains no `HashMap`/`HashSet` token at the call site, and
//!    `<dyn Jitter as Jitter>::ms` contains no `rand` token anywhere the caller
//!    can see.
//! 2. Own the **ambient state** (statics, thread-locals, interior mutability)
//!    that no HVG or DET rule models at all. HVG007 is the only ambient-state
//!    rule in either layer and it fires exclusively on a literal `.lock()` whose
//!    receiver is an ALL-CAPS path **inside a workflow body**; `fetch_add`,
//!    `load`, `RwLock::read`, `OnceLock::get_or_init`, `LocalKey::with`,
//!    `RefCell::borrow_mut` and `Cell::get` are all unmodeled everywhere.
//!
//! Nothing here carries `#[workflow]`, so the proc-macro guardrails never run on
//! it, and every workflow that reaches it does so across a crate boundary that
//! `det_check`'s same-file/same-module one-hop resolver cannot cross.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::Instant;

use harvest_verify_corpus_helpers_deep as deep;

// ── Ambient process state ────────────────────────────────────────────────────

/// Monotonic sequence shared by every execution in the process.
static SEQ: AtomicU64 = AtomicU64::new(1);
/// Round-robin shard cursor.
static ROUND: AtomicUsize = AtomicUsize::new(0);
/// A tunable that a control plane bumps at runtime.
static FACTOR: AtomicU32 = AtomicU32::new(3);
/// Work parked by other executions on this worker.
static PENDING: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Fan-out window, reconfigurable at runtime.
static WINDOW: RwLock<usize> = RwLock::new(2);
/// Feature flags, resolved from the environment on first read and cached.
static FLAGS: OnceLock<Vec<String>> = OnceLock::new();
/// Process start instant, captured lazily on first use.
static START: OnceLock<Instant> = OnceLock::new();

thread_local! {
    /// Per-thread hit counter.
    static HITS: Cell<u32> = const { Cell::new(0) };
    /// Per-thread sequence behind a `RefCell`.
    static TL_SEQ: RefCell<u64> = const { RefCell::new(0) };
    /// Per-thread paging cursor handed out **by reference** to a caller closure.
    static PAGE_CURSOR: RefCell<u32> = const { RefCell::new(0) };
}

/// `SEQ.fetch_add` — an ambient counter read that also mutates.
#[must_use]
pub fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// `SEQ.load` — a pure ambient read (no mutation), used where a *value* rather
/// than a fresh ticket is wanted.
#[must_use]
pub fn seed() -> u64 {
    SEQ.load(Ordering::Relaxed)
}

/// `ROUND.fetch_add % modulus` — an ambient value used as a branch selector.
#[must_use]
pub fn shard_of(modulus: usize) -> usize {
    ROUND.fetch_add(1, Ordering::SeqCst) % modulus.max(1)
}

/// `ROUND.load` reduced to a bool — decides which of two activities goes first.
#[must_use]
pub fn hot_path() -> bool {
    ROUND.load(Ordering::SeqCst) % 2 == 0
}

/// `FACTOR.load` — read from inside a *caller-supplied closure* in the seeded
/// corpus, so the source sits in a closure body the caller owns.
#[must_use]
pub fn factor() -> u32 {
    FACTOR.load(Ordering::Relaxed)
}

/// `PENDING.lock()` + `mem::take` — both the elements and their order are
/// ambient.
#[must_use]
pub fn drain_pending() -> Vec<String> {
    PENDING
        .lock()
        .map_or_else(|_| Vec::new(), |mut guard| std::mem::take(&mut *guard))
}

/// `WINDOW.read()` — `.read()` is not `.lock()`, so HVG007 would miss this even
/// if it were written in a workflow body.
#[must_use]
pub fn window_size() -> usize {
    WINDOW.read().map_or(2, |guard| *guard)
}

/// An environment-derived flag set memoized in a `OnceLock`: ambient *and*
/// order-of-first-use dependent.
#[must_use]
pub fn flag(name: &str) -> bool {
    FLAGS
        .get_or_init(|| {
            deep::env_region("")
                .split(',')
                .map(str::to_string)
                .collect()
        })
        .iter()
        .any(|f| f == name)
}

/// `HITS` (a `Cell`) bumped through `LocalKey::with`.
#[must_use]
pub fn bump_hits() -> u32 {
    HITS.with(|c| {
        c.set(c.get().wrapping_add(1));
        c.get()
    })
}

/// `TL_SEQ` (a `RefCell`) bumped through `LocalKey::with`.
#[must_use]
pub fn next_tl_seq() -> u64 {
    TL_SEQ.with(|c| {
        *c.borrow_mut() += 1;
        *c.borrow()
    })
}

/// Hands the ambient `PAGE_CURSOR` to a **caller-supplied closure** by
/// reference. The interior mutation therefore happens in the caller's closure
/// body, in the caller's crate, over state the caller never constructed.
pub fn with_page_cursor<R, F>(f: F) -> R
where
    F: FnOnce(&RefCell<u32>) -> R,
{
    PAGE_CURSOR.with(f)
}

// ── Pass-throughs onto the two-hops-deep crate ───────────────────────────────

/// Wall-clock batch label (one hop over `deep::stamp`).
#[must_use]
pub fn batch_label() -> String {
    format!("batch-{}", deep::stamp())
}

/// Wall-clock backoff, one hop over `deep::stamp`, used as a timer duration.
#[must_use]
pub fn backoff_secs() -> u64 {
    deep::stamp() % 30
}

/// Budget left against a lazily-captured process start `Instant`.
#[must_use]
pub fn remaining_secs(budget: u64) -> u64 {
    let start = START.get_or_init(Instant::now);
    budget.saturating_sub(deep::elapsed_secs(*start))
}

/// Thread-id parity (one hop over `deep::worker_slot`).
#[must_use]
pub fn worker_slot_parity() -> u64 {
    deep::worker_slot() % 2
}

/// Process tag (one hop over `deep::origin_tag`).
#[must_use]
pub fn origin_tag() -> String {
    deep::origin_tag()
}

/// Deployment region (one hop over `deep::env_region`).
#[must_use]
pub fn region() -> String {
    deep::env_region("us")
}

/// Three-hop chain, outermost link: pure arithmetic, no source of its own.
#[must_use]
pub fn a(base: u64) -> u64 {
    b(base).wrapping_add(1)
}

/// Three-hop chain, middle link: pure arithmetic plus the call into the leaf.
#[must_use]
pub fn b(base: u64) -> u64 {
    deep::fine_stamp() ^ base
}

/// A fresh v4 UUID. HVG002/DET003 both know `Uuid::new_v4` — and both are blind
/// to it here, one crate away.
#[must_use]
pub fn idem_key() -> uuid::Uuid {
    uuid::Uuid::new_v4()
}

/// A coin flip. HVG002/DET002 both know `rand::random` — one crate away, blind.
#[must_use]
pub fn coin() -> bool {
    rand::random::<bool>()
}

/// A non-durable sleep. Not a taint source at all: a **forbidden effect**, only
/// reachability can find it.
pub async fn pace() {
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
}

// ── Type-level launderers ────────────────────────────────────────────────────

/// The generic launderer for AC3 mandatory #1.
///
/// Contains no `HashMap`/`HashSet` token: the hash-order iteration is
/// `<T as IntoIterator>::into_iter`, and only the **call site's** substitution
/// (`pairs::<HashMap<String, u32>>`) reveals what `T` actually is.
pub fn pairs<T>(source: T) -> Vec<(String, u32)>
where
    T: IntoIterator<Item = (String, u32)>,
{
    source.into_iter().collect()
}

/// A trait whose only implementation iterates a hash collection.
pub trait Plan {
    /// The steps, in whatever order the hasher chose.
    fn steps(&self) -> Vec<String>;
}

impl Plan for HashSet<String> {
    fn steps(&self) -> Vec<String> {
        self.iter().cloned().collect()
    }
}

/// Destroys the order of a **deterministic** input by round-tripping it through
/// a `HashMap` and re-iterating the map — the disorder is manufactured entirely
/// inside this helper, from clean data.
#[must_use]
pub fn normalize(rows: Vec<(String, u32)>) -> Vec<(String, u32)> {
    let indexed: HashMap<String, u32> = rows.into_iter().collect();
    indexed.into_iter().collect()
}

/// Positional selection over hash order: `keys().next()` picks whichever key the
/// hasher happened to put first.
#[must_use]
pub fn any_key(map: &HashMap<String, u32>) -> Option<String> {
    map.keys().next().cloned()
}

/// A non-commutative fold over hash order: the multiset of values is stable but
/// the joined string is not.
#[must_use]
pub fn values_joined(map: &HashMap<String, u32>) -> String {
    map.values()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Invokes a caller-supplied thunk. Used to move a closure's body across the
/// crate boundary.
pub fn apply<F>(f: F) -> u32
where
    F: FnOnce() -> u32,
{
    f()
}

/// Maps a caller-supplied closure over `items`.
pub fn apply_all<F>(items: Vec<u32>, f: F) -> Vec<u32>
where
    F: Fn(u32) -> u32,
{
    items.into_iter().map(f).collect()
}

// ── dyn dispatch: exactly one impl (devirtualizable) ─────────────────────────

/// Retry jitter.
pub trait Jitter {
    /// Milliseconds to wait.
    fn ms(&self) -> u64;
}

/// The one and only `Jitter` implementation in the analyzed set.
struct Live;

impl Jitter for Live {
    fn ms(&self) -> u64 {
        rand::random::<u64>() % 500
    }
}

/// Constructs the trait object **inside the analyzed set**, so an RTA-lite pass
/// over unsizing coercions sees exactly one concrete type flowing into
/// `dyn Jitter` and can devirtualize `<dyn Jitter as Jitter>::ms` to
/// `<Live as Jitter>::ms`. The verdict for the caller must therefore be
/// `nondeterminism-found`, **not** `unknown`.
#[must_use]
pub fn default_jitter() -> Box<dyn Jitter> {
    Box::new(Live)
}

// ── dyn dispatch: two impls (NOT devirtualizable) ────────────────────────────

/// A value fetcher with more than one implementation.
pub trait Fetcher {
    /// The fetched value.
    fn get(&self) -> u64;
}

/// A deterministic implementation.
pub struct Fixed(pub u64);

/// A wall-clock implementation.
pub struct Drifting;

impl Fetcher for Fixed {
    fn get(&self) -> u64 {
        self.0
    }
}

impl Fetcher for Drifting {
    fn get(&self) -> u64 {
        deep::stamp()
    }
}

/// One of the two `dyn Fetcher` construction sites.
#[must_use]
pub fn fixed_fetcher(v: u64) -> Box<dyn Fetcher> {
    Box::new(Fixed(v))
}

/// The other `dyn Fetcher` construction site. With two impl types unsized in
/// the analyzed set, RTA-lite cannot pick one, so a caller that dispatches
/// through `dyn Fetcher` must be reported `unknown` with a `dyn-dispatch`
/// boundary — never guessed.
#[must_use]
pub fn drifting_fetcher() -> Box<dyn Fetcher> {
    Box::new(Drifting)
}

// ── Indirect call through a function pointer ─────────────────────────────────

fn first_or_zero(xs: &[u32]) -> u32 {
    xs.first().copied().unwrap_or(0)
}

fn drifting_pick(xs: &[u32]) -> u32 {
    xs.first()
        .copied()
        .unwrap_or(0)
        .wrapping_add(u32::try_from(deep::stamp() % 7).unwrap_or(0))
}

/// Hands back a **function pointer**. At the call site MIR emits an indirect
/// call (`_0 = move _1(move _2)`) with no callee path at all, so the analyzer
/// must report an `indirect-call` boundary rather than follow it.
#[must_use]
pub fn picker(drift: bool) -> fn(&[u32]) -> u32 {
    if drift { drifting_pick } else { first_or_zero }
}

// ── FFI ──────────────────────────────────────────────────────────────────────

unsafe extern "C" {
    /// libc `abs`; stands in for any foreign function whose body has no MIR.
    fn abs(input: i32) -> i32;
}

/// Calls into C. There is no MIR for `abs`, so the analyzer must stop with an
/// `ffi` boundary rather than assume the call is pure.
#[must_use]
pub fn native_abs(v: i32) -> i32 {
    // SAFETY: `abs` is a pure libc function over a plain `i32`.
    unsafe { abs(v) }
}

// ── Raw-pointer read of a `static mut` ───────────────────────────────────────

/// Mutable process-global state.
pub static mut TICK: u64 = 0;

/// Reads `TICK` through a raw pointer. Arbitrary raw-pointer reads are outside
/// the analyzer's memory model, so the honest answer is an
/// `unsafe-raw-pointer` boundary, not a guess in either direction.
#[must_use]
pub fn tick() -> u64 {
    // SAFETY: `TICK` is a plain `u64`; the read is aligned and initialized.
    unsafe { std::ptr::read(&raw const TICK) }
}
