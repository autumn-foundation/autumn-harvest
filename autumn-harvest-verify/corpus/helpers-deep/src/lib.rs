//! `harvest-verify-corpus-helpers-deep` — the **two-hops-away** crate of the
//! issue #962 seeded determinism corpus.
//!
//! Every function here is a genuine non-determinism source wearing an innocent
//! name (`stamp`, `worker_slot`, `origin_tag`, …). Nothing in this crate is a
//! `#[workflow]`, so:
//!
//! * **HVG001–HVG011** never look at it — the `#[workflow]` proc macro lints the
//!   annotated function **body only** (`DeterminismVisitor::visit_item_fn` over
//!   the item the attribute is attached to). There is no attribute here.
//! * **DET001–DET011** never look at it either — `det_check::check_paths` scans
//!   `#[workflow]` bodies plus **one hop** of helpers resolved **same-file +
//!   same-module**. This crate is two crates and many files away.
//!
//! Consequently a corpus workflow that reaches any of these through
//! `harvest-verify-corpus-helpers` is invisible to the entire syntactic layer
//! while being flagrantly non-deterministic — which is exactly the point.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Wall-clock epoch seconds. The workhorse source for the "SystemTime two
/// crates deep" family (AC3 mandatory #4).
#[must_use]
pub fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Wall-clock sub-second nanos — the leaf of the three-hop chain, chosen so the
/// value differs between two runs a second apart *and* two runs a millisecond
/// apart.
#[must_use]
pub fn fine_stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
}

/// Monotonic elapsed seconds since `since`.
///
/// `Instant::elapsed` is deliberate: neither HVG001 nor DET001 has an
/// `.elapsed()` pattern — both match only the `Instant::now` / `SystemTime::now`
/// *constructors* — so this call escapes the syntactic layer even when written
/// directly in a workflow body.
#[must_use]
pub fn elapsed_secs(since: Instant) -> u64 {
    since.elapsed().as_secs()
}

/// A hash of the current OS thread id. Differs between worker threads, so a
/// replay on another thread takes the other branch.
#[must_use]
pub fn worker_slot() -> u64 {
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    hasher.finish()
}

/// A per-process tag. `std::process::id()` is DET005 — a **Warning**, which
/// never blocks a build — and has no HVG twin at all.
#[must_use]
pub fn origin_tag() -> String {
    format!("w{}", std::process::id())
}

/// An environment lookup spelled with `var_os`.
///
/// DET004's pattern table has `env::var(` / `env::vars(` / `env::args(` but
/// **no** `var_os` spelling, so this escapes det_check even in a same-module
/// helper. HVG003 does cover `env::var_os`, but only in a workflow body.
#[must_use]
pub fn env_region(default: &str) -> String {
    std::env::var_os("HARVEST_CORPUS_REGION")
        .and_then(|v| v.into_string().ok())
        .unwrap_or_else(|| default.to_string())
}
