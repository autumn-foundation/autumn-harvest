//! The retry jitter stays in its range for every input (issue #1792).
//!
//! The Kani proofs in `policy.rs` stub the mixer, because CBMC does not
//! finish with the real one. These proofs call the real mixer of
//! `mixers.rs`, so no stub is in the argument.

use crate::mixers::mix64;
use vstd::prelude::*;

verus! {

// mirror: autumn-harvest/src/policy.rs uniform_inclusive
pub fn uniform_inclusive(seed: u64, lo: u64, hi: u64) -> (r: u64)
    ensures
        lo <= hi ==> lo <= r && r <= hi,
{
    let range = hi.wrapping_sub(lo).wrapping_add(1);
    let offset = if range == 0 {
        mix64(seed)
    } else {
        mix64(seed) % range
    };
    lo.wrapping_add(offset)
}

// mirror: autumn-harvest/src/policy.rs full_jitter_nanos
pub fn full_jitter_nanos(hi: u64, seed: u64) -> (r: u64)
    ensures
        r <= hi,
{
    uniform_inclusive(seed, 0, hi)
}

// mirror: autumn-harvest/src/policy.rs equal_jitter_nanos
pub fn equal_jitter_nanos(hi: u64, seed: u64) -> (r: u64)
    ensures
        hi / 2 <= r && r <= hi,
{
    if hi <= 1 {
        return hi;
    }
    uniform_inclusive(seed, hi / 2, hi)
}

} // verus!
