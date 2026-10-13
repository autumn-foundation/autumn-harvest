//! A bulk redrive puts each row inside its own slot (issue #1832).
//!
//! `dlq::spread_offset_nanos` gives row `index` of `count` an offset in a
//! window. The offset is the slot start plus a jitter of
//! `floor(slot * mix / 2^64)`. These proofs show three things for every
//! input:
//!
//! - No step overflows. The product `slot * mix` can pass `u128::MAX`, so
//!   the code splits `slot` at bit 64.
//! - The split computes the exact jitter.
//! - The jitter is below one slot, so a row never leaves its slot, and no
//!   row leaves the window.

use vstd::arithmetic::div_mod::{lemma_div_multiples_vanish_fancy, lemma_fundamental_div_mod};
use vstd::arithmetic::mul::{lemma_mul_inequality, lemma_mul_strict_inequality};
use vstd::prelude::*;

verus! {

/// 2^64.
pub open spec fn two64() -> int {
    0x1_0000_0000_0000_0000
}

/// The jitter of a slot, `floor(slot * mix / 2^64)`, in exact arithmetic.
pub open spec fn spec_jitter(slot: int, mix: int) -> int {
    (slot * mix) / two64()
}

/// `a.saturating_add(b)` or `a.saturating_mul(b)` as a spec.
pub open spec fn saturate(n: int) -> int {
    if n > u128::MAX as int { u128::MAX as int } else { n }
}

/// The split of `slot` at bit 64, as the code computes it.
proof fn lemma_split(slot: u128) by (bit_vector)
    ensures
        slot >> 64 < 0x1_0000_0000_0000_0000u128,
        slot & 0xffff_ffff_ffff_ffffu128 < 0x1_0000_0000_0000_0000u128,
        slot == (slot >> 64) * 0x1_0000_0000_0000_0000u128 + (slot & 0xffff_ffff_ffff_ffffu128),
{
}

/// A shift right by 64 is a division by 2^64.
proof fn lemma_shr64(p: u128) by (bit_vector)
    ensures
        p >> 64 == p / 0x1_0000_0000_0000_0000u128,
{
}

/// `floor((h * 2^64 + l) * m / 2^64) == h * m + floor(l * m / 2^64)`.
proof fn lemma_split_jitter(h: int, l: int, m: int)
    requires
        0 <= h,
        0 <= l,
        0 <= m,
    ensures
        spec_jitter(h * two64() + l, m) == h * m + (l * m) / two64(),
{
    let d = two64();
    let q = (l * m) / d;
    let r = (l * m) % d;
    lemma_fundamental_div_mod(l * m, d);
    assert((h * d + l) * m == d * (h * m + q) + r) by (nonlinear_arith)
        requires l * m == d * q + r;
    lemma_div_multiples_vanish_fancy(h * m + q, r, d);
}

/// The jitter is below one slot, because `mix < 2^64`.
proof fn lemma_jitter_below_slot(slot: int, mix: int)
    requires
        0 <= slot,
        0 <= mix < two64(),
    ensures
        0 <= spec_jitter(slot, mix),
        slot > 0 ==> spec_jitter(slot, mix) < slot,
        slot == 0 ==> spec_jitter(slot, mix) == 0,
{
    assert(0 <= slot * mix) by (nonlinear_arith)
        requires 0 <= slot, 0 <= mix;
    lemma_fundamental_div_mod(slot * mix, two64());
    if slot > 0 {
        lemma_mul_strict_inequality(mix, two64(), slot);
        assert(slot * mix < slot * two64()) by (nonlinear_arith)
            requires mix * slot < two64() * slot;
        assert(spec_jitter(slot, mix) < slot) by (nonlinear_arith)
            requires
                slot * mix < slot * two64(),
                slot * mix == two64() * spec_jitter(slot, mix) + (slot * mix) % two64(),
                0 <= (slot * mix) % two64(),
                0 < two64();
    }
}

/// The offset of row `index` lies in its slot, and in the window.
pub proof fn lemma_offset_in_slot(index: int, count: int, window: int, mix: int)
    requires
        0 <= index < count,
        0 <= window,
        0 <= mix < two64(),
    ensures
        ({
            let slot = window / count;
            let offset = slot * index + spec_jitter(slot, mix);
            &&& slot * index <= offset
            &&& slot > 0 ==> offset < slot * (index + 1)
            &&& window > 0 ==> offset < window
        }),
{
    let slot = window / count;
    lemma_jitter_below_slot(slot, mix);
    lemma_fundamental_div_mod(window, count);
    assert(0 <= slot) by (nonlinear_arith)
        requires 0 <= window, 0 < count, slot == window / count;
    lemma_mul_inequality(index + 1, count, slot);
    assert(slot * (index + 1) <= slot * count) by (nonlinear_arith)
        requires (index + 1) * slot <= count * slot;
    assert(slot * (index + 1) == slot * index + slot) by (nonlinear_arith);
    assert(slot * count <= window) by (nonlinear_arith)
        requires
            window == count * slot + window % count,
            0 <= window % count;
    if window > 0 && slot == 0 {
        assert(slot * index == 0) by (nonlinear_arith)
            requires slot == 0;
    }
}

// mirror: autumn-harvest/src/dlq.rs spread_offset_nanos
pub fn spread_offset_nanos(index: u128, count: u128, window: u128, mix: u64) -> (r: u128)
    requires
        count > 0,
    ensures
        ({
            let slot = (window / count) as int;
            r == saturate(saturate(slot * index) + spec_jitter(slot, mix as int))
        }),
        index < count ==> r == (window / count) as int * index
            + spec_jitter((window / count) as int, mix as int),
{
    let slot = window / count;
    let mix = u128::from(mix);
    proof {
        lemma_split(slot);
        assert((slot >> 64) * mix < 0x1_0000_0000_0000_0000 * 0x1_0000_0000_0000_0000)
            by (nonlinear_arith)
            requires
                slot >> 64 < 0x1_0000_0000_0000_0000u128,
                mix < 0x1_0000_0000_0000_0000u128;
        assert((slot & 0xffff_ffff_ffff_ffffu128) * mix
            < 0x1_0000_0000_0000_0000 * 0x1_0000_0000_0000_0000)
            by (nonlinear_arith)
            requires
                slot & 0xffff_ffff_ffff_ffffu128 < 0x1_0000_0000_0000_0000u128,
                mix < 0x1_0000_0000_0000_0000u128;
    }
    let high = (slot >> 64) * mix;
    let low = ((slot & u128::from(u64::MAX)) * mix) >> 64;
    proof {
        let h = (slot >> 64) as int;
        let l = (slot & 0xffff_ffff_ffff_ffffu128) as int;
        lemma_shr64(((slot & 0xffff_ffff_ffff_ffffu128) * mix) as u128);
        lemma_split_jitter(h, l, mix as int);
        lemma_jitter_below_slot(slot as int, mix as int);
        if index < count {
            lemma_offset_in_slot(index as int, count as int, window as int, mix as int);
        }
    }
    slot.saturating_mul(index).saturating_add(high + low)
}

} // verus!
