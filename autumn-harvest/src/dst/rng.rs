//! A seeded pseudo-random number generator for the simulator.

/// The `SplitMix64` generator.
///
/// The simulator owns this generator so that a seed gives the same run on
/// every platform and every `rand` release. The algorithm is fixed by a
/// golden test.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Make a generator from `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next 64 random bits.
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..n`. Returns 0 when `n` is 0.
    ///
    /// The modulo has a small bias. The simulator needs a fixed mapping,
    /// not a uniform one.
    pub const fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }

    /// `true` with probability `percent` / 100.
    pub const fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    #[test]
    fn seed_zero_matches_the_reference_sequence() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(rng.next_u64(), 0x06c4_5d18_8009_454f);
    }

    #[test]
    fn below_stays_in_range_and_handles_zero() {
        let mut rng = SplitMix64::new(7);
        assert_eq!(rng.below(0), 0);
        for _ in 0..1000 {
            assert!(rng.below(5) < 5);
        }
    }

    #[test]
    fn equal_seeds_give_equal_sequences() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
}
