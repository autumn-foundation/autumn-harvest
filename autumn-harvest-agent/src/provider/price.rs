//! The price of a model, and the cost of one call.

use crate::TokenUsage;

/// The price of one model, in billionths of a US dollar per token.
///
/// A price per million tokens in US dollars is the same number in millionths
/// of a dollar per token. This unit is a thousand times smaller again. So a
/// cache read of `$0.01` per million tokens is a whole number: `10`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Price {
    /// One uncached prompt token.
    pub input: u64,
    /// One output token.
    pub output: u64,
    /// One prompt token read from the cache.
    pub cache_read: u64,
    /// One prompt token written to the cache.
    pub cache_write: u64,
}

impl Price {
    /// A price with no cache rates. A cache read or write costs as much as an
    /// uncached prompt token.
    #[must_use]
    pub const fn flat(input: u64, output: u64) -> Self {
        Self {
            input,
            output,
            cache_read: input,
            cache_write: input,
        }
    }

    /// The cost of one call with `usage`, in millionths of a US dollar.
    ///
    /// `usage.input_tokens` holds the whole prompt. The cache parts are
    /// priced at their own rates, and the rest at the input rate. The cost
    /// rounds to the nearest millionth.
    #[must_use]
    pub fn cost_usd_micros(&self, usage: &TokenUsage) -> u64 {
        let uncached = usage
            .input_tokens
            .saturating_sub(usage.cache_read_tokens)
            .saturating_sub(usage.cache_write_tokens);
        let nanos = u128::from(uncached) * u128::from(self.input)
            + u128::from(usage.cache_read_tokens) * u128::from(self.cache_read)
            + u128::from(usage.cache_write_tokens) * u128::from(self.cache_write)
            + u128::from(usage.output_tokens) * u128::from(self.output);
        u64::try_from(nanos.saturating_add(500) / 1_000).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_part_of_the_prompt_has_its_own_rate() {
        let price = Price {
            input: 4_000,
            output: 20_000,
            cache_read: 200,
            cache_write: 5_000,
        };
        let mut usage = TokenUsage::new(3_000_000, 1_000_000);
        usage.cache_read_tokens = 1_000_000;
        usage.cache_write_tokens = 1_000_000;
        assert_eq!(price.cost_usd_micros(&usage), 29_200_000);
    }

    #[test]
    fn a_flat_price_rounds_to_the_nearest_millionth() {
        let price = Price::flat(150, 600);
        assert_eq!(price.cost_usd_micros(&TokenUsage::new(1, 0)), 0);
        assert_eq!(price.cost_usd_micros(&TokenUsage::new(0, 1)), 1);
        assert_eq!(price.cost_usd_micros(&TokenUsage::new(1_000, 1_000)), 750);
        let max = TokenUsage::new(u32::MAX, u32::MAX);
        assert!(Price::flat(u64::MAX, u64::MAX).cost_usd_micros(&max) > 0);
    }
}
