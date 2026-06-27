//! Retry jitter.
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

fn main() {
    let seed = 0xdecafbad_u64;
    let policy = RetryPolicy::exponential(6, std::time::Duration::from_secs(1))
        .with_jitter(JitterPolicy::Equal);

    println!("retry-jitter-example");
    println!("seed={seed}");
    for attempt in 1..policy.max_attempts {
        let delay = policy
            .next_delay_with_seed(attempt, seed)
            .expect("attempt is in range");
        println!("attempt={attempt} delay_ms={}", delay.as_millis());
    }
}
