use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use criterion::{Criterion, criterion_group, criterion_main};

fn bench_retry_jitter(c: &mut Criterion) {
    let base = RetryPolicy::exponential(10, std::time::Duration::from_millis(250));
    let full = base.clone().with_jitter(JitterPolicy::Full);
    let equal = base.clone().with_jitter(JitterPolicy::Equal);
    let deco = base.clone().with_jitter(JitterPolicy::Decorrelated);
    let seed = 0x1234_5678_9abc_def0_u64;

    c.bench_function("retry_delay_none", |b| {
        b.iter(|| {
            for attempt in 1..10 {
                std::hint::black_box(base.next_delay_with_seed(attempt, seed));
            }
        })
    });
    c.bench_function("retry_delay_full", |b| {
        b.iter(|| {
            for attempt in 1..10 {
                std::hint::black_box(full.next_delay_with_seed(attempt, seed));
            }
        })
    });
    c.bench_function("retry_delay_equal", |b| {
        b.iter(|| {
            for attempt in 1..10 {
                std::hint::black_box(equal.next_delay_with_seed(attempt, seed));
            }
        })
    });
    c.bench_function("retry_delay_decorrelated", |b| {
        b.iter(|| {
            for attempt in 1..10 {
                std::hint::black_box(deco.next_delay_with_seed(attempt, seed));
            }
        })
    });
}

criterion_group!(benches, bench_retry_jitter);
criterion_main!(benches);
