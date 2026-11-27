## Breaker tape (Nova)

`breaker_tape` replays a traffic trace through the production
`CircuitBreakerRegistry`. It answers how fast one breaker policy detects an
outage, how many calls it wastes on a dead dependency, and how long it keeps a
healed dependency dark.

`replay` takes call times and outages. A call inside an outage fails with a
retryable error, and every other call succeeds. The report lists each call as
dispatched, probed or short-circuited. It counts wasted, shielded and blocked
healthy calls. `OutageScore` gives the detection lag and the recovery lag of
each outage. `steady_calls` builds a uniform trace. The module is pure and
in-memory. It adds no event, no migration and no query.

Ten unit tests and one doctest in `breaker_tape.rs` cover a healthy tape, a
dense outage, probes, sparse traffic, an outage past the tape, a short outage,
back-to-back outages, the rolling window and unsorted input.
