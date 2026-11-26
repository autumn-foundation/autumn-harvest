## Outage ride-through (Nova)

`retry_ride_through` joins the retry envelope and the retry storm model for
one call. It answers how long an outage a retry policy always survives
before its deadline, and how long a healed dependency waits for the next try.

`profile` lists the start window of each attempt during an outage. A failed
attempt takes `failure_cost`, so the same policy can be checked against a
refused connection and against a hang. `recovery` classifies one outage as
guaranteed, possible or exhausted for every jitter seed.
`guaranteed_ride_through` gives the longest outage that every seed survives.
`worst_recovery_lag` gives the longest wait after the outage ends. The module
is pure and in-memory. It adds no event, no migration and no query.

Twelve unit tests and one doctest in `retry_ride_through.rs` cover the windows,
the deadline, jitter, a hang, the conditional failure window, the recovery
lag and the attempt horizon.
