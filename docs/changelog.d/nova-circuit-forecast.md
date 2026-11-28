## Circuit breaker outage forecast (Nova)

`circuit_forecast` replays the circuit breaker state machine against a steady
stream of calls and a dependency that is down for a given time. It answers
how a `CircuitBreakerPolicy` behaves before the outage happens.

`forecast` returns a `BreakerForecast`. It gives the trip time, the calls
that hit the dead dependency, the arrivals that the breaker turns away inside
the outage, the probes and failed probes, and the close time. `recovery_lag`
gives how long a healed dependency stays shielded, and
`shielded_after_recovery` counts the healthy calls turned away in that time.
`Traffic` sets the arrival interval and the result latency, so a slow
dependency shows how many more calls start before the trip.

The module is pure and in-memory. It adds no event, no migration and no
query. Eight unit tests and one doctest cover the threshold, the window, the
latency, the cooldown trade-off and the arrival cap. One test drives the live
`CircuitBreakerRegistry` over 216 policy, latency and outage combinations and
asserts that its counts equal the forecast.
