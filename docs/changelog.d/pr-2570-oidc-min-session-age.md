## OIDC: the shortest session age is two seconds

The login time of an OIDC session has a one-second resolution. A session is
live while its stored age is under `max_session_age`. With the old minimum of
one second, a login late in a second expired at the next clock tick. The user
then looped through the identity provider, which the minimum was there to
stop. CI saw it as a flaky `an_old_session_must_log_in_again`.

- `OidcLogin::with_max_session_age` raises an age under two seconds to two
  seconds. Each session then lasts at least one real second.
- A session can still end up to one second before its age. The docs say so.
- The unit test `the_smallest_age_keeps_a_session_across_one_clock_tick`
  pins the case without a timing race. The contract test now waits past
  three ticks.
