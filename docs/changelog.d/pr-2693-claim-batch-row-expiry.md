## Batch claim: an expired run stops only its own row (issue #2693)

The batch claim re-checks each run deadline against the clock that it reads
after the rate-limit bucket lock. Before, `run_expired_now` was one `EXISTS`
over the whole batch. A run that passed its deadline while the claim waited
on the lock stopped every row of the batch, and the worker saw an empty
claim. The batch form now lists the expired rows in `run_expired_now`. A new
materialized CTE, `claim_live`, holds the other rows, and `rate_limit_debit`
and `claimed` read it in place of `candidate`. An expired row then spends no
token and takes no concurrency-key lock. A per-row filter inside `claimed`
would let Postgres test the key lock first, and hold it for a row that the
claim then drops. The single-row and window claim texts do not change.

Evidence:

- `a_run_that_expires_during_the_claim_stops_only_its_own_row` holds the
  bucket lock past the deadline of the first row. It claimed no row before
  the change. It now claims the three other rows, and the expired row stays
  `PENDING` with its token unspent.
- `an_expired_row_takes_no_key_lock_and_its_peers_still_claim` expires a
  keyed row and an unkeyed row during one claim. The claim takes the two
  live rows and holds no advisory lock. A per-row filter in `claimed` held
  one.
- pgbench: k = 8 costs 1.19x k = 1.
