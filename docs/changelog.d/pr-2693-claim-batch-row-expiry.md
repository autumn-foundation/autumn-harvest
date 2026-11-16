## Batch claim: an expired run stops only its own row (issue #2693)

The batch claim re-checks each run deadline against the clock that it reads
after the rate-limit bucket lock. Before, `run_expired_now` was one `EXISTS`
over the whole batch. A run that passed its deadline while the claim waited
on the lock stopped every row of the batch, and the worker saw an empty
claim. The batch form now lists the expired rows, and `rate_limit_debit` and
`claimed` skip those rows only. The single-row and window claim texts do not
change.

Evidence: `a_run_that_expires_during_the_claim_stops_only_its_own_row` holds
the bucket lock past the deadline of the first row. It claimed no row before
the change. It now claims the three rows of the other runs, and the expired
row stays `PENDING` with its token unspent.
