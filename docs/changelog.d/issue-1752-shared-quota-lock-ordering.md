## Fix — one shared quota-lock ordering for debounce and throttle (issue #1752)

The `debounce` and `throttle` scanners each held a copy of the same five
functions. They order a claimed batch by advisory-lock id, so two scanner
transactions cannot form an ABBA deadlock (issue #1230 Finding 2).

The functions now live once, in `quota_lock_order`. Each scanner row type
implements `QuotaLockRow`, which gives the workflow name and the quota input.
Both scanners call `order_due_rows_for_deadlock_free_firing` from there.

The `quota-lock-ordering-sync.py` audit and its CI step are removed. They
only kept the two copies identical, and there is now one copy.

Behavior does not change. The lock order, the stable sort, and the single
`hashtext` round trip are the same. The throttle fairness caveat now sits at
its call site.

No migration, no route change, and no `harvest_events` change.

Tests: the ordering tests moved to `quota_lock_order` and run on a test row
type. A source test checks that neither scanner defines a local copy again.
