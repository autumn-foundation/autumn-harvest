## Engine — Timeout lanes share one refill page (issue #2126)

Before this change, the four task-timeout lanes read the same page of live
rows with four statements. Lanes at one sweep clock and one cursor now share
one statement, and each lane tests its predicate once. Lanes that start a
sweep in one pass take one clock. A later page also bounds the plain index
key, so its index scan stops at the page. At 100,000 live rows over 8
passes, refill buffers fall 75% (60,528 → 14,958), refill temp blocks fall
85% (39,383 → 6,017) and refill server time falls 75% (2,871 ms → 709 ms).
Each row keeps the same reason. No migration, no schema change, no
`harvest_events` write.

Tests: `lanes_at_one_position_share_one_page_read`,
`lanes_in_step_read_each_later_page_once` and
`a_shared_refill_gives_each_row_its_first_reason` (`scanner_lease_tests`),
and `a_seeded_pass_gives_each_row_its_first_reason`
(`timeout_refill_shared_page_perf`). Evidence:
[`docs/performance-timeout-refill-shared-page.md`](../performance-timeout-refill-shared-page.md).
