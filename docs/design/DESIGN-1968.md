# Design — Issue #1968: calendar defects

Issue #1968 lists three defects in `calendar.rs`:

1. The docs give the business-day scan window as `n * 7 + 14`. The code uses
   `n * 7 + 30`.
2. With a 31-day closure, `add_business_days(a, 0)` rejects and
   `add_business_days(a, 1)` succeeds. The `SCAN_BOUND_SLACK_DAYS` comment
   says that this must not occur.
3. `apply_skip_policy` panics at `NaiveDate::MAX` and `NaiveDate::MIN`.

**No migration. No new `WorkflowEvent` variant. No route change.**

---

## 0. Planning record

### 0.1 Brainstorm — how can item 2 be fixed?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Change the comment only. Say that the guarantee holds up to 30 days. | Rejected. The defect stays. A test cannot prove it. |
| B2 | Use `derived_scan_bound(MAX_BUSINESS_DAYS)` for every `n`. | Rejected. A degenerate calendar then scans 70 years before it rejects. |
| B3 | Use `7 * max(n, 1) + 30`. | Rejected. `n = 1` against `n = 2` has the same defect: a 38-day closure rejects `n = 1` and accepts `n = 2`. |
| B4 | Limit the length of one run of consecutive non-business days, not the total scan. | **Adopted.** See §1. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Count the run off by one. A 30-day closure then rejects. | Two boundary tests: a 30-day closure resolves, a 31-day closure rejects. |
| R2 | Let the run bound accept a calendar that excludes every day. The scan then never stops. | The run bound applies to every examined date. A test excludes 60 days and expects `ScanExhausted`. |
| R3 | Move the coverage or overflow checks. | The loop keeps their order. The existing tests stay green. |
| R4 | Change a recorded workflow result on replay. | `timer_business_days` freezes the result in a side effect. Replay does not run the scan. |
| R5 | Fix the `+` in the scan loop but keep `-` unchecked. | Tests for `MAX` with `RunNextBusinessDay` and `MIN` with `RunPrevBusinessDay`. |
| R6 | Fix the first step but keep the loop step unchecked. | A test starts one day before `MAX`, with both days excluded. |
| R7 | Make the scan expensive. | Worst case is `(n + 1) * 31` dates, which is 113,181 for `MAX_BUSINESS_DAYS`. The old worst case was 25,581. Both are cheap. The frozen `skipped` list grows the same way. Only a calendar with one business day in each 31 days gets there. |

### 0.3 Six thinking hats — run bound (B4)

| Hat | Notes |
|-----|-------|
| White | The old bound is `7n + 30`. It is not monotone in `n`. The workflow path freezes each result. `ScanExhausted { scanned_days }` is serialized. |
| Red | "A closure longer than a month is degenerate" is easy to explain. |
| Black | Before, a large `n` accepted a closure longer than 30 days. It now rejects. Such a calendar is degenerate. `n = 0` rejected it already. The change also relaxes: many short closures no longer reject a large `n`. The changelog fragment states both. |
| Yellow | Monotone by construction: the scan for `m < n` is a prefix of the scan for `n`. The rejection text becomes exactly true. One constant holds the bound. |
| Green | B1, B2 and B3 in §0.1. |
| Blue | Red phase: tests for items 2 and 3 fail. Green phase: change the loops. Refactor phase: correct the docs for item 1, then review. |

---

## 1. Design

### 1.1 Run bound (items 1 and 2)

`MAX_NON_BUSINESS_RUN_DAYS = 30` replaces `SCAN_BOUND_SLACK_DAYS` and
`derived_scan_bound`. The scan counts consecutive non-business days. A
business day sets the count to zero. When the count gets to 31, the scan
returns `ScanExhausted { scanned_days: 31 }`.

`n = 0` keeps its old accept set: it rejects only when the 31 days from the
anchor are all non-business days.

### 1.2 Checked skip scan (item 3)

`apply_skip_policy_with` uses `checked_add_days` and `checked_sub_days`. When
the scan leaves the `NaiveDate` range, it returns `None`, as for an exhausted
365-day window.

## 2. Tests

| Test | Phase |
|------|-------|
| `smaller_n_is_never_rejected_where_larger_n_succeeds` (unit) | Red, then green |
| `a_long_closure_after_the_anchor_rejects` (unit) | Red, then green |
| `a_month_long_closure_resolves_for_every_n` (unit) | Green guard |
| `add_business_days_scan_exhaustion_rejects` (unit, value now 31) | Red, then green |
| `apply_skip_policy_*naivedate*` (unit, three tests) | Red, then green |
| `business_day_props` (property): monotone in `n`; never panics | Red, then green |
