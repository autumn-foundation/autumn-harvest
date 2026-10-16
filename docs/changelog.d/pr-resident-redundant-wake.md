## Fix — A redundant wake keeps resident activities parked (issue #2008)

**Behaviour change.** A workflow task can run right after a decision that
already read every result. Its delta then holds the events of the last
suspension and no result. The resident path used to decline that
delta with `ResumeDeclined::NoResolution`. The worker then dropped the
parked future and replayed the whole history, and the decision reported
`declined`.

Now each parked activity waits again, with its live sender. The cycle runs
as the speculation of issue #2008: it must only wait, and it must not read
the replay position. Otherwise it declines with
`ResumeDeclined::SiblingStillParked`, as before. A lone timer or signal
cannot wait again this way, so its redundant wake still declines with
`NoResolution`. A delta shorter than the events of the last suspension
also still declines.

**Why.** `resident_outcome_tests::an_agent_loop_with_parallel_tool_calls_hits_after_the_first_decision`
failed in about 10 to 25% of runs against Postgres 16. Each failure was one
`declined` decision with a delta of only `ActivityScheduled`. The outcome
was correct, but the cold replay was wasted work.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.** `a_wake_with_nothing_new_waits_again` resumes with only the
events of the last suspension, checks that the cycle only waits again,
and then completes on the real result. It failed before the fix.
`a_wake_with_nothing_new_declines_for_a_lone_timer` pins the timer
decline. The "own events only" row left
`deltas_that_replay_could_read_differently_decline`. Its "empty delta"
row still declines.
