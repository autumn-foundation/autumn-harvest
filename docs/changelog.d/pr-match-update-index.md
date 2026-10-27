perf(replay): the recorded-update lookup is no longer quadratic.
`HistoryMatcher::match_update` scanned the whole history for each update.
It now builds an index of update results once the scans have walked as many
events as the history holds. Behavior is unchanged: the first result wins and
a cut view still hides its tail. Measured with `benches/update_lookup_profile.rs`
under callgrind: 32.19M to 9.11M instructions at 1000 updates (-71.7%),
415.4M to 35.1M at 4000 (-91.5%). The cost is now linear in the history.
