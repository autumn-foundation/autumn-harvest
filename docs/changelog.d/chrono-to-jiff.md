## Dates and times move from `chrono` to `jiff` (ADR 0018)

Harvest now uses `jiff` 1.0 for every instant, duration, calendar date and
time zone. `chrono` and `chrono-tz` are gone from every Harvest crate except
`autumn-harvest-plugin`. The plugin keeps `chrono` behind its `webhooks` and
`sqlite` features only, because the `autumn-web` API there holds `chrono`
types. `deny.toml` bans both crates everywhere else. ADR 0018 records the
decision. `docs/upgrading/chrono-to-jiff.md` maps each type and call for an
embedder.

- **Public API.** `DateTime<Utc>` became `jiff::Timestamp`,
  `chrono::Duration` became `jiff::SignedDuration`, `NaiveDate` became
  `jiff::civil::Date` and `chrono_tz::Tz` became `jiff::tz::TimeZone`.
  `autumn_harvest::jiff` re-exports the crate. `autumn_harvest::chrono` is
  removed.
- **`autumn_harvest::time`.** New helpers keep the exact results of the old
  calls where a result is a contract: `rfc3339`, `rfc3339_opts` and
  `utc_display` write the bytes that `chrono` wrote. `time::parse` reads
  the text that `chrono` read. `time::zone` resolves each of the 597
  `chrono-tz` names with the same case rules. `from_std_bounded` and
  `checked_add_chrono_range` keep the `chrono` range rules.
  `time::cron::CronZoned` adapts `croner` 4 to jiff 1.0.
- **Diesel.** `time::sql` holds the wrapper types: `SqlTimestamp` and
  `NullableTimestamp` clamp a stored value past the jiff range,
  `PgDuration` keeps the `interval` rules of the Diesel `chrono` feature,
  and `SqlDate` skips a calendar date past the range. `split_queryable!`
  and `split_insertable!` take a wrapper after `as`.
- **Stored data.** No migration and no data rewrite. A process reads the
  rows, events, Redis envelopes and JSON that an older process wrote,
  including a `chrono` value past the year 9999, which clamps. A duration
  in a model keeps the `[seconds, nanoseconds]` JSON form.
- **Range.** jiff ends in the year 9999. A deadline sum that `chrono`
  held now saturates or stores a NULL deadline, so it does not panic.
- **Text.** A `serde` timestamp has the fewest fraction digits that keep
  it exact. `Display` prints RFC 3339. Pinned text keeps its bytes.
- **Determinism.** HVG001, DET001, the host-clock write guard and the MIR
  verifier flag `jiff` clock reads, and still flag `chrono` clock reads.

**Recorded differences.** A calendar exclusion date past the year 9999 is
refused on input. A leap second reads one second earlier. A zone offset
after 2099 follows the current DST rule of the zone. The upgrade guide
lists each one.

**Tests.** Golden tests compare the format helpers with `chrono` output.
Tests of the parse port pin the inputs that the `chrono` grammar accepts
and refuses. The
`croner` adapter matched `croner` on `chrono` in two million comparisons.
Each range, duration, parse and storage difference that the semantic audit
found has a regression test.
