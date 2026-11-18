# Upgrading: `chrono` to `jiff`

Harvest now uses [`jiff`](https://docs.rs/jiff) 1.0 for dates and times. It
no longer uses `chrono` or `chrono-tz`. [ADR 0018](../adr/0018-jiff-date-and-time.md)
records the decision. This page tells an embedder what to change.

The change needs no migration and no data rewrite. A process with this
change reads the rows, events and JSON that an older process wrote. An
older process reads what this one writes.

---

## 1. Types

| `chrono` | `jiff` |
|---|---|
| `DateTime<Utc>` | `jiff::Timestamp` |
| `chrono::Duration`, `TimeDelta` | `jiff::SignedDuration` |
| `NaiveDate` | `jiff::civil::Date` |
| `NaiveDateTime` | `jiff::civil::DateTime` |
| `chrono_tz::Tz` | `jiff::tz::TimeZone` |
| `DateTime<Tz>` | `jiff::ZonedDateTime` |

`autumn_harvest::jiff` re-exports the crate. Add `jiff = "1"` to your own
manifest if you name its types directly.

## 2. Common calls

| `chrono` | `jiff` |
|---|---|
| `Utc::now()` | `Timestamp::now()` |
| `Duration::seconds(n)` | `SignedDuration::from_secs(n)` |
| `Duration::milliseconds(n)` | `SignedDuration::from_millis(n)` |
| `Duration::minutes(n)`, `hours(n)` | `SignedDuration::from_mins(n)`, `from_hours(n)` |
| `Duration::days(n)` | `autumn_harvest::time::days(n)` |
| `Duration::from_std(d)` | `SignedDuration::try_from(d)` |
| `d.to_std()` | `std::time::Duration::try_from(d)` |
| `d.num_seconds()` | `d.as_secs()` |
| `d.num_milliseconds()` | `autumn_harvest::time::num_milliseconds(d)` |
| `a - b` (two instants) | `a - b`, or `a.duration_since(b)` |
| `t.checked_add_signed(d)` | `t.checked_add(d).ok()` |
| `t.timestamp()`, `timestamp_millis()` | `t.as_second()`, `t.as_millisecond()` |
| `DateTime::from_timestamp_millis(ms)` | `Timestamp::from_millisecond(ms).ok()` |
| `Utc.with_ymd_and_hms(..).single()` | `autumn_harvest::time::utc_ymd_hms(..)` |
| `DateTime::parse_from_rfc3339(s)` | `s.parse::<Timestamp>()` |
| `t.to_rfc3339()` | `autumn_harvest::time::rfc3339(t)` |
| `t.to_rfc3339_opts(f, z)` | `autumn_harvest::time::rfc3339_opts(t, f, z)` |
| `"Europe/Paris".parse::<Tz>()` | `TimeZone::get("Europe/Paris")` |

The `autumn_harvest::time` helpers keep the exact results of the `chrono`
calls. Use them where the old text or the old `i64` is a contract.

`SignedDuration::try_from(d)` accepts up to `i64::MAX` seconds.
`Duration::from_std(d)` refused a value past `i64::MAX` milliseconds. Use
`autumn_harvest::time::from_std_bounded(d)` where that bound matters.

`s.parse::<Timestamp>()` refuses some text that `parse_from_rfc3339`
read: more than 9 fraction digits, and a U+2212 minus sign in the offset.
It also refuses an instant after `9999-12-30T22:00:00.999999999Z`, the
end of the jiff range.

## 3. Text and JSON

- **JSON.** A `serde` timestamp is still RFC 3339 with a `Z` suffix. It now
  has the fewest fraction digits that keep the value exact:
  `2026-11-18T09:30:00.1Z`, not `2026-11-18T09:30:00.100Z`. Parse the field
  as an instant. Do not compare the strings byte for byte.
- **`Display`.** `Timestamp` prints `2026-11-18T09:30:00Z`. `chrono` printed
  `2026-11-18 09:30:00 UTC`. Use `autumn_harvest::time::utc_display` if you
  need the old form.
- **Fields that Harvest pins.** The audit chain, cursors and the API fields
  that `chrono` formatted with a fixed format keep their exact bytes.
- **Durations.** A duration in a Harvest model, such as
  `WorkflowExecution::execution_timeout`, keeps the `chrono` JSON form
  `[seconds, nanoseconds]`. jiff writes a `SignedDuration` as an ISO 8601
  string, such as `PT1H`. Use `autumn_harvest::time::serde_secs_nanos` on
  your own field if an older reader expects the pair.

## 4. Range

`jiff` holds the years -9999 to 9999. `chrono` held about 262 000 years.
Harvest clamps a stored timestamp past that range to `Timestamp::MIN` or
`Timestamp::MAX` when it reads one. That covers rows, stored events and
Redis envelopes. `autumn_harvest::time::serde_clamped` does the same for
your own JSON field. A calendar exclusion date past 9999-12-31 can match no
fire date, so Harvest skips it on a read. Your own code that writes such a
value must use `Timestamp::MAX`.

## 5. Time zones

A cron schedule in a named zone resolves the zone with
`autumn_harvest::time::zone`. It reads the host database
(`/usr/share/zoneinfo`, or `TZDIR`). A name that the host lacks, such as
`US/Eastern` on a Debian host without `tzdata-legacy`, comes from the copy
that `jiff` bundles. `chrono-tz` always used its bundled copy. Each of the
597 names that `chrono-tz` 0.10 knows still resolves, with the same case
rules. No other name resolves. `localtime`, `posixrules` and `Factory` are
refused, as before. Keep `tzdata` in step across a fleet if a zone that you
schedule in changes its rules.

The offsets match `chrono-tz` through 2099. `chrono-tz` kept the last
standard-time offset of a zone after 2099. jiff applies the current DST
rule of the zone. So a preview, backfill or fire after 2099 in a DST zone
moves by the DST delta.

Call `autumn_harvest::time::zone`, not `TimeZone::get`, for a name that
`chrono-tz` accepted. `TimeZone::get` ignores case and has no fallback.

## 6. Diesel

Diesel has no `jiff` feature. If you query Harvest tables with Diesel, use
the wrapper types:

```rust,ignore
use jiff_diesel::ToDiesel;

harvest_workflow_executions::table
    .filter(harvest_workflow_executions::started_at.ge(since.to_diesel()))
```

A derived model names its wrapper per field:

```rust,ignore
#[derive(Queryable, Selectable)]
#[diesel(table_name = my_table)]
struct Row {
    #[diesel(deserialize_as = jiff_diesel::Timestamp)]
    created_at: jiff::Timestamp,
}
```

`autumn_harvest::time::sql::PgDuration` maps a `SignedDuration` to an
`interval` with the old rules.
