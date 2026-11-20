# ADR 0018: Dates and times use jiff

## Status

Accepted.

## Context

- Harvest used `chrono` 0.4 for every instant and every elapsed time, and
  `chrono-tz` for cron schedules in a named time zone.
- `chrono` keeps an instant as a `DateTime<Utc>` and an elapsed time as a
  `TimeDelta`. The zone is a type parameter. Code that needs a zone at run
  time, such as a cron schedule in a user's zone, needs a second crate.
- `jiff` 1.0 shipped on 2026-11-15 with a stability commitment for 1.x. It
  has one type for an instant (`Timestamp`), one for an exact elapsed time
  (`SignedDuration`), and one for a zoned value (`ZonedDateTime`). It
  bundles the IANA database and resolves a zone name at run time.
- RustSec marks `chrono` and `chrono-tz` unmaintained, with no safe
  upgrade (RUSTSEC-2026-0543 and RUSTSEC-2026-0555). The advisories name
  `jiff` as a replacement.
- Diesel has no `jiff` feature. A foreign trait cannot be implemented for a
  foreign type, so Harvest cannot add the Diesel impls itself.
- Some text that holds a timestamp is a contract. The audit chain hashes
  one, a cursor encodes one, and API fields return one. `chrono` and `jiff`
  print different text for the same instant.

## Decision

1. An instant is a `jiff::Timestamp`. An elapsed time is a
   `jiff::SignedDuration`. A calendar date is a `jiff::civil::Date`. A
   named zone is a `jiff::tz::TimeZone`. These types are in the public API.
   `autumn_harvest::jiff` re-exports the crate, as `autumn_harvest::chrono`
   did before.
2. `jiff` builds with `tzdb-bundle-always`. A host without
   `/usr/share/zoneinfo` still resolves a zone name, as `chrono-tz` did. A
   host database, when present, takes precedence, so a zone follows the
   newest rules on the host. `autumn_harvest::time::zone` resolves a name.
   It falls back to the bundled copy for a name that the host lacks, and
   it keeps the case rule of `chrono-tz`. It accepts exactly the 597 names
   of `chrono-tz` 0.10. It refuses `Factory`, and the host-only names
   `localtime` and `posixrules`. Each name resolves with the same offsets
   through 2099. After 2099, `chrono-tz` kept the last standard-time
   offset. jiff applies the current DST rule of the zone, so a DST zone
   differs by its DST delta after 2099.
3. Diesel reads and writes through wrapper types:
   - `jiff-diesel` 1.0 for `timestamptz`, `timestamp` and `date`. A query
     argument uses `jiff_diesel::ToDiesel::to_diesel`.
   - `autumn_harvest::time::sql` for an `interval`. `PgDuration` keeps the
     rules of the Diesel `chrono` feature: whole days in `days`, the rest
     in `microseconds`, and a 30-day month on a read.
   - A derived model keeps jiff field types and names the wrapper with
     `#[diesel(serialize_as, deserialize_as)]`. The `split_queryable!` and
     `split_insertable!` macros take the same wrapper after `as`.
4. `croner` keeps its parser. `autumn_harvest::time::cron::CronZoned`
   implements the `croner` time trait for a jiff 1.0 `ZonedDateTime`. It
   follows the jiff 0.2 implementation in `croner` line for line. A sweep
   of two years, eight zones and twelve patterns found no difference from
   `croner` on `chrono` in two million comparisons.
5. Contract text keeps the bytes that `chrono` wrote.
   `autumn_harvest::time::format` writes the `to_rfc3339`,
   `to_rfc3339_opts` and `Display` forms of `chrono` exactly. A sweep of
   400 000 instants over every format found no difference. New text uses
   the jiff formats.
6. A `serde` timestamp uses the jiff format: RFC 3339, a `Z` suffix and
   the fewest fraction digits that keep the value exact. `chrono` wrote 0,
   3, 6 or 9 digits. Each library reads the output of the other, so stored
   JSON stays readable in both directions. A stored event or Redis envelope
   reads its timestamps through `time::serde_clamped`. It reads a year that
   `chrono` wrote past 9999 as `Timestamp::MAX`.
   A model duration keeps the `chrono` form `[seconds, nanoseconds]` through
   `time::serde_secs_nanos`. The jiff form is an ISO 8601 string, and the
   two forms cannot read each other.
7. No Harvest crate depends on `chrono` or `chrono-tz`. `autumn-web` 0.10
   held `chrono` types in its webhook delivery log and its tenant restore
   target, so the plugin converted at that boundary. `autumn-web` 0.11
   takes `jiff` values, so the plugin passes them through. `deny.toml`
   bans both crates, and lists the crates that may still wrap them.

## Consequences

- An embedder that names a Harvest time type changes its own code. The
  upgrade guide `docs/upgrading/chrono-to-jiff.md` maps each type and
  method.
- A `chrono` sentinel at the end of its range, such as
  `DateTime::<Utc>::MAX_UTC`, is past the jiff range, which ends in the
  year 9999. A reader of a stored timestamp clamps such a value to
  `Timestamp::MAX`. A calendar exclusion date past 9999-12-31 can match no
  fire date, so a read skips it. A request that sends such a date gets a
  422. `chrono` accepted that request.
- A sum of an instant and a duration that passes the jiff range, but not
  the `chrono` range, saturates to `Timestamp::MAX` or `Timestamp::MIN`.
  `time::checked_add_chrono_range` does it. So a far deadline stays far.
- A leap second `:60` reads as second 59 with its fraction, as jiff reads
  it. `chrono` read the instant one second later.
- `harvest token bootstrap --expires-at` refuses a U+2212 minus sign in the
  offset. Postgres gets the raw text and cannot read that sign.
- A JSON timestamp can have fewer fraction digits than before. A client
  that compares timestamp strings byte for byte, not as instants, sees the
  change.
- The determinism lint and the MIR verifier flag `jiff` clock reads, and
  still flag `chrono` clock reads in user code.
- `chrono-tz` compiled its zone rules into the binary. `jiff` prefers the
  host database. Two hosts with different `tzdata` packages can disagree on
  a zone whose rules changed between those packages. Set `TZDIR` to one
  copy, or keep `tzdata` in step across a fleet, where that matters.
