## Phase — `HarvestMetricsRecorder` gains framework-neutral Prometheus rendering (issue #1611)

Part of epic #1605's Tier 2 (make the non-autumn-web integration path
first-class). `HarvestMetricsRecorder` aggregates the ADR-0001 §7
catalogue metrics entirely in-process, but the only way to read them
back out was `impl MetricsSource`, whose rendering happens inside
autumn-web's shared `/actuator/prometheus` endpoint. A standalone
embedder could enable the recorder — `HarvestBuilder::telemetry(..)`
accepts it, and the engine's background samplers run under it — with no
way to serve what accumulated. The module docs called this "a built-in
Prometheus scrape endpoint," true only on the plugin path.

**The change.** `HarvestMetricsRecorder::render_prometheus(&self) ->
String` renders the same families `MetricsSource::collect` builds — one
source of truth, not a second rendering path to keep in sync — into
Prometheus text exposition format: `# HELP`/`# TYPE` per family, one
line per sample, counter/gauge kind, and the same backslash/quote/
newline label-value escaping and `+Inf`/`-Inf`/`NaN` value formatting
the plugin path's own renderer uses. A standalone embedder serves it
from a route of their own in three lines; no exporter, no new
dependency.

**Wired into the reference example.** `examples/standalone-runner` had
no metrics recorder at all before this — the epic's own audit named it
as one of the gaps a standalone embedder inherits by copying the
example. It now builds a `HarvestMetricsRecorder`, threads it into
`HarvestBuilder::telemetry(..)` via `standalone_builder(metrics)`, and
serves it at `GET /metrics` with the same `text/plain; version=0.0.4`
content type the plugin path's endpoint uses.

No new `WorkflowEvent` variant, no migration, no replay impact, no
change to the plugin path's `/actuator/prometheus` output.

**Tests.** `autumn-harvest-plugin/src/metrics_scrape.rs` gains six unit
tests: empty-before-recording, a labeled counter line with HELP/TYPE,
an unlabeled counter line, the histogram count/sum decomposition, and
one test per escaped character (backslash, double-quote, newline).
`examples/standalone-runner/src/tests.rs` gains an HTTP-level test
(`metrics_route_renders_a_recorded_sample_as_prometheus_text`) that
records a sample, hits `GET /metrics` through the assembled router with
no database, and asserts the content type and rendered line — the same
no-DB pattern the router's other tests (`health_route_needs_no_database`,
`openapi_document_is_ungated`) already use.
