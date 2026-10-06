# OpenTelemetry Collector recipe (issue #1838)

Harvest emits its metrics through the `metrics-rs` adapter. This page sends
them to any OTLP backend through an OpenTelemetry Collector. It also copies the
metrics that match an OTel messaging semantic convention under the semconv
name.

Harvest keeps its own `harvest.*` names. The copies are additive, so
dashboards, alerts and SLO rules keep working. ADR 0004 records why Harvest has
no native OTLP exporter: [`docs/adr/0004-security-extras.md`](../adr/0004-security-extras.md).

## Prerequisites

1. Install `metrics-exporter-prometheus` with the `MetricsRsRecorder`, as in
   [the telemetry escape hatch](../telemetry.md#escape-hatch-metrics-exporter-prometheus-for-full-histograms-otlp-or-a-custom-recorder).
   The exporter listens on port 9000 by default.
2. Set explicit buckets for `harvest_activity_duration`. Without buckets the
   exporter renders a summary, and the Collector copies a summary, not a
   histogram.
3. Use an `otelcol-contrib` build. The `metricstransform` processor is in the
   contrib distribution only.

## Mapped metrics

| Harvest metric | Semconv metric | `messaging.operation.name` | `messaging.operation.type` | `messaging.destination.name` from |
|---|---|---|---|---|
| `harvest.queue.dispatched` | `messaging.client.consumed.messages` | `claim` | `receive` | `queue` |
| `harvest.activity.duration` | `messaging.process.duration` | `process` | `process` | `queue` |

Each copy also gets `messaging.system = harvest`. The other labels of the
source stay on the copy.

The source of truth is `telemetry::SEMCONV_METRIC_MAPPINGS`. The test
`otel_semconv_docs` renders the recipe below from that table. If you change the
table, paste the rendered recipe here.

## Unmapped metrics

Other metrics keep their Harvest names only:

- Connector metrics (`harvest.connector.*`). The connector does not know the
  broker, so `messaging.system` would be wrong.
- Queue depth, oldest pending age and schedule-to-start. No semconv metric
  has that meaning.
- RPC conventions. Harvest emits no RPC or HTTP server metric. Autumn-web owns
  the HTTP metrics.

## Recipe

Change the scrape target and the OTLP endpoint for your deployment. Add TLS
settings to the exporter for a remote backend.

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: harvest
          scrape_interval: 15s
          metrics_path: /metrics
          static_configs:
            - targets: ["harvest:9000"]

processors:
  metricstransform/harvest-semconv:
    transforms:
      - include: ^harvest_queue_dispatched(_total)?$
        match_type: regexp
        action: insert
        new_name: messaging.client.consumed.messages
        operations:
          - action: update_label
            label: queue
            new_label: messaging.destination.name
          - action: add_label
            new_label: messaging.system
            new_value: harvest
          - action: add_label
            new_label: messaging.operation.name
            new_value: claim
          - action: add_label
            new_label: messaging.operation.type
            new_value: receive
      - include: ^harvest_activity_duration$
        match_type: regexp
        action: insert
        new_name: messaging.process.duration
        operations:
          - action: update_label
            label: queue
            new_label: messaging.destination.name
          - action: add_label
            new_label: messaging.system
            new_value: harvest
          - action: add_label
            new_label: messaging.operation.name
            new_value: process
          - action: add_label
            new_label: messaging.operation.type
            new_value: process
  batch: {}

exporters:
  otlp:
    endpoint: otel-backend:4317

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [metricstransform/harvest-semconv, batch]
      exporters: [otlp]
```

The counter pattern accepts the name with and without `_total`. The
Prometheus receiver can trim that suffix, depending on the Collector version
and its `NormalizeName` setting.

`action: insert` keeps the source series and adds the copy. Use `update` in
place of `insert` only if you want the semconv name alone.
