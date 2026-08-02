# KubeJob Telemetry

KubeJob exposes standard .NET `Meter` and `ActivitySource` instrumentation. It does **not** reference the OpenTelemetry SDK, Prometheus, OTLP, or any exporter.

## Ownership

```text
KubeJob packages      define Meters, instruments, and business Activities
Host application       selects listeners, sampling, and exporters
Monitoring platform    stores, queries, and alerts on exported telemetry
```

Registering KubeJob adds `IMeterFactory` through `services.AddMetrics()`. This does not export data or make a network connection. Without a `MeterListener` or telemetry provider, KubeJob's metric hot paths skip tag construction and duration measurement.

## Publishers

| Publisher | Purpose |
| --- | --- |
| `KubeJob.ControlPlane` | accepted submissions and idempotency replays |
| `KubeJob.Worker` | active local attempts and handler execution duration |
| `KubeJob.Storage.PostgreSQL` | wait time for KubeJob's bounded database-operation gate |
| `KubeJob.Transport.RabbitMQ` | execution-envelope publishing and publisher confirms |
| `KubeJob` (`ActivitySource`) | KubeJob business traces such as `kubejob.submit` |

The stable names are available from `KubeJob.Core.Telemetry.KubeJobTelemetry`.

## OpenTelemetry host setup

The application chooses the OpenTelemetry packages and exporter. For example:

```csharp
builder.Services
    .AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("KubeJob.ControlPlane")
        .AddMeter("KubeJob.Worker")
        .AddMeter("KubeJob.Storage.PostgreSQL")
        .AddMeter("KubeJob.Transport.RabbitMQ"))
    .WithTracing(tracing => tracing
        .AddSource("KubeJob"));
```

Add a Prometheus or OTLP exporter in the host only when the deployment needs one. KubeJob deliberately does not configure exporter endpoints or expose exporter options.

## Metrics contract

Current instruments include:

```text
kubejob.job.submissions
kubejob.job.idempotency_hits
kubejob.control_plane.admission.duration
kubejob.control_plane.lease_reaper.reclaimed
kubejob.control_plane.outbox.publish_lag
kubejob.worker.active_attempts
kubejob.worker.handler.duration
kubejob.storage.database_gate_wait.duration
kubejob.rabbitmq.execution.published
kubejob.rabbitmq.execution.publish_failures
kubejob.rabbitmq.execution.publish.duration
kubejob.rabbitmq.execution.broker_retries
kubejob.rabbitmq.execution.reconciliation_handoffs
kubejob.control_plane.ordering.wait_duration
kubejob.control_plane.ordering.blocked_runs
kubejob.control_plane.ordering.oldest_blocked_age
kubejob.control_plane.ordering.active_keys
kubejob.control_plane.ordering.strictfifo_blocked_runs
kubejob.control_plane.ordering.retry_blocked_runs
kubejob.control_plane.ordering.lane_blocked_runs
```

Duration units are seconds (`s`). `kubejob.worker.active_attempts` is an `UpDownCounter`; its start and finish tags are deliberately identical. `kubejob.control_plane.admission.duration` is a histogram tagged with `kubejob.admission.status` (e.g. `admitted`, `run_not_found`, `run_already_running`); `kubejob.control_plane.outbox.publish_lag` is a histogram of the elapsed time between an outbox row becoming available and its publication to a transport. `kubejob.control_plane.lease_reaper.reclaimed`, `kubejob.rabbitmq.execution.broker_retries`, and `kubejob.rabbitmq.execution.reconciliation_handoffs` are monotonic counters (`{attempt}` / `{message}`).

The `kubejob.control_plane.ordering.*` instruments expose the cached ordering-backlog snapshot refreshed by `OrderingMetricsRefreshService` (see `docs/v2/ordering-observability.md`): `wait_duration` is a histogram (`s`); `blocked_runs`, `strictfifo_blocked_runs`, `retry_blocked_runs`, and `lane_blocked_runs` are observable gauges (`{run}`); `oldest_blocked_age` an observable gauge (`s`); `active_keys` an observable gauge (`{key}`). `lane_blocked_runs` is currently populated only by the in-memory store (dev/test); it stays empty on PostgreSQL deployments.

## Cardinality and sensitive data

KubeJob never records these as metric or default trace attributes:

```text
RunId, AttemptId, LeaseToken, IdempotencyKey, MessageId,
Payload JSON, UserId, OrderId, exception message, stack trace
```

Worker execution kind is a fixed enum (`pull` or `broker_dispatch`). Do not add arbitrary business identifiers as labels. Use logs or a sampled trace for per-run diagnosis.

## Global runtime state

KubeJob does not execute PostgreSQL queries from `ObservableGauge` callbacks. Prometheus scrapes must not become unbounded database load. Global state such as pending Outbox count, queue backlog, and oldest-ready age remains available through the Dashboard today; an optional bounded snapshot collector is a future feature.
