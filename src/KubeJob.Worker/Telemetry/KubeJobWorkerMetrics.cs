using System.Diagnostics;
using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Worker.Telemetry;

public enum WorkerExecutionKind
{
    PostgresManaged,
    BrokerNative
}

public enum WorkerHandlerOutcome
{
    Succeeded,
    Canceled,
    TimedOut,
    PayloadInvalid,
    Failed
}

public sealed class KubeJobWorkerMetrics : IDisposable
{
    private const string ExecutionKindTagName = "kubejob.execution.kind";

    private readonly Meter _meter;
    private readonly UpDownCounter<long> _activeAttempts;
    private readonly Histogram<double> _handlerDuration;

    public KubeJobWorkerMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(
            KubeJobTelemetry.WorkerMeterName,
            typeof(KubeJobWorkerMetrics).Assembly.GetName().Version?.ToString());

        _activeAttempts = _meter.CreateUpDownCounter<long>(
            "kubejob.worker.active_attempts",
            unit: "{attempt}",
            description: "Number of KubeJob executions currently owned by this worker process.");
        _handlerDuration = _meter.CreateHistogram<double>(
            "kubejob.worker.handler.duration",
            unit: "s",
            description: "Duration of KubeJob handler invocation, excluding durable completion reporting.");
    }

    public void AttemptStarted(WorkerExecutionKind executionKind) =>
        RecordActiveAttempt(1, executionKind);

    public void AttemptFinished(WorkerExecutionKind executionKind) =>
        RecordActiveAttempt(-1, executionKind);

    public bool IsHandlerDurationEnabled => _handlerDuration.Enabled;

    public void HandlerCompleted(TimeSpan duration, string outcome)
    {
        if (!_handlerDuration.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { "kubejob.outcome", outcome }
        };
        _handlerDuration.Record(duration.TotalSeconds, tags);
    }

    public void Dispose() => _meter.Dispose();

    private void RecordActiveAttempt(long change, WorkerExecutionKind executionKind)
    {
        if (!_activeAttempts.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            {
                ExecutionKindTagName,
                executionKind == WorkerExecutionKind.PostgresManaged
                    ? "postgres_managed"
                    : "broker_native"
            }
        };
        _activeAttempts.Add(change, tags);
    }
}
