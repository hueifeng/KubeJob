using System.Diagnostics;
using System.Diagnostics.Metrics;
using KubeJob.Core.Telemetry;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Worker.Telemetry;

public enum WorkerExecutionKind
{
    Pull,
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
            description: "Duration of KubeJob handler invocation, excluding durable completion or broker acknowledgement.");
    }

    public void AttemptStarted(WorkerExecutionKind executionKind) =>
        RecordActiveAttempt(1, executionKind);

    public void AttemptFinished(WorkerExecutionKind executionKind) =>
        RecordActiveAttempt(-1, executionKind);

    public bool IsHandlerDurationEnabled => _handlerDuration.Enabled;

    public void HandlerCompleted(
        TimeSpan duration,
        string outcome,
        WorkerExecutionKind executionKind = WorkerExecutionKind.Pull)
    {
        if (!_handlerDuration.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { "kubejob.outcome", outcome },
            { ExecutionKindTagName, GetExecutionKindTag(executionKind) }
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
            { ExecutionKindTagName, GetExecutionKindTag(executionKind) }
        };
        _activeAttempts.Add(change, tags);
    }

    private static string GetExecutionKindTag(WorkerExecutionKind executionKind) =>
        executionKind == WorkerExecutionKind.Pull
            ? "postgres_managed"
            : "broker_native";
}
