using System.Diagnostics.Metrics;
using FluentAssertions;
using KubeJob.Worker.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Tests.Telemetry;

public sealed class WorkerMetricsTests
{
    [Fact]
    public void Active_attempt_counter_uses_matching_start_and_finish_tags()
    {
        var measurements = new List<(long Value, KeyValuePair<string, object?>[] Tags)>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "KubeJob.Worker"
                && instrument.Name == "kubejob.worker.active_attempts")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            measurements.Add((measurement, tags.ToArray()));
        });
        listener.Start();

        var services = new ServiceCollection();
        services.AddMetrics();
        using var provider = services.BuildServiceProvider();
        using var metrics = new KubeJobWorkerMetrics(
            provider.GetRequiredService<IMeterFactory>());

        metrics.AttemptStarted(WorkerExecutionKind.Pull);
        metrics.AttemptFinished(WorkerExecutionKind.Pull);

        measurements.Select(measurement => measurement.Value).Should().Equal(1, -1);
        measurements[0].Tags.Should().Equal(measurements[1].Tags);
        measurements[0].Tags.Should().ContainSingle(tag =>
            tag.Key == "kubejob.execution.kind" && (string?)tag.Value == "pull");
    }

    [Fact]
    public void Disabled_active_attempt_metrics_do_not_allocate_tags()
    {
        var services = new ServiceCollection();
        services.AddMetrics();
        using var provider = services.BuildServiceProvider();
        using var metrics = new KubeJobWorkerMetrics(
            provider.GetRequiredService<IMeterFactory>());

        metrics.AttemptStarted(WorkerExecutionKind.Pull);
        metrics.AttemptFinished(WorkerExecutionKind.Pull);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var index = 0; index < 1_000; index++)
        {
            metrics.AttemptStarted(WorkerExecutionKind.Pull);
            metrics.AttemptFinished(WorkerExecutionKind.Pull);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        allocated.Should().Be(0);
    }

    [Fact]
    public void Handler_duration_records_seconds_with_completion_outcome()
    {
        var measurements = new List<(double Value, KeyValuePair<string, object?>[] Tags)>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "KubeJob.Worker"
                && instrument.Name == "kubejob.worker.handler.duration")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, measurement, tags, _) =>
        {
            measurements.Add((measurement, tags.ToArray()));
        });
        listener.Start();

        var services = new ServiceCollection();
        services.AddMetrics();
        using var provider = services.BuildServiceProvider();
        using var metrics = new KubeJobWorkerMetrics(
            provider.GetRequiredService<IMeterFactory>());

        metrics.HandlerCompleted(TimeSpan.FromMilliseconds(250), "succeeded");

        measurements.Should().ContainSingle();
        measurements[0].Value.Should().BeApproximately(0.25, 0.0001);
        measurements[0].Tags.Should().ContainSingle(tag =>
            tag.Key == "kubejob.outcome" && (string?)tag.Value == "succeeded");
    }
}
