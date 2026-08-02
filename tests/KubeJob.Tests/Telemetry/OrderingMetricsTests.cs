using System.Diagnostics.Metrics;
using FluentAssertions;
using KubeJob.ControlPlane.Telemetry;
using KubeJob.Core.Runtime;
using KubeJob.Core.Telemetry;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Tests.Telemetry;

public sealed class OrderingMetricsTests
{
    [Fact]
    public async Task UpdateOrderingBacklog_surfaces_per_queue_gauges_on_scrape()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        await using var provider = services.BuildServiceProvider();
        var meterFactory = provider.GetRequiredService<IMeterFactory>();
        var metrics = provider.GetRequiredService<KubeJobControlPlaneMetrics>();

        var blockedByQueue = new Dictionary<string, int>(StringComparer.Ordinal);
        var ageByQueue = new Dictionary<string, double>(StringComparer.Ordinal);
        var keysByQueue = new Dictionary<string, int>(StringComparer.Ordinal);

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, meterFactory)
                && instrument.Meter.Name == KubeJobTelemetry.ControlPlaneMeterName
                && instrument.Name is "kubejob.control_plane.ordering.blocked_runs"
                    or "kubejob.control_plane.ordering.oldest_blocked_age"
                    or "kubejob.control_plane.ordering.active_keys")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, _) =>
        {
            var queue = ReadQueue(tags);
            switch (instrument.Name)
            {
                case "kubejob.control_plane.ordering.blocked_runs":
                    blockedByQueue[queue] = measurement;
                    break;
                case "kubejob.control_plane.ordering.active_keys":
                    keysByQueue[queue] = measurement;
                    break;
            }
        });
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name == "kubejob.control_plane.ordering.oldest_blocked_age")
            {
                ageByQueue[ReadQueue(tags)] = measurement;
            }
        });
        listener.Start();

        metrics.UpdateOrderingBacklog(new[]
        {
            new OrderingBacklogSample("default", BlockedRuns: 2, OldestBlockedAgeSeconds: 12.5, ActiveKeys: 3, StrictFifoBlocked: 0, RetryBlockedRuns: 0, LaneBreakdown: Array.Empty<LaneBacklogSample>()),
            new OrderingBacklogSample("orders", BlockedRuns: 0, OldestBlockedAgeSeconds: 0, ActiveKeys: 1, StrictFifoBlocked: 0, RetryBlockedRuns: 0, LaneBreakdown: Array.Empty<LaneBacklogSample>())
        });
        listener.RecordObservableInstruments();

        blockedByQueue["default"].Should().Be(2);
        keysByQueue["default"].Should().Be(3);
        ageByQueue["default"].Should().Be(12.5);
        blockedByQueue["orders"].Should().Be(0);
        keysByQueue["orders"].Should().Be(1);
        ageByQueue["orders"].Should().Be(0);
    }

    [Fact]
    public async Task OrderingAdmitted_records_keyordered_wait_duration_tagged_by_queue()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        await using var provider = services.BuildServiceProvider();
        var meterFactory = provider.GetRequiredService<IMeterFactory>();
        var metrics = provider.GetRequiredService<KubeJobControlPlaneMetrics>();

        var recorded = new List<(string Queue, double Seconds)>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, meterFactory)
                && instrument.Meter.Name == KubeJobTelemetry.ControlPlaneMeterName
                && instrument.Name == "kubejob.control_plane.ordering.wait_duration")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
        {
            if (instrument.Name == "kubejob.control_plane.ordering.wait_duration")
            {
                recorded.Add((ReadQueue(tags), measurement));
            }
        });
        listener.Start();

        metrics.IsOrderingWaitEnabled.Should().BeTrue();
        metrics.OrderingAdmitted(TimeSpan.FromSeconds(5), "default");
        metrics.OrderingAdmitted(TimeSpan.FromSeconds(7.5), "orders");

        recorded.Should().HaveCount(2);
        recorded.Should().Contain(entry => entry.Queue == "default" && entry.Seconds == 5);
        recorded.Should().Contain(entry => entry.Queue == "orders" && entry.Seconds == 7.5);
    }

    private static string ReadQueue(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "kubejob.queue" && tag.Value is string queue)
            {
                return queue;
            }
        }

        return "<unknown>";
    }
}