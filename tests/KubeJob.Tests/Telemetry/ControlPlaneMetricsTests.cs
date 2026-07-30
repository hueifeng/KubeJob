using System.Diagnostics;
using System.Diagnostics.Metrics;
using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using KubeJob.Core.Telemetry;
using KubeJob.Server.ControlPlane;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;

namespace KubeJob.Tests.Telemetry;

public sealed class ControlPlaneMetricsTests
{
    [Fact]
    public async Task Submission_records_accepted_and_idempotency_hit_as_distinct_metrics()
    {
        var accepted = 0L;
        var idempotencyHits = 0L;

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        await using var provider = services.BuildServiceProvider();
        var meterFactory = provider.GetRequiredService<IMeterFactory>();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument.Meter.Scope, meterFactory)
                && instrument.Meter.Name == "KubeJob.ControlPlane"
                && instrument.Name is "kubejob.job.submissions"
                    or "kubejob.job.idempotency_hits")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            switch (instrument.Name)
            {
                case "kubejob.job.submissions":
                    accepted += measurement;
                    break;
                case "kubejob.job.idempotency_hits":
                    idempotencyHits += measurement;
                    break;
            }
        });
        listener.Start();

        var request = new EnqueueJobRequest(
            "telemetry.sample",
            "{\"value\":42}",
            IdempotencyKey: "telemetry:42");

        await controlPlane.SubmitAsync(request);
        await controlPlane.SubmitAsync(request);

        accepted.Should().Be(1);
        idempotencyHits.Should().Be(1);
    }

    [Fact]
    public async Task Submission_emits_a_business_activity_without_sensitive_identifiers()
    {
        var started = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, KubeJobTelemetry.ActivitySource),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = started.Add
        };
        ActivitySource.AddActivityListener(listener);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        await using var provider = services.BuildServiceProvider();
        var controlPlane = provider.GetRequiredService<JobControlPlane>();

        await controlPlane.SubmitAsync(new EnqueueJobRequest(
            "trace.sample",
            "{\"secret\":\"must-not-be-a-tag\"}",
            IdempotencyKey: "trace-key"));

        var activity = started.Last(activity => activity.OperationName == "kubejob.submit");
        activity.Tags.Should().NotContain(tag => tag.Key == "kubejob.run.id");
        activity.Tags.Should().NotContain(tag => tag.Key == "kubejob.payload");
        activity.Tags.Should().NotContain(tag => tag.Key == "kubejob.idempotency.key");
    }
}
