using System.Text.Json;
using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Runtime;
using KubeJob.Core.Scheduling;
using KubeJob.Core.Transport;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KubeJob.Tests.Runtime;

public sealed class BrokerNativeScheduleRuntimeTests
{
    [Fact]
    public async Task BrokerNative_schedule_publishes_confirmed_message_and_advances_without_creating_run()
    {
        var store = new InMemoryJobRuntimeStore();
        var due = DateTimeOffset.UtcNow.AddSeconds(-5);
        var schedule = NewSchedule(due);
        await store.UpsertAsync(schedule, CancellationToken.None);

        var publisher = new CapturingPublisher();
        var registry = new MessageTransportRegistry(new[] { publisher });
        var resolver = new FixedRuntimeResolver(
            new QueueRuntimeRoute
            {
                Mode = QueueRuntimeMode.BrokerNative,
                TransportId = publisher.TransportId
            });
        var runtimeOptions = Options.Create(new JobRuntimeOptions
        {
            SchedulePollInterval = TimeSpan.FromMilliseconds(10),
            ScheduleFailureDelay = TimeSpan.FromMilliseconds(10),
            ScheduleClaimDuration = TimeSpan.FromSeconds(5),
            ScheduleBatchSize = 8,
            ScheduleReconcileConcurrency = 1
        });

        using var service = new ScheduleReconcilerService(
            store,
            runtimeOptions,
            NullLogger<ScheduleReconcilerService>.Instance,
            resolver,
            registry);

        await service.StartAsync(CancellationToken.None);
        try
        {
            var request = await publisher.Published.Task.WaitAsync(TimeSpan.FromSeconds(5));
            request.Kind.Should().Be(TransportMessageKind.Job);
            request.Destination.Should().Be("reports");
            request.RoutingKey.Should().Be("reports");

            var message = JsonSerializer.Deserialize<BrokerNativeJobMessage>(
                request.Message.Body.Span,
                new JsonSerializerOptions(JsonSerializerDefaults.Web));
            message.Should().NotBeNull();
            message!.MessageId.Should().Be(
                ScheduleReconcilerService.CreateOccurrenceId(schedule.Id, due));
            message.JobKey.Should().Be(schedule.JobKey);
            message.Queue.Should().Be(schedule.Queue);
            message.PayloadJson.Should().Be(schedule.PayloadJson);
            message.IdempotencyKey.Should().Be($"schedule:{schedule.Id}:{due.UtcTicks}");

            await EventuallyAsync(async () =>
            {
                var persisted = await store.GetAsync(schedule.Id, CancellationToken.None);
                return persisted?.LastFireAt == due
                    && persisted.NextFireAt > due;
            });

            var occurrenceId = ScheduleReconcilerService.CreateOccurrenceId(schedule.Id, due);
            (await store.GetRunAsync(occurrenceId, CancellationToken.None)).Should().BeNull();

            var outbox = await store.ClaimPendingAsync(
                DateTimeOffset.UtcNow.AddMinutes(1),
                TimeSpan.FromSeconds(5),
                10,
                CancellationToken.None);
            outbox.Should().BeEmpty(
                "BrokerNative schedule occurrences are published directly and must not create managed work outbox rows");
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task BrokerNative_schedule_rejects_strong_running_state_policy()
    {
        var store = new InMemoryJobRuntimeStore();
        var delivery = new QueueDeliveryOptions();
        var schedules = new KubeJob.Server.ControlPlane.ScheduleControlPlane(
            store,
            new QueueCatalog(Options.Create(delivery)),
            runtimeResolver: new FixedRuntimeResolver(
                new QueueRuntimeRoute
                {
                    Mode = QueueRuntimeMode.BrokerNative,
                    TransportId = "test"
                }));

        var action = async () => await schedules.CreateCronAsync(
            "broker-native-overlap",
            new UpsertCronScheduleRequest(
                "report.generate",
                "{}",
                "* * * * *",
                Queue: "reports",
                ConcurrencyPolicy: ScheduleConcurrencyPolicy.SkipIfRunning));

        var exception = await action.Should().ThrowAsync<ControlPlaneValidationException>();
        exception.Which.Code.Should().Be("broker_native_schedule_policy_unsupported");
    }

    private static JobScheduleRecord NewSchedule(DateTimeOffset nextFireAt) => new()
    {
        Id = "broker-native-report",
        JobKey = "report.generate",
        PayloadJson = "{\"kind\":\"daily\"}",
        CronExpression = "* * * * *",
        TimeZoneId = "UTC",
        Queue = "reports",
        Priority = 0,
        MisfirePolicy = MisfirePolicy.FireOnce,
        ConcurrencyPolicy = ScheduleConcurrencyPolicy.Allow,
        MaxAttempts = 3,
        TimeoutSeconds = 30,
        Enabled = true,
        NextFireAt = nextFireAt,
        CreatedAt = DateTimeOffset.UtcNow,
        UpdatedAt = DateTimeOffset.UtcNow
    };

    private static async Task EventuallyAsync(Func<Task<bool>> predicate)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (await predicate())
            {
                return;
            }

            await Task.Delay(10);
        }

        (await predicate()).Should().BeTrue();
    }

    private sealed class FixedRuntimeResolver : IQueueRuntimeResolver
    {
        private readonly QueueRuntimeRoute _route;

        public FixedRuntimeResolver(QueueRuntimeRoute route) => _route = route;

        public QueueRuntimeRoute Resolve(string logicalQueue) => _route.Clone();
    }

    private sealed class CapturingPublisher : IMessageTransportPublisher
    {
        public string TransportId => "test";

        public MessageTransportCapabilities Capabilities =>
            MessageTransportCapabilities.DurablePublish;

        public TaskCompletionSource<TransportPublishRequest> Published { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Published.TrySetResult(request);
            return ValueTask.CompletedTask;
        }
    }
}
