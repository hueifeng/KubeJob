using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Runtime;

public sealed class V3PostMergeHardeningTests
{
    private static readonly JobKey<TestPayload> Job = new("hardening.test");

    [Fact]
    public async Task Default_noop_notifier_does_not_create_managed_wake_outbox_rows()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IJobClient>();
        await client.EnqueueAsync(
            Job,
            new TestPayload(1),
            new JobEnqueueOptions { Queue = "managed" });

        var outbox = provider.GetRequiredService<IOutboxStore>();
        var claimed = await outbox.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        claimed.Should().BeEmpty();
    }

    [Fact]
    public async Task Explicit_notifier_keeps_managed_wake_outbox_durable()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.UseKubeJobWorkAvailableNotifier<TestNotifier>();
        using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IJobClient>();
        await client.EnqueueAsync(
            Job,
            new TestPayload(2),
            new JobEnqueueOptions { Queue = "managed" });

        var outbox = provider.GetRequiredService<IOutboxStore>();
        var claimed = await outbox.ClaimPendingAsync(
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            10,
            CancellationToken.None);

        claimed.Should().ContainSingle();
        claimed[0].EventType.Should().Be(OutboxEventTypes.WorkAvailable);
    }

    [Fact]
    public async Task BrokerNative_batch_uses_transport_batch_publisher_once()
    {
        var publisher = new RecordingBatchPublisher();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.ConfigureKubeJobQueueRuntimes(options =>
        {
            options.Queues["native"] = new QueueRuntimeRoute
            {
                Mode = QueueRuntimeMode.BrokerNative,
                TransportId = RecordingBatchPublisher.Id
            };
        });
        services.AddSingleton<IMessageTransportPublisher>(publisher);
        using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IJobClient>();
        var handles = await client.EnqueueBatchAsync(
            Job,
            new[]
            {
                (new TestPayload(1), (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "native" }),
                (new TestPayload(2), (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "native" }),
                (new TestPayload(3), (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "native" })
            });

        publisher.BatchCalls.Should().Be(1);
        publisher.SingleCalls.Should().Be(0);
        publisher.Requests.Should().HaveCount(3);
        handles.Select(x => x.JobId)
            .Should().Equal(publisher.Requests.Select(x => x.Message.MessageId));
    }

    [Fact]
    public async Task BrokerNative_batch_falls_back_for_single_publish_transport()
    {
        var publisher = new RecordingSinglePublisher();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKubeJobServer();
        services.ConfigureKubeJobQueueRuntimes(options =>
        {
            options.Queues["native"] = new QueueRuntimeRoute
            {
                Mode = QueueRuntimeMode.BrokerNative,
                TransportId = RecordingSinglePublisher.Id
            };
        });
        services.AddSingleton<IMessageTransportPublisher>(publisher);
        using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IJobClient>();
        await client.EnqueueBatchAsync(
            Job,
            new[]
            {
                (new TestPayload(1), (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "native" }),
                (new TestPayload(2), (JobEnqueueOptions?)new JobEnqueueOptions { Queue = "native" })
            });

        publisher.SingleCalls.Should().Be(2);
    }

    private sealed record TestPayload(int Value);

    private sealed class TestNotifier : IWorkAvailableNotifier
    {
        public ValueTask PublishAsync(
            WorkAvailableSignal signal,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class RecordingBatchPublisher : IMessageTransportBatchPublisher
    {
        public const string Id = "recording-batch";
        public string TransportId => Id;
        public MessageTransportCapabilities Capabilities => MessageTransportCapabilities.DurablePublish;
        public int BatchCalls { get; private set; }
        public int SingleCalls { get; private set; }
        public List<TransportPublishRequest> Requests { get; } = new();

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            SingleCalls++;
            Requests.Add(request);
            return ValueTask.CompletedTask;
        }

        public ValueTask PublishBatchAsync(
            IReadOnlyList<TransportPublishRequest> requests,
            CancellationToken cancellationToken = default)
        {
            BatchCalls++;
            Requests.AddRange(requests);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingSinglePublisher : IMessageTransportPublisher
    {
        public const string Id = "recording-single";
        public string TransportId => Id;
        public MessageTransportCapabilities Capabilities => MessageTransportCapabilities.DurablePublish;
        public int SingleCalls { get; private set; }

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            SingleCalls++;
            return ValueTask.CompletedTask;
        }
    }
}
