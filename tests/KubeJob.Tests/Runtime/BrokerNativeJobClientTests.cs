using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Tests.Runtime;

public sealed class BrokerNativeJobClientTests
{
    private static readonly JobKey<TestPayload> JobKey = new("test.broker-native.client");

    [Fact]
    public async Task BrokerNative_rejects_idempotency_key_until_real_deduplication_exists()
    {
        var publisher = new RecordingBatchPublisher();
        using var provider = CreateProvider(publisher, "native");
        var client = provider.GetRequiredService<IJobClient>();

        Func<Task> act = async () => await client.EnqueueAsync(
            JobKey,
            new TestPayload(1),
            new JobEnqueueOptions
            {
                Queue = "native",
                IdempotencyKey = "order:1"
            });

        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*Inbox/deduplication*");
        publisher.SinglePublishCount.Should().Be(0);
        publisher.BatchPublishCount.Should().Be(0);
    }

    [Fact]
    public async Task BrokerNative_batch_uses_transport_batch_publisher_and_marks_handle_capabilities()
    {
        var publisher = new RecordingBatchPublisher();
        using var provider = CreateProvider(publisher, "native-a", "native-b");
        var client = provider.GetRequiredService<IJobClient>();
        var batch = new (TestPayload Payload, JobEnqueueOptions? Options)[]
        {
            (new TestPayload(1), new JobEnqueueOptions { Queue = "native-a" }),
            (new TestPayload(2), new JobEnqueueOptions { Queue = "native-b" })
        };

        var handles = await client.EnqueueBatchAsync(JobKey, batch);

        handles.Should().HaveCount(2);
        handles.Should().OnlyContain(handle =>
            handle.RuntimeMode == QueueRuntimeMode.BrokerNative
            && handle.TransportId == RecordingBatchPublisher.Id
            && !handle.SupportsStrongStatus
            && !handle.SupportsStrongCancellation);
        publisher.SinglePublishCount.Should().Be(0);
        publisher.BatchPublishCount.Should().Be(1);
        publisher.PublishedRequests.Should().HaveCount(2);
        publisher.PublishedRequests.Select(request => request.Destination)
            .Should().Equal("native-a", "native-b");
    }

    private static ServiceProvider CreateProvider(
        RecordingBatchPublisher publisher,
        params string[] brokerNativeQueues)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageTransportPublisher>(publisher);
        services.AddKubeJobServer();
        services.ConfigureKubeJobQueueRuntimes(options =>
        {
            foreach (var queue in brokerNativeQueues)
            {
                options.Queues[queue] = new QueueRuntimeRoute
                {
                    Mode = QueueRuntimeMode.BrokerNative,
                    TransportId = RecordingBatchPublisher.Id
                };
            }
        });
        return services.BuildServiceProvider();
    }

    private sealed record TestPayload(int Value);

    private sealed class RecordingBatchPublisher : IMessageTransportBatchPublisher
    {
        public const string Id = "recording";

        public string TransportId => Id;

        public MessageTransportCapabilities Capabilities =>
            MessageTransportCapabilities.DurablePublish;

        public int SinglePublishCount { get; private set; }

        public int BatchPublishCount { get; private set; }

        public List<TransportPublishRequest> PublishedRequests { get; } = new();

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SinglePublishCount++;
            PublishedRequests.Add(request);
            return ValueTask.CompletedTask;
        }

        public ValueTask PublishBatchAsync(
            IReadOnlyList<TransportPublishRequest> requests,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            BatchPublishCount++;
            PublishedRequests.AddRange(requests);
            return ValueTask.CompletedTask;
        }
    }
}
