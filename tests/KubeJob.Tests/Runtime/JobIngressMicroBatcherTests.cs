using System.Collections.Concurrent;
using FluentAssertions;
using KubeJob.Core.Runtime;
using KubeJob.Transport.RabbitMQ;

namespace KubeJob.Tests.Runtime;

public sealed class JobIngressMicroBatcherTests
{
    [Fact]
    public async Task Flushes_immediately_when_the_batch_reaches_its_size_limit()
    {
        var ingress = new RecordingIngress();
        await using var batcher = new JobIngressMicroBatcher(
            ingress,
            batchSize: 2,
            batchWait: TimeSpan.FromSeconds(1));

        var first = batcher.SubmitAsync(Message("one"), CancellationToken.None).AsTask();
        var second = batcher.SubmitAsync(Message("two"), CancellationToken.None).AsTask();

        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromMilliseconds(250));

        ingress.Batches.Should().ContainSingle();
        ingress.Batches.Single().Should().HaveCount(2);
    }

    [Fact]
    public async Task Flushes_a_partial_batch_after_the_wait_window()
    {
        var ingress = new RecordingIngress();
        await using var batcher = new JobIngressMicroBatcher(
            ingress,
            batchSize: 100,
            batchWait: TimeSpan.FromMilliseconds(20));

        var result = await batcher.SubmitAsync(Message("one"), CancellationToken.None)
            .AsTask()
            .WaitAsync(TimeSpan.FromMilliseconds(250));

        result.JobId.Should().Be("one");
        ingress.Batches.Should().ContainSingle();
        ingress.Batches.Single().Should().ContainSingle();
    }

    [Fact]
    public async Task Disposing_flushes_the_partial_tail_batch()
    {
        var ingress = new RecordingIngress();
        var batcher = new JobIngressMicroBatcher(
            ingress,
            batchSize: 100,
            batchWait: TimeSpan.FromSeconds(1));
        var submission = batcher.SubmitAsync(Message("tail"), CancellationToken.None).AsTask();

        await Task.Delay(20);
        await batcher.DisposeAsync();

        (await submission).JobId.Should().Be("tail");
        ingress.Batches.Should().ContainSingle();
        ingress.Batches.Single().Should().ContainSingle();
    }

    private static JobIngressMessage Message(string messageId) => new(
        "tests",
        messageId,
        new EnqueueJobRequest("test.job", "{}"));

    private sealed class RecordingIngress : IJobMessageIngressBatch
    {
        public ConcurrentQueue<IReadOnlyList<JobIngressMessage>> Batches { get; } = new();

        public ValueTask<JobIngressResult> SubmitAsync(
            JobIngressMessage message,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(new JobIngressResult(message.MessageId, Existing: false));

        public ValueTask<IReadOnlyList<JobIngressResult>> SubmitBatchAsync(
            IReadOnlyList<JobIngressMessage> messages,
            CancellationToken cancellationToken = default)
        {
            Batches.Enqueue(messages.ToArray());
            return ValueTask.FromResult<IReadOnlyList<JobIngressResult>>(
                messages.Select(message => new JobIngressResult(message.MessageId, Existing: false)).ToArray());
        }
    }
}
