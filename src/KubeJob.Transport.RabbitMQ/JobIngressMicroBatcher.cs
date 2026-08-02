using System.Threading.Channels;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Bounded micro-batcher for broker ingress. A batch flushes as soon as it is
/// full or when its oldest message reaches the configured wait time. A batch
/// containing a permanent error falls back to per-message submission so one
/// poison message cannot make valid messages requeue indefinitely.
/// </summary>
public sealed class JobIngressMicroBatcher : IAsyncDisposable
{
    private readonly IJobMessageIngress _ingress;
    private readonly IJobMessageIngressBatch? _batchIngress;
    private readonly int _batchSize;
    private readonly TimeSpan _batchWait;
    private readonly Channel<PendingMessage> _channel;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;

    public JobIngressMicroBatcher(
        IJobMessageIngress ingress,
        int batchSize,
        TimeSpan batchWait)
    {
        _ingress = ingress;
        _batchIngress = ingress as IJobMessageIngressBatch;
        _batchSize = batchSize;
        _batchWait = batchWait;
        _channel = Channel.CreateBounded<PendingMessage>(new BoundedChannelOptions(batchSize * 2)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        _loop = Task.Run(ProcessAsync);
    }

    public async ValueTask<JobIngressResult> SubmitAsync(
        JobIngressMessage message,
        CancellationToken cancellationToken)
    {
        var pending = new PendingMessage(message);
        await _channel.Writer.WriteAsync(pending, cancellationToken);
        return await pending.Completion.Task.WaitAsync(cancellationToken);
    }

    private async Task ProcessAsync()
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(_stopping.Token))
            {
                var batch = new List<PendingMessage>(_batchSize)
                {
                    await _channel.Reader.ReadAsync(_stopping.Token)
                };
                using var timeout = new CancellationTokenSource(_batchWait);

                while (batch.Count < _batchSize)
                {
                    while (batch.Count < _batchSize && _channel.Reader.TryRead(out var next))
                    {
                        batch.Add(next);
                    }

                    if (batch.Count == _batchSize)
                    {
                        break;
                    }

                    try
                    {
                        if (!await _channel.Reader.WaitToReadAsync(timeout.Token))
                        {
                            break;
                        }
                    }
                    catch (OperationCanceledException) when (timeout.IsCancellationRequested)
                    {
                        break;
                    }
                }

                await SubmitBatchAsync(batch);
            }
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            while (_channel.Reader.TryRead(out var pending))
            {
                pending.Completion.TrySetException(exception);
            }
        }
    }

    private async Task SubmitBatchAsync(IReadOnlyList<PendingMessage> batch)
    {
        try
        {
            var results = _batchIngress is null
                ? await SubmitSequentiallyAsync(batch)
                : await _batchIngress.SubmitBatchAsync(
                    batch.Select(x => x.Message).ToArray(),
                    _stopping.Token);
            if (results.Count != batch.Count)
            {
                throw new InvalidOperationException("Ingress batch result count did not match its request count.");
            }

            for (var index = 0; index < batch.Count; index++)
            {
                batch[index].Completion.TrySetResult(results[index]);
            }
        }
        catch (ControlPlaneValidationException)
        {
            await SubmitIndividuallyAsync(batch);
        }
        catch (IdempotencyConflictException)
        {
            await SubmitIndividuallyAsync(batch);
        }
        catch (Exception exception)
        {
            foreach (var pending in batch)
            {
                pending.Completion.TrySetException(exception);
            }
        }
    }

    private async Task SubmitIndividuallyAsync(IReadOnlyList<PendingMessage> batch)
    {
        foreach (var pending in batch)
        {
            try
            {
                var result = await _ingress.SubmitAsync(pending.Message, _stopping.Token);
                pending.Completion.TrySetResult(result);
            }
            catch (Exception exception)
            {
                pending.Completion.TrySetException(exception);
            }
        }
    }

    private async ValueTask<IReadOnlyList<JobIngressResult>> SubmitSequentiallyAsync(
        IReadOnlyList<PendingMessage> batch)
    {
        var results = new JobIngressResult[batch.Count];
        for (var index = 0; index < batch.Count; index++)
        {
            results[index] = await _ingress.SubmitAsync(batch[index].Message, _stopping.Token);
        }

        return results;
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        try
        {
            await _loop;
        }
        finally
        {
            _stopping.Dispose();
        }
    }

    private sealed class PendingMessage
    {
        public PendingMessage(JobIngressMessage message)
        {
            Message = message;
        }

        public JobIngressMessage Message { get; }

        public TaskCompletionSource<JobIngressResult> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
