using Confluent.Kafka;

namespace KubeJob.Transport.Kafka;

/// <summary>
/// Keeps each Kafka partition strictly serial while permitting different
/// partitions to execute in parallel. The consumer thread remains responsible
/// for pause/resume and offset commits, avoiding concurrent consumer access.
/// </summary>
internal sealed class KafkaPartitionDispatcher
{
    private readonly SemaphoreSlim _slots;
    private readonly Dictionary<TopicPartition, Pending> _pending = [];

    public KafkaPartitionDispatcher(int maxConcurrency)
    {
        _slots = new SemaphoreSlim(maxConcurrency, maxConcurrency);
    }

    public bool TryDispatch(
        ConsumeResult<string, byte[]> record,
        Func<ConsumeResult<string, byte[]>, CancellationToken, Task> handler,
        CancellationToken stoppingToken)
    {
        if (_pending.ContainsKey(record.TopicPartition))
        {
            return false;
        }

        _pending.Add(record.TopicPartition, new Pending(record, ExecuteAsync(record, handler, stoppingToken)));
        return true;
    }

    public async Task DrainCompletedAsync(
        IConsumer<string, byte[]> consumer,
        Func<ConsumeResult<string, byte[]>, Task> commit,
        Action<ConsumeResult<string, byte[]>, Exception> recover)
    {
        var completed = _pending
            .Where(pair => pair.Value.Operation.IsCompleted)
            .Select(pair => pair.Key)
            .ToArray();

        foreach (var partition in completed)
        {
            var pending = _pending[partition];
            _pending.Remove(partition);
            try
            {
                await pending.Operation;
                await commit(pending.Record);
            }
            catch (Exception exception)
            {
                recover(pending.Record, exception);
            }
            finally
            {
                consumer.Resume([partition]);
            }
        }
    }

    public async Task DrainAsync()
    {
        await Task.WhenAll(_pending.Values.Select(item => item.Operation));
        _pending.Clear();
    }

    private async Task ExecuteAsync(
        ConsumeResult<string, byte[]> record,
        Func<ConsumeResult<string, byte[]>, CancellationToken, Task> handler,
        CancellationToken stoppingToken)
    {
        await _slots.WaitAsync(stoppingToken);
        try
        {
            await handler(record, stoppingToken);
        }
        finally
        {
            _slots.Release();
        }
    }

    private sealed record Pending(ConsumeResult<string, byte[]> Record, Task Operation);
}
