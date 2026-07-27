using System.Threading.Channels;
using KubeJob.Core.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Coalesces completion calls at the control plane. The caller still awaits
/// the individual durable result, so transport ACK timing is unchanged.
/// </summary>
public sealed class CompletionBatcher
{
    private readonly IJobCompletionStore _store;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<CompletionBatcher>? _logger;
    private readonly object _startGate = new();
    private Task? _loop;
    private Channel<PendingCompletion> _channel;

    public CompletionBatcher(
        IJobCompletionStore store,
        IOptions<JobRuntimeOptions> options,
        ILogger<CompletionBatcher>? logger = null)
    {
        _store = store;
        _options = options.Value;
        _logger = logger;
        _options.Validate();
        _channel = CreateChannel();
    }

    public async ValueTask<CompleteAttemptResponse> EnqueueAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        var pending = new PendingCompletion(request);
        EnsureStarted();
        await _channel.Writer.WriteAsync(pending, cancellationToken);
        return await pending.Completion.Task.WaitAsync(cancellationToken);
    }

    private Channel<PendingCompletion> CreateChannel() =>
        Channel.CreateBounded<PendingCompletion>(new BoundedChannelOptions(
            Math.Max(256, _options.CompletionBatchSize * 8))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private void EnsureStarted()
    {
        if (_loop is not null && !_loop.IsCompleted)
        {
            return;
        }

        lock (_startGate)
        {
            if (_loop is null || _loop.IsCompleted)
            {
                // Reset the channel so callers blocked on a full queue or
                // hung after a loop fault don't wait forever.
                _channel = CreateChannel();
                _loop = Task.Run(ProcessLoopAsync);
            }
        }
    }

    private async Task ProcessLoopAsync()
    {
        while (await _channel.Reader.WaitToReadAsync())
        {
            var batch = new List<PendingCompletion>(_options.CompletionBatchSize);
            batch.Add(await _channel.Reader.ReadAsync());
            var deadline = Task.Delay(_options.CompletionFlushInterval);

            while (batch.Count < _options.CompletionBatchSize)
            {
                while (_channel.Reader.TryRead(out var item))
                {
                    batch.Add(item);
                    if (batch.Count >= _options.CompletionBatchSize)
                    {
                        break;
                    }
                }

                if (batch.Count >= _options.CompletionBatchSize)
                {
                    break;
                }

                var available = _channel.Reader.WaitToReadAsync().AsTask();
                if (await Task.WhenAny(available, deadline) != available)
                {
                    break;
                }
            }

            try
            {
                var responses = await _store.CompleteBatchAsync(
                    batch.Select(x => x.Request).ToArray(),
                    _options.RetryDelay,
                    CancellationToken.None);
                if (responses.Count != batch.Count)
                {
                    throw new InvalidOperationException(
                        $"Completion batch returned {responses.Count} results for {batch.Count} requests.");
                }

                for (var index = 0; index < batch.Count; index++)
                {
                    batch[index].Completion.TrySetResult(responses[index]);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "KubeJob completion batch failed");
                foreach (var item in batch)
                {
                    item.Completion.TrySetException(ex);
                }
            }
        }

        _logger?.LogWarning("KubeJob completion batcher loop exited; will restart on next enqueue");
    }

    private sealed class PendingCompletion
    {
        public PendingCompletion(CompleteAttemptRequest request)
        {
            Request = request;
        }

        public CompleteAttemptRequest Request { get; }

        public TaskCompletionSource<CompleteAttemptResponse> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
