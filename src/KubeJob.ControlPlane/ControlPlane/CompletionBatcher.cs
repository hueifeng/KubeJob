using System.Threading.Channels;
using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Coalesces already-persisted completion intents into micro-batches.
/// Persistence is the durable ownership boundary; this batcher only performs
/// the final Run/Attempt state transition and may safely be lost on restart.
/// </summary>
public sealed class CompletionBatcher
{
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<CompletionBatcher>? _logger;
    private readonly CompletionShard[] _shards;

    public CompletionBatcher(
        ICompletionIntentStore intents,
        IOptions<JobRuntimeOptions> options,
        ILogger<CompletionBatcher>? logger = null)
    {
        if (intents is not ICompletionIntentFinalizer finalizer)
        {
            throw new InvalidOperationException(
                $"{intents.GetType().Name} must implement {nameof(ICompletionIntentFinalizer)}.");
        }

        _options = options.Value;
        _logger = logger;
        _options.Validate();

        var shardCount = Math.Max(1, Math.Min(_options.CompletionBatcherShardCount, 64));
        _shards = new CompletionShard[shardCount];
        for (var i = 0; i < shardCount; i++)
        {
            _shards[i] = new CompletionShard(finalizer, _options, logger, shardIndex: i);
        }
    }

    /// <summary>
    /// Enqueue a persisted completion intent. The worker blocks until its
    /// specific durable state transition finishes.
    /// </summary>
    public async ValueTask<CompleteAttemptResponse> EnqueueAsync(
        CompleteAttemptRequest request,
        CancellationToken cancellationToken)
    {
        var shardIndex = (request.RunId.GetHashCode() & 0x7FFFFFFF) % _shards.Length;
        return await _shards[shardIndex].EnqueueAsync(request, cancellationToken);
    }

    private sealed class CompletionShard
    {
        private readonly ICompletionIntentFinalizer _finalizer;
        private readonly JobRuntimeOptions _options;
        private readonly ILogger<CompletionBatcher>? _logger;
        private readonly int _shardIndex;
        private readonly object _startGate = new();
        private Task? _loop;
        private Channel<PendingCompletion> _channel;

        public CompletionShard(
            ICompletionIntentFinalizer finalizer,
            JobRuntimeOptions options,
            ILogger<CompletionBatcher>? logger,
            int shardIndex)
        {
            _finalizer = finalizer;
            _options = options;
            _logger = logger;
            _shardIndex = shardIndex;
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
                    _channel = CreateChannel();
                    _loop = Task.Run(ProcessLoopAsync);
                }
            }
        }

        private async Task ProcessLoopAsync()
        {
            while (await _channel.Reader.WaitToReadAsync())
            {
                var batch = new List<PendingCompletion>(_options.CompletionBatchSize)
                {
                    await _channel.Reader.ReadAsync()
                };
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
                    var responses = await _finalizer.FinalizeBatchAsync(
                        batch.Select(x => x.Request).ToArray(),
                        _options.RetryPolicy,
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
                    _logger?.LogError(ex, "KubeJob completion batch failed (shard={Shard})", _shardIndex);
                    foreach (var item in batch)
                    {
                        item.Completion.TrySetException(ex);
                    }
                }
            }

            _logger?.LogWarning(
                "KubeJob completion batcher loop exited (shard={Shard}); will restart on next enqueue",
                _shardIndex);
        }
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
