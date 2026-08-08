using System.Collections.Concurrent;
using System.Threading.Channels;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// Publishes best-effort PostgresManaged wake hints after durable Run commit.
/// Signals are coalesced by logical Queue so a submission burst does not create
/// one broker notification per Run. Losing a signal is safe because workers
/// still poll PostgreSQL and Claim remains the only execution authority.
/// </summary>
public sealed class ManagedWorkAvailableDispatcher : BackgroundService
{
    private readonly IWorkAvailableNotifier _notifier;
    private readonly ILogger<ManagedWorkAvailableDispatcher> _logger;
    private readonly ConcurrentDictionary<string, WorkAvailableSignal> _pending =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _scheduled =
        new(StringComparer.Ordinal);
    private readonly Channel<string> _queues = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    public ManagedWorkAvailableDispatcher(
        IWorkAvailableNotifier notifier,
        ILogger<ManagedWorkAvailableDispatcher> logger)
    {
        _notifier = notifier;
        _logger = logger;
    }

    /// <summary>
    /// Enqueues one best-effort wake for an already-durable, currently due Run.
    /// Future-dated Runs keep the durable delayed wake path until that lower-
    /// frequency path is migrated separately.
    /// </summary>
    public void Signal(JobRunRecord run)
    {
        ArgumentNullException.ThrowIfNull(run);

        if (run.Phase != JobPhase.Pending || run.AvailableAt > DateTimeOffset.UtcNow)
        {
            return;
        }

        var signal = WorkAvailableSignal.ForRun(run);
        _pending[run.Queue] = signal;

        if (_scheduled.TryAdd(run.Queue, 0))
        {
            _queues.Writer.TryWrite(run.Queue);
        }
    }

    public void Signal(IEnumerable<JobRunRecord> runs)
    {
        ArgumentNullException.ThrowIfNull(runs);
        foreach (var run in runs)
        {
            Signal(run);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (await _queues.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (_queues.Reader.TryRead(out var queue))
                {
                    _scheduled.TryRemove(queue, out _);
                    if (!_pending.TryRemove(queue, out var signal))
                    {
                        continue;
                    }

                    try
                    {
                        await _notifier.PublishAsync(signal, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        // Wake publication is deliberately best effort. A failed
                        // signal must not become a second durable queue: normal
                        // PostgreSQL polling is the recovery path.
                        _logger.LogWarning(
                            exception,
                            "KubeJob dropped best-effort work-available wake for queue {Queue}; workers will fall back to PostgreSQL polling",
                            signal.Queue);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
