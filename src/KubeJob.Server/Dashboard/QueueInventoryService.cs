using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Dashboard;

public sealed record QueueInventoryEntry(
    string LogicalQueue,
    QueueRuntimeMode RuntimeMode,
    string? TransportId,
    string? ExecutionLane,
    string? ConsumerGroup,
    ExecutionOrderingMode? OrderingMode,
    int ActiveWorkerCount);

public sealed record DashboardQueuesViewModel(
    IReadOnlyList<QueueInventoryEntry> Entries,
    int ActiveWorkerCount,
    DateTimeOffset ObservedAt);

/// <summary>
/// Product-level queue inventory. Broker-specific exchange/queue/retry/DLQ
/// topology is deliberately hidden; operations see the logical Queue and its
/// single execution authority.
/// </summary>
public sealed class QueueInventoryService
{
    private readonly IOptions<QueueDeliveryOptions> _managedOptions;
    private readonly IOptions<QueueRuntimeOptions> _runtimeOptions;
    private readonly QueueCatalog _managedCatalog;
    private readonly IQueueRuntimeResolver _runtimeResolver;
    private readonly IJobRuntimeDashboardStore _dashboard;

    public QueueInventoryService(
        IOptions<QueueDeliveryOptions> managedOptions,
        IOptions<QueueRuntimeOptions> runtimeOptions,
        QueueCatalog managedCatalog,
        IQueueRuntimeResolver runtimeResolver,
        IJobRuntimeDashboardStore dashboard)
    {
        _managedOptions = managedOptions;
        _runtimeOptions = runtimeOptions;
        _managedCatalog = managedCatalog;
        _runtimeResolver = runtimeResolver;
        _dashboard = dashboard;
    }

    public async ValueTask<DashboardQueuesViewModel> ReadAsync(
        int maximumWorkerSessions,
        CancellationToken cancellationToken)
    {
        var sessions = await _dashboard.GetWorkerSessionsAsync(maximumWorkerSessions, cancellationToken);
        var activeWorkers = sessions
            .Where(session => session.State is WorkerSessionState.Ready or WorkerSessionState.Draining)
            .ToArray();
        var workerQueues = activeWorkers
            .SelectMany(session => session.Queues)
            .Distinct(StringComparer.Ordinal)
            .ToHashSet(StringComparer.Ordinal);

        var queues = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var key in _managedOptions.Value.Queues.Keys)
        {
            queues.Add(key.Trim());
        }

        foreach (var key in _runtimeOptions.Value.Queues.Keys)
        {
            queues.Add(key.Trim());
        }

        foreach (var workerQueue in workerQueues)
        {
            queues.Add(workerQueue);
        }

        var entries = new List<QueueInventoryEntry>(queues.Count);
        foreach (var logicalQueue in queues)
        {
            var runtime = _runtimeResolver.Resolve(logicalQueue);
            DeliveryTarget? managed = runtime.Mode == QueueRuntimeMode.PostgresManaged
                ? _managedCatalog.Resolve(logicalQueue).Target
                : null;

            entries.Add(new QueueInventoryEntry(
                logicalQueue,
                runtime.Mode,
                runtime.TransportId,
                managed?.ExecutionLane,
                managed?.ConsumerGroup,
                managed?.OrderingMode,
                activeWorkers.Count(worker => worker.Queues.Contains(logicalQueue, StringComparer.Ordinal))));
        }

        return new DashboardQueuesViewModel(entries, activeWorkers.Length, DateTimeOffset.UtcNow);
    }
}
