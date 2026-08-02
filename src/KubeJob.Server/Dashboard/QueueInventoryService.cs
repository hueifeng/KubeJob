using KubeJob.Core.Runtime;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.Extensions.Options;

namespace KubeJob.Server.Dashboard;

/// <summary>
/// One logical queue as seen by operations: the resolved delivery target
/// (profile/lane/group/transport/ordering) and the physical queues it maps to
/// on the configured transport.
/// </summary>
public sealed record QueueInventoryEntry(
    string LogicalQueue,
    ExecutionDeliveryProfile Profile,
    string ExecutionLane,
    string ConsumerGroup,
    string? TransportId,
    ExecutionOrderingMode OrderingMode,
    int ActiveWorkerCount,
    IReadOnlyList<string> PhysicalQueueNames);

public sealed record DashboardQueuesViewModel(
    IReadOnlyList<QueueInventoryEntry> Entries,
    int ActiveWorkerCount,
    DateTimeOffset ObservedAt);

/// <summary>
/// Composes the queue catalog, worker session state, and the registered
/// execution transports into the operational queue inventory shown on the
/// Dashboard. Queues appear when they are explicitly configured in
/// <see cref="QueueDeliveryOptions"/> or registered by a worker session.
/// </summary>
public sealed class QueueInventoryService
{
    private readonly IOptions<QueueDeliveryOptions> _options;
    private readonly QueueCatalog _catalog;
    private readonly IExecutionTransportRegistry _transports;
    private readonly IJobRuntimeDashboardStore _dashboard;

    public QueueInventoryService(
        IOptions<QueueDeliveryOptions> options,
        QueueCatalog catalog,
        IExecutionTransportRegistry transports,
        IJobRuntimeDashboardStore dashboard)
    {
        _options = options;
        _catalog = catalog;
        _transports = transports;
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

        var options = _options.Value;
        var queues = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var key in options.Queues.Keys)
        {
            queues.Add(key);
        }

        foreach (var workerQueue in workerQueues)
        {
            queues.Add(workerQueue);
        }

        var entries = new List<QueueInventoryEntry>(queues.Count);
        foreach (var logicalQueue in queues)
        {
            var route = _catalog.Resolve(logicalQueue);
            var physicalQueues = ResolvePhysicalQueues(logicalQueue, route.Target);
            entries.Add(new QueueInventoryEntry(
                logicalQueue,
                route.Target.Profile,
                route.Target.ExecutionLane,
                route.Target.ConsumerGroup,
                route.Target.TransportId,
                route.Target.OrderingMode,
                activeWorkers.Count(worker => worker.Queues.Contains(logicalQueue, StringComparer.Ordinal)),
                physicalQueues));
        }

        return new DashboardQueuesViewModel(entries, activeWorkers.Length, DateTimeOffset.UtcNow);
    }

    private IReadOnlyList<string> ResolvePhysicalQueues(
        string logicalQueue,
        DeliveryTarget target)
    {
        if (string.IsNullOrWhiteSpace(target.TransportId))
        {
            return Array.Empty<string>();
        }

        try
        {
            return _transports.Resolve(target.TransportId).ResolvePhysicalQueueNames(logicalQueue);
        }
        catch (InvalidOperationException)
        {
            // The transport for this queue is not registered on this host
            // (e.g. a control-plane replica without the RabbitMQ extensions).
            return Array.Empty<string>();
        }
    }
}
