using System.Text;
using KubeJob.Core.Runtime;

namespace KubeJob.Transport.RabbitMQ;

/// <summary>
/// Maps a run's <see cref="ExecutionEnvelope.PartitionKey"/> (the control-plane
/// ConcurrencyKey) onto a fixed-N physical execution lane via a two-layer
/// virtual-slot indirection: <c>PartitionKey → Slot (CRC16) → Lane (mapping table)</c>.
///
/// <para>
/// The two-layer design (<see cref="LaneAssignment"/>) isolates the key hashing
/// from physical lane assignment. When <see cref="RabbitMqExecutionOptions.ExecutionLaneCount"/>
/// changes, the <see cref="LaneMappingTable"/> is recomputed; only slots whose lane
/// assignment actually changed are affected. Most existing key→lane bindings survive.
/// </para>
///
/// <para>
/// The durable KeyOrdered claim gate is unchanged and remains the source of
/// correctness. Co-locating same-key runs on one lane queue only reduces wasted
/// broker Retry round-trips when the gate blocks a later same-key run.
/// </para>
/// </summary>
internal static class ExecutionLaneRouter
{
    private static volatile LaneMappingTable _currentMapping = LaneMappingTable.CreateUniform(1);

    /// <summary>
    /// Current lane mapping table. May be atomically swapped during
    /// rolling rebalancing.
    /// </summary>
    internal static LaneMappingTable CurrentMapping
    {
        get => _currentMapping;
        set => _currentMapping = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    /// Stable FNV-1a hash retained as the transport-neutral compatibility
    /// primitive for callers/tests that need a deterministic hash value. Lane
    /// assignment itself uses the virtual-slot mapping table below.
    /// </summary>
    public static uint StableHash(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;
        var hash = offsetBasis;
        foreach (var byteValue in Encoding.UTF8.GetBytes(value))
        {
            hash ^= byteValue;
            hash *= prime;
        }

        return hash;
    }

    /// <summary>
    /// Returns the physical lane index for <paramref name="partitionKey"/>.
    /// Lane 0 is always used when <c>laneCount</c> is 1.
    /// </summary>
    public static int GetLane(string? partitionKey, int laneCount)
    {
        if (laneCount <= 1)
        {
            return 0;
        }

        // Ensure the mapping table matches the requested lane count.
        // In production the mapping is set once at startup and swapped
        // during rebalancing via a background reconciler.
        var mapping = _currentMapping;
        if (mapping.LaneCount != laneCount)
        {
            var newMapping = LaneMappingTable.CreateUniform(
                laneCount, previous: mapping, version: mapping.Version + 1);
            // CAS: only swap if _currentMapping is still the same instance we read.
            // If another thread already swapped, we accept their version.
            Interlocked.CompareExchange(ref _currentMapping, newMapping, mapping);
            mapping = _currentMapping; // re-read to get the winner's version
        }

        return LaneAssignment.GetLaneFor(partitionKey, mapping);
    }
}