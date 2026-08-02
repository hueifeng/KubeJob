namespace KubeJob.Core.Runtime;

/// <summary>
/// Maps a <see cref="JobRunRecord.ConcurrencyKey"/> to a deterministic virtual
/// slot (0–16383), then resolves the slot to an execution lane via a pluggable
/// <see cref="LaneMappingTable"/>. The two-layer indirection enables incremental
/// lane rebalancing: when <c>laneCount</c> changes, only the mapping table is
/// updated and most existing key→lane assignments remain stable.
///
/// <para>
/// Design inspired by Redis Cluster hash slots and Kafka partition assignment.
/// </para>
/// </summary>
public static class LaneAssignment
{
    /// <summary>
    /// Number of virtual hash slots, matching Redis Cluster.
    /// </summary>
    public const int TotalSlots = 16384;

    /// <summary>
    /// Maps a partition key to a virtual slot (0–16383).
    /// An empty or null key always maps to slot 0 (legacy/un-keyed runs).
    /// </summary>
    public static int GetSlot(string? partitionKey)
    {
        if (string.IsNullOrEmpty(partitionKey))
        {
            return 0;
        }

        return Linq.Crc16.Compute(partitionKey) % TotalSlots;
    }

    /// <summary>
    /// Resolves a virtual slot to an execution lane index using the mapping
    /// table. The returned index is guaranteed to be in [0, laneCount).
    /// </summary>
    public static int GetLane(int slot, LaneMappingTable mapping)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        if (slot is < 0 or >= TotalSlots)
        {
            throw new ArgumentOutOfRangeException(nameof(slot), slot,
                $"Slot must be in [0, {TotalSlots}).");
        }

        return mapping.Map(slot);
    }

    /// <summary>
    /// Convenience shortcut: <c>GetLane(GetSlot(key), mapping)</c>.
    /// </summary>
    public static int GetLaneFor(string? partitionKey, LaneMappingTable mapping)
    {
        return GetLane(GetSlot(partitionKey), mapping);
    }
}
