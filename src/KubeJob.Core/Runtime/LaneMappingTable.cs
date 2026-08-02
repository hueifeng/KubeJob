using System.Collections.Immutable;

namespace KubeJob.Core.Runtime;

/// <summary>
/// Maps 16384 virtual hash slots (0–16383) to physical execution lanes.
/// The table is computed deterministically from <c>LaneCount</c> and is
/// shared across all nodes in the cluster. Default strategy is uniform
/// distribution (<c>slot % LaneCount</c>), preserving today's behaviour.
///
/// <para>
/// When <see cref="LaneCount"/> changes, the table is recomputed.
/// Only slots whose lane assignment actually changed are affected;
/// the majority of existing key→lane bindings remain stable.
/// </para>
/// </summary>
public sealed class LaneMappingTable
{
    private readonly ImmutableArray<byte> _slotToLane;
    private readonly uint _version;

    /// <summary>
    /// Number of physical execution lanes this table maps to.
    /// </summary>
    public int LaneCount { get; }

    /// <summary>
    /// Monotonic version used to detect mapping table changes during rolling
    /// deployments without coordination.
    /// </summary>
    public uint Version => _version;

    /// <summary>
    /// Percentage of slots that would change assignment if this table
    /// were replaced by a new one (0–100). Used to decide whether a migration
    /// window is needed.
    /// </summary>
    public double RemappingRatio { get; }

    private LaneMappingTable(ImmutableArray<byte> slotToLane, int laneCount, uint version, double remappingRatio)
    {
        _slotToLane = slotToLane;
        _version = version;
        LaneCount = laneCount;
        RemappingRatio = remappingRatio;
    }

    /// <summary>
    /// Creates a uniform-distribution mapping for <paramref name="laneCount"/>.
    /// Lane 0 is always used when laneCount is 1.
    /// </summary>
    public static LaneMappingTable CreateUniform(int laneCount, uint version = 0)
    {
        return CreateUniform(laneCount, null, version);
    }

    /// <summary>
    /// Creates a uniform-distribution mapping, computing the remapping ratio
    /// against the <paramref name="previous"/> table (if provided).
    /// </summary>
    public static LaneMappingTable CreateUniform(
        int laneCount,
        LaneMappingTable? previous,
        uint version)
    {
        if (laneCount is < 1 or > byte.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(laneCount), laneCount,
                "Lane count must be in [1, 255].");
        }

        var builder = ImmutableArray.CreateBuilder<byte>(LaneAssignment.TotalSlots);
        var changedSlots = 0;

        for (var slot = 0; slot < LaneAssignment.TotalSlots; slot++)
        {
            var lane = (byte)(slot % laneCount);
            builder.Add(lane);

            if (previous != null)
            {
                var previousLane = previous._slotToLane[slot];
                if (previousLane != lane && previousLane < previous.LaneCount)
                {
                    changedSlots++;
                }
            }
        }

        var remappingRatio = previous != null
            ? 100.0 * changedSlots / LaneAssignment.TotalSlots
            : 0.0;

        return new LaneMappingTable(builder.MoveToImmutable(), laneCount, version, remappingRatio);
    }

    /// <summary>
    /// Maps a virtual slot (0–16383) to a lane index [0, LaneCount).
    /// </summary>
    public int Map(int slot)
    {
        if (slot is < 0 or >= LaneAssignment.TotalSlots)
        {
            throw new ArgumentOutOfRangeException(nameof(slot), slot,
                $"Slot must be in [0, {LaneAssignment.TotalSlots}).");
        }

        return _slotToLane[slot];
    }

    /// <summary>
    /// All slot→lane mappings as a read-only span. Useful for bulk analysis.
    /// </summary>
    public ReadOnlySpan<byte> GetAllMappings() => _slotToLane.AsSpan();
}
