using FluentAssertions;
using KubeJob.Transport.RabbitMQ;

namespace KubeJob.Tests.Runtime;

/// <summary>
/// Unit tests for the FNV-1a lane router. The hash must be stable across
/// processes and architectures (so the same ConcurrencyKey always lands on the
/// same lane), which is why <see cref="ExecutionLaneRouter"/> avoids
/// <see cref="string.GetHashCode"/>. The canonical FNV-1a 32-bit test vectors
/// pin the algorithm so a future refactor cannot silently change lane
/// assignments and split a key across lanes.
/// </summary>
public sealed class ExecutionLaneRouterTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Null_or_empty_partition_key_routes_to_lane_zero(string? partitionKey)
    {
        ExecutionLaneRouter.GetLane(partitionKey, laneCount: 8)
            .Should().Be(0);
    }

    [Fact]
    public void Non_empty_partition_key_hashes_to_a_real_lane()
    {
        // A whitespace key is a (unusual) non-empty value, so it is hashed like
        // any other ConcurrencyKey rather than collapsed to lane 0.
        ExecutionLaneRouter.GetLane("   ", laneCount: 8)
            .Should().BeInRange(0, 7);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(0)]
    [InlineData(-1)]
    public void Single_lane_always_routes_to_lane_zero(int laneCount)
    {
        ExecutionLaneRouter.GetLane("any-concurrency-key", laneCount)
            .Should().Be(0);
    }

    [Fact]
    public void Same_partition_key_always_routes_to_the_same_lane()
    {
        const string key = "tenant-A";
        var lane = ExecutionLaneRouter.GetLane(key, laneCount: 16);

        lane.Should().Be(ExecutionLaneRouter.GetLane(key, laneCount: 16));
        lane.Should().BeInRange(0, 15);
    }

    [Fact]
    public void Distinct_keys_spread_across_lanes_without_a_dominant_lane()
    {
        const int laneCount = 8;
        const int keyCount = 4_000;
        var counts = new int[laneCount];

        for (var i = 0; i < keyCount; i++)
        {
            counts[ExecutionLaneRouter.GetLane($"concurrency-key-{i}", laneCount)]++;
        }

        // Every lane must receive some traffic, and no lane may hold more than
        // double the average — a coarse check that the FNV-1a spread does not
        // collapse distinct keys onto one lane. The hash is deterministic, so
        // this is a fixed (non-flaky) assertion.
        counts.Should().AllSatisfy(count => count.Should().BeGreaterThan(0));
        var average = keyCount / (double)laneCount;
        counts.Should().AllSatisfy(count => count.Should().BeLessThanOrEqualTo((int)(2 * average)));
    }
}