using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using Xunit;

namespace KubeJob.Tests.Runtime;

public sealed class ScheduleRetryJitterTests
{
    [Fact]
    public void Apply_jitter_stays_within_half_and_half_again_of_the_delay()
    {
        var delay = TimeSpan.FromSeconds(5);
        var lowerBound = TimeSpan.FromTicks(delay.Ticks / 2);
        var upperBound = TimeSpan.FromTicks((long)(delay.Ticks * 1.5));

        for (var i = 0; i < 1_000; i++)
        {
            var jittered = ScheduleReconcilerService.ApplyJitter(delay);
            jittered.Should().BeGreaterThanOrEqualTo(lowerBound);
            jittered.Should().BeLessThanOrEqualTo(upperBound);
        }
    }
}
