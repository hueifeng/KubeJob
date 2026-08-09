using FluentAssertions;
using KubeJob.Core.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class RetryPolicyTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    public void Fixed_strategy_always_returns_base_delay(int attemptCount)
    {
        var policy = new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        policy.ComputeDelay(attemptCount, new Random(1)).Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Exponential_strategy_grows_with_attempt_count()
    {
        var policy = new RetryPolicy(
            BackoffStrategy.Exponential,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(10),
            Multiplier: 2.0);

        policy.ComputeDelay(1).Should().Be(TimeSpan.FromSeconds(1));
        policy.ComputeDelay(2).Should().Be(TimeSpan.FromSeconds(2));
        policy.ComputeDelay(3).Should().Be(TimeSpan.FromSeconds(4));
        policy.ComputeDelay(4).Should().Be(TimeSpan.FromSeconds(8));
    }

    [Fact]
    public void Exponential_strategy_clamps_at_max_delay()
    {
        var policy = new RetryPolicy(
            BackoffStrategy.Exponential,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(5),
            Multiplier: 2.0);

        policy.ComputeDelay(10).Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Zero_jitter_ratio_is_deterministic()
    {
        var policy = new RetryPolicy(
            BackoffStrategy.Exponential,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(10),
            Multiplier: 2.0,
            JitterRatio: 0.0);

        var first = policy.ComputeDelay(3, new Random(1));
        var second = policy.ComputeDelay(3, new Random(2));

        first.Should().Be(second).And.Be(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public void Jitter_ratio_stays_within_bounds()
    {
        var policy = new RetryPolicy(
            BackoffStrategy.Fixed,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(10),
            JitterRatio: 0.5);
        var random = new Random(42);

        for (var i = 0; i < 100; i++)
        {
            var delay = policy.ComputeDelay(1, random);
            delay.TotalSeconds.Should().BeInRange(5, 15);
        }
    }

    [Theory]
    [MemberData(nameof(InvalidPolicies))]
    public void Validate_throws_on_invalid_configuration(RetryPolicy policy)
    {
        var act = policy.Validate;

        act.Should().Throw<InvalidOperationException>();
    }

    public static IEnumerable<object[]> InvalidPolicies()
    {
        yield return new object[]
        {
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(5))
        };
        yield return new object[]
        {
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5))
        };
        yield return new object[]
        {
            new RetryPolicy(BackoffStrategy.Exponential, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), Multiplier: 0.5)
        };
        yield return new object[]
        {
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), JitterRatio: -0.1)
        };
        yield return new object[]
        {
            new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), JitterRatio: 1.1)
        };
    }

    [Fact]
    public void Valid_configuration_does_not_throw()
    {
        var policy = new RetryPolicy(BackoffStrategy.Exponential, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5), Multiplier: 2.0, JitterRatio: 0.2);

        var act = policy.Validate;

        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 3, true)]
    [InlineData(2, 3, true)]
    [InlineData(3, 3, false)]
    [InlineData(4, 3, false)]
    public void CanRetry_uses_attempt_number_and_max_attempts(int attempt, int maxAttempts, bool expected)
    {
        var policy = new RetryPolicy(BackoffStrategy.Fixed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

        policy.CanRetry(attempt, maxAttempts).Should().Be(expected);
    }
}
