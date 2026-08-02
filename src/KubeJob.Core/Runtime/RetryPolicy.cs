namespace KubeJob.Core.Runtime;

public enum BackoffStrategy
{
    /// <summary>Same delay for every retry.</summary>
    Fixed,

    /// <summary>Delay = BaseDelay * Multiplier^(attempt-1)</summary>
    Exponential,

    /// <summary>Delay = BaseDelay * attempt</summary>
    Linear,

    /// <summary>
    /// Exponential with full jitter: delay = random(0, exponential_delay).
    /// Recommended by AWS Architecture Blog for reducing thundering-herd effects.
    /// </summary>
    ExponentialWithJitter
}

public sealed record RetryPolicy(
    BackoffStrategy Strategy,
    TimeSpan BaseDelay,
    TimeSpan MaxDelay,
    double Multiplier = 2.0,
    double JitterRatio = 0.0)
{
    public TimeSpan ComputeDelay(int attemptCount) => ComputeDelay(attemptCount, Random.Shared);

    public TimeSpan ComputeDelay(int attemptCount, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        var delay = Strategy switch
        {
            BackoffStrategy.Fixed => BaseDelay,
            BackoffStrategy.Exponential => TimeSpanFromSeconds(
                BaseDelay.TotalSeconds * Math.Pow(Multiplier, Math.Max(0, attemptCount - 1))),
            BackoffStrategy.Linear => TimeSpanFromSeconds(
                BaseDelay.TotalSeconds * attemptCount),
            BackoffStrategy.ExponentialWithJitter => AddFullJitter(
                TimeSpanFromSeconds(
                    BaseDelay.TotalSeconds * Math.Pow(Multiplier, Math.Max(0, attemptCount - 1))),
                random),
            _ => throw new ArgumentOutOfRangeException(nameof(Strategy), Strategy, null)
        };

        // Apply proportional jitter when configured (and not already using full jitter).
        if (Strategy != BackoffStrategy.ExponentialWithJitter && JitterRatio > 0)
        {
            var jitterFactor = 1 + JitterRatio * (random.NextDouble() * 2 - 1);
            delay = TimeSpanFromSeconds(delay.TotalSeconds * jitterFactor);
        }

        if (delay > MaxDelay)
        {
            delay = MaxDelay;
        }

        return delay;
    }

    /// <summary>
    /// Full jitter: random delay between 0 and <paramref name="baseDelay"/>.
    /// Reference: <see href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/"/>
    /// </summary>
    private static TimeSpan AddFullJitter(TimeSpan baseDelay, Random random)
    {
        var totalMs = (long)baseDelay.TotalMilliseconds;
        if (totalMs <= 0) return TimeSpan.Zero;
        return TimeSpan.FromMilliseconds(random.NextInt64(0, totalMs + 1));
    }

    public void Validate()
    {
        if (BaseDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("RetryPolicy.BaseDelay must be positive.");
        }

        if (MaxDelay < BaseDelay)
        {
            throw new InvalidOperationException("RetryPolicy.MaxDelay cannot be less than BaseDelay.");
        }

        if ((Strategy == BackoffStrategy.Exponential || Strategy == BackoffStrategy.ExponentialWithJitter)
            && Multiplier < 1)
        {
            throw new InvalidOperationException("RetryPolicy.Multiplier must be at least 1 for Exponential backoff.");
        }

        if (JitterRatio is < 0 or > 1)
        {
            throw new InvalidOperationException("RetryPolicy.JitterRatio must be between 0 and 1.");
        }
    }

    private static TimeSpan TimeSpanFromSeconds(double seconds) =>
        TimeSpan.FromSeconds(Math.Clamp(seconds, 0, TimeSpan.MaxValue.TotalSeconds));
}
