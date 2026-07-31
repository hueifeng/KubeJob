namespace KubeJob.Core.Runtime;

public enum BackoffStrategy
{
    Fixed,
    Exponential
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
            _ => throw new ArgumentOutOfRangeException(nameof(Strategy), Strategy, null)
        };

        if (delay > MaxDelay)
        {
            delay = MaxDelay;
        }

        if (JitterRatio > 0)
        {
            var jitter = delay.TotalSeconds * JitterRatio * ((random.NextDouble() * 2) - 1);
            delay = TimeSpanFromSeconds(Math.Max(0, delay.TotalSeconds + jitter));
        }

        return delay;
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

        if (Strategy == BackoffStrategy.Exponential && Multiplier < 1)
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
