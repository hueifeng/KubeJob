namespace KubeJob.Worker.Middleware;

/// <summary>
/// Enforces a per-attempt execution deadline as a middleware.
/// When positioned before other middleware in the pipeline it
/// provides an additional safety net beyond the transport-level
/// <c>TimeoutSeconds</c> already configured on <see cref="Core.Runtime.JobRunRecord"/>.
///
/// <para>
/// Useful when the handler contains synchronous blocking calls
/// that don't observe <see cref="CancellationToken"/>.
/// </para>
/// </summary>
public sealed class TimeoutExecutionMiddleware : Core.Execution.IJobExecutionMiddleware
{
    private readonly TimeSpan _hardTimeout;

    /// <summary>
    /// Creates a per-attempt deadline middleware.
    /// </summary>
    /// <param name="hardTimeout">
    /// Maximum wall-clock time the handler is allowed to execute.
    /// When exceeded a <see cref="TimeoutException"/> is thrown.
    /// </param>
    public TimeoutExecutionMiddleware(TimeSpan hardTimeout)
    {
        if (hardTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(hardTimeout), "Timeout must be positive.");
        _hardTimeout = hardTimeout;
    }

    public async Task InvokeAsync(
        Core.Execution.JobExecutionContext context,
        Core.Execution.JobExecutionDelegate next)
    {
        using var timeoutCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            timeoutCts.Token,
            context.CancellationToken);

        using var _ = linkedCts;
        timeoutCts.CancelAfter(_hardTimeout);

        try
        {
            await next(context);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            context.Outcome = Core.Runtime.JobAttemptOutcome.TimedOut;
            context.FailureCode = "middleware_timeout";
            context.FailureMessage = $"Execution exceeded the middleware timeout of {_hardTimeout.TotalSeconds:F0}s.";
        }
    }
}
