using System.Text.Json;
using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.Sample.Unified.Jobs;

public sealed record DashboardDemoPayload(
    string Scenario,
    int DelayMilliseconds = 250,
    int FailUntilAttempt = 0);

[KubeJob("sample.dashboard-demo")]
public sealed class DashboardDemoJob : IKubeJob<DashboardDemoPayload>
{
    private readonly ILogger<DashboardDemoJob> _logger;

    public DashboardDemoJob(ILogger<DashboardDemoJob> logger)
    {
        _logger = logger;
    }

    public async ValueTask ExecuteAsync(
        DashboardDemoPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        var scenario = payload.Scenario.Trim().ToLowerInvariant();
        var delay = TimeSpan.FromMilliseconds(Math.Clamp(payload.DelayMilliseconds, 0, 120_000));

        _logger.LogInformation(
            "Running dashboard demo scenario {Scenario} for run {RunId}, attempt {AttemptNumber}",
            scenario,
            context.RunId,
            context.AttemptNumber);

        switch (scenario)
        {
            case "success":
                await Task.Delay(delay, cancellationToken);
                return;

            case "retry-then-success":
                if (context.AttemptNumber <= Math.Max(0, payload.FailUntilAttempt))
                {
                    throw new InvalidOperationException(
                        $"Demo transient failure on attempt {context.AttemptNumber}.");
                }

                await Task.Delay(delay, cancellationToken);
                return;

            case "always-fail":
                throw new InvalidOperationException(
                    $"Demo retryable failure on attempt {context.AttemptNumber}.");

            case "permanent-failure":
                throw new JsonException(
                    "Demo payload validation failed permanently. This scenario intentionally exercises the permanent-failure path.");

            case "timeout":
            case "long-running":
                await Task.Delay(delay, cancellationToken);
                return;

            default:
                throw new JsonException($"Unknown dashboard demo scenario '{payload.Scenario}'.");
        }
    }
}
