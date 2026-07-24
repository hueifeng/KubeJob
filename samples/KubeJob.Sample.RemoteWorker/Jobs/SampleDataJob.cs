using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.Sample.RemoteWorker.Jobs;

public sealed record SampleDataPayload(string Message, int Steps = 5);

[KubeJob("sample.data")]
public sealed class SampleDataJob : IKubeJob<SampleDataPayload>
{
    private readonly ILogger<SampleDataJob> _logger;

    public SampleDataJob(ILogger<SampleDataJob> logger)
    {
        _logger = logger;
    }

    public async ValueTask ExecuteAsync(
        SampleDataPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        var steps = Math.Clamp(payload.Steps, 1, 100);
        _logger.LogInformation(
            "Executing {Message} as run {RunId}, attempt {AttemptNumber}, worker {WorkerId}",
            payload.Message,
            context.RunId,
            context.AttemptNumber,
            context.Worker.WorkerId);

        for (var step = 1; step <= steps; step++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _logger.LogInformation("Sample step {Step}/{Steps}", step, steps);
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
    }
}
