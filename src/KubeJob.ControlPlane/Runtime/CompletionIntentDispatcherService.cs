using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// Replays durable completion intents left behind when the control-plane
/// process stops after accepting a worker completion and before the state
/// transition commits. Normal traffic is still served by CompletionBatcher.
/// </summary>
public sealed class CompletionIntentDispatcherService : BackgroundService
{
    private readonly ICompletionIntentStore _intents;
    private readonly IJobCompletionStore _completions;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<CompletionIntentDispatcherService> _logger;

    public CompletionIntentDispatcherService(
        ICompletionIntentStore intents,
        IJobCompletionStore completions,
        IOptions<JobRuntimeOptions> options,
        ILogger<CompletionIntentDispatcherService> logger)
    {
        _intents = intents;
        _completions = completions;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();
        using var timer = new PeriodicTimer(_options.CompletionIntentPollInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var pending = await _intents.GetPendingAsync(
                        _options.CompletionIntentBatchSize,
                        stoppingToken);
                    foreach (var request in pending)
                    {
                        var response = await _completions.CompleteAsync(
                            request,
                            _options.RetryPolicy,
                            stoppingToken);
                        if (!response.Accepted)
                        {
                            await _intents.RemoveAsync(request.AttemptId, stoppingToken);
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KubeJob completion intent recovery iteration failed");
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
