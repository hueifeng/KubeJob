using KubeJob.Core.Runtime;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// Replays durable completion intents left behind after the control plane has
/// accepted a worker completion. Persisted intents own their Attempt even after
/// the original worker lease/session expires.
/// </summary>
public sealed class CompletionIntentDispatcherService : BackgroundService
{
    private readonly ICompletionIntentStore _intents;
    private readonly ICompletionIntentFinalizer _finalizer;
    private readonly JobRuntimeOptions _options;
    private readonly ILogger<CompletionIntentDispatcherService> _logger;

    public CompletionIntentDispatcherService(
        ICompletionIntentStore intents,
        IJobCompletionStore completions,
        IOptions<JobRuntimeOptions> options,
        ILogger<CompletionIntentDispatcherService> logger)
    {
        _intents = intents;
        _ = completions; // retained for constructor compatibility with existing composition/tests
        _finalizer = intents as ICompletionIntentFinalizer
            ?? throw new InvalidOperationException(
                $"{intents.GetType().Name} must implement {nameof(ICompletionIntentFinalizer)}.");
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _options.Validate();
        using var timer = new PeriodicTimer(_options.CompletionIntentPollInterval);
        try
        {
            // Recover immediately on startup instead of waiting one poll period.
            while (!stoppingToken.IsCancellationRequested)
            {
                await DispatchOnceAsync(stoppingToken);
                if (!await timer.WaitForNextTickAsync(stoppingToken))
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task DispatchOnceAsync(CancellationToken stoppingToken)
    {
        try
        {
            var pending = await _intents.GetPendingAsync(
                _options.CompletionIntentBatchSize,
                stoppingToken);
            foreach (var request in pending)
            {
                var response = await _finalizer.FinalizeAsync(
                    request,
                    _options.RetryPolicy,
                    stoppingToken);
                if (!response.Accepted)
                {
                    await _intents.RemoveAsync(request.AttemptId, stoppingToken);
                    _logger.LogWarning(
                        "Discarded stale or conflicting KubeJob completion intent for attempt {AttemptId}: {Reason}",
                        request.AttemptId,
                        response.RejectionReason);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KubeJob completion intent recovery iteration failed");
        }
    }
}
