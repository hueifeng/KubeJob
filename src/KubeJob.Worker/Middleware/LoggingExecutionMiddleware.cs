using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KubeJob.Worker.Middleware;

/// <summary>
/// Logs the start, completion, and failure of every job attempt
/// using structured logging. Analogous to ASP.NET Core's
/// <c>HttpLoggingMiddleware</c>.
/// </summary>
public sealed class LoggingExecutionMiddleware : Core.Execution.IJobExecutionMiddleware
{
    public Task InvokeAsync(
        Core.Execution.JobExecutionContext context,
        Core.Execution.JobExecutionDelegate next)
    {
        var logger = context.ServiceProvider.GetRequiredService<ILogger<LoggingExecutionMiddleware>>();
        logger.LogDebug(
            "KubeJob attempt {AttemptId} ({JobKey}) starting",
            context.AttemptId,
            context.Items.TryGetValue("_JobKey", out var key) ? key : "?");

        return next(context);
    }
}
