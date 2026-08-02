namespace KubeJob.Core.Execution;

/// <summary>
/// Delegate that represents the next middleware or handler in the execution pipeline.
/// </summary>
public delegate Task JobExecutionDelegate(JobExecutionContext context);

/// <summary>
/// Middleware that wraps handler execution with cross-cutting concerns
/// (logging, metrics, exception mapping, timeout enforcement, etc.).
///
/// Inspired by ASP.NET Core middleware and MassTransit filter pipeline patterns.
/// Middleware is invoked in registration order; the inner-most element is the
/// actual <c>IKubeJob&lt;TPayload&gt;.ExecuteAsync</c> call.
/// </summary>
public interface IJobExecutionMiddleware
{
    /// <summary>
    /// Invokes the middleware, optionally calling <paramref name="next"/> to
    /// continue the pipeline.
    /// </summary>
    /// <param name="context">The execution context shared across the entire pipeline.</param>
    /// <param name="next">The next middleware or handler in the chain.</param>
    Task InvokeAsync(JobExecutionContext context, JobExecutionDelegate next);
}
