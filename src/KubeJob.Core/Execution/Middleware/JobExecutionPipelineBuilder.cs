using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Core.Execution;

/// <summary>
/// Composes a chain of <see cref="IJobExecutionMiddleware"/> instances
/// ending with the handler invocation. The pipeline is built once per Consumer
/// and reused for every dispatched attempt.
///
/// <para>
/// Default (no middleware registered): zero-overhead — the delegate directly
/// calls the handler.
/// </para>
/// </summary>
public sealed class JobExecutionPipelineBuilder
{
    private readonly List<Func<JobExecutionDelegate, JobExecutionDelegate>> _components = [];

    /// <summary>
    /// Registers a middleware type that will be resolved from DI for every execution.
    /// </summary>
    public JobExecutionPipelineBuilder Use<TMiddleware>()
        where TMiddleware : IJobExecutionMiddleware
    {
        _components.Add(next => async context =>
        {
            var middleware = context.ServiceProvider.GetRequiredService<TMiddleware>();
            await middleware.InvokeAsync(context, next);
        });
        return this;
    }

    /// <summary>
    /// Registers a middleware by runtime type (non-generic variant).
    /// The type must implement <see cref="IJobExecutionMiddleware"/>.
    /// </summary>
    public JobExecutionPipelineBuilder Use(Type middlewareType)
    {
        if (!typeof(IJobExecutionMiddleware).IsAssignableFrom(middlewareType))
        {
            throw new ArgumentException(
                $"Type '{middlewareType.FullName}' must implement {typeof(IJobExecutionMiddleware).FullName}.",
                nameof(middlewareType));
        }

        _components.Add(next => async context =>
        {
            var middleware = (IJobExecutionMiddleware)context.ServiceProvider
                .GetRequiredService(middlewareType);
            await middleware.InvokeAsync(context, next);
        });
        return this;
    }

    /// <summary>
    /// Registers a filtering middleware that only executes when
    /// <paramref name="predicate"/> evaluates to <c>true</c>
    /// (short-circuits to <c>next</c> otherwise).
    /// </summary>
    public JobExecutionPipelineBuilder UseWhen<TMiddleware>(
        Func<JobExecutionContext, bool> predicate)
        where TMiddleware : IJobExecutionMiddleware
    {
        _components.Add(next => async context =>
        {
            if (predicate(context))
            {
                var middleware = context.ServiceProvider.GetRequiredService<TMiddleware>();
                await middleware.InvokeAsync(context, next);
            }
            else
            {
                await next(context);
            }
        });
        return this;
    }

    /// <summary>
    /// Builds the final <see cref="JobExecutionDelegate"/> that wraps the
    /// handler invocation with all registered middleware.
    /// </summary>
    /// <param name="handler">The terminal handler invocation delegate.</param>
    public JobExecutionDelegate Build(JobExecutionDelegate handler)
    {
        // No middleware registered: return handler directly (zero-overhead fast path).
        if (_components.Count == 0)
        {
            return handler;
        }

        var pipeline = handler;
        for (var i = _components.Count - 1; i >= 0; i--)
        {
            pipeline = _components[i](pipeline);
        }

        return pipeline;
    }
}
