using KubeJob.Core.Runtime;
using System.Text.Json;

namespace KubeJob.Worker.Middleware;

/// <summary>
/// Translates known user-facing exceptions into explicit outcomes
/// so that workers can distinguish retryable failures from permanent
/// ones without leaking implementation details into business handlers.
///
/// <para>
/// By default the worker treats every unhandled exception as
/// <see cref="JobAttemptOutcome.RetryableFailure"/>. Register this
/// middleware with a user-provided mapping to promote specific
/// exception types to <see cref="JobAttemptOutcome.PermanentFailure"/>
/// (e.g. validation errors, duplicate-request errors, etc.).
/// </para>
/// </summary>
public sealed class ExceptionMappingMiddleware : Core.Execution.IJobExecutionMiddleware
{
    private readonly IReadOnlyDictionary<Type, ExceptionMapping> _mappings;

    /// <summary>
    /// Creates a middleware that transforms the outcome of matching exceptions.
    /// </summary>
    /// <param name="mappings">
    /// A dictionary keyed by exception type. When an exception of a key type
    /// (or any subtype) is caught, the corresponding <see cref="ExceptionMapping"/>
    /// describes how the worker should classify the failure.
    /// </param>
    public ExceptionMappingMiddleware(IReadOnlyDictionary<Type, ExceptionMapping> mappings)
    {
        _mappings = mappings ?? throw new ArgumentNullException(nameof(mappings));
    }

    public async Task InvokeAsync(
        Core.Execution.JobExecutionContext context,
        Core.Execution.JobExecutionDelegate next)
    {
        try
        {
            await next(context);
        }
        catch (JsonException)
        {
            // Payload deserialization failures are already handled by the
            // JsonException catch in WorkerRuntimeService. Let them propagate.
            throw;
        }
        catch (OperationCanceledException)
        {
            // Cancellation / timeout must always propagate.
            throw;
        }
        catch (Exception ex)
        {
            if (TryLookup(ex, out var mapping))
            {
                context.Outcome = mapping.Outcome;
                context.FailureCode = mapping.FailureCode ?? "mapped_exception";
                context.FailureMessage = mapping.Message ?? ex.Message;
                return; // pipeline completes without throwing → outcome-driven reporting
            }

            throw;
        }
    }

    private bool TryLookup(Exception ex, out ExceptionMapping mapping)
    {
        foreach (var (type, m) in _mappings)
        {
            if (type.IsInstanceOfType(ex))
            {
                mapping = m;
                return true;
            }
        }

        mapping = default!;
        return false;
    }
}

/// <summary>
/// Describes how a specific exception type should be classified by the
/// <see cref="ExceptionMappingMiddleware"/>.
/// </summary>
public sealed record ExceptionMapping
{
    /// <summary>
    /// The outcome to report when this exception is caught.
    /// </summary>
    public required JobAttemptOutcome Outcome { get; init; }

    /// <summary>
    /// Machine-readable failure code reported to the control plane.
    /// </summary>
    public string? FailureCode { get; init; }

    /// <summary>
    /// Human-readable failure message. When <c>null</c> the exception's
    /// <c>Message</c> is used.
    /// </summary>
    public string? Message { get; init; }
}
