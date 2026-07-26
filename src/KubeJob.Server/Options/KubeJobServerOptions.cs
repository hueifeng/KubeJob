using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Server.Options;

/// <summary>
/// Configuration options for the KubeJob V2 control plane.
/// </summary>
public sealed class KubeJobServerOptions
{
    /// <summary>
    /// Optional ASP.NET Core authorization policy for job submission, status,
    /// cancellation, attempt history, and schedule-management endpoints.
    /// </summary>
    public string? ClientAuthorizationPolicy { get; set; }

    /// <summary>
    /// Optional ASP.NET Core authorization policy for worker registration,
    /// heartbeat, claim, lease renewal, and completion endpoints. Distributed
    /// deployments should configure this policy.
    /// </summary>
    public string? WorkerAuthorizationPolicy { get; set; }

    /// <summary>
    /// Storage providers replace the reference in-memory state machine by
    /// registering the V2 runtime store interfaces.
    /// </summary>
    public Action<IServiceCollection>? StorageConfigurator { get; set; }

    /// <summary>
    /// Uses the reference in-memory V2 state machine. This is already the
    /// default and is intended for tests, samples, and single-process hosts.
    /// </summary>
    public KubeJobServerOptions UseInMemory()
    {
        StorageConfigurator = null;
        return this;
    }

    internal string? GetNormalizedClientAuthorizationPolicy() =>
        NormalizePolicy(ClientAuthorizationPolicy);

    internal string? GetNormalizedWorkerAuthorizationPolicy() =>
        NormalizePolicy(WorkerAuthorizationPolicy);

    private static string? NormalizePolicy(string? policy) =>
        string.IsNullOrWhiteSpace(policy)
            ? null
            : policy.Trim();
}
