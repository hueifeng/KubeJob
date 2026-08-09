using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Server.Options;

/// <summary>
/// Configuration options for the KubeJob control plane.
/// </summary>
public sealed class KubeJobServerOptions
{
    /// <summary>
    /// ASP.NET Core authorization policy for job submission, status,
    /// cancellation, attempt history, and schedule-management endpoints.
    /// When omitted, KubeJob applies the host's default authorization policy.
    /// </summary>
    public string? ClientAuthorizationPolicy { get; set; }

    /// <summary>
    /// ASP.NET Core authorization policy for worker registration,
    /// heartbeat, claim, lease renewal, and completion endpoints. Distributed
    /// deployments should configure this policy.
    /// </summary>
    public string? WorkerAuthorizationPolicy { get; set; }

    /// <summary>
    /// Explicitly permits anonymous KubeJob HTTP endpoints. This is intended
    /// only for local development and tests; production endpoints require a
    /// configured named policy or the host's default authorization policy.
    /// </summary>
    public bool AllowAnonymousEndpoints { get; set; }

    /// <summary>
    /// Storage providers replace the reference in-memory state machine by
    /// registering the runtime store interfaces.
    /// </summary>
    public Action<IServiceCollection>? StorageConfigurator { get; set; }

    /// <summary>
    /// Uses the reference in-memory state machine. This is already the
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
