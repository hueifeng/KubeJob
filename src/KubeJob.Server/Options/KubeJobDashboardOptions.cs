namespace KubeJob.Server.Options;

/// <summary>
/// Configures the embedded KubeJob operations dashboard.
/// </summary>
public sealed class KubeJobDashboardOptions
{
    /// <summary>
    /// Route prefix used by all dashboard pages. The value is normalized without
    /// leading or trailing slashes.
    /// </summary>
    public string RoutePrefix { get; set; } = "kubejob";

    /// <summary>
    /// ASP.NET Core authorization policy applied only to the dashboard
    /// controller. When omitted, KubeJob applies the host's default policy.
    /// </summary>
    public string? AuthorizationPolicy { get; set; }

    /// <summary>
    /// Explicitly permits anonymous dashboard access. Use only for local
    /// development; dashboard pages can expose operational metadata.
    /// </summary>
    public bool AllowAnonymousAccess { get; set; }

    /// <summary>
    /// Controls whether serialized job payloads are rendered on Run detail pages.
    /// Payloads are hidden by default because they may contain secrets or personal data.
    /// </summary>
    public bool ShowPayloads { get; set; }

    /// <summary>
    /// Enables operational write actions such as canceling Runs and enabling or
    /// disabling Schedules. The dashboard is read-only by default.
    /// </summary>
    public bool AllowMutatingActions { get; set; }

    /// <summary>
    /// Maximum Worker Sessions returned by the simple operations view. Runs have
    /// dedicated pagination; Worker Sessions are deliberately bounded instead.
    /// </summary>
    public int MaximumWorkerSessions { get; set; } = 250;

    /// <summary>
    /// Maximum Schedules returned by the simple operations view.
    /// </summary>
    public int MaximumSchedules { get; set; } = 250;

    internal string GetNormalizedRoutePrefix()
        => string.IsNullOrWhiteSpace(RoutePrefix)
            ? "kubejob"
            : RoutePrefix.Trim('/');

    internal string? GetNormalizedAuthorizationPolicy()
        => string.IsNullOrWhiteSpace(AuthorizationPolicy)
            ? null
            : AuthorizationPolicy.Trim();

    internal int GetNormalizedMaximumWorkerSessions() =>
        Math.Clamp(MaximumWorkerSessions, 1, 1000);

    internal int GetNormalizedMaximumSchedules() =>
        Math.Clamp(MaximumSchedules, 1, 1000);
}
