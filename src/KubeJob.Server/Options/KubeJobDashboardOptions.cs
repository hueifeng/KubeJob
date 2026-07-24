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
    /// Optional ASP.NET Core authorization policy applied only to the dashboard
    /// controller. Authentication and the named policy are configured by the host.
    /// </summary>
    public string? AuthorizationPolicy { get; set; }

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

    internal string GetNormalizedRoutePrefix()
        => string.IsNullOrWhiteSpace(RoutePrefix)
            ? "kubejob"
            : RoutePrefix.Trim('/');

    internal string? GetNormalizedAuthorizationPolicy()
        => string.IsNullOrWhiteSpace(AuthorizationPolicy)
            ? null
            : AuthorizationPolicy.Trim();
}
