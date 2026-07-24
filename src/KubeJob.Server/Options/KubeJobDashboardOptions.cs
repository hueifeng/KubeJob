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

    internal string GetNormalizedRoutePrefix()
        => string.IsNullOrWhiteSpace(RoutePrefix)
            ? "kubejob"
            : RoutePrefix.Trim('/');

    internal string? GetNormalizedAuthorizationPolicy()
        => string.IsNullOrWhiteSpace(AuthorizationPolicy)
            ? null
            : AuthorizationPolicy.Trim();
}
