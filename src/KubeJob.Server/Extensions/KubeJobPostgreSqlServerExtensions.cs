using KubeJob.Server.Options;
using KubeJob.Storage.PostgreSQL.Extensions;

namespace KubeJob.Server.Extensions;

public static class KubeJobPostgreSqlServerExtensions
{
    public static KubeJobServerOptions UsePostgreSql(
        this KubeJobServerOptions options,
        string connectionString)
        => options.UsePostgreSql(connectionString, configure: null);

    public static KubeJobServerOptions UsePostgreSql(
        this KubeJobServerOptions options,
        string connectionString,
        Action<PostgreSqlStorageOptions>? configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException(
                "A PostgreSQL connection string is required.",
                nameof(connectionString));
        }

        options.StorageConfigurator = services =>
            services.AddKubeJobPostgreSql(connectionString, configure);
        return options;
    }
}